//! Price convention: `i64` at a fixed `1e-9` scale, `i64::MAX` the only null
//! sentinel. Venue sentinel schemes and decimal scaling are translated here so
//! downstream code never branches on venue price conventions.

/// How a venue encodes "no price". Translated to [`Price::NULL`] at the adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SentinelScheme {
    /// Signed-max of an `i32` (`0x7FFF_FFFF`) means null.
    SignedMaxI32,
    /// Signed-min of an `i64` means null.
    SignedMinI64,
    /// An `i32` with its high bit (bit 31) set means null.
    HighBitI32,
    /// A specific per-field null value supplied by the adapter.
    Specific(i64),
    /// Every raw value is a real price; the venue has no null sentinel.
    NoSentinel,
}

impl SentinelScheme {
    /// True when `raw` is this scheme's null encoding.
    pub const fn is_null_raw(self, raw: i64) -> bool {
        match self {
            SentinelScheme::SignedMaxI32 => raw == i32::MAX as i64,
            SentinelScheme::SignedMinI64 => raw == i64::MIN,
            SentinelScheme::HighBitI32 => (raw as u32) & 0x8000_0000 != 0,
            SentinelScheme::Specific(v) => raw == v,
            SentinelScheme::NoSentinel => false,
        }
    }

    /// The raw value this scheme uses to encode null.
    pub const fn null_raw(self) -> i64 {
        match self {
            SentinelScheme::SignedMaxI32 => i32::MAX as i64,
            SentinelScheme::SignedMinI64 => i64::MIN,
            SentinelScheme::HighBitI32 => 0x8000_0000,
            SentinelScheme::Specific(v) => v,
            SentinelScheme::NoSentinel => Price::NULL.0,
        }
    }
}

/// A venue price that does not fit the [`Price`] envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriceError {
    /// The scaled value does not fit below [`Price::NULL`].
    Overflow {
        /// Venue value as received.
        raw: i64,
        /// Implied decimal places of `raw`.
        decimals: u8,
    },
    /// The venue value has a nonzero digit finer than the `1e-9` tick.
    SubTick {
        /// Venue value as received.
        raw: i64,
        /// Implied decimal places of `raw`.
        decimals: u8,
    },
}

impl std::fmt::Display for PriceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceError::Overflow { raw, decimals } => {
                write!(
                    f,
                    "price overflows the 1e-9 envelope: raw={raw} decimals={decimals}"
                )
            }
            PriceError::SubTick { raw, decimals } => {
                write!(
                    f,
                    "price finer than the 1e-9 tick: raw={raw} decimals={decimals}"
                )
            }
        }
    }
}

impl std::error::Error for PriceError {}

/// Normalized price: `i64` at `1e-9`, `i64::MAX` = null.
///
/// The envelope is the range of that scale: the smallest step is `1e-9`, the
/// largest real price is `i64::MAX - 1` ticks (about `9.22e9` units), and
/// `i64::MAX` itself is [`Price::NULL`]. [`Price::try_from_venue`] reports input
/// outside the envelope. [`Price::from_venue`] is for `decimals` up to 9 and
/// values the caller knows fit.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Price(pub i64);

impl Price {
    /// The only null sentinel.
    pub const NULL: Price = Price(i64::MAX);

    /// True when this is the null sentinel.
    pub const fn is_null(self) -> bool {
        self.0 == i64::MAX
    }

    /// Raw normalized value at `1e-9`.
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Convert a raw venue price (with `decimals` implied decimal places under
    /// the given sentinel scheme) to normalized `1e-9`. Null encodings map to
    /// [`Price::NULL`]. `decimals` must be at most 9 and the scaled value must
    /// fit the envelope; [`Price::try_from_venue`] handles both cases.
    pub fn from_venue(raw: i64, decimals: u8, scheme: SentinelScheme) -> Price {
        debug_assert!(decimals <= 9, "from_venue scales at most 9 decimals");
        if scheme.is_null_raw(raw) {
            return Price::NULL;
        }
        Price(raw * scale(decimals))
    }

    /// Like [`Price::from_venue`] for `decimals` up to 9, but reports input
    /// outside the envelope. `decimals` above 9 is accepted when the extra
    /// digits are all zero; `from_venue` and [`Price::to_venue`] do not scale
    /// that range.
    ///
    /// # Errors
    ///
    /// [`PriceError::Overflow`] when the scaled value does not fit below
    /// [`Price::NULL`], [`PriceError::SubTick`] when the value is finer than `1e-9`.
    pub fn try_from_venue(
        raw: i64,
        decimals: u8,
        scheme: SentinelScheme,
    ) -> Result<Price, PriceError> {
        if scheme.is_null_raw(raw) {
            return Ok(Price::NULL);
        }
        let scaled = match decimals.checked_sub(9) {
            None | Some(0) => raw.checked_mul(scale(decimals)),
            Some(extra) => match 10i64.checked_pow(u32::from(extra)) {
                Some(div) if raw % div == 0 => Some(raw / div),
                None if raw == 0 => Some(0),
                _ => return Err(PriceError::SubTick { raw, decimals }),
            },
        };
        match scaled {
            Some(v) if v != i64::MAX => Ok(Price(v)),
            _ => Err(PriceError::Overflow { raw, decimals }),
        }
    }

    /// Inverse of [`Price::from_venue`] at the same `decimals`/scheme. Null maps
    /// back to the scheme's null encoding. Lossless when the original venue
    /// value had no more than `decimals` places.
    pub fn to_venue(self, decimals: u8, scheme: SentinelScheme) -> i64 {
        if self.is_null() {
            return scheme.null_raw();
        }
        self.0 / scale(decimals)
    }
}

/// `10^(9 - decimals)`, saturating to 1 once decimals reaches 9.
fn scale(decimals: u8) -> i64 {
    10i64.pow(9u32.saturating_sub(decimals as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every venue's null encoding collapses to the single normalized sentinel,
    // and decimal scaling lands prices on the 1e-9 grid.
    #[test]
    fn venue_sentinel_table() {
        let schemes = [
            SentinelScheme::SignedMaxI32,
            SentinelScheme::SignedMinI64,
            SentinelScheme::HighBitI32,
            SentinelScheme::Specific(-1),
        ];
        for s in schemes {
            assert_eq!(Price::from_venue(s.null_raw(), 4, s), Price::NULL, "{s:?}");
        }
        // scale table: value 12345 at N decimals -> 12345 * 10^(9-N)
        assert_eq!(
            Price::from_venue(12345, 2, SentinelScheme::NoSentinel).raw(),
            123_450_000_000
        );
        assert_eq!(
            Price::from_venue(12345, 4, SentinelScheme::NoSentinel).raw(),
            1_234_500_000
        );
        assert_eq!(Price::from_venue(1, 9, SentinelScheme::NoSentinel).raw(), 1);
    }

    // Round trip venue -> normalized -> venue is lossless within the declared
    // decimals for real (non-null) prices across the scheme + decimal space.
    #[test]
    fn round_trip_property() {
        let raws = [0i64, 1, 7, 100, 999, 12_345, 250_000, 2_100_000_000];
        for decimals in 0u8..=9 {
            for &raw in &raws {
                let n = Price::from_venue(raw, decimals, SentinelScheme::NoSentinel);
                assert_eq!(
                    n.to_venue(decimals, SentinelScheme::NoSentinel),
                    raw,
                    "raw={raw} d={decimals}"
                );
            }
        }
        // null round-trips back to each scheme's own null encoding
        let s = SentinelScheme::SignedMinI64;
        assert_eq!(Price::NULL.to_venue(4, s), s.null_raw());
    }

    #[test]
    fn try_from_venue_matches_from_venue_inside_envelope() {
        for decimals in 0u8..=9 {
            for raw in [0i64, 1, 12_345, -250, 2_100_000_000] {
                let s = SentinelScheme::NoSentinel;
                assert_eq!(
                    Price::try_from_venue(raw, decimals, s),
                    Ok(Price::from_venue(raw, decimals, s)),
                    "raw={raw} d={decimals}"
                );
            }
        }
        let s = SentinelScheme::SignedMaxI32;
        assert_eq!(Price::try_from_venue(s.null_raw(), 4, s), Ok(Price::NULL));
    }

    #[test]
    fn try_from_venue_rejects_over_max() {
        let s = SentinelScheme::NoSentinel;
        let over = i64::MAX / 1_000 + 1;
        assert_eq!(
            Price::try_from_venue(over, 6, s),
            Err(PriceError::Overflow {
                raw: over,
                decimals: 6
            })
        );
        // The last real tick is accepted, the null encoding is not a price.
        assert_eq!(
            Price::try_from_venue(i64::MAX - 1, 9, s),
            Ok(Price(i64::MAX - 1))
        );
        assert!(matches!(
            Price::try_from_venue(i64::MAX, 9, s),
            Err(PriceError::Overflow { .. })
        ));
    }

    #[test]
    fn try_from_venue_rejects_sub_tick() {
        let s = SentinelScheme::NoSentinel;
        assert_eq!(
            Price::try_from_venue(12_345_678_915, 10, s),
            Err(PriceError::SubTick {
                raw: 12_345_678_915,
                decimals: 10
            })
        );
        assert_eq!(
            Price::try_from_venue(1, 255, s),
            Err(PriceError::SubTick {
                raw: 1,
                decimals: 255
            })
        );
        // Trailing zeros past the tick are exact.
        assert_eq!(
            Price::try_from_venue(1_234_567_890, 10, s),
            Ok(Price(123_456_789))
        );
        assert_eq!(Price::try_from_venue(0, 255, s), Ok(Price(0)));
    }
}
