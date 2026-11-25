//! Serde shapes of the three public subscriptions and the fixed-point
//! decimal parsing they share.
//!
//! Every numeric string is converted while serde walks the frame, so a value
//! outside its envelope fails the whole frame before the adapter emits
//! anything. Strings borrow from the frame; none is copied.

use md_events::{Price, SentinelScheme};
use serde::{Deserialize, Deserializer, de::Error};
use serde_json::value::RawValue;

/// Quantity scale exponent: sizes are carried as integers at `1e-8`.
pub const QTY_SCALE: i8 = -8;

const QTY_DECIMALS: u8 = QTY_SCALE.unsigned_abs();

/// Decimal places of a funding rate in `Stats::aux`.
const RATE_DECIMALS: u8 = 12;

/// Longest coin name that fits `Definition::raw_symbol`.
const SYMBOL_LEN: usize = 20;

/// The outer shape of every frame. The payload stays raw until the channel
/// says which type to read it as.
#[derive(Deserialize)]
pub struct Envelope<'a> {
    /// Channel name: `trades`, `l2Book`, `activeAssetCtx`, or another.
    pub channel: &'a str,
    /// Payload; absent on `pong`.
    #[serde(default, borrow)]
    pub data: Option<&'a RawValue>,
}

/// One trade print.
#[derive(Deserialize)]
pub struct WsTrade<'a> {
    /// Coin the trade printed on.
    #[serde(borrow)]
    pub coin: Coin<'a>,
    /// Aggressor side.
    pub side: WireSide,
    /// Price at `1e-9`.
    pub px: Px,
    /// Size at `1e-8`.
    pub sz: Qty,
    /// Block time as epoch nanoseconds.
    pub time: EpochNs,
    /// Trade id.
    pub tid: u64,
}

/// A whole book push: bids first, then asks, best level first.
#[derive(Deserialize)]
pub struct WsBook<'a> {
    /// Coin the book belongs to.
    #[serde(borrow)]
    pub coin: Coin<'a>,
    /// Block time as epoch nanoseconds.
    pub time: EpochNs,
    /// `[bids, asks]`.
    pub levels: (Vec<WsLevel>, Vec<WsLevel>),
}

/// One price level. The order count `n` is not carried into `Level`.
#[derive(Deserialize)]
pub struct WsLevel {
    /// Price at `1e-9`.
    pub px: Px,
    /// Size at `1e-8`.
    pub sz: Qty,
}

/// Statistics push for one perpetual.
#[derive(Deserialize)]
pub struct WsActiveAssetCtx<'a> {
    /// Coin the statistics belong to.
    #[serde(borrow)]
    pub coin: Coin<'a>,
    /// The statistics.
    pub ctx: PerpsAssetCtx,
}

/// The perpetual fields the adapter maps.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpsAssetCtx {
    /// Funding rate at `1e-12`.
    pub funding: Rate,
    /// Open interest in coin units at `1e-8`.
    pub open_interest: Qty,
    /// Oracle price at `1e-9`.
    pub oracle_px: Px,
    /// Mark price at `1e-9`.
    pub mark_px: Px,
}

/// A perpetual coin name, borrowed from the frame and at most 20 bytes. Spot
/// names (`@<index>` or `BASE/QUOTE`) are rejected.
#[derive(Clone, Copy)]
pub struct Coin<'a>(&'a str);

impl Coin<'_> {
    /// The name zero-padded to `Definition::raw_symbol`.
    pub fn padded(self) -> [u8; SYMBOL_LEN] {
        let mut out = [0u8; SYMBOL_LEN];
        out[..self.0.len()].copy_from_slice(self.0.as_bytes());
        out
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for Coin<'a> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let name = <&str>::deserialize(d)?;
        if name.is_empty() || name.len() > SYMBOL_LEN {
            return Err(D::Error::custom(
                "coin name is empty or longer than 20 bytes",
            ));
        }
        if name.starts_with('@') || name.contains('/') {
            return Err(D::Error::custom("coin name is a spot market"));
        }
        Ok(Coin(name))
    }
}

/// Aggressor side of a trade: `B` buyer, `A` seller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireSide {
    /// The buyer took liquidity.
    Buy,
    /// The seller took liquidity.
    Sell,
}

impl<'de> Deserialize<'de> for WireSide {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match <&str>::deserialize(d)? {
            "B" => Ok(WireSide::Buy),
            "A" => Ok(WireSide::Sell),
            _ => Err(D::Error::custom("trade side is not B or A")),
        }
    }
}

/// A price converted to the `1e-9` envelope by [`Price::try_from_venue`].
#[derive(Clone, Copy)]
pub struct Px(pub i64);

impl<'de> Deserialize<'de> for Px {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (raw, decimals) = split_decimal(<&str>::deserialize(d)?)
            .filter(|&(raw, _)| raw >= 0)
            .ok_or_else(|| D::Error::custom("price is not a non-negative decimal"))?;
        Price::try_from_venue(raw, decimals, SentinelScheme::NoSentinel)
            .map(|p| Px(p.raw()))
            .map_err(D::Error::custom)
    }
}

/// A non-negative size at `1e-8`.
#[derive(Clone, Copy)]
pub struct Qty(pub u64);

impl<'de> Deserialize<'de> for Qty {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (raw, decimals) = split_decimal(<&str>::deserialize(d)?)
            .ok_or_else(|| D::Error::custom("size is not a decimal number"))?;
        rescale(raw, decimals, QTY_DECIMALS)
            .and_then(|v| u64::try_from(v).ok())
            .map(Qty)
            .ok_or_else(|| D::Error::custom("size is negative or outside the 1e-8 range"))
    }
}

/// A signed funding rate at `1e-12`.
#[derive(Clone, Copy)]
pub struct Rate(pub i64);

impl<'de> Deserialize<'de> for Rate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (raw, decimals) = split_decimal(<&str>::deserialize(d)?)
            .ok_or_else(|| D::Error::custom("rate is not a decimal number"))?;
        rescale(raw, decimals, RATE_DECIMALS)
            .map(Rate)
            .ok_or_else(|| D::Error::custom("rate is outside the 1e-12 range"))
    }
}

/// A wire millisecond epoch time held as epoch nanoseconds.
#[derive(Clone, Copy)]
pub struct EpochNs(pub u64);

impl<'de> Deserialize<'de> for EpochNs {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        u64::deserialize(d)?
            .checked_mul(1_000_000)
            .map(EpochNs)
            .ok_or_else(|| D::Error::custom("time overflows epoch nanoseconds"))
    }
}

/// Split `[-]digits[.digits]` into the digits as an integer and the count of
/// digits after the point. Exponents, signs other than a leading `-`, and
/// values past `i64` are rejected.
fn split_decimal(text: &str) -> Option<(i64, u8)> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, frac) = body.split_once('.').unwrap_or((body, ""));
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    let mut raw: i64 = 0;
    for digit in whole.bytes().chain(frac.bytes()) {
        if !digit.is_ascii_digit() {
            return None;
        }
        raw = raw.checked_mul(10)?.checked_add(i64::from(digit - b'0'))?;
    }
    let decimals = u8::try_from(frac.len()).ok()?;
    Some((if negative { -raw } else { raw }, decimals))
}

/// Move `raw` from `decimals` places to `target` places. Finer digits are
/// accepted only when they are zero.
fn rescale(raw: i64, decimals: u8, target: u8) -> Option<i64> {
    if decimals <= target {
        raw.checked_mul(10i64.checked_pow(u32::from(target - decimals))?)
    } else {
        let divisor = 10i64.checked_pow(u32::from(decimals - target)).unwrap_or(0);
        match divisor {
            0 => (raw == 0).then_some(0),
            d => (raw % d == 0).then_some(raw / d),
        }
    }
}

#[cfg(test)]
mod tests;
