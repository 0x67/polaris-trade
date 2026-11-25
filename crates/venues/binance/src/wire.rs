//! Serde shapes of the Binance Spot frames and the fixed-point decimal
//! parsing they share.
//!
//! Every numeric string is converted while serde walks the frame, so a value
//! outside its envelope fails the whole frame before the adapter changes any
//! state. The symbol borrows from the frame.

use md_events::{Price, SentinelScheme};
use serde::{Deserialize, Deserializer, de::Error};

/// Quantity scale exponent: sizes are carried as integers at `1e-8`.
pub(crate) const QTY_SCALE: i8 = -8;

const QTY_DECIMALS: u8 = QTY_SCALE.unsigned_abs();

/// First pass over a market stream frame: only the event type.
#[derive(Deserialize)]
pub(crate) struct EventKind<'a> {
    /// `depthUpdate`, `trade`, or another event; absent on a command reply.
    #[serde(default, rename = "e")]
    pub kind: Option<&'a str>,
}

/// One diff depth event.
#[derive(Deserialize)]
pub(crate) struct WsDepth<'a> {
    /// Event time as epoch nanoseconds.
    #[serde(rename = "E")]
    pub time: EpochNs,
    /// Symbol the event belongs to.
    #[serde(rename = "s")]
    pub symbol: &'a str,
    /// First update id in the event.
    #[serde(rename = "U")]
    pub first: u64,
    /// Last update id in the event.
    #[serde(rename = "u")]
    pub last: u64,
    /// Bid levels to set; a zero size removes the level.
    #[serde(rename = "b")]
    pub bids: Vec<WsLevel>,
    /// Ask levels to set; a zero size removes the level.
    #[serde(rename = "a")]
    pub asks: Vec<WsLevel>,
}

/// One trade event.
#[derive(Deserialize)]
pub(crate) struct WsTrade<'a> {
    /// Symbol the trade printed on.
    #[serde(rename = "s")]
    pub symbol: &'a str,
    /// Trade id.
    #[serde(rename = "t")]
    pub id: u64,
    /// Price at `1e-9`.
    #[serde(rename = "p")]
    pub price: Px,
    /// Size at `1e-8`.
    #[serde(rename = "q")]
    pub qty: Qty,
    /// Trade time as epoch nanoseconds.
    #[serde(rename = "T")]
    pub time: EpochNs,
    /// Whether the buyer was the resting side, so the seller took liquidity.
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

/// The REST depth snapshot. A reply without `lastUpdateId` is an error body.
#[derive(Deserialize)]
pub(crate) struct RestDepth {
    /// Update id the book state is current to.
    #[serde(default, rename = "lastUpdateId")]
    pub last_update_id: Option<u64>,
    /// Bid levels, best first.
    #[serde(default)]
    pub bids: Vec<WsLevel>,
    /// Ask levels, best first.
    #[serde(default)]
    pub asks: Vec<WsLevel>,
}

/// One `[price, size]` pair.
#[derive(Clone, Copy, Deserialize)]
pub(crate) struct WsLevel(pub Px, pub Qty);

/// A price converted to the `1e-9` envelope by [`Price::try_from_venue`].
#[derive(Clone, Copy)]
pub(crate) struct Px(pub i64);

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
pub(crate) struct Qty(pub u64);

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

/// A wire millisecond epoch time held as epoch nanoseconds.
#[derive(Clone, Copy)]
pub(crate) struct EpochNs(pub u64);

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
