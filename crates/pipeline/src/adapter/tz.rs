//! jiff-backed [`TzResolver`] over the IANA database compiled into the binary.

use jiff::{
    SignedDuration,
    civil::{Date, Time},
    tz::{AmbiguousOffset, TimeZoneDatabase},
};

use super::session::TzResolver;

const NS_PER_SEC: i64 = 1_000_000_000;

/// [`TzResolver`] over the IANA database `jiff-tzdb` compiles into the binary.
///
/// Reads no host zoneinfo, so a replay anchors the same on every machine and
/// moves only when the locked `jiff-tzdb` version does. A local time inside a
/// transition takes the offset in effect before it, jiff's "compatible" rule
/// spelled out so an upstream default change cannot move archived anchors: a
/// time in a spring-forward gap lands after the gap, a time in a fall-back
/// fold on its earlier instant.
#[derive(Clone, Debug)]
pub struct BundledTzdb {
    db: TimeZoneDatabase,
}

impl BundledTzdb {
    /// Open the bundled database.
    pub fn new() -> Self {
        BundledTzdb {
            db: TimeZoneDatabase::bundled(),
        }
    }
}

impl Default for BundledTzdb {
    fn default() -> Self {
        Self::new()
    }
}

impl TzResolver for BundledTzdb {
    fn offset_ns(&self, tz: &str, yyyymmdd: u32, local_ns: i64) -> Option<i64> {
        let zone = self.db.get(tz).ok()?;
        let date = Date::new(
            i16::try_from(yyyymmdd / 10_000).ok()?,
            i8::try_from(yyyymmdd / 100 % 100).ok()?,
            i8::try_from(yyyymmdd % 100).ok()?,
        )
        .ok()?;
        let local = date
            .to_datetime(Time::midnight())
            .checked_add(SignedDuration::from_nanos(local_ns))
            .ok()?;
        let offset = match zone.to_ambiguous_timestamp(local).offset() {
            AmbiguousOffset::Unambiguous { offset } => offset,
            AmbiguousOffset::Gap { before, .. } | AmbiguousOffset::Fold { before, .. } => before,
        };
        Some(i64::from(offset.seconds()) * NS_PER_SEC)
    }
}
