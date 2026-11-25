//! Trading-day model: when a venue's session rolls and which sessions it skips.
//!
//! Resolution runs once per trade date, at session open and at every roll.
//! Per-record code keeps the resolved `u64` anchor and stays integer arithmetic
//! on UTC nanoseconds, so no zone lookup or enum match lands on the hot path.

use time::{Date, Month};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;
/// Julian day number of 1970-01-01.
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;

/// A venue's trading-time model: how its session rolls and which sessions it
/// does not trade.
///
/// Two axes because FX needs both at once: its value date rolls daily at 17:00
/// New York, and it closes over the weekend. One weekly variant would make the
/// trade date seven days long.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionModel {
    /// When one session ends and the next begins.
    pub roll: RollModel,
    /// Which sessions do not trade.
    pub closure: ClosureModel,
}

/// When a session rolls to the next trade date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollModel {
    /// Sessions roll on UTC midnight. Crypto.
    Continuous,
    /// Sessions roll at local midnight under a constant offset, local time being
    /// UTC plus `offset_ns`. A UTC+9 venue is `+9h`; no zone database is consulted.
    DailyFixed {
        /// Local minus UTC, in nanoseconds.
        offset_ns: i64,
    },
    /// Sessions roll `local_ns` after local midnight in an IANA zone whose
    /// offset is derived per date through a [`TzResolver`]. NASDAQ is
    /// `America/New_York` at `0`. A negative `local_ns` rolls the evening
    /// before: FX at `-7h` opens at 17:00 New York and labels that session
    /// with the next date, the Sunday-into-Monday fold.
    DailyZoned {
        /// IANA zone name.
        tz: &'static str,
        /// Roll time relative to local midnight of the labelled date, in
        /// nanoseconds; negative rolls the evening before.
        local_ns: i64,
    },
}

/// Which sessions a venue does not trade.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosureModel {
    /// Every session trades. Holiday calendars are not modeled.
    None,
    /// Closed from the anchor of the `close_dow` session until the anchor of
    /// the `open_dow` session. Days count from Sunday (`0`) to Saturday (`6`):
    /// FX closes Friday (`5`) and reopens Sunday (`0`).
    Weekend {
        /// Weekday of the first closed session.
        close_dow: u8,
        /// Weekday of the first session after the closure.
        open_dow: u8,
    },
}

/// Supplies a time zone's UTC offset at one local date and time.
///
/// A seam so `md-pipeline` carries no zone database itself: continuous and
/// fixed-offset rolls never call it, and [`NoTzdb`] serves an embedder that
/// links none. An implementation must pin how it resolves a local time inside
/// a transition gap or fold; a library default that moves would silently move
/// archived anchors.
pub trait TzResolver {
    /// Offset in nanoseconds (local minus UTC) in effect `local_ns` after local
    /// midnight of `yyyymmdd` in zone `tz`, or `None` when the zone is unknown,
    /// `yyyymmdd` is not a valid date, or the local time is out of range.
    fn offset_ns(&self, tz: &str, yyyymmdd: u32, local_ns: i64) -> Option<i64>;
}

/// Resolver without a zone database. Every lookup is `None`, so a
/// [`RollModel::DailyZoned`] session fails at open instead of anchoring on UTC.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoTzdb;

impl TzResolver for NoTzdb {
    fn offset_ns(&self, _tz: &str, _yyyymmdd: u32, _local_ns: i64) -> Option<i64> {
        None
    }
}

impl SessionModel {
    /// Epoch-ns UTC anchor of the `session_date` (`yyyymmdd`) session, the
    /// `session_midnight_ns` that windowing floors from.
    ///
    /// Call at session open and again at every trade-date roll, never per
    /// record, and never reuse one date's anchor for another: a zoned offset
    /// changes at a DST transition. `None` for a malformed date, an anchor
    /// before the epoch, or a zone `tz` cannot resolve.
    pub fn resolve_anchor(&self, session_date: u32, tz: &dyn TzResolver) -> Option<u64> {
        u64::try_from(self.roll.anchor_ns(date_of(session_date)?, tz)?).ok()
    }

    /// `yyyymmdd` label of the session holding `ts` (epoch ns UTC): the date
    /// whose anchor is at or before `ts` while the next date's is after it.
    /// A continuous roll labels by UTC date. Called at roll, not per record.
    pub fn session_label(&self, ts: u64, tz: &dyn TzResolver) -> Option<u32> {
        let ts = i64::try_from(ts).ok()?;
        let local_day = ts.checked_sub(self.roll.local_ns())?.div_euclid(NS_PER_DAY);
        let guess = date_from_epoch_day(local_day)?;
        // anchors rise with the date and an offset under 24h keeps the session
        // date within one day of the local-midnight guess
        for date in [guess.next_day()?, guess, guess.previous_day()?] {
            if self.roll.anchor_ns(date, tz)? <= ts {
                return yyyymmdd(date);
            }
        }
        None
    }

    /// Length of the `session_date` session: the next date's anchor minus this
    /// one's, so a day partition ends where the next session starts instead of
    /// a fixed 24h on.
    ///
    /// `None` when either anchor does not resolve or the span is not 23h, 24h,
    /// or 25h: a transition of any other size (a half-hour DST rule, a skipped
    /// calendar date) would not tile into day windows.
    pub fn session_span_ns(&self, session_date: u32, tz: &dyn TzResolver) -> Option<u64> {
        let date = date_of(session_date)?;
        let start = self.roll.anchor_ns(date, tz)?;
        let span = self
            .roll
            .anchor_ns(date.next_day()?, tz)?
            .checked_sub(start)?;
        if [23, 24, 25].map(|h| h * NS_PER_HOUR).contains(&span) {
            u64::try_from(span).ok()
        } else {
            None
        }
    }

    /// Whether the `session_date` session trades under the closure model. A
    /// malformed date names no session and is `false`.
    pub fn is_trading_date(&self, session_date: u32) -> bool {
        let Some(date) = date_of(session_date) else {
            return false;
        };
        match self.closure {
            ClosureModel::None => true,
            ClosureModel::Weekend {
                close_dow,
                open_dow,
            } => {
                let dow = i32::from(date.weekday().number_days_from_sunday());
                let since_close = (dow - i32::from(close_dow)).rem_euclid(7);
                let closed_days = (i32::from(open_dow) - i32::from(close_dow)).rem_euclid(7);
                since_close >= closed_days
            }
        }
    }
}

impl RollModel {
    /// Roll time after local midnight; only a zoned roll has one.
    fn local_ns(self) -> i64 {
        match self {
            RollModel::DailyZoned { local_ns, .. } => local_ns,
            RollModel::Continuous | RollModel::DailyFixed { .. } => 0,
        }
    }

    /// Signed epoch-ns UTC anchor of `date`, `None` on overflow or an
    /// unresolved zone.
    fn anchor_ns(self, date: Date, tz: &dyn TzResolver) -> Option<i64> {
        let epoch_day = i64::from(date.to_julian_day() - UNIX_EPOCH_JULIAN_DAY);
        let midnight = epoch_day.checked_mul(NS_PER_DAY)?;
        match self {
            RollModel::Continuous => Some(midnight),
            RollModel::DailyFixed { offset_ns } => midnight.checked_sub(offset_ns),
            RollModel::DailyZoned { tz: zone, local_ns } => {
                let offset = tz.offset_ns(zone, yyyymmdd(date)?, local_ns)?;
                midnight.checked_add(local_ns)?.checked_sub(offset)
            }
        }
    }
}

/// Calendar date of a `yyyymmdd` value, `None` when it names no real day.
fn date_of(yyyymmdd: u32) -> Option<Date> {
    let year = i32::try_from(yyyymmdd / 10_000).ok()?;
    let month = Month::try_from(u8::try_from(yyyymmdd / 100 % 100).ok()?).ok()?;
    let day = u8::try_from(yyyymmdd % 100).ok()?;
    Date::from_calendar_date(year, month, day).ok()
}

/// `yyyymmdd` value of a date, `None` before year 0.
fn yyyymmdd(date: Date) -> Option<u32> {
    let year = u32::try_from(date.year()).ok()?;
    Some(year * 10_000 + u32::from(u8::from(date.month())) * 100 + u32::from(date.day()))
}

/// Date `day` days after 1970-01-01.
fn date_from_epoch_day(day: i64) -> Option<Date> {
    let julian = i32::try_from(day)
        .ok()?
        .checked_add(UNIX_EPOCH_JULIAN_DAY)?;
    Date::from_julian_day(julian).ok()
}
