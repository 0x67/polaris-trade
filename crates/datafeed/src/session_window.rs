//! Trade-date session windows under a venue's [`SessionModel`]:
//! [`resolve_session`] places a receive instant in its session and returns the
//! `[start, end)` anchors, and the civil-date helpers step a `YYYYMMDD` label
//! by whole days.

use md_pipeline::{BundledTzdb, SessionModel};

/// Days since the epoch for a `YYYYMMDD` civil date (Hinnant days-from-civil).
pub(crate) fn days_from_civil(yyyymmdd: u32) -> i64 {
    let y = (yyyymmdd / 10_000) as i64;
    let m = ((yyyymmdd / 100) % 100) as i64;
    let d = (yyyymmdd % 100) as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYYMMDD` civil date for days since the epoch (Hinnant civil-from-days).
fn civil_from_days(z: i64) -> u32 {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u32) * 10_000 + (m as u32) * 100 + (d as u32)
}

/// `YYYYMMDD` trade date `delta` days after `yyyymmdd`. Advances the date-dir
/// name across a session roll by the observed session-label advance.
pub(crate) fn add_days(yyyymmdd: u32, delta: i64) -> u32 {
    civil_from_days(days_from_civil(yyyymmdd) + delta)
}

/// One trade-date session: its `yyyymmdd` label and `[start, end)`
/// anchors, epoch ns. `end` is the next date's anchor, so a DST session spans
/// 23h or 25h.
pub(crate) struct OpenSession {
    pub(crate) label: u32,
    pub(crate) start: u64,
    pub(crate) end: u64,
}

/// Label and `[start, end)` anchors of the session holding `ts_recv`, or
/// `None` when the venue's session model cannot place it (an unknown
/// zone). A session that opens before the epoch starts at `0`, so a
/// synthetic replay stamped from `0` still lands in its first session.
pub(crate) fn resolve_session(
    model: &SessionModel,
    tzdb: &BundledTzdb,
    ts_recv: u64,
) -> Option<OpenSession> {
    let label = model.session_label(ts_recv, tzdb)?;
    let start = match model.resolve_anchor(label, tzdb) {
        Some(start) => start,
        // Only the session holding the epoch can open before it. Any other
        // unresolved anchor is a zone failure and must not clamp.
        None if model.session_label(0, tzdb) == Some(label) => 0,
        None => return None,
    };
    Some(OpenSession {
        label,
        start,
        end: model.resolve_anchor(add_days(label, 1), tzdb)?,
    })
}
