//! Live adapter anchor roll. A live wire carries time since the venue's
//! midnight only, so the anchor the adapter adds it to must follow the trade
//! date. [`AnchorRoll`] places each live frame by its receive time and hands the
//! session runner the next anchor when a frame reaches the open session's end.
//! Zone lookups run once per trade date; per frame it is one `u64` compare.

use md_pipeline::BundledTzdb;

use crate::{session::SessionError, session_window::resolve_session, venue::VenueDescriptor};

/// End of the session whose anchor the live adapter holds. Starts at `0`, so
/// the first frame always installs one; a live reconnect that rebuilds the
/// adapter rebuilds the session state holding this too.
///
/// The roll follows receive time: a prior-date frame received past the end
/// carries the new anchor. NASDAQ New York midnight sits outside trading
/// hours.
pub(crate) struct AnchorRoll {
    venue: &'static VenueDescriptor,
    tzdb: BundledTzdb,
    end: u64,
}

impl AnchorRoll {
    pub(crate) fn new(venue: &'static VenueDescriptor) -> Self {
        Self {
            venue,
            tzdb: BundledTzdb::new(),
            end: 0,
        }
    }

    /// Whether a frame received at `ts_recv` needs a new anchor first.
    #[inline]
    pub(crate) fn due(&self, ts_recv: u64) -> bool {
        ts_recv >= self.end
    }

    /// Open the session holding `ts_recv` and return its anchor.
    ///
    /// # Errors
    ///
    /// [`SessionError::AnchorUnresolved`] when the venue's session model
    /// cannot place `ts_recv` (an unknown zone).
    pub(crate) fn roll(&mut self, ts_recv: u64) -> Result<u64, SessionError> {
        let open = resolve_session(&self.venue.session, &self.tzdb, ts_recv).ok_or(
            SessionError::AnchorUnresolved {
                venue: self.venue.name,
                ts_recv,
            },
        )?;
        self.end = open.end;
        Ok(open.start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::NASDAQ;

    const HOUR: u64 = 3_600_000_000_000;

    // NASDAQ springs forward on 2026-03-08, so that session spans 23h. The
    // tracker must take each end from the next date's anchor, not a fixed day.
    #[test]
    fn rolls_across_new_york_spring_forward() {
        let tz = BundledTzdb::new();
        let anchor = |date| NASDAQ.session.resolve_anchor(date, &tz).expect("anchor");
        let (a7, a8, a9) = (anchor(20260307), anchor(20260308), anchor(20260309));
        assert_eq!(a8 - a7, 24 * HOUR);
        assert_eq!(a9 - a8, 23 * HOUR, "spring forward shortens 2026-03-08");

        let mut roll = AnchorRoll::new(&NASDAQ);
        assert!(
            roll.due(a7 + 10 * HOUR),
            "a fresh tracker installs on the first frame"
        );
        assert_eq!(roll.roll(a7 + 10 * HOUR).expect("roll"), a7);
        assert!(!roll.due(a8 - 1));
        assert!(roll.due(a8));
        assert_eq!(roll.roll(a8).expect("roll"), a8);
        assert!(!roll.due(a9 - 1), "the 23h session has not ended");
        assert!(roll.due(a9));
        assert_eq!(roll.roll(a9 + HOUR).expect("roll"), a9);
    }
}
