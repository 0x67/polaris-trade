//! Superset trading-state transition machine over `md_events::TradingState`.
//! Tracks each instrument's current state and classifies every applied
//! transition (halt entry/exit, session open/close). Venue reason text stays
//! on the wire `Status` record's `venue_reason`, never duplicated here; the
//! superset enum is deliberately venue-vocabulary-agnostic so it covers
//! single-code, multi-state-name, and state-plus-reason venue encodings
//! (market-wide and single-stock circuit breakers alike) without a venue
//! conditional.

use std::collections::HashMap;

use md_events::TradingState;

/// One instrument's `from -> to` state change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transition {
    pub instrument: u32,
    pub from: TradingState,
    pub to: TradingState,
}

impl Transition {
    /// Entering halt from any non-halt state.
    pub const fn is_halt_entry(&self) -> bool {
        !matches!(self.from, TradingState::Halt) && matches!(self.to, TradingState::Halt)
    }

    /// Leaving halt back to any non-halt state.
    pub const fn is_halt_exit(&self) -> bool {
        matches!(self.from, TradingState::Halt) && !matches!(self.to, TradingState::Halt)
    }

    /// Session moves into continuous trading from a pre-trade phase.
    pub const fn is_session_open(&self) -> bool {
        matches!(
            self.from,
            TradingState::Unknown | TradingState::PreOpen | TradingState::OpeningAuction
        ) && matches!(self.to, TradingState::Continuous)
    }

    /// Session moves out of continuous/closing trading into a post-trade phase.
    pub const fn is_session_close(&self) -> bool {
        matches!(
            self.from,
            TradingState::Continuous | TradingState::ClosingAuction
        ) && matches!(self.to, TradingState::PostClose | TradingState::Closed)
    }
}

/// Per-instrument current trading state. Instruments never seen default to
/// `Unknown` until their first status arrives.
#[derive(Clone, Debug, Default)]
pub struct StatusMachine {
    current: HashMap<u32, TradingState>,
}

impl StatusMachine {
    /// New machine, every instrument starts `Unknown`.
    pub fn new() -> Self {
        StatusMachine::default()
    }

    /// Current state for an instrument, `Unknown` if never set.
    pub fn state(&self, instrument: u32) -> TradingState {
        self.current
            .get(&instrument)
            .copied()
            .unwrap_or(TradingState::Unknown)
    }

    /// Apply a new state for an instrument, returning the classified
    /// transition. Applying the same state twice is a no-op transition (both
    /// `from` and `to` equal); callers checking `is_halt_entry`/`is_halt_exit`
    /// get `false` for both, matching a duplicate/redundant status event.
    pub fn apply(&mut self, instrument: u32, to: TradingState) -> Transition {
        let from = self.state(instrument);
        self.current.insert(instrument, to);
        Transition {
            instrument,
            from,
            to,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One fixture per venue encoding family (single code, multi-state name,
    // state-plus-reason w/ market-wide or single-stock breaker), same enum.
    #[test]
    fn transition_table() {
        struct Case {
            name: &'static str,
            from: TradingState,
            to: TradingState,
            halt_entry: bool,
            halt_exit: bool,
            session_open: bool,
            session_close: bool,
        }
        let cases = [
            Case {
                name: "single-code venue: pre-open to opening auction",
                from: TradingState::PreOpen,
                to: TradingState::OpeningAuction,
                halt_entry: false,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "single-code venue: opening auction to continuous",
                from: TradingState::OpeningAuction,
                to: TradingState::Continuous,
                halt_entry: false,
                halt_exit: false,
                session_open: true,
                session_close: false,
            },
            Case {
                name: "single-code venue: continuous to closed",
                from: TradingState::Continuous,
                to: TradingState::Closed,
                halt_entry: false,
                halt_exit: false,
                session_open: false,
                session_close: true,
            },
            Case {
                name: "multi-state-name venue: continuous to halt",
                from: TradingState::Continuous,
                to: TradingState::Halt,
                halt_entry: true,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "multi-state-name venue: halt to intraday auction resume",
                from: TradingState::Halt,
                to: TradingState::IntradayAuction,
                halt_entry: false,
                halt_exit: true,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "multi-state-name venue: intraday auction back to continuous",
                from: TradingState::IntradayAuction,
                to: TradingState::Continuous,
                halt_entry: false,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "state-plus-reason venue: continuous to suspended (market-wide breaker)",
                from: TradingState::Continuous,
                to: TradingState::Suspended,
                halt_entry: false,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "state-plus-reason venue: continuous to halt (single-stock breaker)",
                from: TradingState::Continuous,
                to: TradingState::Halt,
                halt_entry: true,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "state-plus-reason venue: halt to continuous (single-stock resume)",
                from: TradingState::Halt,
                to: TradingState::Continuous,
                halt_entry: false,
                halt_exit: true,
                session_open: false,
                session_close: false,
            },
            Case {
                name: "state-plus-reason venue: repeated halt is not a fresh entry",
                from: TradingState::Halt,
                to: TradingState::Halt,
                halt_entry: false,
                halt_exit: false,
                session_open: false,
                session_close: false,
            },
        ];

        for c in cases {
            let t = Transition {
                instrument: 1,
                from: c.from,
                to: c.to,
            };
            assert_eq!(t.is_halt_entry(), c.halt_entry, "{}: halt_entry", c.name);
            assert_eq!(t.is_halt_exit(), c.halt_exit, "{}: halt_exit", c.name);
            assert_eq!(
                t.is_session_open(),
                c.session_open,
                "{}: session_open",
                c.name
            );
            assert_eq!(
                t.is_session_close(),
                c.session_close,
                "{}: session_close",
                c.name
            );
        }
    }

    // Applying a new state updates what `state()` reports and the returned
    // transition captures the exact prior value, per instrument.
    #[test]
    fn machine_tracks_per_instrument() {
        let mut m = StatusMachine::new();
        assert_eq!(m.state(1), TradingState::Unknown);

        let t1 = m.apply(1, TradingState::PreOpen);
        assert_eq!(t1.from, TradingState::Unknown);
        assert_eq!(t1.to, TradingState::PreOpen);
        assert_eq!(m.state(1), TradingState::PreOpen);

        // instrument 2 is untouched by instrument 1's transitions
        assert_eq!(m.state(2), TradingState::Unknown);

        let t2 = m.apply(1, TradingState::Continuous);
        assert_eq!(t2.from, TradingState::PreOpen);
        assert!(t2.is_session_open());
    }
}
