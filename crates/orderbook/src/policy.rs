//! Venue policy seam. Venue-specific behavior enters the engine only through
//! this trait at fixed hook points; the mechanical core carries zero venue
//! conditionals, so adding a venue never edits engine internals.

use std::collections::HashMap;

use md_events::{Action, BreakRefKind, TradingState};

/// Pre-book acceptance decision for an incoming order event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    /// Apply to the book normally.
    Accept,
    /// Do not apply; the event is refused for this state.
    Drop,
    /// Apply, but the result is marked suspect (e.g. events during a halt).
    Flag,
}

/// A board classification, matching `Definition.board`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BoardId(pub [u8; 4]);

/// What a board's instruments feed. A bitset so a board can serve several
/// purposes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PurposeSet(pub u8);

impl PurposeSet {
    /// Contributes to book reconstruction.
    pub const BOOK: PurposeSet = PurposeSet(1 << 0);
    /// Contributes to OHLCV bars.
    pub const OHLCV: PurposeSet = PurposeSet(1 << 1);
    /// Contributes to charting series.
    pub const CHART: PurposeSet = PurposeSet(1 << 2);
    /// Contributes to the trades tape and volume.
    pub const TAPE: PurposeSet = PurposeSet(1 << 3);
    /// Contributes to rankings.
    pub const RANKINGS: PurposeSet = PurposeSet(1 << 4);
    /// No purposes.
    pub const NONE: PurposeSet = PurposeSet(0);

    /// True when every purpose in `other` is set here.
    pub const fn contains(self, other: PurposeSet) -> bool {
        self.0 & other.0 == other.0
    }

    /// Union.
    pub const fn with(self, other: PurposeSet) -> PurposeSet {
        PurposeSet(self.0 | other.0)
    }
}

/// Declarative board-to-purpose routing (replaces scattered board string
/// comparisons). Boards with no entry fall back to `default`.
#[derive(Clone, Debug, Default)]
pub struct BoardPurposeMap {
    map: HashMap<BoardId, PurposeSet>,
    default: PurposeSet,
}

impl BoardPurposeMap {
    /// New map with an explicit fallback for unlisted boards.
    pub fn new(default: PurposeSet) -> Self {
        BoardPurposeMap {
            map: HashMap::new(),
            default,
        }
    }

    /// Route a board to a purpose set.
    pub fn set(&mut self, board: BoardId, purposes: PurposeSet) -> &mut Self {
        self.map.insert(board, purposes);
        self
    }

    /// Purposes for a board, or the default.
    pub fn purposes(&self, board: BoardId) -> PurposeSet {
        self.map.get(&board).copied().unwrap_or(self.default)
    }
}

/// What a trade break reverses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakScope {
    /// Nothing (the break is a no-op for our aggregates).
    None,
    /// Volume/value/frequency aggregates only; OHLC untouched.
    AggregatesOnly,
    /// Both the book and the aggregates.
    BookAndAggregates,
}

/// The only surface through which venue behavior reaches the engine.
pub trait VenuePolicy {
    /// Acceptance decision for an order event given the instrument's state.
    fn book_gate(&self, state: TradingState, action: Action) -> Gate;
    /// Purposes a board's instruments serve.
    fn purposes(&self, board: BoardId) -> PurposeSet;
    /// Whether an execution reaches the tape (false for non-printable legs).
    fn tape_gate(&self, printable: bool, action: Action) -> bool;
    /// What a trade break of the given reference kind reverses.
    fn on_trade_break(&self, kind: BreakRefKind) -> BreakScope;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn board(s: &str) -> BoardId {
        let mut b = [0u8; 4];
        b[..s.len()].copy_from_slice(s.as_bytes());
        BoardId(b)
    }

    // Board map under test: MAIN feeds everything including OHLCV; AUX1 and
    // AUX2 are tape-only.
    #[test]
    fn board_purpose_routing() {
        let mut m = BoardPurposeMap::new(PurposeSet::NONE);
        m.set(
            board("MAIN"),
            PurposeSet::BOOK
                .with(PurposeSet::OHLCV)
                .with(PurposeSet::CHART)
                .with(PurposeSet::TAPE)
                .with(PurposeSet::RANKINGS),
        );
        m.set(board("AUX1"), PurposeSet::TAPE);
        m.set(board("AUX2"), PurposeSet::TAPE);

        assert!(m.purposes(board("MAIN")).contains(PurposeSet::OHLCV));
        assert!(!m.purposes(board("AUX1")).contains(PurposeSet::OHLCV));
        assert!(m.purposes(board("AUX1")).contains(PurposeSet::TAPE));
        assert!(!m.purposes(board("AUX2")).contains(PurposeSet::BOOK));
        assert_eq!(
            m.purposes(board("XX")),
            PurposeSet::NONE,
            "unlisted board default"
        );
    }
}
