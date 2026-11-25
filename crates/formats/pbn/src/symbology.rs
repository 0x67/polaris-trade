//! Running instrument symbology. Definition records carry the normalized
//! instrument metadata (symbol, board, asset class); a stream opens with a
//! Definition preamble and may re-emit Definitions intraday, so symbology is a
//! running map updated on every Definition, not a frozen head snapshot. A reader
//! resolves instrument_id to its Definition from the stream alone, no external
//! map.

use std::collections::HashMap;

use md_events::Definition;

/// instrument_id to the latest Definition seen on the stream.
#[derive(Clone, Debug, Default)]
pub struct Symbology {
    map: HashMap<u32, Definition>,
}

impl Symbology {
    /// Empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a Definition, superseding any prior one for its instrument_id. An
    /// intraday redefinition updates in place.
    pub fn ingest(&mut self, def: Definition) {
        self.map.insert(def.hdr.instrument_id, def);
    }

    /// Resolve the current Definition for an instrument.
    pub fn resolve(&self, instrument_id: u32) -> Option<&Definition> {
        self.map.get(&instrument_id)
    }

    /// Board code for an instrument, if defined.
    pub fn board(&self, instrument_id: u32) -> Option<[u8; 4]> {
        self.map.get(&instrument_id).map(|d| d.board)
    }

    /// Iterate the current Definitions, one per known instrument.
    pub fn definitions(&self) -> impl Iterator<Item = &Definition> {
        self.map.values()
    }

    /// Instruments matching a board code, for a venue-attribute filter that
    /// resolves through Definitions to an instrument-id set.
    pub fn instruments_on_board(&self, board: [u8; 4]) -> Vec<u32> {
        self.map
            .values()
            .filter(|d| d.board == board)
            .map(|d| d.hdr.instrument_id)
            .collect()
    }

    /// Number of instruments known.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if no Definition has been ingested.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use md_events::AssetClass;

    use super::*;

    fn def(instrument_id: u32, board: &[u8; 4], lot: u32) -> Definition {
        Definition::new(
            instrument_id,
            5,
            1,
            u64::from(instrument_id),
            [b'X'; 20],
            *board,
            AssetClass::Equity,
            0,
            lot,
        )
    }

    #[test]
    fn resolve_and_board() {
        let mut s = Symbology::new();
        assert!(s.is_empty());
        s.ingest(def(777, b"MAIN", 100));
        s.ingest(def(778, b"AUX1", 100));
        assert_eq!(s.len(), 2);
        assert_eq!(s.resolve(777).unwrap().lot_size, 100);
        assert_eq!(s.board(777), Some(*b"MAIN"));
        assert_eq!(s.board(778), Some(*b"AUX1"));
        assert_eq!(s.board(999), None);
    }

    // Intraday redefinition supersedes the prior record for the same id.
    #[test]
    fn intraday_supersede() {
        let mut s = Symbology::new();
        s.ingest(def(777, b"MAIN", 100));
        s.ingest(def(777, b"MAIN", 500));
        assert_eq!(s.len(), 1);
        assert_eq!(s.resolve(777).unwrap().lot_size, 500);
    }

    // Board filtering resolves through Definitions to an instrument-id set:
    // the basis for a venue-attribute filter like "Trade on the MAIN board".
    #[test]
    fn board_filter_instrument_set() {
        let mut s = Symbology::new();
        s.ingest(def(1, b"MAIN", 100));
        s.ingest(def(2, b"AUX1", 100));
        s.ingest(def(3, b"MAIN", 100));
        let mut main_board = s.instruments_on_board(*b"MAIN");
        main_board.sort_unstable();
        assert_eq!(main_board, vec![1, 3]);
    }
}
