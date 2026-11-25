//! Minted instrument identity with point-in-time alias resolution. We mint our
//! own stable ids because venue numbering is arbitrary and unstable (daily
//! reset, reuse after expiry, generation renumbering); none of that leaks to
//! consumers.

use std::collections::HashMap;

use md_events::Definition;

use super::definition::DefinitionEvent;

/// Venue registry id (publisher analog). Shared across a venue's feed
/// generations so continuity survives a generation switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VenueId(pub u16);

/// Minted, stable instrument id. Never reused, never day-scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstrumentId(pub u32);

/// Minting key: a (venue, symbol, board) triple. Generation and venue-native id
/// are deliberately excluded so the same instrument keeps one id across feeds.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct InstrumentKey {
    venue: VenueId,
    symbol: [u8; 20],
    board: [u8; 4],
}

/// One (venue, native id) -> instrument mapping, effective from `from_ts`.
/// A native id reused after expiry gets a second interval.
#[derive(Clone, Copy, Debug)]
struct AliasInterval {
    from_ts: u64,
    id: InstrumentId,
}

/// Mints stable ids on first sight of a (venue, symbol, board) key and resolves
/// venue-native ids to them point-in-time. Rebuildable purely from its own
/// definition stream.
pub struct InstrumentRegistry {
    minted: HashMap<InstrumentKey, InstrumentId>,
    aliases: HashMap<(VenueId, u64), Vec<AliasInterval>>,
    symbols: HashMap<(VenueId, [u8; 20]), InstrumentId>,
    next: u32,
}

impl Default for InstrumentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl InstrumentRegistry {
    /// Empty registry; minting starts at id 1 (0 reads as unset).
    pub fn new() -> Self {
        InstrumentRegistry {
            minted: HashMap::new(),
            aliases: HashMap::new(),
            symbols: HashMap::new(),
            next: 1,
        }
    }

    /// Mint (or return the existing) id for the event's (venue, symbol, board).
    /// A nonzero venue-native id is recorded as an alias effective from `ev.ts`;
    /// zero means the venue has no numeric id and the symbol index is the only key.
    pub fn upsert(&mut self, ev: &DefinitionEvent) -> InstrumentId {
        let key = InstrumentKey {
            venue: ev.venue,
            symbol: ev.raw_symbol,
            board: ev.board,
        };
        let id = match self.minted.get(&key) {
            Some(&id) => id,
            None => {
                let id = InstrumentId(self.next);
                self.next += 1;
                self.minted.insert(key, id);
                self.symbols.entry((ev.venue, ev.raw_symbol)).or_insert(id);
                id
            }
        };
        if ev.venue_native_id != 0 {
            self.push_alias(ev.venue, ev.venue_native_id, ev.ts, id);
        }
        id
    }

    /// The definition record to emit for a minted id, with the tail absent.
    /// A venue that publishes tail terms sets `tail` on the returned record.
    pub fn definition_record(ev: &DefinitionEvent, id: InstrumentId) -> Definition {
        Definition::new(
            id.0,
            ev.venue.0,
            ev.ts,
            ev.venue_native_id,
            ev.raw_symbol,
            ev.board,
            ev.asset_class,
            ev.qty_scale,
            ev.lot_size,
        )
    }

    /// Resolve a venue-native id to the instrument live at `ts`: the alias
    /// interval with the greatest `from_ts` not after `ts`. Native id 0 has no
    /// alias, so a venue without numeric ids uses [`Self::resolve_symbol`].
    pub fn resolve(&self, venue: VenueId, native_id: u64, ts: u64) -> Option<InstrumentId> {
        let intervals = self.aliases.get(&(venue, native_id))?;
        intervals
            .iter()
            .filter(|iv| iv.from_ts <= ts)
            .max_by_key(|iv| iv.from_ts)
            .map(|iv| iv.id)
    }

    /// Instrument minted for a venue symbol, whatever its board. When one symbol
    /// trades on several boards, the first minted board wins.
    pub fn resolve_symbol(&self, venue: VenueId, raw_symbol: &[u8; 20]) -> Option<InstrumentId> {
        self.symbols.get(&(venue, *raw_symbol)).copied()
    }

    /// Rebuild state from an emitted definition record. Replaying the whole
    /// stream reconstructs the registry exactly, with no persisted mapping.
    pub fn apply_record(&mut self, def: &Definition) {
        let id = InstrumentId(def.hdr.instrument_id);
        let key = InstrumentKey {
            venue: VenueId(def.hdr.venue),
            symbol: def.raw_symbol,
            board: def.board,
        };
        self.minted.entry(key).or_insert(id);
        self.symbols
            .entry((VenueId(def.hdr.venue), def.raw_symbol))
            .or_insert(id);
        if def.venue_native_id != 0 {
            self.push_alias(
                VenueId(def.hdr.venue),
                def.venue_native_id,
                def.hdr.ts_event,
                id,
            );
        }
        if def.hdr.instrument_id >= self.next {
            self.next = def.hdr.instrument_id + 1;
        }
    }

    fn push_alias(&mut self, venue: VenueId, native_id: u64, from_ts: u64, id: InstrumentId) {
        let intervals = self.aliases.entry((venue, native_id)).or_default();
        // hot path: adapters upsert on every symbol-bearing message, so a
        // re-sighting of the live mapping must be a no-op. Intervals stay
        // sorted by from_ts, so last() is the mapping live at any later ts;
        // only a mapping CHANGE records a new interval.
        if intervals
            .last()
            .is_some_and(|iv| iv.id == id && iv.from_ts <= from_ts)
        {
            return;
        }
        if intervals
            .iter()
            .any(|iv| iv.from_ts == from_ts && iv.id == id)
        {
            return;
        }
        intervals.push(AliasInterval { from_ts, id });
        intervals.sort_by_key(|iv| iv.from_ts);
    }
}

#[cfg(test)]
mod tests {
    use md_events::AssetClass;

    use super::*;

    fn sym(s: &str) -> [u8; 20] {
        let mut b = [0u8; 20];
        b[..s.len()].copy_from_slice(s.as_bytes());
        b
    }

    fn board(s: &str) -> [u8; 4] {
        let mut b = [0u8; 4];
        b[..s.len()].copy_from_slice(s.as_bytes());
        b
    }

    fn ev(venue: u16, symbol: &str, brd: &str, native: u64, ts: u64) -> DefinitionEvent {
        DefinitionEvent {
            venue: VenueId(venue),
            raw_symbol: sym(symbol),
            board: board(brd),
            venue_native_id: native,
            ts,
            asset_class: AssetClass::Equity,
            qty_scale: 0,
            lot_size: 100,
        }
    }

    // Same (venue, symbol, board) always mints one id; a different board is a
    // different instrument.
    #[test]
    fn mint_stable() {
        let mut r = InstrumentRegistry::new();
        let a = r.upsert(&ev(1, "CCCC", "MAIN", 73, 100));
        let a2 = r.upsert(&ev(1, "CCCC", "MAIN", 73, 200));
        let aux1 = r.upsert(&ev(1, "CCCC", "AUX1", 74, 100));
        assert_eq!(a, a2, "re-sighting the same key returns the same id");
        assert_ne!(a, aux1, "different board is a different instrument");
    }

    // Adapters upsert on every symbol-bearing message (every add order on
    // NASDAQ), so re-sighting the live mapping must not accumulate intervals:
    // one interval per mapping change, not one per sighting. Unbounded growth
    // here made a full-day replay quadratic (alias vec re-sorted per message).
    #[test]
    fn resight_same_mapping_records_no_new_interval() {
        let mut r = InstrumentRegistry::new();
        let first = r.upsert(&ev(1, "AAAA", "MAIN", 500, 1_000));
        for ts in 1_001..2_000u64 {
            assert_eq!(r.upsert(&ev(1, "AAAA", "MAIN", 500, ts)), first);
        }
        assert_eq!(
            r.aliases[&(VenueId(1), 500)].len(),
            1,
            "re-sighting the live mapping must not add intervals"
        );

        // an actual remap still records its interval and resolves point-in-time
        let second = r.upsert(&ev(1, "BBBB", "MAIN", 500, 5_000));
        assert_eq!(r.aliases[&(VenueId(1), 500)].len(), 2);
        assert_eq!(r.resolve(VenueId(1), 500, 1_500), Some(first));
        assert_eq!(r.resolve(VenueId(1), 500, 6_000), Some(second));
    }

    // A native id reused after expiry resolves to whichever instrument was live
    // at the queried time; history is never rewritten.
    #[test]
    fn point_in_time_alias() {
        let mut r = InstrumentRegistry::new();
        let first = r.upsert(&ev(1, "AAAA", "MAIN", 500, 1_000));
        // native id 500 reassigned to a new instrument later
        let second = r.upsert(&ev(1, "BBBB", "MAIN", 500, 5_000));
        assert_ne!(first, second);
        assert_eq!(
            r.resolve(VenueId(1), 500, 1_500),
            Some(first),
            "before reuse"
        );
        assert_eq!(
            r.resolve(VenueId(1), 500, 6_000),
            Some(second),
            "after reuse"
        );
        assert_eq!(r.resolve(VenueId(1), 500, 10), None, "before any mapping");
    }

    // Two feed generations of the same venue use different native ids for the
    // same symbol+board; both resolve to one minted id, no migration table.
    #[test]
    fn cross_generation() {
        let mut r = InstrumentRegistry::new();
        let gen1 = r.upsert(&ev(1, "CCCC", "MAIN", 722, 1_000)); // old generation native id
        let gen2 = r.upsert(&ev(1, "CCCC", "MAIN", 73, 2_000)); // new generation native id
        assert_eq!(gen1, gen2, "one instrument across the fork");
        assert_eq!(r.resolve(VenueId(1), 722, 1_500), Some(gen1));
        assert_eq!(r.resolve(VenueId(1), 73, 2_500), Some(gen2));
    }

    // Replaying the emitted definition stream into a fresh registry reproduces
    // every resolution: state is rebuildable with no persisted mapping.
    #[test]
    fn roundtrip_rebuild() {
        let mut src = InstrumentRegistry::new();
        let events = [
            ev(1, "CCCC", "MAIN", 722, 1_000),
            ev(1, "CCCC", "MAIN", 73, 2_000),
            ev(1, "CCCC", "AUX1", 74, 2_000),
        ];
        let mut records = Vec::new();
        for e in &events {
            let id = src.upsert(e);
            records.push(InstrumentRegistry::definition_record(e, id));
        }

        let mut rebuilt = InstrumentRegistry::new();
        for rec in &records {
            rebuilt.apply_record(rec);
        }

        for (venue, native, ts) in [(1u16, 722u64, 1_500u64), (1, 73, 2_500), (1, 74, 2_500)] {
            assert_eq!(
                rebuilt.resolve(VenueId(venue), native, ts),
                src.resolve(VenueId(venue), native, ts),
                "rebuilt resolution diverged for native {native}"
            );
        }
    }

    // Venues without a numeric id key instruments by symbol alone: distinct
    // symbols mint distinct ids, and a rebuild from records resolves each
    // symbol to the id it had.
    #[test]
    fn symbol_keyed_instruments_survive_rebuild() {
        let mut src = InstrumentRegistry::new();
        let a = ev(2, "AAPL", "MAIN", 0, 1_000);
        let b = ev(2, "MSFT", "MAIN", 0, 1_000);
        let ia = src.upsert(&a);
        let ib = src.upsert(&b);
        assert_ne!(ia, ib);
        assert_eq!(src.upsert(&a), ia, "same symbol re-resolves to the same id");
        assert!(src.aliases.is_empty(), "native id 0 records no alias");
        assert_eq!(src.resolve_symbol(VenueId(2), &sym("AAPL")), Some(ia));
        assert_eq!(src.resolve_symbol(VenueId(2), &sym("MSFT")), Some(ib));

        let mut rebuilt = InstrumentRegistry::new();
        rebuilt.apply_record(&InstrumentRegistry::definition_record(&a, ia));
        rebuilt.apply_record(&InstrumentRegistry::definition_record(&b, ib));
        assert_eq!(rebuilt.resolve_symbol(VenueId(2), &sym("AAPL")), Some(ia));
        assert_eq!(rebuilt.resolve_symbol(VenueId(2), &sym("MSFT")), Some(ib));
        assert_eq!(rebuilt.resolve_symbol(VenueId(1), &sym("AAPL")), None);
        assert_eq!(rebuilt.resolve(VenueId(2), 0, 2_000), None);
    }

    fn sight_universe(names: &[String]) -> InstrumentRegistry {
        let mut r = InstrumentRegistry::new();
        for pass in 0..3u64 {
            for (i, name) in names.iter().enumerate() {
                r.upsert(&ev(1, name, "MAIN", i as u64 + 1, 1_000 + pass));
            }
        }
        r
    }

    // The old quadratic push_alias showed up as interval growth per
    // re-sighting: every index must hold one entry per instrument.
    #[test]
    fn indexes_stay_one_entry_per_instrument_over_10k_universe() {
        const N: usize = 10_000;
        let names: Vec<String> = (0..N).map(|i| format!("S{i}")).collect();
        let r = sight_universe(&names);
        assert_eq!(r.minted.len(), N);
        assert_eq!(r.symbols.len(), N);
        assert_eq!(r.aliases.len(), N);
        assert!(r.aliases.values().all(|v| v.len() == 1));
    }

    // Entry counts miss per-call scans that grow no index. Four times the
    // universe must cost nowhere near sixteen times as long; the 10x bound
    // leaves wide room for runner noise around the linear 4x.
    #[test]
    fn upsert_cost_scales_linearly_with_universe() {
        let time = |n: usize| {
            let names: Vec<String> = (0..n).map(|i| format!("S{i}")).collect();
            (0..3)
                .map(|_| {
                    let t = std::time::Instant::now();
                    sight_universe(&names);
                    t.elapsed()
                })
                .min()
                .expect("three runs")
        };
        let (small, large) = (time(10_000), time(40_000));
        assert!(large < small * 10, "10k took {small:?}, 40k took {large:?}");
    }
}
