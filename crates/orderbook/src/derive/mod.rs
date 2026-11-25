//! Derived-view computation from the normalized record stream only. Depth
//! (mbp-10), best bid/ask, and running vwap are computed from `Mbo`/`Level`/
//! `Trade` records exclusively; every public method here takes one of those
//! record types (never raw or wire bytes), which is the type-level guarantee
//! that derived views share one derivation path with everything downstream.

pub mod ohlcv;

use std::{collections::HashMap, hash::BuildHasher};

use md_events::{
    Action, Bbo, BidAskPair, Level, LevelAction, Mbo, Mbp10, Price, RecordFlags, RecordHeader,
    Side, Trade, rtype,
};
use rustc_hash::FxBuildHasher;

use crate::{engine::BookDepth, policy::PurposeSet};

const DEPTH: usize = 10;

/// Build a derived record's header from its triggering source event: copies
/// `venue`/`instrument_id`/`ts_event`/`ts_recv`/`venue_seq` per the fan-out
/// invariant; `ts_egress`/`egress_seq` stay 0 until the egress sequencer
/// stamps them.
pub(crate) fn derived_header(src: RecordHeader, rt: u8, size: usize) -> RecordHeader {
    RecordHeader {
        length: (size / 4) as u8,
        rtype: rt,
        venue: src.venue,
        instrument_id: src.instrument_id,
        ts_event: src.ts_event,
        ts_recv: src.ts_recv,
        ts_egress: 0,
        venue_seq: src.venue_seq,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    }
}

/// A `Clear` maps to `Action::Reset` for either side. With a named side the
/// derived `Mbp10` carries that action and the other side's rows unchanged.
fn level_to_action(la: LevelAction) -> Action {
    match la {
        LevelAction::New => Action::Add,
        LevelAction::Change => Action::Modify,
        LevelAction::Delete => Action::Cancel,
        LevelAction::Clear => Action::Reset,
    }
}

/// Book-derived output for one applied `Mbo`/`Level` event. Both `None` when
/// the instrument's board purposes exclude book participation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BookDerive {
    pub mbp10: Option<Mbp10>,
    pub bbo: Option<Bbo>,
}

/// Trade-derived output for one applied `Trade` record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TradeDerive {
    /// True when the board's purposes include `TAPE` and this print counted
    /// toward volume/vwap.
    pub tape: bool,
    /// Running volume-weighted average price (normalized `1e-9` scale) after
    /// this print; `None` until the first `TAPE`-eligible print.
    pub vwap: Option<i64>,
}

/// Computes mbp-10 depth, bbo, trades-tape eligibility, and running vwap.
/// Depth and bbo are read from the caller's already-applied `BookEngine`
/// through `BookDepth`, not a second copy of the ladder; only the running vwap
/// lives here, since it has no home in the book. Every book method requires the
/// book to already reflect the event (the caller applies to the book first).
pub struct DeriveEngine<S: BuildHasher = FxBuildHasher> {
    vwap: HashMap<u32, (u128, u64), S>,
}

impl<S> Default for DeriveEngine<S>
where
    S: BuildHasher + Clone + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl DeriveEngine<FxBuildHasher> {
    /// New derive engine using the benchmark-selected default hasher. Existing
    /// callers need no explicit type parameter.
    pub fn new() -> Self {
        DeriveEngine::with_hasher(FxBuildHasher)
    }
}

impl<S> DeriveEngine<S>
where
    S: BuildHasher + Clone,
{
    /// New derive engine with an explicit map hasher for caller selection and
    /// fair benchmark comparisons.
    pub fn with_hasher(hasher: S) -> Self {
        DeriveEngine {
            vwap: HashMap::with_hasher(hasher),
        }
    }

    /// Derive depth and bbo for one order-level event from `book`, gated by
    /// `purposes`. `book` must already reflect `ev`.
    pub fn apply_mbo(&self, ev: &Mbo, book: &impl BookDepth, purposes: PurposeSet) -> BookDerive {
        self.apply_mbo_batch(core::slice::from_ref(ev), book, purposes)
    }

    /// Derive once from the final book state for one source event's MBO
    /// segment. `book` must already reflect every row in `events`; the derived
    /// records carry the last row's action/price/qty and header. All rows in a
    /// non-empty batch must target the same instrument and carry the same
    /// source sequence and timestamps.
    pub fn apply_mbo_batch(
        &self,
        events: &[Mbo],
        book: &impl BookDepth,
        purposes: PurposeSet,
    ) -> BookDerive {
        let Some(last) = events.last() else {
            return BookDerive::default();
        };
        for ev in events {
            debug_assert_eq!(ev.hdr.instrument_id, last.hdr.instrument_id);
            debug_assert_eq!(ev.hdr.venue_seq, last.hdr.venue_seq);
            debug_assert_eq!(ev.hdr.ts_event, last.hdr.ts_event);
            debug_assert_eq!(ev.hdr.ts_recv, last.hdr.ts_recv);
        }
        book_derive(
            book,
            last.hdr,
            last.price,
            last.qty,
            last.action,
            last.side,
            purposes,
        )
    }

    /// Derive depth and bbo for one MBP-native level event from `book`, gated
    /// by `purposes`. `book` must already reflect `ev`.
    ///
    /// A `Clear` yields `Action::Reset` for either side. With a named side the
    /// `Mbp10` keeps the other side's rows, so `Reset` is not always a full wipe.
    pub fn apply_level(
        &self,
        ev: &Level,
        book: &impl BookDepth,
        purposes: PurposeSet,
    ) -> BookDerive {
        book_derive(
            book,
            ev.hdr,
            ev.price,
            ev.qty,
            level_to_action(ev.action),
            ev.side,
            purposes,
        )
    }

    /// Apply one normalized trade print; updates the running vwap and reports
    /// tape eligibility, both gated by `purposes` (`TAPE` purpose gates
    /// tape/vwap participation).
    pub fn apply_trade(&mut self, ev: &Trade, purposes: PurposeSet) -> TradeDerive {
        let inst = ev.hdr.instrument_id;
        if !purposes.contains(PurposeSet::TAPE) {
            return TradeDerive {
                tape: false,
                vwap: self.vwap_of(inst),
            };
        }
        let acc = self.vwap.entry(inst).or_insert((0, 0));
        acc.0 += ev.price as u128 * ev.qty as u128;
        acc.1 += ev.qty;
        TradeDerive {
            tape: true,
            vwap: self.vwap_of(inst),
        }
    }

    /// Reverse a broken trade's contribution to the running vwap accumulator.
    /// `DeriveEngine` keeps no per-trade memo of its own (unlike
    /// `OhlcvEngine`); the caller supplies the original print's price/qty.
    /// No-op for an instrument with no accumulator (nothing to reverse).
    pub fn apply_trade_break(&mut self, price: i64, qty: u64, instrument: u32) {
        let Some(acc) = self.vwap.get_mut(&instrument) else {
            return;
        };
        acc.0 = acc.0.saturating_sub(price as u128 * qty as u128);
        acc.1 = acc.1.saturating_sub(qty);
    }

    fn vwap_of(&self, instrument: u32) -> Option<i64> {
        let (value, volume) = *self.vwap.get(&instrument)?;
        (volume > 0).then(|| (value / volume as u128) as i64)
    }
}

/// Build mbp-10 depth and bbo straight from the already-applied book. Rows
/// pre-filled with the null sentinel so a side missing at some depth reads null
/// price (i64::MAX), never a real price of 0. `book` must already reflect the
/// triggering event; `hdr`/`price`/`qty`/`action`/`side` are that event's
/// metadata, stamped onto the derived records.
fn book_derive(
    book: &impl BookDepth,
    hdr: RecordHeader,
    price: i64,
    qty: u64,
    action: Action,
    side: Side,
    purposes: PurposeSet,
) -> BookDerive {
    if !purposes.contains(PurposeSet::BOOK) {
        return BookDerive::default();
    }
    let inst = hdr.instrument_id;
    let mut bid_top = [(Price::NULL.raw(), 0u64, 0u32); DEPTH];
    let mut ask_top = bid_top;
    let bid_n = book.top_levels(inst, Side::Bid, &mut bid_top);
    let ask_n = book.top_levels(inst, Side::Ask, &mut ask_top);
    let mut levels = [BidAskPair::default(); DEPTH];
    let depth = bid_n.max(ask_n);
    for (i, level) in levels.iter_mut().enumerate().take(depth) {
        let (bid_px, bid_sz, bid_ct) = bid_top[i];
        let (ask_px, ask_sz, ask_ct) = ask_top[i];
        *level = BidAskPair {
            bid_px,
            ask_px,
            bid_sz,
            ask_sz,
            bid_ct,
            ask_ct,
        };
    }
    let mbp10 = Mbp10 {
        hdr: derived_header(hdr, rtype::MBP10, core::mem::size_of::<Mbp10>()),
        price,
        qty,
        action,
        side,
        depth: depth as u8,
        levels,
        _pad: [0u8; 5],
    };
    let bbo = Bbo {
        hdr: derived_header(hdr, rtype::BBO, core::mem::size_of::<Bbo>()),
        pair: levels[0],
    };
    BookDerive {
        mbp10: Some(mbp10),
        bbo: Some(bbo),
    }
}

#[cfg(test)]
mod tests {
    use md_events::{BreakRefKind, Record, TradingState, rtype};

    use super::*;
    use crate::{
        engine::BookEngine,
        policy::{BoardId, BoardPurposeMap, BreakScope, Gate, VenuePolicy},
    };

    // Accept-everything policy so the book applies every event; the derive
    // tests drive depth off the real book, matching the pipeline wiring.
    #[derive(Default)]
    struct AcceptAll;

    impl VenuePolicy for AcceptAll {
        fn book_gate(&self, _state: TradingState, _action: Action) -> Gate {
            Gate::Accept
        }
        fn purposes(&self, _board: BoardId) -> PurposeSet {
            PurposeSet::BOOK
        }
        fn tape_gate(&self, printable: bool, _action: Action) -> bool {
            printable
        }
        fn on_trade_break(&self, _kind: BreakRefKind) -> BreakScope {
            BreakScope::AggregatesOnly
        }
    }

    fn book() -> BookEngine<AcceptAll> {
        BookEngine::new(AcceptAll)
    }

    // Apply one event to the book, then derive from the updated book, the same
    // order the pipeline uses.
    fn step(
        b: &mut BookEngine<AcceptAll>,
        d: &DeriveEngine,
        ev: &Mbo,
        purposes: PurposeSet,
    ) -> BookDerive {
        b.apply_mbo(ev);
        d.apply_mbo(ev, b, purposes)
    }

    fn hdr(inst: u32, ts_event: u64, venue_seq: u64) -> RecordHeader {
        RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: inst,
            ts_event,
            ts_recv: ts_event,
            ts_egress: 0,
            venue_seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        }
    }

    fn mbo(inst: u32, oid: u64, side: Side, action: Action, price: i64, qty: u64) -> Mbo {
        Mbo {
            hdr: hdr(inst, 0, oid),
            order_id: oid,
            price,
            qty,
            action,
            side,
            participant: [0; 7],
            _pad: [0u8; 7],
        }
    }

    // Depth and bbo track add/cancel straight off the book; a board without
    // BOOK purpose contributes nothing.
    #[test]
    fn mbp10_and_bbo_track_book() {
        let mut b = book();
        let e = DeriveEngine::new();
        step(
            &mut b,
            &e,
            &mbo(1, 10, Side::Bid, Action::Add, 100, 5),
            PurposeSet::BOOK,
        );
        let out = step(
            &mut b,
            &e,
            &mbo(1, 11, Side::Bid, Action::Add, 101, 3),
            PurposeSet::BOOK,
        );
        let mbp10 = out.mbp10.expect("book purpose emits mbp10");
        assert_eq!(mbp10.levels[0].bid_px, 101);
        assert_eq!(mbp10.levels[0].bid_sz, 3);
        assert_eq!(mbp10.levels[1].bid_px, 100);
        assert_eq!(mbp10.depth, 2, "two bid rows, ask side stays empty");
        assert_eq!(out.bbo.unwrap().pair.bid_px, 101);
        assert_eq!(Mbp10::RTYPE, rtype::MBP10);

        let gated = step(
            &mut b,
            &e,
            &mbo(1, 12, Side::Bid, Action::Add, 99, 1),
            PurposeSet::TAPE,
        );
        assert!(gated.mbp10.is_none(), "no BOOK purpose, no depth output");
        assert!(gated.bbo.is_none());
    }

    // A modify moves the resting level; the derived depth reads the book's new
    // best after the move.
    #[test]
    fn modify_moves_level() {
        let mut b = book();
        let e = DeriveEngine::new();
        step(
            &mut b,
            &e,
            &mbo(1, 1, Side::Bid, Action::Add, 100, 5),
            PurposeSet::BOOK,
        );
        let out = step(
            &mut b,
            &e,
            &mbo(1, 1, Side::Bid, Action::Modify, 102, 5),
            PurposeSet::BOOK,
        );
        assert_eq!(b.best(1, Side::Bid), Some((102, 5)));
        assert_eq!(out.mbp10.unwrap().levels[0].bid_px, 102);
    }

    // A cross-id replacement is two MBO mutations but one source event. The book
    // applies both rows; derive emits one record describing the final book,
    // stamped with the final row's action/price/qty and header.
    #[test]
    fn replace_batch_derives_once_from_final_state() {
        let mut b = book();
        let e = DeriveEngine::new();
        b.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, 100, 5));

        let mut cancel = mbo(1, 1, Side::Bid, Action::Cancel, 100, 5);
        cancel.hdr = hdr(1, 50, 99);
        let mut add = mbo(1, 2, Side::Bid, Action::Add, 102, 7);
        add.hdr = hdr(1, 50, 99);
        b.apply_mbo(&cancel);
        b.apply_mbo(&add);
        let out = e.apply_mbo_batch(&[cancel, add], &b, PurposeSet::BOOK);

        assert_eq!(b.best(1, Side::Bid), Some((102, 7)));
        let mbp10 = out.mbp10.expect("book batch emits one final mbp10");
        assert_eq!(mbp10.action, Action::Add);
        assert_eq!(mbp10.price, 102);
        assert_eq!(mbp10.qty, 7);
        assert_eq!(mbp10.hdr.venue_seq, 99);
        assert_eq!(mbp10.levels[0].bid_px, 102);
        assert_eq!(mbp10.levels[0].bid_sz, 7);
        assert_eq!(
            out.bbo.expect("book batch emits one final bbo").pair.bid_px,
            102
        );
    }

    // An empty source batch has no derived output.
    #[test]
    fn empty_mbo_batch_is_noop() {
        let b = book();
        let e = DeriveEngine::new();
        assert_eq!(
            e.apply_mbo_batch(&[], &b, PurposeSet::BOOK),
            BookDerive::default()
        );
        assert_eq!(b.best(1, Side::Bid), None);
    }

    // Vwap is the running volume-weighted average across printable trades;
    // a print outside the TAPE purpose leaves it unchanged.
    #[test]
    fn vwap_running_average() {
        let mut e = DeriveEngine::new();
        let trade = |price: i64, qty: u64| Trade {
            hdr: hdr(1, 0, 0),
            price,
            qty,
            aggressor: Side::Bid,
            trade_ref: 1,
            _pad: [0u8; 7],
        };
        let out1 = e.apply_trade(&trade(100, 10), PurposeSet::TAPE);
        assert!(out1.tape);
        assert_eq!(out1.vwap, Some(100));

        let out2 = e.apply_trade(&trade(110, 10), PurposeSet::TAPE);
        assert_eq!(out2.vwap, Some(105), "avg of 100x10 and 110x10 is 105");

        let out3 = e.apply_trade(&trade(500, 10), PurposeSet::NONE);
        assert!(!out3.tape);
        assert_eq!(out3.vwap, Some(105), "non-tape print does not move vwap");
    }

    // Board-to-purpose routing gates tape/vwap: only a board carrying TAPE
    // contributes.
    #[test]
    fn only_tape_board_feeds_vwap() {
        let mut m = BoardPurposeMap::new(PurposeSet::NONE);
        let main_board = BoardId(*b"MAIN");
        m.set(main_board, PurposeSet::TAPE);

        let mut e = DeriveEngine::new();
        let trade = Trade {
            hdr: hdr(1, 0, 0),
            price: 100,
            qty: 10,
            aggressor: Side::Bid,
            trade_ref: 1,
            _pad: [0u8; 7],
        };
        let out = e.apply_trade(&trade, m.purposes(main_board));
        assert!(out.tape);
        let untouched = e.apply_trade(&trade, m.purposes(BoardId([0; 4])));
        assert!(!untouched.tape, "unlisted board defaults to NONE purposes");
    }

    // A depth level with no resting orders on that side must pad price to the
    // only null sentinel, never a real price of 0 (an empty ask reading as a
    // free-price ask is a normalization bug, not just an ugly zero).
    #[test]
    fn mbp_padding_null() {
        let mut b = book();
        let e = DeriveEngine::new();
        let out = step(
            &mut b,
            &e,
            &mbo(1, 1, Side::Bid, Action::Add, 100, 5),
            PurposeSet::BOOK,
        );
        let mbp10 = out.mbp10.expect("book purpose emits mbp10");
        assert_eq!(
            mbp10.levels[0].ask_px,
            Price::NULL.raw(),
            "empty ask side is null, not zero"
        );
        assert_eq!(mbp10.levels[0].ask_sz, 0);
        assert_eq!(mbp10.levels[0].ask_ct, 0);
        assert_eq!(out.bbo.unwrap().pair.ask_px, Price::NULL.raw());
    }

    // A level Clear with no side wipes the instrument's book, and with a side
    // empties that side only; other instruments keep their book.
    #[test]
    fn level_clear_empties_its_side_or_the_whole_book() {
        let level = |inst, side, action, price, qty| Level {
            hdr: RecordHeader {
                rtype: rtype::LEVEL,
                ..hdr(inst, 0, 0)
            },
            price,
            qty,
            num_deletes: 0,
            side,
            level: 0,
            action,
            _pad: [0u8; 3],
        };
        let mut b = book();
        let e = DeriveEngine::new();
        b.apply_level(&level(2, Side::Ask, LevelAction::New, 50, 1));
        for (side, depth, ask_left, orders) in
            [(Side::None, 0, None, 0), (Side::Bid, 2, Some((101, 4)), 1)]
        {
            b.apply_level(&level(1, Side::Bid, LevelAction::New, 100, 5));
            b.apply_level(&level(1, Side::Ask, LevelAction::New, 101, 4));
            b.apply_mbo(&mbo(1, 7, Side::Bid, Action::Add, 99, 2));
            b.apply_mbo(&mbo(1, 8, Side::Ask, Action::Add, 102, 3));

            let clear = level(1, side, LevelAction::Clear, 0, 0);
            b.apply_level(&clear);
            let mbp10 = e
                .apply_level(&clear, &b, PurposeSet::BOOK)
                .mbp10
                .expect("book purpose emits mbp10");
            assert_eq!(
                (mbp10.action, mbp10.depth),
                (Action::Reset, depth),
                "{side:?}"
            );
            assert_eq!(b.best(1, Side::Bid), None, "{side:?}");
            assert_eq!(b.instrument_order_count(1), orders, "{side:?}");
            assert_eq!(
                b.queue_position(1, Side::Ask, 8),
                (orders == 1).then_some(0),
                "{side:?}"
            );
            assert_eq!(b.best(1, Side::Ask), ask_left, "{side:?}");
            assert_eq!(b.best(2, Side::Ask), Some((50, 1)), "{side:?}");
            b.clear(1);
        }
    }

    // A venue resync (Clear then the fresh ladder) must leave the same book and
    // the same derived depth as a book built from that ladder alone.
    #[test]
    fn resync_rebuilds_the_snapshot_book() {
        let level = |side, action, price, qty| Level {
            hdr: RecordHeader {
                rtype: rtype::LEVEL,
                ..hdr(1, 0, 0)
            },
            price,
            qty,
            num_deletes: 0,
            side,
            level: 0,
            action,
            _pad: [0u8; 3],
        };
        let ladder = [
            level(Side::Bid, LevelAction::New, 100, 5),
            level(Side::Bid, LevelAction::New, 99, 2),
            level(Side::Ask, LevelAction::New, 101, 4),
            level(Side::Ask, LevelAction::New, 103, 6),
        ];
        let e = DeriveEngine::new();
        let mut fresh = book();
        let mut resynced = book();
        resynced.apply_level(&level(Side::Bid, LevelAction::New, 98, 9));
        resynced.apply_level(&level(Side::Ask, LevelAction::New, 102, 8));
        resynced.apply_level(&level(Side::None, LevelAction::Clear, 0, 0));

        let mut last = [BookDerive::default(); 2];
        for (b, out) in [&mut fresh, &mut resynced].into_iter().zip(&mut last) {
            for ev in &ladder {
                b.apply_level(ev);
                *out = e.apply_level(ev, &*b, PurposeSet::BOOK);
            }
        }

        assert_eq!(last[0], last[1], "derived depth after resync");
        assert_eq!(last[1].mbp10.expect("book purpose").levels[1].bid_px, 99);
        for side in [Side::Bid, Side::Ask] {
            assert_eq!(fresh.best(1, side), resynced.best(1, side), "{side:?}");
        }
    }

    // A TradeBreak must reverse DeriveEngine's own vwap accumulator, not just
    // OhlcvEngine's; the two accumulators are independent state.
    #[test]
    fn vwap_break_reversal() {
        let mut e = DeriveEngine::new();
        let trade = |price: i64, qty: u64| Trade {
            hdr: hdr(1, 0, 0),
            price,
            qty,
            aggressor: Side::Bid,
            trade_ref: 1,
            _pad: [0u8; 7],
        };
        e.apply_trade(&trade(100, 10), PurposeSet::TAPE);
        let out = e.apply_trade(&trade(110, 10), PurposeSet::TAPE);
        assert_eq!(out.vwap, Some(105), "avg of 100x10 and 110x10 is 105");

        e.apply_trade_break(110, 10, 1);
        assert_eq!(
            e.vwap_of(1),
            Some(100),
            "break reverses the second print's contribution"
        );
    }
}
