//! Fuzz `orderbook_engine::BookEngine::apply_mbo` against an independent
//! reference book. `arbitrary` draws a bounded op sequence (add / modify /
//! cancel / fill / trade / reset) built as `Mbo` events. After every op the
//! engine's resting orders, per-level aggregates, and best bid/ask must match
//! a plain `BTreeMap` model that tracks the same state without ever calling
//! the engine. A mismatch is an engine conservation bug.
//!
//! Reads `AuditOrderState` / `AuditLevelState` via the engine's `testing`
//! feature. Policy gating is not under test here: the target supplies a local
//! accept-all `VenuePolicy`, matching the pattern of
//! `crates/orderbook/benches/book_ops.rs`.
#![no_main]

use std::collections::{BTreeMap, btree_map::Entry};

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use md_events::{Action, BreakRefKind, Mbo, RecordFlags, RecordHeader, Side, TradingState, rtype};
use orderbook_engine::{
    AuditLevelState, AuditOrderState, BoardId, BookEngine, BreakScope, Gate, PurposeSet,
    VenuePolicy,
};

const INST: u32 = 1;
const PRICE_BASE: i64 = 1_000_000;
const MAX_OPS: usize = 256;

/// Accept-everything policy; gating is out of scope for the conservation model.
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

/// One drawn op. Bounded field widths keep order-id and price pools small so
/// modify / cancel / fill land on existing orders and levels collide.
#[derive(Arbitrary, Debug)]
enum Op {
    Add {
        ask: bool,
        id: u8,
        price: u8,
        qty: u16,
    },
    Modify {
        ask: bool,
        id: u8,
        price: u8,
        qty: u16,
    },
    Cancel {
        ask: bool,
        id: u8,
        qty: u16,
    },
    Fill {
        ask: bool,
        id: u8,
        qty: u16,
    },
    Trade {
        ask: bool,
        price: u8,
        qty: u16,
    },
    Reset,
}

fn side_of(ask: bool) -> Side {
    if ask { Side::Ask } else { Side::Bid }
}

fn price_of(p: u8) -> i64 {
    PRICE_BASE + i64::from(p)
}

// resting quantity stays >= 1: keeps a level's aggregate nonzero while any
// order rests, so an emptied level always drops rather than lingering at 0.
fn qty_of(q: u16) -> u64 {
    u64::from(q) + 1
}

fn mbo(action: Action, side: Side, id: u64, price: i64, qty: u64) -> Mbo {
    Mbo {
        hdr: RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: INST,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id: id,
        price,
        qty,
        action,
        side,
        participant: [0; 7],
        _pad: [0u8; 7],
    }
}

/// Independent book model. Key `(ask, order_id)` mirrors the engine's
/// `(instrument, side, order_id)` with instrument fixed and side as a bool.
/// Level aggregates derive from the order map, never from the engine.
#[derive(Default)]
struct Reference {
    orders: BTreeMap<(bool, u64), (i64, u64)>,
}

impl Reference {
    // Add always replaces any order at the same key (engine `remove_existing`).
    fn add(&mut self, ask: bool, id: u64, price: i64, qty: u64) {
        self.orders.insert((ask, id), (price, qty));
    }

    // Modify only a resting order; an absent key is UnknownOrder, no change.
    fn modify(&mut self, ask: bool, id: u64, price: i64, qty: u64) {
        if let Entry::Occupied(mut e) = self.orders.entry((ask, id)) {
            e.insert((price, qty));
        }
    }

    // Cancel and fill both decrement by min(req, resting); full removal at zero.
    fn reduce(&mut self, ask: bool, id: u64, req: u64) {
        if let Some((_price, qty)) = self.orders.get_mut(&(ask, id)) {
            let dq = req.min(*qty);
            let remaining = *qty - dq;
            if remaining == 0 {
                self.orders.remove(&(ask, id));
            } else {
                *qty = remaining;
            }
        }
    }

    fn reset(&mut self) {
        self.orders.clear();
    }

    // Per-level aggregate qty and resting-order count, summed from the orders.
    fn levels(&self) -> BTreeMap<(bool, i64), (u64, u32)> {
        let mut out = BTreeMap::new();
        for (&(ask, _id), &(price, qty)) in &self.orders {
            let e = out.entry((ask, price)).or_insert((0u64, 0u32));
            e.0 += qty;
            e.1 += 1;
        }
        out
    }

    // Best price per side: highest bid, lowest ask.
    fn best(&self, ask: bool) -> Option<(i64, u64)> {
        let levels = self.levels();
        let sided = levels
            .iter()
            .filter(|((a, _), _)| *a == ask)
            .map(|((_, p), (q, _))| (*p, *q));
        if ask {
            sided.min_by_key(|(p, _)| *p)
        } else {
            sided.max_by_key(|(p, _)| *p)
        }
    }

    fn check(&self, engine: &BookEngine<AcceptAll>) {
        assert_eq!(engine.order_count(), self.orders.len(), "order count");
        for (&(ask, id), &(price, qty)) in &self.orders {
            assert_eq!(
                engine.order_state(INST, side_of(ask), id),
                Some(AuditOrderState { price, qty }),
                "order state ask={ask} id={id}"
            );
        }
        for ((ask, price), (qty, count)) in self.levels() {
            assert_eq!(
                engine.level_state(INST, side_of(ask), price),
                Some(AuditLevelState { qty, count }),
                "level state ask={ask} price={price}"
            );
        }
        assert_eq!(engine.best(INST, Side::Bid), self.best(false), "best bid");
        assert_eq!(engine.best(INST, Side::Ask), self.best(true), "best ask");
    }
}

fn apply(op: &Op, engine: &mut BookEngine<AcceptAll>, reference: &mut Reference) {
    match *op {
        Op::Add {
            ask,
            id,
            price,
            qty,
        } => {
            let (s, i, p, q) = (side_of(ask), u64::from(id), price_of(price), qty_of(qty));
            engine.apply_mbo(&mbo(Action::Add, s, i, p, q));
            reference.add(ask, i, p, q);
        }
        Op::Modify {
            ask,
            id,
            price,
            qty,
        } => {
            let (s, i, p, q) = (side_of(ask), u64::from(id), price_of(price), qty_of(qty));
            engine.apply_mbo(&mbo(Action::Modify, s, i, p, q));
            reference.modify(ask, i, p, q);
        }
        Op::Cancel { ask, id, qty } => {
            let (s, i, q) = (side_of(ask), u64::from(id), qty_of(qty));
            // engine reads the resting price from the order, ignores this one
            engine.apply_mbo(&mbo(Action::Cancel, s, i, 0, q));
            reference.reduce(ask, i, q);
        }
        Op::Fill { ask, id, qty } => {
            let (s, i, q) = (side_of(ask), u64::from(id), qty_of(qty));
            engine.apply_mbo(&mbo(Action::Fill, s, i, 0, q));
            reference.reduce(ask, i, q);
        }
        Op::Trade { ask, price, qty } => {
            // off-book print: engine applies but the book stays unchanged
            let (s, p, q) = (side_of(ask), price_of(price), qty_of(qty));
            engine.apply_mbo(&mbo(Action::Trade, s, 0, p, q));
        }
        Op::Reset => {
            engine.apply_mbo(&mbo(Action::Reset, Side::Bid, 0, 0, 0));
            reference.reset();
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let mut engine: BookEngine<AcceptAll> = BookEngine::new(AcceptAll);
    let mut reference = Reference::default();
    for _ in 0..MAX_OPS {
        if u.is_empty() {
            break;
        }
        let Ok(op) = Op::arbitrary(&mut u) else { break };
        apply(&op, &mut engine, &mut reference);
        reference.check(&engine);
    }
});
