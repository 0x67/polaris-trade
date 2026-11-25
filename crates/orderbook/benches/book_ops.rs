//! Book-op microbench comparing the two `LevelBook` containers. Custom harness
//! (no criterion) so it builds offline and depends only on md-events. Full run:
//! `cargo bench --bench book_ops`. Smoke: `cargo bench --bench book_ops -- --test`.

use std::{
    collections::{HashMap, hash_map::RandomState},
    hash::BuildHasher,
    hint::black_box,
    time::Instant,
};

use md_events::{Action, BreakRefKind, Mbo, RecordFlags, RecordHeader, Side, TradingState, rtype};
use orderbook_engine::{
    BTreeLevelBook, BookEngine, SoaLevelBook,
    level::LevelBook,
    policy::{BoardId, BreakScope, Gate, PurposeSet, VenuePolicy},
};
use rustc_hash::FxBuildHasher;

/// Accept-everything policy for the bench.
struct NoopPolicy;

impl VenuePolicy for NoopPolicy {
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

fn mbo(oid: u64, action: Action, price: i64, qty: u64) -> Mbo {
    Mbo {
        hdr: RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: 1,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id: oid,
        price,
        qty,
        action,
        side: Side::Bid,
        participant: [0; 7],
        _pad: [0u8; 7],
    }
}

fn run_engine<L, S>(name: &str, ops: u64, prices: i64, smoke: bool)
where
    L: LevelBook,
    S: BuildHasher + Clone + Default,
{
    let mut engine: BookEngine<NoopPolicy, L, S> =
        BookEngine::with_hasher(NoopPolicy, S::default());
    let start = Instant::now();
    for i in 0..ops {
        let price = 1_000_000 + (i as i64 % prices);
        engine.apply_mbo(&mbo(i, Action::Add, price, 1));
        if i >= prices as u64 {
            // cancel the order that landed `prices` ops ago to churn levels
            engine.apply_mbo(&mbo(i - prices as u64, Action::Cancel, price, 1));
        }
    }
    let elapsed = start.elapsed();
    let best = engine.best(1, Side::Bid);
    assert!(best.is_some(), "{name}: book emptied unexpectedly");
    black_box(best);
    if !smoke {
        let per = elapsed.as_nanos() as f64 / ops as f64;
        println!("{name:>12}: {ops} ops over {prices} prices, {per:.1} ns/op");
    }
}

struct PlainBook<S: BuildHasher> {
    orders: HashMap<(u32, Side, u64), (i64, u64), S>,
    levels: SoaLevelBook,
}

impl<S> PlainBook<S>
where
    S: BuildHasher + Default,
{
    fn new() -> Self {
        PlainBook {
            orders: HashMap::with_hasher(S::default()),
            levels: SoaLevelBook::default(),
        }
    }

    fn apply(&mut self, event: &Mbo) {
        let key = (event.hdr.instrument_id, event.side, event.order_id);
        match event.action {
            Action::Add => {
                if let Some((price, qty)) = self.orders.insert(key, (event.price, event.qty)) {
                    self.levels.reduce(price, qty, true);
                }
                self.levels.add(event.price, event.qty);
            }
            Action::Cancel => {
                if let Some((price, qty)) = self.orders.remove(&key) {
                    self.levels.reduce(price, event.qty.min(qty), true);
                }
            }
            _ => unreachable!("benchmark emits only add and cancel"),
        }
    }
}

fn run_plain<S>(name: &str, ops: u64, prices: i64, smoke: bool)
where
    S: BuildHasher + Default,
{
    let mut book: PlainBook<S> = PlainBook::new();
    let start = Instant::now();
    for i in 0..ops {
        let price = 1_000_000 + (i as i64 % prices);
        book.apply(&mbo(i, Action::Add, price, 1));
        if i >= prices as u64 {
            book.apply(&mbo(i - prices as u64, Action::Cancel, price, 1));
        }
    }
    let elapsed = start.elapsed();
    let best = book.levels.best(Side::Bid);
    assert!(best.is_some(), "{name}: book emptied unexpectedly");
    black_box(best);
    if !smoke {
        let per = elapsed.as_nanos() as f64 / ops as f64;
        println!("{name:>12}: {ops} ops over {prices} prices, {per:.1} ns/op");
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--test");
    let ops = if smoke { 2_000 } else { 2_000_000 };
    run_engine::<SoaLevelBook, RandomState>("std/soa/pool", ops, 1_000, smoke);
    run_engine::<BTreeLevelBook, RandomState>("std/btree/pool", ops, 1_000, smoke);
    run_engine::<SoaLevelBook, FxBuildHasher>("fx/soa/pool", ops, 1_000, smoke);
    run_engine::<BTreeLevelBook, FxBuildHasher>("fx/btree/pool", ops, 1_000, smoke);
    run_plain::<FxBuildHasher>("fx/soa/plain", ops, 1_000, smoke);
    if smoke {
        println!("book_ops smoke ok");
    }
}
