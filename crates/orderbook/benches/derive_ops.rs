//! Derive-view microbench: cost of building mbp-10 depth off the applied book.
//! Depth is read from `BookEngine` through `BookDepth`, so the loop times the
//! book apply plus the derive read together. Full run:
//! `cargo bench --bench derive_ops`. Smoke: `cargo bench --bench derive_ops -- --test`.

use std::{hint::black_box, time::Instant};

use md_events::{Action, BreakRefKind, Mbo, RecordFlags, RecordHeader, Side, TradingState, rtype};
use orderbook_engine::{
    BookEngine, DeriveEngine, PurposeSet,
    policy::{BoardId, BreakScope, Gate, VenuePolicy},
};

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

fn mbo(order_id: u64, action: Action, price: i64) -> Mbo {
    Mbo {
        hdr: RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: 1,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: order_id,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id,
        price,
        qty: 1,
        action,
        side: Side::Bid,
        participant: [0; 7],
        _pad: [0u8; 7],
    }
}

fn run(name: &str, ops: u64, prices: i64, smoke: bool) {
    let mut book: BookEngine<NoopPolicy> = BookEngine::new(NoopPolicy);
    let derive = DeriveEngine::new();
    let start = Instant::now();
    for i in 0..ops {
        let price = 1_000_000 + (i as i64 % prices);
        let add = mbo(i, Action::Add, price);
        book.apply_mbo(&add);
        black_box(derive.apply_mbo(&add, &book, PurposeSet::BOOK));
        if i >= prices as u64 {
            let cancel = mbo(i - prices as u64, Action::Cancel, price);
            book.apply_mbo(&cancel);
            black_box(derive.apply_mbo(&cancel, &book, PurposeSet::BOOK));
        }
    }
    let elapsed = start.elapsed();
    black_box(book.best(1, Side::Bid));
    if !smoke {
        let per = elapsed.as_nanos() as f64 / ops as f64;
        println!("{name:>14}: {ops} ops over {prices} prices, {per:.1} ns/op");
    }
}

fn run_replace_batch(name: &str, ops: u64, prices: i64, smoke: bool) {
    let mut book: BookEngine<NoopPolicy> = BookEngine::new(NoopPolicy);
    let derive = DeriveEngine::new();
    book.apply_mbo(&mbo(0, Action::Add, 1_000_000));
    let start = Instant::now();
    for i in 0..ops {
        let old_id = i;
        let new_id = i + 1;
        let old_price = 1_000_000 + (i as i64 % prices);
        let new_price = 1_000_000 + (new_id as i64 % prices);
        let mut cancel = mbo(old_id, Action::Cancel, old_price);
        let add = mbo(new_id, Action::Add, new_price);
        cancel.hdr = add.hdr;
        book.apply_mbo(&cancel);
        book.apply_mbo(&add);
        black_box(derive.apply_mbo_batch(&[cancel, add], &book, PurposeSet::BOOK));
    }
    let elapsed = start.elapsed();
    black_box(book.best(1, Side::Bid));
    if !smoke {
        let per = elapsed.as_nanos() as f64 / ops as f64;
        println!("{name:>14}: {ops} batches over {prices} prices, {per:.1} ns/batch");
    }
}

fn main() {
    let smoke = std::env::args().any(|arg| arg == "--test");
    let ops = if smoke { 2_000 } else { 2_000_000 };
    run("apply+derive", ops, 1_000, smoke);
    run_replace_batch("replace-batch", ops, 1_000, smoke);
    if smoke {
        println!("derive_ops smoke ok");
    }
}
