//! Decode cost of the three frame kinds through `BinanceAdapter::feed`, with
//! the output drained. The diff is the first one after the recorded snapshot,
//! fed to a freshly primed adapter so each run applies it to a live book.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use md_pipeline::{VenueAdapter, VenueId};
use venue_binance::{BinanceAdapter, Config, MARKET_STREAM, SNAPSHOT_STREAM, Symbol};

const FRAMES: &str = include_str!("../tests/fixtures/session/frames.jsonl");

fn frame(prefix: &str) -> &'static [u8] {
    FRAMES
        .lines()
        .find(|l| l.starts_with(prefix))
        .expect("fixture holds every frame kind")
        .as_bytes()
}

fn adapter() -> BinanceAdapter {
    BinanceAdapter::new(Config {
        venue: VenueId(7),
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
    })
}

fn decode(c: &mut Criterion) {
    let snapshot = frame(r#"{"lastUpdateId""#);
    let trade = frame(r#"{"e":"trade""#);
    let diff = frame(r#"{"e":"depthUpdate","E":1795537595914"#);

    let mut a = adapter();
    let mut seq = 0;
    c.bench_function("feed_trade", |b| {
        b.iter(|| {
            seq += 1;
            a.feed(MARKET_STREAM, black_box(trade), 1, seq);
            a.drain().count()
        });
    });

    c.bench_function("feed_snapshot", |b| {
        b.iter_batched(
            adapter,
            |mut a| {
                a.feed(SNAPSHOT_STREAM, black_box(snapshot), 1, 1);
                a.drain().count()
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("feed_depth_update", |b| {
        b.iter_batched(
            || {
                let mut a = adapter();
                a.feed(SNAPSHOT_STREAM, snapshot, 1, 1);
                a.drain().count();
                a
            },
            |mut a| {
                a.feed(MARKET_STREAM, black_box(diff), 2, 1);
                a.drain().count()
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, decode);
criterion_main!(benches);
