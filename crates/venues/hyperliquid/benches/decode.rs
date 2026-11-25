//! Decode cost of the three channels, one recorded frame of each kind per
//! iteration through `HyperliquidAdapter::feed`, with the output drained.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use md_pipeline::{VenueAdapter, VenueId};
use venue_hyperliquid::{Config, HyperliquidAdapter, STREAM};

const FRAMES: &str = include_str!("../tests/fixtures/session/frames.jsonl");

fn first_frame(channel: &str) -> &'static [u8] {
    let marker = format!("{{\"channel\":\"{channel}\"");
    FRAMES
        .lines()
        .find(|l| l.starts_with(&marker))
        .expect("fixture holds every channel")
        .as_bytes()
}

fn bench_channel(c: &mut Criterion, channel: &str) {
    let frame = first_frame(channel);
    let mut adapter = HyperliquidAdapter::new(Config { venue: VenueId(7) });
    let mut seq = 0;
    c.bench_function(&format!("feed_{channel}"), |b| {
        b.iter(|| {
            seq += 1;
            adapter.feed(STREAM, black_box(frame), 1, seq);
            adapter.drain().count()
        });
    });
}

fn decode(c: &mut Criterion) {
    for channel in ["trades", "l2Book", "activeAssetCtx"] {
        bench_channel(c, channel);
    }
}

criterion_group!(benches, decode);
criterion_main!(benches);
