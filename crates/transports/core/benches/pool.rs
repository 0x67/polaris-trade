//! Pool micro-benchmarks: slab acquire and drop, index frame burst with drop and drain.

use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroUsize},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use transport_core::pool::{IndexFrame, IndexPool, VecPool, backend};

const SLOTS: u32 = 64;
const BURST: u32 = 32;
const SIZE: u32 = 2048;

fn vec_acquire_drop(c: &mut Criterion) {
    let pool = VecPool::new(
        NonZeroUsize::new(SLOTS as usize).unwrap(),
        NonZeroUsize::new(SIZE as usize).unwrap(),
    )
    .unwrap();
    c.bench_function("vec_pool/acquire_drop", |b| {
        b.iter(|| black_box(backend::acquire(&pool)));
    });
}

fn index_frame_drop_drain(c: &mut Criterion) {
    let pool = IndexPool::new(
        NonZeroU32::new(SLOTS).unwrap(),
        NonZeroU32::new(SIZE).unwrap(),
    )
    .unwrap();
    let mut frames: Vec<IndexFrame> = Vec::with_capacity(BURST as usize);
    let mut freed = Vec::with_capacity(SLOTS as usize);
    let mut group = c.benchmark_group("index_pool");
    group.throughput(Throughput::Elements(u64::from(BURST)));
    group.bench_function("frame_drop_drain_burst", |b| {
        b.iter(|| {
            for slot in 0..BURST {
                // SAFETY: slot < SLOTS; every frame dropped and drained below
                // before slot reused; no kernel writes this region.
                frames.push(unsafe { pool.frame(slot, 0, 64) });
            }
            for frame in &frames {
                black_box(frame.as_ref());
            }
            frames.clear();
            pool.drain_freed(&mut freed);
            black_box(&freed);
            freed.clear();
        });
    });
    group.finish();
}

criterion_group!(benches, vec_acquire_drop, index_frame_drop_drain);
criterion_main!(benches);
