//! Bypass shell over mock driver: pre-filled burst drain and idle spin.

use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use transport_core::{
    DatagramRecv, FrameBatch,
    bypass::{BypassTransport, L4, MockDriver},
    pool::IndexPool,
};

const SLOTS: u32 = 64;
const STRIDE: u32 = 2048;
const BURST: usize = 32;
const PAYLOAD: [u8; 64] = [0x5a; 64];

fn shell() -> BypassTransport<MockDriver<L4>> {
    let pool = IndexPool::new(
        NonZeroU32::new(SLOTS).unwrap(),
        NonZeroU32::new(STRIDE).unwrap(),
    )
    .unwrap();
    BypassTransport::new(MockDriver::new(pool))
}

fn drain_burst(c: &mut Criterion) {
    let mut t = shell();
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(BURST).unwrap());
    let mut group = c.benchmark_group("bypass");
    group.throughput(Throughput::Elements(BURST as u64));
    // inject untimed: mock copy stands in for NIC DMA, and mock recycles freed
    // slots there. Timed: shell, receive, frame drop; one clock pair per burst.
    group.bench_function("drain_burst", |b| {
        b.iter_custom(|iters| {
            let mut timed = Duration::ZERO;
            for _ in 0..iters {
                for _ in 0..BURST {
                    t.driver_mut().inject(&PAYLOAD);
                }
                let start = Instant::now();
                black_box(t.recv_burst(&mut out).unwrap());
                for frame in out.drain() {
                    black_box(frame.as_ref());
                }
                timed += start.elapsed();
            }
            timed
        });
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("idle_spin", |b| {
        b.iter(|| black_box(t.recv_burst(&mut out).unwrap()));
    });
    group.finish();
}

criterion_group!(benches, drain_burst);
criterion_main!(benches);
