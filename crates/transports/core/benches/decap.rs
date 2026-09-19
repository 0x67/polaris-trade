//! Decap adapter: pre-filled L2 burst through `UdpDecap` over bypass shell and mock.

#[path = "../tests/support/mod.rs"]
mod support;

use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use transport_core::{
    DatagramRecv, FrameBatch,
    bypass::{BypassTransport, L2, MockDriver},
    decap::UdpDecap,
    pool::IndexPool,
};

const SLOTS: u32 = 64;
const STRIDE: u32 = 2048;
const BURST: usize = 32;
const PORT: u16 = 30_001;

fn decap_burst(c: &mut Criterion) {
    let pool = IndexPool::new(
        NonZeroU32::new(SLOTS).unwrap(),
        NonZeroU32::new(STRIDE).unwrap(),
    )
    .unwrap();
    let burst = NonZeroUsize::new(BURST).unwrap();
    let mut t = UdpDecap::new(
        BypassTransport::new(MockDriver::<L2>::new(pool)),
        PORT,
        None,
        burst,
    );
    let frame = support::udp_frame(PORT, &[0x5a; 64]);
    let mut out = FrameBatch::with_capacity(burst);
    let mut group = c.benchmark_group("decap");
    group.throughput(Throughput::Elements(BURST as u64));
    // inject untimed (stands in for NIC DMA); timed: shell reap, parse, deliver, frame drop
    group.bench_function("udp_burst", |b| {
        b.iter_custom(|iters| {
            let mut timed = Duration::ZERO;
            for _ in 0..iters {
                for _ in 0..BURST {
                    t.inner_mut().driver_mut().inject(&frame);
                }
                let start = Instant::now();
                black_box(t.recv_burst(&mut out).unwrap());
                for datagram in out.drain() {
                    black_box(datagram.as_ref());
                }
                timed += start.elapsed();
            }
            timed
        });
    });
    group.finish();
}

criterion_group!(benches, decap_burst);
criterion_main!(benches);
