//! Socket receive benchmarks on sync `UdpSocket`: drain of pre-filled socket
//! (sends outside timed region) at several burst sizes, and idle spin.

use std::{
    hint::black_box,
    net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket},
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use transport_core::{DatagramRecv, FrameBatch};
use transport_socket::{UdpConfig, UdpSocket};

const BURSTS: [usize; 4] = [1, 8, 32, 64];
const PAYLOAD: [u8; 64] = [0x5a; 64];

fn receiver() -> UdpSocket {
    let mut cfg = UdpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    cfg.slab_count = NonZeroUsize::new(256).unwrap();
    UdpSocket::bind(&cfg).expect("bind receiver")
}

fn prefilled_drain(c: &mut Criterion) {
    let mut rx = receiver();
    let to = rx.local_addr().expect("local addr");
    let tx = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind sender");
    // macOS loopback delivers through input thread, FIFO: marker sent last
    // reaches `probe` only after every earlier datagram reached `rx`
    let probe = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind probe");
    let probe_addr = probe.local_addr().expect("probe addr");
    let mut group = c.benchmark_group("udp_recv_prefilled");
    for burst in BURSTS {
        group.throughput(Throughput::Elements(burst as u64));
        let mut out = FrameBatch::with_capacity(NonZeroUsize::new(burst).unwrap());
        group.bench_with_input(BenchmarkId::from_parameter(burst), &burst, |b, &burst| {
            b.iter_custom(|iters| {
                let mut timed = Duration::ZERO;
                for _ in 0..iters {
                    for _ in 0..burst {
                        tx.send_to(&PAYLOAD, to).expect("send");
                    }
                    tx.send_to(&[], probe_addr).expect("send marker");
                    probe.recv(&mut [0; 1]).expect("marker");
                    let start = Instant::now();
                    let mut got = 0;
                    while got < burst {
                        got += rx.recv_burst(&mut out).expect("recv");
                        for frame in out.drain() {
                            black_box(frame.as_ref());
                        }
                    }
                    timed += start.elapsed();
                }
                timed
            });
        });
    }
    group.finish();
}

fn idle_spin(c: &mut Criterion) {
    let mut rx = receiver();
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(32).unwrap());
    c.bench_function("udp_recv_idle", |b| {
        b.iter(|| black_box(rx.recv_burst(&mut out).expect("recv")));
    });
}

criterion_group!(benches, prefilled_drain, idle_spin);
criterion_main!(benches);
