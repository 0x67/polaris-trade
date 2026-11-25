//! Steady-state `UdpSocket::recv_burst` allocates nothing: pool and batch warm,
//! loopback datagrams queued outside count, every frame dropped inside it.

mod support;

use std::{
    hint::black_box,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use transport_core::{DatagramRecv, FrameBatch};

const BURST: usize = 16;
const WARMUP: usize = 4;
const CYCLES: usize = 64;

#[test]
fn steady_state_recv_burst_allocates_nothing() {
    let mut rx = support::receiver(NonZeroUsize::new(4 * BURST).unwrap());
    let to = rx.local_addr().expect("local addr");
    let tx = support::sender();
    let payload = [0x5a; 256];
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(BURST).unwrap());

    let mut cycle = || {
        allocation_counter::opt_out(|| {
            for _ in 0..BURST {
                tx.send_to(&payload, to).expect("send");
            }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = 0;
        while got < BURST {
            assert!(
                Instant::now() < deadline,
                "{got} of {BURST} datagrams arrived"
            );
            got += rx.recv_burst(&mut out).expect("recv");
            for frame in out.drain() {
                black_box(frame.as_ref());
            }
        }
    };
    for _ in 0..WARMUP {
        cycle();
    }
    let info = allocation_counter::measure(|| {
        for _ in 0..CYCLES {
            cycle();
        }
    });
    assert_eq!(
        info.count_total, 0,
        "{} allocations over {CYCLES} steady-state bursts",
        info.count_total
    );
}
