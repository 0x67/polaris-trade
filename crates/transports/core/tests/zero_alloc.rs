//! Steady-state receive allocates nothing on either mock path: L4 shell, and
//! L2 shell under `UdpDecap`.

mod support;

use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{
    DatagramRecv, FrameBatch,
    bypass::{BypassTransport, L2, L4, MockDriver},
    decap::UdpDecap,
    pool::IndexPool,
};

const BURST: usize = 16;
const BURST_NZ: NonZeroUsize = NonZeroUsize::new(BURST).unwrap();
const SLOTS: NonZeroU32 = NonZeroU32::new(32).unwrap();
const PORT: u16 = 30_001;
const WARMUP: usize = 4;
const CYCLES: usize = 64;

fn pool() -> IndexPool {
    IndexPool::new(SLOTS, NonZeroU32::new(2048).unwrap()).unwrap()
}

// each cycle: inject `BURST`, reap them in one burst, drop every frame
fn assert_bursts_allocate_nothing<T: DatagramRecv>(
    t: &mut T,
    mut inject: impl FnMut(&mut T),
    what: &str,
) {
    let mut out = FrameBatch::with_capacity(BURST_NZ);
    let mut cycle = |t: &mut T| {
        for _ in 0..BURST {
            inject(t);
        }
        assert_eq!(
            t.recv_burst(&mut out).unwrap(),
            BURST,
            "{what}: one burst reaps all"
        );
        for frame in out.drain() {
            black_box(frame.as_ref());
        }
    };
    for _ in 0..WARMUP {
        cycle(t);
    }
    let info = allocation_counter::measure(|| {
        for _ in 0..CYCLES {
            cycle(t);
        }
    });
    assert_eq!(
        info.count_total, 0,
        "{what}: {} allocations over {CYCLES} steady-state bursts",
        info.count_total
    );
}

#[test]
fn l4_shell_bursts_allocate_nothing() {
    let payload = [0x5a; 256];
    let mut t = BypassTransport::new(MockDriver::<L4>::new(pool()));
    assert_bursts_allocate_nothing(&mut t, |t| t.driver_mut().inject(&payload), "l4 shell");
}

#[test]
fn l2_shell_under_decap_bursts_allocate_nothing() {
    let frame = support::udp_frame(PORT, &[0x5a; 256]);
    let shell = BypassTransport::new(MockDriver::<L2>::new(pool()));
    let mut t = UdpDecap::new(shell, PORT, None, BURST_NZ);
    assert_bursts_allocate_nothing(
        &mut t,
        |t| t.inner_mut().driver_mut().inject(&frame),
        "l2 shell under decap",
    );
}
