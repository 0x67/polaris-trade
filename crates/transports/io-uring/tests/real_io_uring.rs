//! `io_uring` against a live kernel, one test per forced receive path
//! (conformance, exhaustion counter, truncation, idle spin without syscall,
//! drop idle and under traffic), plus multicast join and bind without
//! `io_uring` access.
//!
//! Every test but last needs `io_uring`, which default Docker seccomp blocks
//! (privileged container or host); last needs it blocked. Run with
//! `cargo nextest run -p transport_io_uring --run-ignored ignored-only`.
#![cfg(target_os = "linux")]

use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use transport_core::{
    DatagramRecv, FrameBatch, Multicast, MulticastInterface, TransportError,
    pool::IndexFrame,
    testing::conformance::{DatagramHarness, ExhaustionSignal, run_datagram},
};
use transport_io_uring::{IoUringConfig, IoUringUdp, RecvPath};

const DEPTH: NonZeroU32 = NonZeroU32::new(8).unwrap();
const DEADLINE: Duration = Duration::from_secs(5);

fn loopback(slots: u32) -> IoUringConfig {
    let slots = NonZeroU32::new(slots).expect("non-zero slots");
    let mut cfg = IoUringConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    cfg.slots = slots;
    cfg.depth = slots.min(DEPTH);
    cfg
}

fn bind(path: RecvPath, slots: u32) -> IoUringUdp {
    let mut cfg = loopback(slots);
    cfg.path = Some(path);
    let t = IoUringUdp::bind(&cfg).expect("bind: needs io_uring access");
    assert_eq!(t.path(), path, "forced path not taken");
    t
}

fn sender() -> UdpSocket {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind sender")
}

// exactly `n` frames, each burst given room for the rest only
fn recv_n(t: &mut IoUringUdp, n: usize) -> Vec<IndexFrame> {
    let deadline = Instant::now() + DEADLINE;
    let mut got = Vec::with_capacity(n);
    while let Some(room) = NonZeroUsize::new(n - got.len()) {
        assert!(
            Instant::now() < deadline,
            "{} of {n} datagrams within {DEADLINE:?}",
            got.len()
        );
        let mut out = FrameBatch::with_capacity(room);
        t.recv_burst(&mut out).expect("recv_burst");
        got.extend(out.drain());
    }
    got
}

fn check_path(path: RecvPath) {
    conformance(path);
    exhaustion_counts_no_buffer(path);
    truncated_datagram_frees_its_slot(path);
    idle_spin_makes_no_syscall(path);
    drop_confirms_cancellation(path);
}

// datagram stays queued in socket while every slot is held
fn conformance(path: RecvPath) {
    let tx = sender();
    run_datagram(DatagramHarness {
        build: |slots: NonZeroU32| bind(path, slots.get()),
        inject: |t: &mut IoUringUdp, bytes: &[u8]| {
            tx.send_to(bytes, t.local_addr().expect("local addr"))
                .expect("loopback send");
        },
        drops: |t: &IoUringUdp| t.stats().no_buffer,
        exhaustion: ExhaustionSignal::PoolExhausted,
    });
}

fn exhaustion_counts_no_buffer(path: RecvPath) {
    let mut t = bind(path, 2);
    let (tx, to) = (sender(), t.local_addr().expect("local addr"));
    for _ in 0..3 {
        tx.send_to(&[7; 32], to).expect("send");
    }
    let held = recv_n(&mut t, 2);
    let deadline = Instant::now() + DEADLINE;
    let mut out = FrameBatch::with_capacity(NonZeroUsize::MIN);
    loop {
        match t.recv_burst(&mut out) {
            Err(TransportError::PoolExhausted { .. }) => break,
            Ok(0) => assert!(Instant::now() < deadline, "{path:?}: no PoolExhausted"),
            other => panic!("{path:?}: {other:?} while every slot held"),
        }
    }
    assert!(
        t.stats().no_buffer > 0,
        "{path:?}: exhaustion not counted: {:?}",
        t.stats()
    );
    drop(held);
}

// one slot: next datagram lands only if cut one's slot went straight back
fn truncated_datagram_frees_its_slot(path: RecvPath) {
    let mut cfg = loopback(1);
    cfg.path = Some(path);
    cfg.slot_size = NonZeroU32::new(64).unwrap();
    let mut t = IoUringUdp::bind(&cfg).expect("bind: needs io_uring access");
    let (tx, to) = (sender(), t.local_addr().expect("local addr"));
    tx.send_to(&[9; 65], to).expect("send oversized");
    tx.send_to(&[5; 64], to).expect("send fitting");
    assert_eq!(recv_n(&mut t, 1)[0].as_ref(), [5; 64], "{path:?}");
    assert_eq!(t.stats().truncated, 1, "{path:?}: {:?}", t.stats());
}

fn idle_spin_makes_no_syscall(path: RecvPath) {
    let mut t = bind(path, 16);
    let (tx, to) = (sender(), t.local_addr().expect("local addr"));
    for _ in 0..4 {
        tx.send_to(&[1; 64], to).expect("send");
    }
    drop(recv_n(&mut t, 4));
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(8).unwrap());
    // recycle and re-arm after traffic submit once, then nothing is pending
    for _ in 0..3 {
        assert_eq!(t.recv_burst(&mut out).expect("recv_burst"), 0);
    }
    let before = t.stats().syscalls;
    for _ in 0..10_000 {
        assert_eq!(t.recv_burst(&mut out).expect("recv_burst"), 0);
    }
    assert_eq!(
        t.stats().syscalls,
        before,
        "{path:?}: idle recv_burst entered kernel"
    );
}

// drop waits for cancellation, never for its timeout (timeout leaks region)
fn assert_drop_confirms_cancellation(t: IoUringUdp, path: RecvPath, case: &str) {
    let started = Instant::now();
    drop(t);
    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(500),
        "{path:?} {case}: drop took {took:?}, cancellation not confirmed"
    );
}

fn drop_confirms_cancellation(path: RecvPath) {
    // every recv armed, none completing
    assert_drop_confirms_cancellation(bind(path, 64), path, "idle");

    let mut t = bind(path, 64);
    let to = t.local_addr().expect("local addr");
    let stop = Arc::new(AtomicBool::new(false));
    let blaster = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let tx = sender();
            while !stop.load(Ordering::Relaxed) {
                // full socket buffer drops on loopback; only pressure matters
                let _ = tx.send_to(&[3; 256], to);
            }
        })
    };
    drop(recv_n(&mut t, 16));
    assert_drop_confirms_cancellation(t, path, "under traffic");
    // freed region may now back these; any late kernel write would show
    let canaries: Vec<Vec<u8>> = (0..32).map(|_| vec![0xA5; 64 * 1024]).collect();
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    blaster.join().expect("sender thread");
    assert!(
        canaries.iter().flatten().all(|&b| b == 0xA5),
        "{path:?}: memory written after drop"
    );
}

#[test]
#[ignore = "needs io_uring: privileged container or host"]
fn legacy_path() {
    check_path(RecvPath::Legacy);
}

#[test]
#[ignore = "needs io_uring: privileged container or host"]
fn buf_ring_path() {
    check_path(RecvPath::BufRing);
}

#[test]
#[ignore = "needs io_uring: privileged container or host"]
fn multishot_path() {
    check_path(RecvPath::Multishot);
}

#[test]
#[ignore = "needs io_uring and a multicast route: privileged container or host"]
fn multicast_join_receives_group_datagram() {
    let group = Ipv4Addr::new(239, 255, 42, 1);
    let cfg = IoUringConfig::new(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)));
    let mut t = IoUringUdp::bind(&cfg).expect("bind: needs io_uring access");
    t.join_multicast(group.into(), MulticastInterface::default())
        .expect("join group");
    let port = t.local_addr().expect("local addr").port();
    // default multicast loop hands own group traffic back to local members
    UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
        .expect("bind sender")
        .send_to(b"group datagram", (group, port))
        .expect("send to group");
    assert_eq!(recv_n(&mut t, 1)[0].as_ref(), b"group datagram");
}

#[test]
#[ignore = "needs io_uring blocked: default Docker seccomp, unprivileged"]
fn bind_without_io_uring_access_is_unavailable() {
    let got = IoUringUdp::bind(&loopback(16));
    assert!(
        matches!(
            got,
            Err(TransportError::Unavailable {
                backend: "io-uring",
                error: Some(_),
                ..
            })
        ),
        "{got:?}"
    );
}
