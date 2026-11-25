//! `ReadySet` over `MioUdp` legs: data queued before registration, idle leg
//! beside active one, re-report after full drain (Windows re-arm through
//! `try_io`), wake for datagram sent while blocked; `MioTcp` read readiness.
//!
//! `MioTcp` writable after would-block is proved in `tests/tcp.rs`.
#![cfg(feature = "mio")]

mod support;

use std::{
    num::NonZeroUsize,
    thread,
    time::{Duration, Instant},
};

use transport_core::{DatagramRecv, FrameBatch};
use transport_socket::mio::{MioUdp, ReadySet, ReadyToken};

// bound on every blocking wait; a lost wakeup fails instead of hanging CI
const WAIT: Duration = Duration::from_secs(5);

fn leg() -> MioUdp {
    MioUdp::from_socket(support::receiver(NonZeroUsize::new(8).unwrap()))
}

fn ready_set() -> ReadySet {
    ReadySet::new(NonZeroUsize::new(8).unwrap()).expect("ready set")
}

// tokens reported readable by first `wait` returning any, within `WAIT`
fn wait_readable(set: &mut ReadySet) -> Vec<ReadyToken> {
    let deadline = Instant::now() + WAIT;
    let mut ready = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "no readiness within {WAIT:?}: lost wakeup");
        set.wait(Some(left), &mut ready).expect("wait");
        let tokens: Vec<_> = ready
            .iter()
            .filter(|r| r.readable)
            .map(|r| r.token)
            .collect();
        if !tokens.is_empty() {
            return tokens;
        }
    }
}

// wait for `token`, drain, repeat until `n` datagrams arrived
fn receive(set: &mut ReadySet, leg: &mut MioUdp, token: ReadyToken, n: usize) {
    let mut total = 0;
    while total < n {
        assert_eq!(wait_readable(set), [token], "{total} of {n} received");
        total += drain(leg);
    }
    assert_eq!(total, n, "datagrams received");
}

// bursts of one datagram until `Ok(0)`, as edge-triggered contract demands
fn drain(leg: &mut MioUdp) -> usize {
    let mut batch = FrameBatch::with_capacity(NonZeroUsize::MIN);
    let mut total = 0;
    loop {
        match leg.recv_burst(&mut batch).expect("recv") {
            0 => return total,
            n => total += n,
        }
        batch.drain().for_each(drop);
    }
}

#[test]
fn first_wait_reports_data_queued_before_registration() {
    let mut leg = leg();
    let sender = support::sender();
    sender
        .send_to(b"early", leg.local_addr().expect("local addr"))
        .expect("send");
    // loopback enqueues within send_to; pause covers slower stacks
    thread::sleep(Duration::from_millis(20));

    let mut set = ready_set();
    set.register(&mut leg, ReadyToken(7)).expect("register");
    receive(&mut set, &mut leg, ReadyToken(7), 1);
}

#[test]
fn idle_leg_does_not_hide_active_leg() {
    let (mut a, mut b) = (leg(), leg());
    let b_addr = b.local_addr().expect("local addr");
    let mut set = ready_set();
    set.register(&mut a, ReadyToken(0)).expect("register a");
    set.register(&mut b, ReadyToken(1)).expect("register b");
    // mio tracks OS socket: registration survives move into consumer
    let mut legs = Vec::from([a, b]);

    support::sender().send_to(b"b", b_addr).expect("send");
    receive(&mut set, &mut legs[1], ReadyToken(1), 1);
    assert_eq!(drain(&mut legs[0]), 0);
}

#[test]
fn drained_leg_is_reported_again_for_new_data() {
    let mut leg = leg();
    let addr = leg.local_addr().expect("local addr");
    let sender = support::sender();
    let mut set = ready_set();
    set.register(&mut leg, ReadyToken(3)).expect("register");

    for round in 1..=3 {
        // more datagrams than one burst takes: partial drains before `Ok(0)`
        for _ in 0..round {
            sender.send_to(b"x", addr).expect("send");
        }
        receive(&mut set, &mut leg, ReadyToken(3), round);
    }
}

#[test]
fn wait_wakes_for_datagram_sent_while_blocked() {
    let mut leg = leg();
    let addr = leg.local_addr().expect("local addr");
    let mut set = ready_set();
    set.register(&mut leg, ReadyToken(5)).expect("register");
    assert_eq!(drain(&mut leg), 0, "starts idle");

    let late = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        support::sender().send_to(b"late", addr).expect("send");
    });
    receive(&mut set, &mut leg, ReadyToken(5), 1);
    late.join().expect("sender thread");
}

#[test]
fn tcp_stream_reported_readable_for_peer_bytes() {
    use std::{io::Write, mem::MaybeUninit, net::TcpListener};

    use transport_core::StreamRecv;
    use transport_socket::{TcpConfig, mio::MioTcp};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let cfg = TcpConfig::new(listener.local_addr().expect("listener addr"));
    let mut tcp = MioTcp::connect(&cfg).expect("connect");
    let (mut peer, _) = listener.accept().expect("accept");
    let mut set = ready_set();
    set.register(&mut tcp, ReadyToken(9)).expect("register");

    peer.write_all(b"hi").expect("peer write");
    // fresh stream is writable too; only readable reports count here
    assert_eq!(wait_readable(&mut set), [ReadyToken(9)]);
    let mut buf = [MaybeUninit::<u8>::uninit(); 8];
    assert_eq!(tcp.recv_into(&mut buf).expect("recv"), 2);
}
