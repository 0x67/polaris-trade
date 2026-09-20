//! Re-request send failure never blocks receive: legs keep delivering while
//! requester socket is broken, and failed attempt backs off one rate-limit
//! interval instead of retrying on every poll.

pub mod support;

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    rc::Rc,
    time::{Duration, Instant},
};

use client_moldudp::{MoldUdpError, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_core::{
    DatagramRecv, DatagramSend, FrameBatch, PoolStats, Transport, TransportError,
};

const SESSION: [u8; 10] = *b"SESSIONRF1";

/// Requester whose every send fails as unreachable server's does; counts attempts.
struct BrokenRequester {
    attempts: Rc<Cell<usize>>,
}

impl Transport for BrokenRequester {
    fn name(&self) -> &'static str {
        "broken-requester"
    }
}

impl DatagramRecv for BrokenRequester {
    type Frame = Vec<u8>;

    fn recv_burst(&mut self, _: &mut FrameBatch<Vec<u8>>) -> Result<usize, TransportError> {
        Ok(0)
    }

    fn pool_stats(&self) -> PoolStats {
        PoolStats {
            capacity: 1,
            in_use: 0,
        }
    }
}

impl DatagramSend for BrokenRequester {
    fn send_to(&mut self, _: &[u8], _: SocketAddr) -> Result<usize, TransportError> {
        self.attempts.set(self.attempts.get() + 1);
        Err(TransportError::Io {
            stage: "send_to",
            error: io::ErrorKind::ConnectionRefused.into(),
        })
    }
}

#[test]
fn failing_requester_never_blocks_leg_delivery() {
    let leg = support::udp_leg();
    let to = leg.local_addr().expect("leg addr");
    let tx = support::sender();
    let send = |seq: u64| {
        let packet = support::mold_packet(&SESSION, seq, format!("m{seq}").as_bytes());
        tx.send_to(&packet, to).expect("send");
    };
    let attempts = Rc::new(Cell::new(0));
    // 1 s rate-limit interval: whole test runs inside one
    let cfg = MoldUdpReceiverConfig {
        max_rerequests_per_gap_per_sec: 1,
        ..MoldUdpReceiverConfig::default()
    };
    let requester = BrokenRequester {
        attempts: Rc::clone(&attempts),
    };
    let mut rx = MoldUdpReceiver::from_legs(&cfg, smallvec![leg])
        .expect("receiver")
        .with_requester(requester, "127.0.0.1:9".parse().expect("addr"));

    send(1);
    send(3);
    assert_eq!(support::sequences(&support::drain(&mut rx, 1)), [1]);
    let deadline = Instant::now() + Duration::from_secs(5);
    while rx.stats().pending_gaps.is_empty() {
        assert!(Instant::now() < deadline, "gap at 2 never recorded");
        match rx.poll() {
            Ok(None) | Err(MoldUdpError::GapDetected) => {}
            other => panic!("expected gap, got {other:?}"),
        }
    }

    // gap pending, send fails: every poll still reads legs and reports idle
    for _ in 0..5 {
        match rx.poll() {
            Ok(None) => {}
            other => panic!("send failure must not surface, got {other:?}"),
        }
    }
    assert_eq!(
        attempts.get(),
        1,
        "failed send backs off, not retried per poll"
    );

    send(2);
    send(4);
    assert_eq!(support::sequences(&support::drain(&mut rx, 3)), [2, 3, 4]);
    assert_eq!(attempts.get(), 1);
}
