//! Session enforcement over loopback UDP leg driven by sync `poll`: first
//! datagram locks session id, later datagram with other id is rejected.

pub mod support;

use std::time::{Duration, Instant};

use client_moldudp::{MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;

#[test]
fn later_session_mismatch_rejected_after_first_locks_it() {
    let leg = support::udp_leg();
    let addr = leg.local_addr().expect("leg addr");
    let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])
        .expect("receiver");
    let tx = support::sender();
    let session_a = *b"SESSION_A0";
    let session_b = *b"SESSION_B0";

    tx.send_to(&support::mold_packet(&session_a, 1, b"first"), addr)
        .expect("send");
    let got = support::drain(&mut rx, 1);
    assert_eq!(got[0].payload, b"first");

    tx.send_to(&support::mold_packet(&session_b, 2, b"second"), addr)
        .expect("send");
    let deadline = Instant::now() + Duration::from_secs(5);
    let err = loop {
        match rx.poll() {
            Ok(None) => assert!(Instant::now() < deadline, "second datagram never arrived"),
            Ok(Some(MoldUdpOutcome::Frame(f))) => panic!("mismatched session delivered {f:?}"),
            Ok(Some(other)) => panic!("unexpected outcome {other:?}"),
            Err(e) => break e,
        }
    };
    match err {
        MoldUdpError::SessionMismatch { expected, got } => {
            assert_eq!(expected, session_a);
            assert_eq!(got, session_b);
        }
        other => panic!("expected SessionMismatch, got {other}"),
    }
}
