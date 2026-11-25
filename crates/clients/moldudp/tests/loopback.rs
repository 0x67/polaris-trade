//! Async receive end to end: real UDP sender feeds N packets to `AsyncUdp` leg,
//! `recv().await` delivers them in order.

pub mod support;

use client_moldudp::{MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_socket::tokio::AsyncUdp;

const N: u64 = 5;

#[tokio::test]
async fn recv_delivers_n_frames_in_order() {
    let leg = AsyncUdp::from_socket(support::udp_leg()).expect("register leg");
    let addr = leg.local_addr().expect("leg addr");
    let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])
        .expect("receiver");

    let session = *b"SESSIONID1";
    let tx = support::sender();
    for seq in 1..=N {
        let packet = support::mold_packet(&session, seq, format!("msg-{seq}").as_bytes());
        tx.send_to(&packet, addr).expect("send");
    }

    for seq in 1..=N {
        match rx.recv().await.expect("recv") {
            MoldUdpOutcome::Frame(frame) => {
                assert_eq!(frame.sequence(), seq);
                assert_eq!(frame.as_ref(), format!("msg-{seq}").as_bytes());
            }
            other => panic!("expected borrowed frame, got {other:?}"),
        }
    }
}
