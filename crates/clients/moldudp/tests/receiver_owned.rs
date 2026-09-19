//! `recv_owned`: outcome crosses real thread boundary (`Send`) and its bytes
//! match what real UDP sender put on wire.

pub mod support;

use client_moldudp::{MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_socket::tokio::AsyncUdp;

#[tokio::test]
async fn recv_owned_crosses_thread_boundary() {
    let leg = AsyncUdp::from_socket(support::udp_leg()).expect("register leg");
    let addr = leg.local_addr().expect("leg addr");
    let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])
        .expect("receiver");

    let payload = b"owned-payload";
    support::sender()
        .send_to(&support::mold_packet(b"SESSIONID1", 1, payload), addr)
        .expect("send");

    let owned = match rx.recv_owned().await.expect("recv_owned") {
        MoldUdpOutcome::Owned(owned) => owned,
        other => panic!("expected owned frame, got {other:?}"),
    };
    assert_eq!(owned.sequence(), 1);

    let bytes = std::thread::spawn(move || owned.as_ref().to_vec())
        .join()
        .expect("owned frame crossed thread boundary");
    assert_eq!(bytes, payload);
}
