//! Winsock quirks the receive path absorbs: `WSAECONNRESET` after unreachable
//! send, large queued datagram under `AsyncReady` probe. Datagram longer than
//! slab is every OS now, so it lives in `tests/udp.rs`.
#![cfg(windows)]

mod support;

use std::{
    num::NonZeroUsize,
    thread,
    time::{Duration, Instant},
};

use transport_core::{DatagramRecv, DatagramSend, FrameBatch};
use transport_socket::UdpFrame;

// payloads of every frame received until `n` arrive; any error fails test
fn receive<T: DatagramRecv<Frame = UdpFrame>>(t: &mut T, n: usize) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut batch = FrameBatch::with_capacity(NonZeroUsize::new(4).unwrap());
    let mut got = Vec::new();
    while got.len() < n {
        assert!(
            Instant::now() < deadline,
            "{} of {n} datagrams arrived",
            got.len()
        );
        match t.recv_burst(&mut batch) {
            Ok(0) => thread::sleep(Duration::from_millis(1)),
            Ok(_) => got.extend(batch.drain().map(|f| f.as_ref().to_vec())),
            Err(e) => panic!("receive loop ended: {e}"),
        }
    }
    got
}

#[test]
fn connreset_after_unreachable_send_does_not_end_receive_loop() {
    let mut rx = support::receiver(NonZeroUsize::new(8).unwrap());
    let closed = support::sender().local_addr().expect("free port");
    // port closed: ICMP port unreachable surfaces on rx's next recv
    rx.send_to(b"nobody", closed).expect("send to closed port");
    thread::sleep(Duration::from_millis(50));

    support::sender()
        .send_to(b"data", rx.local_addr().expect("local addr"))
        .expect("send");
    assert_eq!(receive(&mut rx, 1), [b"data".to_vec()]);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn large_queued_datagram_makes_async_udp_ready() {
    use transport_core::AsyncReady;
    use transport_socket::tokio::AsyncUdp;

    let mut udp = AsyncUdp::from_socket(support::receiver(NonZeroUsize::new(4).unwrap()))
        .expect("register with runtime");
    support::sender()
        .send_to(&[0x11; 1400], udp.local_addr().expect("local addr"))
        .expect("send");
    tokio::time::timeout(Duration::from_secs(5), udp.ready())
        .await
        .expect("ready within 5 s")
        .expect("ready, not WSAEMSGSIZE");
    assert_eq!(receive(&mut udp, 1), [vec![0x11; 1400]]);
}
