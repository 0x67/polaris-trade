//! `AsyncReady` reflects real socket state, not tokio's cached readiness.
//!
//! Receive calls socket2 directly and never clears that cache, so plain
//! `readable().await` would resolve at once forever after first packet. Two
//! send/drain cycles; `ready()` must stay pending on idle socket after each.
#![cfg(feature = "tokio")]

mod support;

use std::{io::Write, mem::MaybeUninit, net::TcpListener, num::NonZeroUsize, time::Duration};

use tokio::time::timeout;
use transport_core::{AsyncReady, DatagramRecv, FrameBatch, StreamRecv};
use transport_socket::{
    TcpConfig,
    tokio::{AsyncUdp, TcpStream},
};

// pending this long on idle socket counts as blocked; longer only slows test
const IDLE: Duration = Duration::from_millis(150);
const WAKE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn async_udp_ready_stays_pending_after_drain_across_two_cycles() {
    let receiver = support::receiver(NonZeroUsize::new(8).unwrap());
    let mut udp = AsyncUdp::from_socket(receiver).expect("register with runtime");
    let addr = udp.local_addr().expect("local addr");
    let sender = support::sender();
    let mut batch = FrameBatch::with_capacity(NonZeroUsize::new(4).unwrap());

    for cycle in 0..2 {
        sender.send_to(b"hi", addr).expect("send");
        timeout(WAKE, udp.ready())
            .await
            .expect("ready after send")
            .expect("ready ok");
        assert_eq!(
            udp.recv_burst(&mut batch).expect("recv"),
            1,
            "cycle {cycle}"
        );
        batch.drain().for_each(drop);
        assert_eq!(
            udp.recv_burst(&mut batch).expect("recv"),
            0,
            "cycle {cycle}: drained"
        );

        let idle = timeout(IDLE, udp.ready()).await;
        assert!(
            idle.is_err(),
            "cycle {cycle}: ready() resolved on idle socket: {idle:?}"
        );
    }
}

#[tokio::test]
async fn tcp_ready_stays_pending_after_drain_across_two_cycles() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let cfg = TcpConfig::new(listener.local_addr().expect("listener addr"));
    let mut tcp = TcpStream::connect(&cfg).await.expect("connect");
    // handshake done: accept returns queued connection at once
    let (mut peer, _) = listener.accept().expect("accept");
    let mut buf = [MaybeUninit::<u8>::uninit(); 8];

    for cycle in 0..2 {
        peer.write_all(b"hi").expect("peer write");
        timeout(WAKE, tcp.ready())
            .await
            .expect("ready after write")
            .expect("ready ok");
        assert_eq!(tcp.recv_into(&mut buf).expect("recv"), 2, "cycle {cycle}");
        assert_eq!(
            tcp.recv_into(&mut buf).expect("recv"),
            0,
            "cycle {cycle}: drained"
        );

        let idle = timeout(IDLE, tcp.ready()).await;
        assert!(
            idle.is_err(),
            "cycle {cycle}: ready() resolved on idle stream: {idle:?}"
        );
    }
}
