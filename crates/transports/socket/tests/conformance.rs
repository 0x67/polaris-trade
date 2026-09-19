//! Conformance suite on every socket type: datagram contract on `UdpSocket`
//! and `AsyncUdp` (exhaustion signalled as `PoolExhausted`), sync and async
//! stream contract on tokio `TcpStream`.

mod support;

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{
    DatagramRecv,
    testing::conformance::{DatagramHarness, ExhaustionSignal, run_datagram},
};
use transport_socket::UdpSocket;

// fresh loopback receiver per case, wrapped by `wrap`; one std sender injects
fn datagram_contract<T: DatagramRecv>(
    mut wrap: impl FnMut(UdpSocket) -> T,
    addr: impl Fn(&T) -> SocketAddr,
) {
    let sender = support::sender();
    run_datagram(DatagramHarness {
        build: |slabs: NonZeroU32| {
            let slabs = NonZeroUsize::try_from(slabs).expect("slab count fits usize");
            wrap(support::receiver(slabs))
        },
        inject: |t: &mut T, bytes: &[u8]| {
            sender.send_to(bytes, addr(t)).expect("loopback send");
        },
        // sockets report exhaustion as error, count no drops
        drops: |_: &T| 0,
        exhaustion: ExhaustionSignal::PoolExhausted,
    });
}

#[test]
fn udp_socket_meets_datagram_contract() {
    datagram_contract(|s| s, |t| t.local_addr().expect("local addr"));
}

#[cfg(feature = "tokio")]
#[test]
fn async_udp_meets_datagram_contract() {
    use transport_socket::tokio::AsyncUdp;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .expect("tokio runtime");
    let _entered = rt.enter();
    datagram_contract(
        |s| AsyncUdp::from_socket(s).expect("register with runtime"),
        |t| t.local_addr().expect("local addr"),
    );
}

// connected transport plus accepted std peer
#[cfg(feature = "tokio")]
fn tcp_pair<T>(
    connect: impl FnOnce(&transport_socket::TcpConfig) -> T,
) -> (T, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let cfg = transport_socket::TcpConfig::new(listener.local_addr().expect("listener addr"));
    let t = connect(&cfg);
    let (peer, _) = listener.accept().expect("accept");
    (t, peer)
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_tcp_meets_stream_contract() {
    use transport_core::testing::conformance::run_stream;
    use transport_socket::tokio::TcpStream;

    // idle workers drive reactor while this thread runs sync cases, so
    // `try_send` sees writable readiness return after would-block
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    run_stream(|| tcp_pair(|cfg| rt.block_on(TcpStream::connect(cfg)).expect("connect")));
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_tcp_meets_async_stream_contract() {
    use std::time::Duration;

    use transport_core::testing::conformance::run_stream_async;
    use transport_socket::{TcpConfig, tokio::TcpStream};

    let pair = || async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let cfg = TcpConfig::new(listener.local_addr().expect("listener addr"));
        let t = TcpStream::connect(&cfg).await.expect("connect");
        // handshake done: accept returns queued connection at once
        let (peer, _) = listener.accept().expect("accept");
        (t, peer)
    };
    tokio::time::timeout(Duration::from_mins(1), run_stream_async(pair))
        .await
        .expect("async stream contract within 60 s");
}
