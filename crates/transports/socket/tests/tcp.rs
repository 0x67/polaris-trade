//! TCP cases beyond conformance suite (`tests/conformance.rs` covers empty
//! read, ordered bytes, `PeerClosed`, resumed `try_send`, 8 MiB `send_all`):
//! config rejected before connecting, connect refusal and timeout mapping, and
//! partial write under bounded socket buffers resumed on `ReadySet` writable
//! readiness.

#[cfg(any(feature = "mio", feature = "tokio"))]
use std::net::{SocketAddr, TcpListener};

#[cfg(any(feature = "mio", feature = "tokio"))]
use transport_core::TransportError;
#[cfg(any(feature = "mio", feature = "tokio"))]
use transport_socket::TcpConfig;

// unspecified remote would reach listener on localhost if not rejected first
#[cfg(any(feature = "mio", feature = "tokio"))]
fn assert_rejects_invalid_before_connect(
    connect: impl Fn(&TcpConfig) -> Result<(), TransportError>,
) {
    use std::{io, time::Duration};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let port = listener.local_addr().expect("listener addr").port();
    let unspecified = TcpConfig::new(SocketAddr::from(([0, 0, 0, 0], port)));
    let port_zero = TcpConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)));
    let mut no_timeout = TcpConfig::new(SocketAddr::from(([127, 0, 0, 1], port)));
    no_timeout.connect_timeout = Duration::ZERO;
    for (cfg, want) in [
        (unspecified, "remote"),
        (port_zero, "remote"),
        (no_timeout, "connect_timeout"),
    ] {
        match connect(&cfg) {
            Err(TransportError::InvalidConfig { field, .. }) => assert_eq!(field, want),
            other => panic!("{cfg:?}: got {other:?}, want InvalidConfig on {want}"),
        }
    }
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let accepted = listener.accept().map(|(_, peer)| peer);
    assert!(
        accepted
            .as_ref()
            .is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock),
        "rejected config still reached listener: {accepted:?}"
    );
}

// closed port refuses: error names remote and keeps OS kind
#[cfg(any(feature = "mio", feature = "tokio"))]
fn assert_refused_is_connect_error(connect: impl Fn(&TcpConfig) -> Result<(), TransportError>) {
    use std::io;

    let remote = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port");
    match connect(&TcpConfig::new(remote)) {
        Err(TransportError::Connect { addr, error }) => {
            assert_eq!(addr, remote);
            assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused, "{error}");
        }
        other => panic!("got {other:?}, want Connect refused"),
    }
}

// full accept queue drops next SYN, so handshake can only time out; Winsock
// `listen` refuses on full queue (WSAECONNREFUSED), so no timeout there
#[cfg(all(
    any(feature = "mio", feature = "tokio"),
    any(target_os = "linux", target_os = "macos")
))]
fn assert_timeout_is_connect_timed_out(connect: impl Fn(&TcpConfig) -> Result<(), TransportError>) {
    use std::{
        io,
        time::{Duration, Instant},
    };

    use socket2::{Domain, Socket, Type};

    const TIMEOUT: Duration = Duration::from_millis(200);
    let socket = || Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket");
    let listener = socket();
    listener
        .bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())
        .expect("bind listener");
    // backlog 1, not 0: macOS reads 0 as `somaxconn` (128 fillers)
    listener.listen(1).expect("listen");
    let remote = listener
        .local_addr()
        .ok()
        .and_then(|a| a.as_socket())
        .expect("listener addr");
    // never accepted: fill queue until one more handshake times out
    let mut fillers = Vec::new();
    loop {
        let filler = socket();
        match filler.connect_timeout(&remote.into(), TIMEOUT) {
            Ok(()) => fillers.push(filler),
            Err(e) if e.kind() == io::ErrorKind::TimedOut => break,
            Err(e) => panic!("filler {}: {e}", fillers.len()),
        }
        assert!(fillers.len() < 16, "accept queue never filled");
    }

    let mut cfg = TcpConfig::new(remote);
    cfg.connect_timeout = TIMEOUT;
    let start = Instant::now();
    match connect(&cfg) {
        Err(TransportError::Connect { addr, error }) => {
            assert_eq!(addr, remote);
            assert_eq!(error.kind(), io::ErrorKind::TimedOut, "{error}");
        }
        other => panic!("got {other:?}, want Connect timed out"),
    }
    let took = start.elapsed();
    assert!(took < Duration::from_secs(2), "timeout took {took:?}");
}

#[cfg(feature = "mio")]
#[test]
fn mio_connect_rejects_invalid_config_before_connecting() {
    use transport_socket::mio::MioTcp;

    assert_rejects_invalid_before_connect(|cfg| MioTcp::connect(cfg).map(drop));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_connect_rejects_invalid_config_before_connecting() {
    use transport_socket::tokio::TcpStream;

    let rt = runtime();
    assert_rejects_invalid_before_connect(|cfg| rt.block_on(TcpStream::connect(cfg)).map(drop));
}

#[cfg(feature = "tokio")]
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

#[cfg(feature = "mio")]
#[test]
fn mio_connect_refused_is_connect_error() {
    use transport_socket::mio::MioTcp;

    assert_refused_is_connect_error(|cfg| MioTcp::connect(cfg).map(drop));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_connect_refused_is_connect_error() {
    use transport_socket::tokio::TcpStream;

    let rt = runtime();
    assert_refused_is_connect_error(|cfg| rt.block_on(TcpStream::connect(cfg)).map(drop));
}

#[cfg(all(feature = "mio", any(target_os = "linux", target_os = "macos")))]
#[test]
fn mio_connect_timeout_is_connect_timed_out() {
    use transport_socket::mio::MioTcp;

    assert_timeout_is_connect_timed_out(|cfg| MioTcp::connect(cfg).map(drop));
}

#[cfg(all(feature = "tokio", any(target_os = "linux", target_os = "macos")))]
#[test]
fn tokio_connect_timeout_is_connect_timed_out() {
    use transport_socket::tokio::TcpStream;

    let rt = runtime();
    assert_timeout_is_connect_timed_out(|cfg| rt.block_on(TcpStream::connect(cfg)).map(drop));
}

#[cfg(feature = "mio")]
#[test]
fn mio_partial_write_resumes_on_writable_readiness() {
    use std::{
        io::Read,
        num::{NonZeroU32, NonZeroUsize},
        thread,
        time::{Duration, Instant},
    };

    use socket2::{Domain, Socket, Type};
    use transport_core::StreamTrySend;
    use transport_socket::mio::{MioTcp, ReadySet, ReadyToken};

    // far above tiny send buffer plus peer receive buffer, so writes must stall
    const LEN: usize = 1024 * 1024;
    const TOKEN: ReadyToken = ReadyToken(0);

    // small send buffer alone does not stall a sender: Windows auto-tunes the
    // receive window into the megabytes, and fixes it when the connection is
    // established, so the peer's buffer has to be set before `listen`, not on
    // an already listening socket. 64 KiB (Linux doubles it) stays far under
    // `LEN` yet drains it in few round trips.
    let listener = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket");
    listener
        .set_recv_buffer_size(64 * 1024)
        .expect("listener recv buffer");
    listener
        .bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())
        .expect("bind listener");
    listener.listen(1).expect("listen");
    let remote = listener
        .local_addr()
        .ok()
        .and_then(|a| a.as_socket())
        .expect("listener addr");
    let mut cfg = TcpConfig::new(remote);
    cfg.send_buf = NonZeroU32::new(4096);
    // sub-MSS segments under Nagle wait on delayed ACKs: tens of ms each
    cfg.nodelay = true;
    let mut tcp = MioTcp::connect(&cfg).expect("connect");
    let (peer, _) = listener.accept().expect("accept");
    // Winsock does not carry the listener's buffer onto the accepted socket,
    // so pin that one too: the sender has only a 4 KiB send buffer and cannot
    // outrun the window update before it stalls.
    peer.set_recv_buffer_size(64 * 1024)
        .expect("peer recv buffer");
    let mut peer = std::net::TcpStream::from(peer);
    let data: Vec<u8> = (0..=250).cycle().take(LEN).collect();

    // peer not reading: short writes, then would-block as `Ok(0)`
    let mut sent = 0;
    loop {
        let n = tcp.try_send(&data[sent..]).expect("try_send");
        if n == 0 {
            break;
        }
        sent += n;
        assert!(
            sent < LEN,
            "peer took whole {LEN} bytes without would-block"
        );
    }
    assert!(sent > 0, "no short write before would-block");

    let mut poll = ReadySet::new(NonZeroUsize::new(4).unwrap()).expect("ready set");
    poll.register(&mut tcp, TOKEN).expect("register");
    let reader = thread::spawn(move || {
        peer.set_read_timeout(Some(Duration::from_secs(10)))?;
        let mut got = vec![0; LEN];
        peer.read_exact(&mut got).map(|()| got)
    });

    // edge-triggered: on each writable report, write until would-block again
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut reports = Vec::new();
    while sent < LEN {
        assert!(
            Instant::now() < deadline,
            "stalled at {sent} of {LEN} bytes"
        );
        poll.wait(Some(Duration::from_secs(1)), &mut reports)
            .expect("wait");
        if reports.iter().any(|r| r.token == TOKEN && r.writable) {
            while sent < LEN {
                match tcp.try_send(&data[sent..]).expect("try_send") {
                    0 => break,
                    n => sent += n,
                }
            }
        }
    }
    let got = reader.join().expect("reader thread").expect("peer read");
    assert!(got == data, "peer bytes differ from sent bytes");
}
