//! TCP cases beyond conformance suite (`tests/conformance.rs` covers empty
//! read, ordered bytes, `PeerClosed`, resumed `try_send`, 8 MiB `send_all`):
//! config rejected before connecting and connect failure mapping.

#[cfg(feature = "tokio")]
use std::net::{SocketAddr, TcpListener};

#[cfg(feature = "tokio")]
use transport_core::TransportError;
#[cfg(feature = "tokio")]
use transport_socket::TcpConfig;

// unspecified remote would reach listener on localhost if not rejected first
#[cfg(feature = "tokio")]
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
#[cfg(feature = "tokio")]
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

#[cfg(feature = "tokio")]
#[test]
fn tokio_connect_refused_is_connect_error() {
    use transport_socket::tokio::TcpStream;

    let rt = runtime();
    assert_refused_is_connect_error(|cfg| rt.block_on(TcpStream::connect(cfg)).map(drop));
}
