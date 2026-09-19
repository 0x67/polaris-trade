//! Socket options read back from kernel through socket2 getters, and options
//! platform cannot apply rejected as `InvalidConfig` before any syscall.
//!
//! Kernels adjust buffer sizes (Linux doubles, macOS TCP adds loopback room),
//! so readback must be at least requested and differ from what same socket
//! kind gets unconfigured.

use std::{
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
};

use socket2::SockRef;
use transport_core::TransportError;
use transport_socket::{UdpConfig, UdpSocket};

// 96 KiB: below Linux `rmem_max` default
const BUF: NonZeroU32 = NonZeroU32::new(96 * 1024).unwrap();

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

// `control`: same socket kind, no options set
fn assert_buffers(sock: &SockRef<'_>, control: &SockRef<'_>, what: &str) {
    let requested = BUF.get() as usize;
    for (name, got, default) in [
        (
            "SO_RCVBUF",
            sock.recv_buffer_size(),
            control.recv_buffer_size(),
        ),
        (
            "SO_SNDBUF",
            sock.send_buffer_size(),
            control.send_buffer_size(),
        ),
    ] {
        let (got, default) = (got.expect(name), default.expect(name));
        assert!(
            got >= requested && got != default,
            "{what} {name}: {got}, requested {requested}, unconfigured {default}"
        );
    }
}

#[test]
fn udp_options_read_back_from_kernel() {
    let mut cfg = UdpConfig::new(loopback());
    cfg.reuse_addr = true;
    cfg.reuse_port = cfg!(unix);
    cfg.recv_buf = Some(BUF);
    cfg.send_buf = Some(BUF);
    let udp = UdpSocket::bind(&cfg).expect("bind");
    let sock = SockRef::from(&udp);

    assert!(sock.reuse_address().expect("SO_REUSEADDR"), "SO_REUSEADDR");
    #[cfg(unix)]
    assert!(sock.reuse_port().expect("SO_REUSEPORT"), "SO_REUSEPORT");
    let control = std::net::UdpSocket::bind(loopback()).expect("control socket");
    assert_buffers(&sock, &SockRef::from(&control), "udp");
}

#[cfg(target_os = "linux")]
#[test]
fn busy_poll_reaches_kernel_or_fails_loudly() {
    let mut cfg = UdpConfig::new(loopback());
    cfg.busy_poll_us = Some(50);
    match UdpSocket::bind(&cfg) {
        Ok(udp) => assert_eq!(SockRef::from(&udp).busy_poll().expect("SO_BUSY_POLL"), 50),
        // raising busy poll needs CAP_NET_ADMIN: unprivileged run must say so
        Err(TransportError::Io {
            stage: "setsockopt(SO_BUSY_POLL)",
            error,
        }) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied,
            "{error}"
        ),
        Err(other) => panic!("got {other}, want SO_BUSY_POLL applied or refused"),
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn busy_poll_off_linux_is_invalid_config() {
    let mut cfg = UdpConfig::new(loopback());
    cfg.busy_poll_us = Some(50);
    assert!(matches!(
        UdpSocket::bind(&cfg),
        Err(TransportError::InvalidConfig {
            field: "busy_poll_us",
            ..
        })
    ));
}

#[cfg(windows)]
#[test]
fn reuse_port_on_windows_is_invalid_config() {
    let mut cfg = UdpConfig::new(loopback());
    cfg.reuse_port = true;
    assert!(matches!(
        UdpSocket::bind(&cfg),
        Err(TransportError::InvalidConfig {
            field: "reuse_port",
            ..
        })
    ));
}
