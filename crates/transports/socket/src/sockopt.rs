//! Socket creation and options through socket2 safe setters, shared by every
//! socket type so an option means the same under every feature.
//!
//! Callers validate config first; platform-gated setters here trust that.

use std::num::NonZeroU32;

use socket2::{Domain, Protocol, Socket, Type};
use transport_core::TransportError;

#[cfg(feature = "tokio")]
use crate::TcpConfig;
use crate::{UdpConfig, io_error};

/// Non-blocking UDP socket, options applied, bound to `cfg.bind`.
pub(crate) fn udp(cfg: &UdpConfig) -> Result<Socket, TransportError> {
    let sock = Socket::new(
        Domain::for_address(cfg.bind),
        Type::DGRAM,
        Some(Protocol::UDP),
    )
    .map_err(io_error("socket"))?;
    if cfg.reuse_addr {
        sock.set_reuse_address(true)
            .map_err(io_error("setsockopt(SO_REUSEADDR)"))?;
    }
    #[cfg(unix)]
    if cfg.reuse_port {
        sock.set_reuse_port(true)
            .map_err(io_error("setsockopt(SO_REUSEPORT)"))?;
    }
    buffers(&sock, cfg.recv_buf, cfg.send_buf)?;
    #[cfg(target_os = "linux")]
    if let Some(us) = cfg.busy_poll_us {
        sock.set_busy_poll(us)
            .map_err(io_error("setsockopt(SO_BUSY_POLL)"))?;
    }
    sock.set_nonblocking(true)
        .map_err(io_error("set_nonblocking"))?;
    sock.bind(&cfg.bind.into())
        .map_err(|error| TransportError::Bind {
            addr: cfg.bind,
            error,
        })?;
    Ok(sock)
}

/// Unconnected TCP socket, options applied, bound to `cfg.local` when set.
/// Caller picks blocking mode and connects.
#[cfg(feature = "tokio")]
pub(crate) fn tcp(cfg: &TcpConfig) -> Result<Socket, TransportError> {
    let sock = Socket::new(
        Domain::for_address(cfg.remote),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .map_err(io_error("socket"))?;
    buffers(&sock, cfg.recv_buf, cfg.send_buf)?;
    if cfg.nodelay {
        sock.set_tcp_nodelay(true)
            .map_err(io_error("setsockopt(TCP_NODELAY)"))?;
    }
    if let Some(local) = cfg.local {
        sock.bind(&local.into())
            .map_err(|error| TransportError::Bind { addr: local, error })?;
    }
    Ok(sock)
}

fn buffers(
    sock: &Socket,
    recv_buf: Option<NonZeroU32>,
    send_buf: Option<NonZeroU32>,
) -> Result<(), TransportError> {
    if let Some(bytes) = recv_buf {
        sock.set_recv_buffer_size(bytes.get() as usize)
            .map_err(io_error("setsockopt(SO_RCVBUF)"))?;
    }
    if let Some(bytes) = send_buf {
        sock.set_send_buffer_size(bytes.get() as usize)
            .map_err(io_error("setsockopt(SO_SNDBUF)"))?;
    }
    Ok(())
}
