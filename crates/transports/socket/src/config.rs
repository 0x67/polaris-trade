//! Socket configs. Required fields are `new` arguments, the rest public with
//! defaults. Constructors call `validate` before first allocation or syscall.

#[cfg(feature = "tokio")]
use std::time::Duration;
use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{TransportError, config::validate};

// socket2 hands option values to setsockopt as C `int`
const C_INT_MAX: u32 = i32::MAX.unsigned_abs();
const DEFAULT_SLAB_COUNT: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
const DEFAULT_SLAB_SIZE: NonZeroUsize = NonZeroUsize::new(2048).unwrap();
#[cfg(feature = "tokio")]
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// UDP socket: bind address, socket options, receive pool shape.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct UdpConfig {
    /// Local address. Port 0 picks ephemeral port; unspecified IP serves multicast.
    pub bind: SocketAddr,
    /// `SO_REUSEADDR`. Default off.
    pub reuse_addr: bool,
    /// `SO_REUSEPORT`, Unix only; set on Windows is `InvalidConfig`. Default off.
    pub reuse_port: bool,
    /// `SO_RCVBUF` bytes; `None` keeps OS default. Linux reports double.
    pub recv_buf: Option<NonZeroU32>,
    /// `SO_SNDBUF` bytes; `None` keeps OS default. Linux reports double.
    pub send_buf: Option<NonZeroU32>,
    /// `SO_BUSY_POLL` microseconds, Linux only; set elsewhere is `InvalidConfig`.
    /// Raising it above `net.core.busy_read` needs `CAP_NET_ADMIN`.
    pub busy_poll_us: Option<u32>,
    /// Receive slabs, so most datagrams caller can hold at once. Default 1024.
    pub slab_count: NonZeroUsize,
    /// Bytes per slab, so largest datagram received whole. Default 2048.
    pub slab_size: NonZeroUsize,
}

impl UdpConfig {
    /// Config binding `bind`, every option at default.
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            reuse_addr: false,
            reuse_port: false,
            recv_buf: None,
            send_buf: None,
            busy_poll_us: None,
            slab_count: DEFAULT_SLAB_COUNT,
            slab_size: DEFAULT_SLAB_SIZE,
        }
    }

    // explicit option platform cannot apply is an error, never ignored
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if cfg!(not(unix)) && self.reuse_port {
            return Err(TransportError::InvalidConfig {
                field: "reuse_port",
                reason: "SO_REUSEPORT unavailable on this OS",
            });
        }
        if let Some(us) = self.busy_poll_us {
            if cfg!(not(target_os = "linux")) {
                return Err(TransportError::InvalidConfig {
                    field: "busy_poll_us",
                    reason: "SO_BUSY_POLL is Linux only",
                });
            }
            validate::at_most("busy_poll_us", us, C_INT_MAX)?;
        }
        buffer_sizes(self.recv_buf, self.send_buf)
    }
}

/// TCP stream: remote peer, optional local bind, socket options.
#[cfg(feature = "tokio")]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TcpConfig {
    /// Peer to connect to. Unspecified IP or port 0 is `InvalidConfig`.
    pub remote: SocketAddr,
    /// Local address to bind before connecting; `None` lets OS pick.
    pub local: Option<SocketAddr>,
    /// `SO_RCVBUF` bytes; `None` keeps OS default. Linux reports double.
    pub recv_buf: Option<NonZeroU32>,
    /// `SO_SNDBUF` bytes; `None` keeps OS default. Linux reports double.
    pub send_buf: Option<NonZeroU32>,
    /// `TCP_NODELAY`. Default off.
    pub nodelay: bool,
    /// Bound on handshake. Zero is `InvalidConfig`. Default 5 s.
    pub connect_timeout: Duration,
}

#[cfg(feature = "tokio")]
impl TcpConfig {
    /// Config connecting to `remote`, every option at default.
    pub fn new(remote: SocketAddr) -> Self {
        Self {
            remote,
            local: None,
            recv_buf: None,
            send_buf: None,
            nodelay: false,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    // unspecified remote would reach localhost on Linux, so reject it
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if self.remote.ip().is_unspecified() || self.remote.port() == 0 {
            return Err(TransportError::InvalidConfig {
                field: "remote",
                reason: "unspecified address or port 0",
            });
        }
        if self.connect_timeout.is_zero() {
            return Err(TransportError::InvalidConfig {
                field: "connect_timeout",
                reason: "zero timeout",
            });
        }
        buffer_sizes(self.recv_buf, self.send_buf)
    }
}

fn buffer_sizes(
    recv_buf: Option<NonZeroU32>,
    send_buf: Option<NonZeroU32>,
) -> Result<(), TransportError> {
    if let Some(bytes) = recv_buf {
        validate::at_most("recv_buf", bytes.get(), C_INT_MAX)?;
    }
    if let Some(bytes) = send_buf {
        validate::at_most("send_buf", bytes.get(), C_INT_MAX)?;
    }
    Ok(())
}
