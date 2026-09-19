//! Socket configs. Required fields are `new` arguments, the rest public with
//! defaults. Constructors call `validate` before first allocation or syscall.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{TransportError, config::validate};

// socket2 hands option values to setsockopt as C `int`
const C_INT_MAX: u32 = i32::MAX.unsigned_abs();
const DEFAULT_SLAB_COUNT: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
const DEFAULT_SLAB_SIZE: NonZeroUsize = NonZeroUsize::new(2048).unwrap();

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
