//! Sync UDP socket, plus send and multicast mappings shared by every UDP type.

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use socket2::Socket;
use transport_core::{
    DatagramRecv, DatagramSend, FrameBatch, Multicast, MulticastInterface, PoolStats, Transport,
    TransportError, pool::VecPool,
};

use crate::{UdpConfig, UdpFrame, io_error, recv};

const NAME: &str = "udp";

/// Sync, non-blocking UDP socket for busy-poll loops. No runtime, no readiness.
///
/// Convert into `tokio::AsyncUdp` or `mio::MioUdp` for readiness; options,
/// pool and pending error move along.
#[derive(Debug)]
pub struct UdpSocket {
    pub(crate) sock: Socket,
    pub(crate) pool: VecPool,
    // met after frames were pushed; returned before next recv
    pub(crate) deferred: Option<TransportError>,
}

impl UdpSocket {
    /// Validate `cfg`, allocate receive pool, then bind non-blocking socket.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] before any allocation or syscall when
    /// `cfg` asks for what this OS cannot apply, or pool is too large;
    /// [`TransportError::Bind`] when bind fails; [`TransportError::Io`] when
    /// socket creation or option fails.
    pub fn bind(cfg: &UdpConfig) -> Result<Self, TransportError> {
        cfg.validate()?;
        let pool = VecPool::new(cfg.slab_count, cfg.slab_size)?;
        let sock = crate::sockopt::udp(cfg)?;
        Ok(Self {
            sock,
            pool,
            deferred: None,
        })
    }

    /// Bound local address, with port OS picked for port 0.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when OS query fails.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        let addr = self.sock.local_addr().map_err(io_error("getsockname"))?;
        // AF_INET and AF_INET6 sockets always have IP address
        addr.as_socket().ok_or_else(|| TransportError::Io {
            stage: "getsockname",
            error: io::ErrorKind::Unsupported.into(),
        })
    }
}

impl Transport for UdpSocket {
    fn name(&self) -> &'static str {
        NAME
    }
}

impl DatagramRecv for UdpSocket {
    type Frame = UdpFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<UdpFrame>) -> Result<usize, TransportError> {
        let sock = &self.sock;
        recv::burst(
            NAME,
            &self.pool,
            out,
            &mut self.deferred,
            |buf| sock.recv_from(buf),
            || recv::peek_ready(sock),
        )
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }
}

impl DatagramSend for UdpSocket {
    #[inline]
    fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> Result<usize, TransportError> {
        sent(self.sock.send_to(buf, &to.into()))
    }
}

impl Multicast for UdpSocket {
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        join(&self.sock, group, iface)
    }
}

#[cfg(unix)]
impl AsFd for UdpSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }
}

#[cfg(windows)]
impl AsSocket for UdpSocket {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.sock.as_socket()
    }
}

/// Map one `send_to` result: full buffer is `Io` of kind `WouldBlock`.
///
/// ENOBUFS (BSD-derived stacks on full interface queue) is wrapped as
/// `WouldBlock`, OS error kept inside.
pub(crate) fn sent(result: io::Result<usize>) -> Result<usize, TransportError> {
    result.map_err(|error| {
        #[cfg(unix)]
        let error = if error.raw_os_error() == Some(libc::ENOBUFS) {
            io::Error::new(io::ErrorKind::WouldBlock, error)
        } else {
            error
        };
        TransportError::Io {
            stage: "send_to",
            error,
        }
    })
}

/// Join `group`; interface field of other address family is `InvalidConfig`.
pub(crate) fn join(
    sock: &Socket,
    group: IpAddr,
    iface: MulticastInterface,
) -> Result<(), TransportError> {
    match group {
        IpAddr::V4(group) => {
            if iface.v6_scope_id.is_some() {
                return Err(TransportError::InvalidConfig {
                    field: "iface.v6_scope_id",
                    reason: "set for IPv4 group",
                });
            }
            let local = iface.v4.unwrap_or(Ipv4Addr::UNSPECIFIED);
            sock.join_multicast_v4(&group, &local)
        }
        IpAddr::V6(group) => {
            if iface.v4.is_some() {
                return Err(TransportError::InvalidConfig {
                    field: "iface.v4",
                    reason: "set for IPv6 group",
                });
            }
            sock.join_multicast_v6(&group, iface.v6_scope_id.unwrap_or(0))
        }
    }
    .map_err(io_error("join_multicast"))
}
