//! UDP for [`ReadySet`](super::ReadySet).

use std::net::{IpAddr, SocketAddr};
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket, BorrowedSocket};

use ::mio::Interest;
use socket2::SockRef;
use transport_core::{
    DatagramRecv, DatagramSend, FrameBatch, Multicast, MulticastInterface, PoolStats, Transport,
    TransportError, pool::VecPool,
};

use super::{ReadySource, sealed::Sealed};
use crate::{UdpFrame, UdpSocket, io_error, recv, udp};

const NAME: &str = "mio-udp";

/// UDP socket watched by [`ReadySet`](super::ReadySet) for read readiness.
///
/// Every read and write goes through mio `try_io`, so Windows re-arms after drain.
#[derive(Debug)]
pub struct MioUdp {
    sock: ::mio::net::UdpSocket,
    pool: VecPool,
    // met after frames were pushed; returned before next recv
    deferred: Option<TransportError>,
}

impl MioUdp {
    /// Wrap `s` for registration, keeping its options and pool.
    pub fn from_socket(s: UdpSocket) -> Self {
        let UdpSocket {
            sock,
            pool,
            deferred,
        } = s;
        Self {
            sock: ::mio::net::UdpSocket::from_std(sock.into()),
            pool,
            deferred,
        }
    }

    /// Bound local address.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when OS query fails.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.sock.local_addr().map_err(io_error("getsockname"))
    }
}

impl Transport for MioUdp {
    fn name(&self) -> &'static str {
        NAME
    }
}

impl DatagramRecv for MioUdp {
    type Frame = UdpFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<UdpFrame>) -> Result<usize, TransportError> {
        let sock = &self.sock;
        recv::burst(
            NAME,
            &self.pool,
            out,
            &mut self.deferred,
            |buf| sock.try_io(|| SockRef::from(sock).recv_from(buf)),
            || sock.try_io(|| recv::peek_ready(&SockRef::from(sock))),
        )
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }
}

impl DatagramSend for MioUdp {
    #[inline]
    fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> Result<usize, TransportError> {
        let sock = &self.sock;
        udp::sent(sock.try_io(|| SockRef::from(sock).send_to(buf, &to.into())))
    }
}

impl Multicast for MioUdp {
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        udp::join(&SockRef::from(&self.sock), group, iface)
    }
}

impl Sealed for MioUdp {
    type Source = ::mio::net::UdpSocket;
    const INTEREST: Interest = Interest::READABLE;

    fn source(&mut self) -> &mut Self::Source {
        &mut self.sock
    }
}

impl ReadySource for MioUdp {}

#[cfg(unix)]
impl AsFd for MioUdp {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }
}

#[cfg(windows)]
impl AsSocket for MioUdp {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.sock.as_socket()
    }
}
