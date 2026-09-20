//! UDP on tokio reactor.

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};

use ::tokio::{io::Interest, runtime::Handle};
use socket2::SockRef;
use transport_core::{
    AsyncReady, DatagramRecv, DatagramSend, FrameBatch, Multicast, MulticastInterface, PoolStats,
    Transport, TransportError, pool::VecPool,
};

use super::NO_RUNTIME;
use crate::{UdpFrame, UdpSocket, io_error, recv, udp};

const NAME: &str = "tokio-udp";

/// UDP socket registered with tokio reactor: sync receive and send, async readiness.
#[derive(Debug)]
pub struct AsyncUdp {
    sock: ::tokio::net::UdpSocket,
    pool: VecPool,
    // met after frames were pushed; returned before next recv
    deferred: Option<TransportError>,
}

impl AsyncUdp {
    /// Register `s` with current tokio runtime, keeping its options and pool.
    ///
    /// # Errors
    ///
    /// [`TransportError::Unavailable`] outside tokio runtime;
    /// [`TransportError::Io`] when reactor registration fails.
    ///
    /// # Panics
    ///
    /// When current runtime was built without IO driver (tokio `from_std`).
    pub fn from_socket(s: UdpSocket) -> Result<Self, TransportError> {
        if Handle::try_current().is_err() {
            return Err(NO_RUNTIME);
        }
        let UdpSocket {
            sock,
            pool,
            deferred,
        } = s;
        let sock = ::tokio::net::UdpSocket::from_std(sock.into()).map_err(io_error("register"))?;
        Ok(Self {
            sock,
            pool,
            deferred,
        })
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

impl Transport for AsyncUdp {
    fn name(&self) -> &'static str {
        NAME
    }
}

impl DatagramRecv for AsyncUdp {
    type Frame = UdpFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<UdpFrame>) -> Result<usize, TransportError> {
        let sock = &self.sock;
        recv::burst(
            NAME,
            &self.pool,
            out,
            &mut self.deferred,
            |buf| recv::datagram(&SockRef::from(sock), buf),
            || recv::peek_ready(&SockRef::from(sock)),
        )
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }
}

impl DatagramSend for AsyncUdp {
    #[inline]
    fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> Result<usize, TransportError> {
        udp::sent(SockRef::from(&self.sock).send_to(buf, &to.into()))
    }
}

impl Multicast for AsyncUdp {
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        udp::join(&SockRef::from(&self.sock), group, iface)
    }
}

impl AsyncReady for AsyncUdp {
    async fn ready(&mut self) -> Result<(), TransportError> {
        loop {
            self.sock.readable().await.map_err(io_error("readable"))?;
            let peek = || recv::peek_ready(&SockRef::from(&self.sock));
            match self.sock.try_io(Interest::READABLE, peek) {
                Ok(()) => return Ok(()),
                // stale readiness now cleared; wait for next edge
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    return Err(TransportError::Io {
                        stage: "peek",
                        error,
                    });
                }
            }
        }
    }
}

#[cfg(unix)]
impl AsFd for AsyncUdp {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }
}

#[cfg(windows)]
impl AsSocket for AsyncUdp {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.sock.as_socket()
    }
}
