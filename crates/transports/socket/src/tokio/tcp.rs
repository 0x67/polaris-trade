//! TCP on tokio reactor.

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::{io, mem::MaybeUninit};

use ::tokio::{
    io::{AsyncWriteExt, Interest},
    net::TcpSocket,
    runtime::Handle,
};
use socket2::{SockRef, Socket};
use transport_core::{
    AsyncReady, StreamRecv, StreamSend, StreamTrySend, Transport, TransportError,
};

use super::NO_RUNTIME;
use crate::{TcpConfig, io_error, recv, written};

const NAME: &str = "tokio-tcp";

/// TCP stream registered with tokio reactor: sync receive and partial write,
/// async whole write and readiness.
#[derive(Debug)]
pub struct TcpStream {
    sock: ::tokio::net::TcpStream,
}

impl TcpStream {
    /// Connect within `cfg.connect_timeout`, options applied before handshake.
    ///
    /// # Errors
    ///
    /// `InvalidConfig` before any syscall, `Unavailable` outside runtime, `Bind`
    /// on `cfg.local`, `Connect` on refusal or timeout, `Io` on socket setup.
    ///
    /// # Panics
    ///
    /// Runtime lacks IO or time driver (tokio registration, `timeout`).
    pub async fn connect(cfg: &TcpConfig) -> Result<Self, TransportError> {
        cfg.validate()?;
        if Handle::try_current().is_err() {
            return Err(NO_RUNTIME);
        }
        let sock = crate::sockopt::tcp(cfg)?;
        sock.set_nonblocking(true)
            .map_err(io_error("set_nonblocking"))?;
        let connect = TcpSocket::from_std_stream(sock.into()).connect(cfg.remote);
        let addr = cfg.remote;
        match ::tokio::time::timeout(cfg.connect_timeout, connect).await {
            Ok(Ok(sock)) => Ok(Self { sock }),
            Ok(Err(error)) => Err(TransportError::Connect { addr, error }),
            Err(_elapsed) => Err(TransportError::Connect {
                addr,
                error: io::ErrorKind::TimedOut.into(),
            }),
        }
    }
}

impl Transport for TcpStream {
    fn name(&self) -> &'static str {
        NAME
    }
}

// SAFETY: `recv::stream` returns `Ok(n)` with `n > 0` only from socket2 `recv`,
// which wrote `dst[..n]` with `n <= dst.len()`; `Ok(0)` claims no bytes.
unsafe impl StreamRecv for TcpStream {
    #[inline]
    fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        recv::stream(dst, |dst| SockRef::from(&self.sock).recv(dst))
    }
}

impl StreamSend for TcpStream {
    async fn send_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.sock.write_all(buf).await.map_err(io_error("send_all"))
    }
}

impl StreamTrySend for TcpStream {
    #[inline]
    fn try_send(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        written(self.sock.try_write(buf))
    }
}

impl AsyncReady for TcpStream {
    async fn ready(&mut self) -> Result<(), TransportError> {
        loop {
            self.sock.readable().await.map_err(io_error("readable"))?;
            let peek = || peek_byte(&SockRef::from(&self.sock));
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

// bytes queued or FIN: next `recv_into` makes progress either way
fn peek_byte(sock: &Socket) -> io::Result<()> {
    sock.peek(&mut [MaybeUninit::uninit()]).map(drop)
}

#[cfg(unix)]
impl AsFd for TcpStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }
}

#[cfg(windows)]
impl AsSocket for TcpStream {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.sock.as_socket()
    }
}
