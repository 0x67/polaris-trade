//! TCP for [`ReadySet`](super::ReadySet), no runtime.

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::{io::Write, mem::MaybeUninit};

use ::mio::Interest;
use socket2::SockRef;
use transport_core::{StreamRecv, StreamTrySend, Transport, TransportError};

use super::{ReadySource, sealed::Sealed};
use crate::{TcpConfig, io_error, recv, written};

const NAME: &str = "mio-tcp";

/// TCP stream for runtime-free sessions: sync receive and partial write,
/// watched by [`ReadySet`](super::ReadySet) for read and write readiness.
///
/// Every syscall goes through mio `try_io`, so Windows re-arms after drain.
#[derive(Debug)]
pub struct MioTcp {
    sock: ::mio::net::TcpStream,
}

impl MioTcp {
    /// Connect to `cfg.remote`, blocking calling thread at most
    /// `cfg.connect_timeout`, then switch socket to non-blocking.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] before any syscall;
    /// [`TransportError::Bind`] when `cfg.local` bind fails;
    /// [`TransportError::Connect`] when handshake fails or times out (kind
    /// `TimedOut`); [`TransportError::Io`] when socket creation or option fails.
    pub fn connect(cfg: &TcpConfig) -> Result<Self, TransportError> {
        cfg.validate()?;
        let sock = crate::sockopt::tcp(cfg)?;
        sock.connect_timeout(&cfg.remote.into(), cfg.connect_timeout)
            .map_err(|error| TransportError::Connect {
                addr: cfg.remote,
                error,
            })?;
        sock.set_nonblocking(true)
            .map_err(io_error("set_nonblocking"))?;
        Ok(Self {
            sock: ::mio::net::TcpStream::from_std(sock.into()),
        })
    }
}

impl Transport for MioTcp {
    fn name(&self) -> &'static str {
        NAME
    }
}

// SAFETY: `recv::stream` returns `Ok(n)` with `n > 0` only from socket2 `recv`,
// which wrote `dst[..n]` with `n <= dst.len()`; `Ok(0)` claims no bytes.
unsafe impl StreamRecv for MioTcp {
    #[inline]
    fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        let sock = &self.sock;
        recv::stream(dst, |dst| sock.try_io(|| SockRef::from(sock).recv(dst)))
    }
}

impl StreamTrySend for MioTcp {
    #[inline]
    fn try_send(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        // mio routes `write` through `try_io`; std write suppresses SIGPIPE
        written(self.sock.write(buf))
    }
}

impl Sealed for MioTcp {
    type Source = ::mio::net::TcpStream;
    const INTEREST: Interest = Interest::READABLE.add(Interest::WRITABLE);

    fn source(&mut self) -> &mut Self::Source {
        &mut self.sock
    }
}

impl ReadySource for MioTcp {}

#[cfg(unix)]
impl AsFd for MioTcp {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }
}

#[cfg(windows)]
impl AsSocket for MioTcp {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.sock.as_socket()
    }
}
