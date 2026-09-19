//! One receive loop for every UDP type, plus stream read mapping.
//!
//! Caller supplies each syscall as closure, so its final `WouldBlock` passes
//! through whichever readiness wrapper owns socket (mio `try_io` re-arms
//! Windows; tokio types call socket2 directly and peek inside tokio `try_io`).

use std::{
    io,
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    slice,
};

use socket2::{SockAddr, Socket};
#[cfg(all(windows, feature = "observability"))]
use transport_core::telemetry::DropReason;
use transport_core::{
    FrameBatch, PoolStats, TransportError,
    pool::{VecPool, VecSlab, backend},
};

// Winsock: datagram longer than buffer; truncated copy landed, rest discarded
#[cfg(windows)]
const WSAEMSGSIZE: i32 = 10040;
// Winsock: ICMP port unreachable for earlier send, reported on later recv; no datagram lost
#[cfg(windows)]
const WSAECONNRESET: i32 = 10054;

const NO_PEER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// One received datagram in pool slab; drop returns slab to its pool.
#[derive(Debug)]
pub struct UdpFrame {
    slab: VecSlab,
    peer: SocketAddr,
}

impl UdpFrame {
    /// Sender address.
    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

impl AsRef<[u8]> for UdpFrame {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.slab.as_ref()
    }
}

/// Reap datagrams into `out` until socket empty, `out` full or pool empty.
///
/// Error left in `deferred` by previous call returns first, before any
/// syscall. Error met after frames were pushed waits in `deferred`, so frames
/// never travel with `Err`. Pool empty with nothing pushed runs `peek`:
/// queued datagram is `PoolExhausted`, idle socket `Ok(0)`.
pub(crate) fn burst(
    name: &'static str,
    pool: &VecPool,
    out: &mut FrameBatch<UdpFrame>,
    deferred: &mut Option<TransportError>,
    mut recv: impl FnMut(&mut [MaybeUninit<u8>]) -> io::Result<(usize, SockAddr)>,
    peek: impl FnOnce() -> io::Result<()>,
) -> Result<usize, TransportError> {
    if let Some(error) = deferred.take() {
        return Err(error);
    }
    debug_assert!(out.spare() > 0, "{name}: recv_burst on full batch");
    let mut pushed = 0;
    #[cfg(feature = "observability")]
    let mut bytes = 0;
    while out.spare() > 0 {
        let Some(mut slab) = backend::acquire(pool) else {
            if pushed == 0 {
                return exhausted(pool, peek);
            }
            break;
        };
        match recv(uninit(backend::buf_mut(&mut slab))) {
            Ok((len, from)) => {
                backend::set_len(&mut slab, len);
                #[cfg(feature = "observability")]
                {
                    bytes += len as u64;
                }
                // AF_INET and AF_INET6 sockets always yield IP sender
                let sender = from.as_socket().unwrap_or(NO_PEER);
                out.push(UdpFrame { slab, peer: sender });
                pushed += 1;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            // dropped slab returns to pool, next pass reuses it
            #[cfg(windows)]
            Err(e) if e.raw_os_error() == Some(WSAEMSGSIZE) => {
                #[cfg(feature = "observability")]
                transport_core::telemetry::record_drops(name, DropReason::Truncated, 1);
            }
            #[cfg(windows)]
            Err(e) if e.raw_os_error() == Some(WSAECONNRESET) => {}
            Err(error) => {
                let error = TransportError::Io {
                    stage: "recv_from",
                    error,
                };
                if pushed == 0 {
                    return Err(error);
                }
                *deferred = Some(error);
                break;
            }
        }
    }
    #[cfg(feature = "observability")]
    transport_core::telemetry::record_recv_burst(name, pushed as u64, bytes);
    Ok(pushed)
}

// pool empty, nothing pushed: tell idle socket from starved one
fn exhausted(
    pool: &VecPool,
    peek: impl FnOnce() -> io::Result<()>,
) -> Result<usize, TransportError> {
    match peek() {
        Ok(()) => {
            let PoolStats { in_use, capacity } = pool.stats();
            Err(TransportError::PoolExhausted { in_use, capacity })
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
        Err(error) => Err(TransportError::Io {
            stage: "peek",
            error,
        }),
    }
}

/// `Ok(())` when datagram queued, `Err(WouldBlock)` when not.
///
/// Idle stays `Err` so tokio `try_io` clears cached readiness and mio `try_io`
/// re-arms Windows. `peek_sender` never fails on large datagram, unlike
/// `peek_from` with small buffer (Winsock `WSAEMSGSIZE`).
pub(crate) fn peek_ready(sock: &Socket) -> io::Result<()> {
    match sock.peek_sender() {
        Ok(_) => Ok(()),
        // next recv_burst swallows reset; reporting ready costs one spurious wake
        #[cfg(windows)]
        Err(e) if e.raw_os_error() == Some(WSAECONNRESET) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Stream read shared by TCP types: empty `dst` and would-block are `Ok(0)`,
/// zero-byte read of non-empty `dst` is `PeerClosed`. `Ok(n)` means `recv`
/// initialised `dst[..n]`.
#[cfg(any(feature = "tokio", feature = "mio"))]
pub(crate) fn stream(
    dst: &mut [MaybeUninit<u8>],
    recv: impl FnOnce(&mut [MaybeUninit<u8>]) -> io::Result<usize>,
) -> Result<usize, TransportError> {
    if dst.is_empty() {
        return Ok(0);
    }
    match recv(dst) {
        Ok(0) => Err(TransportError::PeerClosed),
        Ok(n) => Ok(n),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
        Err(error) => Err(TransportError::Io {
            stage: "recv",
            error,
        }),
    }
}

// slab bytes as socket2 receive buffer
fn uninit(buf: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    // SAFETY: `MaybeUninit<u8>` has `u8` layout; pointer and length come from
    // live `&mut [u8]`, whose borrow returned slice inherits. socket2 receive
    // writes only initialised bytes and never de-initialises, so memory stays
    // valid `[u8]` for slab reads afterwards.
    unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<MaybeUninit<u8>>(), buf.len()) }
}

#[cfg(test)]
mod tests;
