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

#[cfg(not(target_os = "linux"))]
use socket2::MaybeUninitSlice;
use socket2::{SockAddr, Socket};
#[cfg(feature = "observability")]
use transport_core::telemetry::DropReason;
use transport_core::{
    FrameBatch, PoolStats, TransportError,
    pool::{VecPool, VecSlab, backend},
};

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

/// Take datagrams into `out` until socket empty, `out` full or pool empty.
///
/// Error left in `deferred` by previous call returns first, before any
/// syscall. Error met after frames were pushed waits in `deferred`, so frames
/// never travel with `Err`. Pool empty with nothing pushed runs `peek`:
/// queued datagram is `PoolExhausted`, idle socket `Ok(0)`. Datagram `recv`
/// reports cut is no frame: it is dropped, counted, loop goes on.
///
/// `recv` sees slab bytes as `MaybeUninit` but must write only initialised
/// bytes, never `MaybeUninit::uninit()`: slab is read as `[u8]` afterwards.
pub(crate) fn burst(
    name: &'static str,
    pool: &VecPool,
    out: &mut FrameBatch<UdpFrame>,
    deferred: &mut Option<TransportError>,
    mut recv: impl FnMut(&mut [MaybeUninit<u8>]) -> io::Result<(usize, bool, SockAddr)>,
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
            // cut datagram is half a message; dropped slab returns to pool, next pass reuses it
            Ok((_, true, _)) => {
                #[cfg(feature = "observability")]
                transport_core::telemetry::record_drops(name, DropReason::Truncated, 1);
            }
            Ok((len, false, from)) => {
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

/// One datagram into `buf`: bytes stored, whether kernel cut it, sender.
///
/// `recvfrom` with `MSG_TRUNC` returns whole datagram length even when it did
/// not fit, so length past `buf` is the cut; cheaper than `recvmsg` per call.
/// Kernel still writes at most `buf.len()` bytes, so returned length is
/// clamped before it reaches slab.
#[cfg(target_os = "linux")]
pub(crate) fn datagram(
    sock: &Socket,
    buf: &mut [MaybeUninit<u8>],
) -> io::Result<(usize, bool, SockAddr)> {
    let cap = buf.len();
    let (len, from) = sock.recv_from_with_flags(buf, libc::MSG_TRUNC)?;
    Ok((len.min(cap), len > cap, from))
}

/// One datagram into `buf`: bytes stored, whether kernel cut it, sender.
///
/// `recvmsg` path off Linux: `MSG_TRUNC` on receive is Linux only, BSD
/// `recv` has no equivalent, and plain `recv_from` delivers cut payload
/// unsignalled there. socket2 folds Winsock `WSAEMSGSIZE` into same flags.
#[cfg(not(target_os = "linux"))]
pub(crate) fn datagram(
    sock: &Socket,
    buf: &mut [MaybeUninit<u8>],
) -> io::Result<(usize, bool, SockAddr)> {
    let mut bufs = [MaybeUninitSlice::new(buf)];
    let (len, flags, from) = sock.recv_from_vectored(&mut bufs)?;
    Ok((len, flags.is_truncated(), from))
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
    // live `&mut [u8]`, whose borrow returned slice inherits. Only `burst`
    // calls this, and its contract bars `recv` from writing uninitialised
    // bytes, so memory stays valid `[u8]` for slab reads afterwards.
    unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<MaybeUninit<u8>>(), buf.len()) }
}

#[cfg(test)]
mod tests;
