//! Capability traits every backend implements, plus burst container.
//!
//! Receive is synchronous and batch-first so busy-poll and kernel-bypass
//! backends serve every consumer without executor. Frames are owned handles:
//! each carries its own buffer and returns it to its pool on drop.

use std::{
    mem::MaybeUninit,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
};

use crate::{config::MulticastInterface, error::TransportError, pool::PoolStats};

/// Identity every transport carries. Capabilities live in separate traits.
pub trait Transport {
    /// Stable backend name, used as metric label and in errors.
    fn name(&self) -> &'static str;
}

/// Datagram receive: each frame holds one UDP payload.
pub trait DatagramRecv: Transport {
    /// Owned received datagram. Holds its buffer until dropped.
    type Frame: AsRef<[u8]> + Send + 'static;

    /// Take up to `out.spare()` datagrams into `out`, returning count pushed.
    ///
    /// Precondition: `out.spare() > 0` (caller drains first; implementations
    /// debug-assert it), so `Ok(0)` always means idle, never no room.
    ///
    /// # Errors
    ///
    /// [`TransportError::PoolExhausted`]: data pending, no free buffer (only
    /// backends that can observe it). [`TransportError::Io`]: socket or ring failed.
    fn recv_burst(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<usize, TransportError>;

    /// Occupancy of pool backing [`Self::Frame`].
    fn pool_stats(&self) -> PoolStats;
}

/// Link-layer receive: each frame holds one whole Ethernet frame.
pub trait L2Recv: Transport {
    /// Owned received Ethernet frame. Holds its buffer until dropped.
    type Frame: AsRef<[u8]> + Send + 'static;

    /// Take up to `out.spare()` frames into `out`, returning count pushed.
    ///
    /// Precondition: `out.spare() > 0` (caller drains first; implementations
    /// debug-assert it), so `Ok(0)` always means idle, never no room.
    ///
    /// # Errors
    ///
    /// [`TransportError::PoolExhausted`]: data pending, no free buffer (only
    /// backends that can observe it). [`TransportError::Io`]: ring or device failed.
    fn recv_burst(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<usize, TransportError>;

    /// Occupancy of pool backing [`Self::Frame`].
    fn pool_stats(&self) -> PoolStats;
}

/// Addressed datagram send, synchronous and non-blocking.
pub trait DatagramSend: Transport {
    /// Send `buf` as one datagram to `to`, returning bytes sent. Never blocks.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] with stage `"send_to"`. Full socket buffer has
    /// `error.kind() == WouldBlock` (EAGAIN, or ENOBUFS on BSD-derived stacks);
    /// caller decides whether to retry.
    fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> Result<usize, TransportError>;
}

/// Multicast group membership.
pub trait Multicast: Transport {
    /// Join `group` on interface `iface`.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] when backend cannot honour `iface`;
    /// [`TransportError::Io`] when OS rejects join.
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError>;
}

/// Byte-stream receive into caller-owned, possibly uninitialised memory.
///
/// # Safety
///
/// `recv_into` must return `Ok(n)` only after initialising exactly `dst[..n]`,
/// with `n <= dst.len()`. Callers mark those `n` bytes initialised (for example
/// `set_len` on decode buffer), so wrong `n` exposes uninitialised memory.
pub unsafe trait StreamRecv: Transport {
    /// Land ready bytes into `dst`, returning count initialised.
    ///
    /// `Ok(0)` means nothing ready, or `dst` empty. Peer close is never `Ok(0)`.
    ///
    /// # Errors
    ///
    /// [`TransportError::PeerClosed`] once peer has closed;
    /// [`TransportError::Io`] when socket fails.
    fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError>;
}

/// Asynchronous stream send of whole buffer.
pub trait StreamSend: Transport {
    /// Write all of `buf`; resolves only once every byte is written.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when write fails; bytes before failure stay sent.
    fn send_all(&mut self, buf: &[u8]) -> impl Future<Output = Result<(), TransportError>> + Send;
}

/// Synchronous, non-blocking partial stream write.
pub trait StreamTrySend: Transport {
    /// Write as much of `buf` as socket accepts now, returning bytes written.
    ///
    /// `Ok(0)` when nothing fits yet; caller retries on next poll or writable
    /// readiness, resuming from byte returned.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when write fails for any reason other than
    /// would-block.
    fn try_send(&mut self, buf: &[u8]) -> Result<usize, TransportError>;
}

/// Readiness for backends whose readiness is genuinely asynchronous.
///
/// Busy-poll and kernel-bypass backends do not implement it.
pub trait AsyncReady: Transport {
    /// Resolve once next synchronous receive can make progress. Never blocks
    /// polling thread.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when readiness registration or wait fails.
    fn ready(&mut self) -> impl Future<Output = Result<(), TransportError>> + Send;
}

/// Fixed-capacity burst container, reused across bursts.
///
/// Capacity is set once and never grows: backends push only while
/// [`spare`](Self::spare) is non-zero, so steady-state receive allocates
/// nothing. No `Default`: zero capacity is unrepresentable.
#[derive(Debug)]
pub struct FrameBatch<F> {
    frames: Vec<F>,
    // requested capacity; `Vec` may reserve more, `spare` must not see it
    cap: usize,
}

impl<F> FrameBatch<F> {
    /// Allocate room for exactly `cap` frames.
    ///
    /// # Panics
    ///
    /// When `cap` frames exceed `isize::MAX` bytes, as [`Vec::with_capacity`] does.
    pub fn with_capacity(cap: NonZeroUsize) -> Self {
        Self {
            frames: Vec::with_capacity(cap.get()),
            cap: cap.get(),
        }
    }

    /// Frames that fit before batch is full. Bounds every burst.
    pub fn spare(&self) -> usize {
        self.cap.saturating_sub(self.frames.len())
    }

    /// Frames held, filled by burst and not yet drained.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// True when no frame is held.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    // held frames, oldest first; bypass shell sums burst bytes from them
    #[cfg(feature = "observability")]
    pub(crate) fn frames(&self) -> &[F] {
        &self.frames
    }

    /// Append one received frame. Backends call this only while `spare() > 0`,
    /// so correct backend never makes batch reallocate.
    ///
    /// # Panics
    ///
    /// Debug builds panic when batch is full.
    pub fn push(&mut self, frame: F) {
        debug_assert!(self.spare() > 0, "FrameBatch::push on full batch");
        self.frames.push(frame);
    }

    /// Yield held frames by value, keeping allocation for next burst.
    pub fn drain(&mut self) -> impl Iterator<Item = F> + '_ {
        self.frames.drain(..)
    }
}
