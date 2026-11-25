//! Heap slab pool for socket backends. Pure memory: kernel never sees a slab.
//!
//! Every slab allocated and zeroed up front; acquire pops, drop pushes back,
//! so steady state allocates nothing. Write access lives in [`super::backend`].

use std::{
    fmt, mem,
    num::NonZeroUsize,
    sync::{Arc, Mutex, PoisonError},
};

use super::PoolStats;
use crate::error::TransportError;

/// Free list shared by pool and its outstanding slabs.
type FreeList = Arc<Mutex<Vec<Box<[u8]>>>>;

/// Fixed set of equal-size heap slabs.
///
/// Consumers read [`stats`](Self::stats) only; backends take slabs through
/// [`backend::acquire`](super::backend::acquire).
pub struct VecPool {
    pub(super) free: FreeList,
    capacity: usize,
}

impl VecPool {
    /// Allocate `slab_count` zeroed slabs of `slab_size` bytes each.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] when slabs or free list exceed
    /// allocation limit or allocator fails.
    pub fn new(slab_count: NonZeroUsize, slab_size: NonZeroUsize) -> Result<Self, TransportError> {
        const TOO_BIG: TransportError = TransportError::InvalidConfig {
            field: "slab_count * slab_size",
            reason: "pool cannot be allocated",
        };
        // try_reserve: oversized config returns error, never panics or aborts
        let mut free = Vec::new();
        free.try_reserve_exact(slab_count.get())
            .map_err(|_| TOO_BIG)?;
        for _ in 0..slab_count.get() {
            let mut slab = Vec::new();
            slab.try_reserve_exact(slab_size.get())
                .map_err(|_| TOO_BIG)?;
            slab.resize(slab_size.get(), 0);
            free.push(slab.into_boxed_slice());
        }
        Ok(Self {
            free: Arc::new(Mutex::new(free)),
            capacity: slab_count.get(),
        })
    }

    /// Slab count and slabs currently out.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            capacity: self.capacity,
            // pool not `Clone`: one strong ref is pool's, every other a live slab
            in_use: Arc::strong_count(&self.free) - 1,
        }
    }
}

impl fmt::Debug for VecPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecPool")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// One slab taken from [`VecPool`]; drop returns it.
///
/// [`AsRef`] yields filled bytes only. Buffer always fully initialised (zeroed
/// at creation, only ever written with bytes), so reads need no `unsafe`.
pub struct VecSlab {
    pub(super) buf: Box<[u8]>,
    // invariant: `len <= buf.len()`, kept by `backend::set_len` clamp
    pub(super) len: usize,
    pub(super) free: FreeList,
}

impl AsRef<[u8]> for VecSlab {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl fmt::Debug for VecSlab {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecSlab")
            .field("len", &self.len)
            .field("size", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl Drop for VecSlab {
    #[inline]
    fn drop(&mut self) {
        // empty `Box<[u8]>` does not allocate
        let buf = mem::take(&mut self.buf);
        // push/pop never leave list torn, so poisoned list still valid;
        // capacity = slab count, so push never allocates
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(buf);
    }
}
