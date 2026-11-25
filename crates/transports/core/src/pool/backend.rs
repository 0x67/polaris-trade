//! Pool access for backend crates only.
//!
//! Hidden from docs: consumers see [`PoolStats`](super::PoolStats), never pool
//! memory. Slabs here are heap memory kernel never sees, so every function is
//! sound for any caller; path import marks backend-only use.

use std::sync::{Arc, PoisonError};

use super::{VecPool, VecSlab};

/// Take one free slab, filled length zero. `None` when every slab is out.
#[inline]
pub fn acquire(pool: &VecPool) -> Option<VecSlab> {
    // poisoned list still valid: pop/push never leave it torn
    let buf = pool
        .free
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .pop()?;
    Some(VecSlab {
        buf,
        len: 0,
        free: Arc::clone(&pool.free),
    })
}

/// Whole slab buffer, for receive to land datagram into.
#[inline]
pub fn buf_mut(slab: &mut VecSlab) -> &mut [u8] {
    &mut slab.buf
}

/// Record `len` bytes received into [`buf_mut`]; bounds slab's `as_ref`.
///
/// Clamped to slab size in release builds.
///
/// # Panics
///
/// Debug builds panic when `len` exceeds slab size.
#[inline]
pub fn set_len(slab: &mut VecSlab, len: usize) {
    debug_assert!(len <= slab.buf.len(), "filled length above slab size");
    slab.len = len.min(slab.buf.len());
}
