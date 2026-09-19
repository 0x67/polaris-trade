//! Index pool: one contiguous region of fixed-stride slots for kernel-bypass drivers.
//!
//! Slot state implicit: slot sits in driver hands (kernel or NIC ring, free
//! list, freed queue) or in exactly one [`IndexFrame`]. Slot moves to frame only
//! through [`IndexPool::frame`] on reap and back only through
//! [`IndexPool::drain_freed`] on recycle.

use std::{
    fmt, mem,
    num::NonZeroU32,
    slice,
    sync::{Arc, Mutex, PoisonError},
};

use super::{PoolStats, region::Region};
use crate::{config::validate, error::TransportError};

/// Config field named when region size is rejected.
const SIZE_FIELD: &str = "count * stride";

#[derive(Debug)]
struct Shared {
    region: Region,
    stride: u32,
    count: u32,
    // slots of dropped frames; never more than `count` entries
    freed: Mutex<Vec<u32>>,
}

/// `count` slots of `stride` bytes over one page-aligned, zeroed region.
///
/// No `acquire` and not `Clone`: driver alone decides which slots kernel has
/// handed back, and mints frames for them with `unsafe` [`frame`](Self::frame).
#[derive(Debug)]
pub struct IndexPool {
    shared: Arc<Shared>,
}

impl IndexPool {
    /// Allocate zeroed region of `count * stride` bytes.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] naming `count * stride` when size
    /// overflows `usize`, exceeds allocation limit or allocator fails (size
    /// checked before any allocation); naming `count` when freed list fails.
    pub fn new(count: NonZeroU32, stride: NonZeroU32) -> Result<Self, TransportError> {
        let len =
            validate::checked_product(SIZE_FIELD, count.get() as usize, stride.get() as usize)?;
        let region = Region::zeroed(SIZE_FIELD, len)?;
        let mut freed = Vec::new();
        freed.try_reserve_exact(count.get() as usize).map_err(|_| {
            TransportError::InvalidConfig {
                field: "count",
                reason: "freed list cannot be allocated",
            }
        })?;
        Ok(Self {
            shared: Arc::new(Shared {
                region,
                stride: stride.get(),
                count: count.get(),
                freed: Mutex::new(freed),
            }),
        })
    }

    /// Slot count and live frames. Slots in driver hands count as free.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            capacity: self.shared.count as usize,
            // pool not `Clone`, no `Weak`: one strong ref is pool's, every other a live frame
            in_use: Arc::strong_count(&self.shared) - 1,
        }
    }

    /// Region base, for kernel or NIC registration. Slot `s` starts at
    /// `base + s * stride`.
    ///
    /// Region lives until pool and every frame drop; driver keeps pool alive
    /// until kernel or NIC unregisters region.
    pub fn base(&self) -> *mut u8 {
        self.shared.region.base()
    }

    /// Frame over `len` bytes at `offset` in `slot`. Drop queues `slot` for
    /// [`drain_freed`](Self::drain_freed). Debug builds panic on out-of-bounds input.
    ///
    /// # Safety
    ///
    /// `slot < count`; caller owns `slot` (it is in no live [`IndexFrame`] and
    /// in no kernel or NIC ring); `offset + len <= stride`; kernel or NIC has
    /// finished writing those bytes.
    #[inline]
    pub unsafe fn frame(&self, slot: u32, offset: u32, len: u32) -> IndexFrame {
        debug_assert!(slot < self.shared.count, "slot out of range");
        debug_assert!(
            offset
                .checked_add(len)
                .is_some_and(|end| end <= self.shared.stride),
            "frame runs past slot stride"
        );
        IndexFrame {
            shared: Arc::clone(&self.shared),
            slot,
            offset,
            len,
        }
    }

    /// Swap slots freed since last call into `into`.
    ///
    /// O(1) critical section. Steady state allocates nothing when caller
    /// passes empty vector with capacity at least `count`, consumes slots,
    /// clears it and passes it again.
    ///
    /// # Panics
    ///
    /// Debug builds panic when `into` is not empty.
    #[inline]
    pub fn drain_freed(&self, into: &mut Vec<u32>) {
        debug_assert!(into.is_empty(), "drain_freed into non-empty vector");
        // push/swap never leave list torn, so poisoned list still valid
        let mut freed = self
            .shared
            .freed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        mem::swap(&mut *freed, into);
    }
}

/// Bytes of one reaped slot. Drop queues slot for recycle.
pub struct IndexFrame {
    shared: Arc<Shared>,
    slot: u32,
    offset: u32,
    len: u32,
}

impl AsRef<[u8]> for IndexFrame {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        // bounded by `frame` contract: slot < count, offset + len <= stride
        let start = self.slot as usize * self.shared.stride as usize + self.offset as usize;
        // SAFETY: `start` lies inside region (`frame` contract; `count * stride`
        // fit region at construction), so `add` stays in bounds.
        let ptr = unsafe { self.shared.region.base().add(start) };
        // SAFETY: `start..start + len` inside region and inside slot this frame
        // owns; region zeroed at creation, so every byte initialised; `frame`
        // contract says kernel or NIC finished writing, and nobody writes owned
        // slot until drop recycles it.
        unsafe { slice::from_raw_parts(ptr, self.len as usize) }
    }
}

impl fmt::Debug for IndexFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexFrame")
            .field("slot", &self.slot)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl Drop for IndexFrame {
    #[inline]
    fn drop(&mut self) {
        // poisoned list still valid (push/swap never tear it); push allocation-free
        // while `drain_freed` callers keep capacity >= `count`
        self.shared
            .freed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.slot);
    }
}
