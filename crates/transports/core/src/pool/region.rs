//! Page-aligned, zero-initialised heap region, freed on drop.
//!
//! Kernel or NIC may write any slot while another slot is read, so region hands
//! out only its raw base. Never build `&[u8]`/`&mut [u8]` over whole region or
//! over slot caller does not own; derive slot pointers from [`Region::base`].

use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
};

use crate::error::TransportError;

/// Base alignment and length granule. Core makes no syscall, so page size is
/// not queried: 64 KiB covers 4 KiB, 16 KiB and 64 KiB pages.
const ALIGN: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) struct Region {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: region owns its allocation outright and exposes only raw base
// pointer, never reference over bytes; dealloc from any thread is fine for
// global allocator. Access to bytes is synchronised by `IndexPool::frame` contract.
unsafe impl Send for Region {}
// SAFETY: `&Region` yields only copy of base pointer; see `Send` above.
unsafe impl Sync for Region {}

impl Region {
    /// Zeroed region of at least `len` bytes, length rounded up to [`ALIGN`].
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] naming `field` when rounded length
    /// overflows or exceeds allocation limit, or allocator returns null.
    pub(crate) fn zeroed(field: &'static str, len: usize) -> Result<Self, TransportError> {
        // max(1): zero-size layout is UB for `alloc_zeroed`
        let layout = len
            .max(1)
            .checked_next_multiple_of(ALIGN)
            .and_then(|size| Layout::from_size_align(size, ALIGN).ok())
            .ok_or(TransportError::InvalidConfig {
                field,
                reason: "region above allocation limit",
            })?;
        // SAFETY: layout size non-zero (at least `ALIGN`), align power of two.
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).ok_or(TransportError::InvalidConfig {
            field,
            reason: "region allocation failed",
        })?;
        Ok(Self { ptr, layout })
    }

    /// Start of region, aligned to [`ALIGN`].
    pub(crate) fn base(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `alloc_zeroed` with exactly `layout`; drop runs once.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::Region;
    use crate::TransportError;

    #[test]
    fn zeroed_rejects_length_whose_round_up_overflows() {
        assert!(matches!(
            Region::zeroed("slots", usize::MAX),
            Err(TransportError::InvalidConfig { field: "slots", .. })
        ));
    }
}
