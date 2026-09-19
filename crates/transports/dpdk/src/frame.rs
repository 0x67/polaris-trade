//! Received Ethernet frame owning its mbuf.

use std::{ffi::c_void, slice};

use crate::ffi;

/// One received Ethernet frame. Owns one single-segment mbuf, freed on drop.
///
/// Drop may run on any thread: mempool put is multi-producer, per
/// [`DpdkL2::attach`](crate::DpdkL2::attach) contract.
#[derive(Debug)]
pub struct MbufFrame {
    mbuf: *mut c_void,
    // mtod of `mbuf`, cached so `as_ref` makes no FFI call
    data: *const u8,
    len: u16,
}

// SAFETY: frame is sole owner of `mbuf`; bytes stay put until drop, and drop
// frees through multi-producer mempool put, sound from any thread.
unsafe impl Send for MbufFrame {}

impl MbufFrame {
    /// # Safety
    ///
    /// `mbuf` is live single-segment mbuf nobody else owns or frees; `data`
    /// and `len` are its `rte_pktmbuf_mtod` and `rte_pktmbuf_data_len`.
    pub(crate) unsafe fn new(mbuf: *mut c_void, data: *const u8, len: u16) -> Self {
        Self { mbuf, data, len }
    }
}

impl AsRef<[u8]> for MbufFrame {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `new` contract: `data..data + len` is initialised packet data
        // of live mbuf this frame owns, unchanged until drop frees it.
        unsafe { slice::from_raw_parts(self.data, usize::from(self.len)) }
    }
}

impl Drop for MbufFrame {
    fn drop(&mut self) {
        // SAFETY: frame owns `mbuf` (`new` contract) and frees it exactly once, here.
        unsafe { ffi::polaris_dpdk_free(self.mbuf) }
    }
}
