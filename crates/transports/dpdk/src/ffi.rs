//! Bindings to `csrc/shim.c` plus one exported DPDK function.
//!
//! `rte_mbuf` and `rte_mempool` stay opaque: Rust only hands their pointers back.

use std::ffi::{c_uint, c_void};

// signatures mirror csrc/shim.c and DPDK's `rte_mbuf.h`
unsafe extern "C" {
    /// `rte_eth_rx_burst` of at most `nb` mbufs into `mbufs`, then each one's
    /// data pointer, data length and segment count into `data`, `len`,
    /// `nb_segs`. Returns count received; every array must hold `nb` entries.
    pub(crate) fn polaris_dpdk_rx_burst(
        port: u16,
        queue: u16,
        mbufs: *mut *mut c_void,
        data: *mut *const u8,
        len: *mut u16,
        nb_segs: *mut u16,
        nb: u16,
    ) -> u16;

    /// `rte_pktmbuf_free` of one mbuf.
    pub(crate) fn polaris_dpdk_free(mbuf: *mut c_void);

    /// Port-wide `imissed` and `rx_nombuf` from `rte_eth_stats_get`; outputs
    /// untouched when it fails.
    pub(crate) fn polaris_dpdk_rx_drops(port: u16, imissed: *mut u64, rx_nombuf: *mut u64);

    /// Mempool size and `rte_mempool_in_use_count`, which walks every lcore cache.
    pub(crate) fn polaris_dpdk_pool_stats(
        mempool: *const c_void,
        capacity: *mut c_uint,
        in_use: *mut c_uint,
    );

    /// Exported by `librte_mbuf`: frees every segment of each of `count` mbufs.
    pub(crate) fn rte_pktmbuf_free_bulk(mbufs: *mut *mut c_void, count: c_uint);
}
