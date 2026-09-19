#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! DPDK poll-mode receive over caller-initialised EAL, one mbuf per frame.
//!
//! `DpdkL2` polls one receive queue the caller configured and started, and
//! yields whole Ethernet frames through [`transport_core::L2Recv`];
//! wrap it in [`UdpDecap`](transport_core::decap::UdpDecap) to serve datagram
//! consumers. Each frame owns its mbuf and frees it on drop, from any thread.
//! Receive only: no send, no multicast join (PMD owns port, so group delivery
//! is arranged when configuring port or switch).
//!
//! | Feature | Enables |
//! | --- | --- |
//! | none | [`DpdkConfig`] only; builds on any OS with no system library |
//! | `driver-dpdk` | `DpdkL2` and `MbufFrame`; Linux with libdpdk (pkg-config) only |
//! | `observability` | receive and drop metrics through `transport_core::telemetry` |
//!
//! Backend name ([`transport_core::Transport::name`] and
//! metric label): `dpdk`.

mod config;
#[cfg(any(test, feature = "driver-dpdk"))]
mod driver;
#[cfg(feature = "driver-dpdk")]
mod ffi;
#[cfg(feature = "driver-dpdk")]
mod frame;

pub use config::DpdkConfig;
#[cfg(feature = "driver-dpdk")]
pub use frame::MbufFrame;
#[cfg(feature = "driver-dpdk")]
use transport_core::{
    FrameBatch, L2Recv, PoolStats, Transport, TransportError,
    bypass::{BypassTransport, DriverStats},
};

/// L2 receive over one DPDK port queue.
///
/// Never returns `PoolExhausted`: DPDK cannot see pending data without free
/// mbuf, so exhaustion shows as `no_buffer` (port `rx_nombuf`). `nic_missed`
/// is port `imissed`. Both are port-wide (every queue), counted since port
/// start, and read with one `rte_eth_stats_get` per [`stats`](Self::stats)
/// call, which telemetry makes every `STATS_EVERY` bursts. Chained mbufs
/// (mempool data room below frame size) are freed and counted `truncated`.
/// `pool_stats` calls `rte_mempool_in_use_count`, which walks every lcore
/// cache: debug use only, never on data path.
#[cfg(feature = "driver-dpdk")]
#[derive(Debug)]
pub struct DpdkL2(BypassTransport<driver::PmdDriver>);

#[cfg(feature = "driver-dpdk")]
impl DpdkL2 {
    /// Poll `cfg.queue` of `cfg.port`. Never calls `rte_eal_init`; configures nothing.
    ///
    /// # Safety
    /// EAL initialised; `cfg.port` started with `cfg.queue` set up on `mempool`, live
    /// `rte_mempool` without `RTE_MEMPOOL_F_SC_GET` or `RTE_MEMPOOL_F_SP_PUT` (frames
    /// free on any thread); one thread polls `cfg.queue`; `mempool` outlives transport
    /// and every `MbufFrame` it yields.
    ///
    /// # Errors
    /// [`TransportError::InvalidConfig`] (`mempool`) when null, before allocation or DPDK call.
    pub unsafe fn attach(
        cfg: &DpdkConfig,
        mempool: *mut std::ffi::c_void,
    ) -> Result<Self, TransportError> {
        if mempool.is_null() {
            return Err(TransportError::InvalidConfig {
                field: "mempool",
                reason: "null rte_mempool pointer",
            });
        }
        Ok(Self(BypassTransport::new(driver::PmdDriver::new(
            cfg, mempool,
        ))))
    }

    /// Driver counters: `no_buffer` and `nic_missed` port-wide, `truncated`
    /// this queue's chained mbufs. One `rte_eth_stats_get` per call.
    pub fn stats(&self) -> DriverStats {
        self.0.stats()
    }
}

#[cfg(feature = "driver-dpdk")]
impl Transport for DpdkL2 {
    fn name(&self) -> &'static str {
        self.0.name()
    }
}

#[cfg(feature = "driver-dpdk")]
impl L2Recv for DpdkL2 {
    type Frame = MbufFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<MbufFrame>) -> Result<usize, TransportError> {
        self.0.recv_burst(out)
    }

    fn pool_stats(&self) -> PoolStats {
        self.0.pool_stats()
    }
}
