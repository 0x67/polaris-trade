//! Port and queue a DPDK transport polls. Caller configures both before attach.

use std::num::NonZeroU16;

const DEFAULT_BURST: NonZeroU16 = NonZeroU16::new(32).unwrap();

/// Receive queue to poll and burst bound. Required fields are `new` arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DpdkConfig {
    /// DPDK port id, configured and started by caller.
    pub port: u16,
    /// Receive queue on `port`, set up by caller; one thread polls it.
    pub queue: u16,
    /// Most mbufs one `rte_eth_rx_burst` takes; sizes receive scratch. Default 32.
    ///
    /// Vector PMDs round request down to multiple of 4 or 8, so smaller request
    /// yields nothing: keep burst batch drained so each call offers full `burst`.
    pub burst: NonZeroU16,
}

impl DpdkConfig {
    /// Queue `queue` of port `port`, burst 32.
    pub fn new(port: u16, queue: u16) -> Self {
        Self {
            port,
            queue,
            burst: DEFAULT_BURST,
        }
    }
}
