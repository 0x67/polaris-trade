//! Kernel-bypass shell shared by every ring or poll-mode driver.
//!
//! [`Driver`] owns ring, buffers and counters. [`BypassTransport`] wraps it and
//! does what every backend would repeat: burst telemetry, drop-counter deltas
//! every [`STATS_EVERY`] calls, [`Reap::Exhausted`] with nothing pushed as
//! [`TransportError::PoolExhausted`], and deferral of error met after frames
//! were pushed. Driver [`Layer`] picks receive trait: [`L4`] yields
//! [`DatagramRecv`], [`L2`] yields [`L2Recv`] for [`UdpDecap`](crate::decap::UdpDecap).

#[cfg(feature = "testing")]
mod mock;

#[cfg(feature = "testing")]
pub use mock::MockDriver;

use crate::{DatagramRecv, FrameBatch, L2Recv, PoolStats, Transport, TransportError};

/// `recv_burst` calls between drop-counter reads while metrics gate is on.
///
/// Bounds report lag, so drops during all-empty stretch still surface, and
/// keeps counter reads that cost syscall (`AF_XDP`, DPDK) off every idle spin.
pub const STATS_EVERY: u32 = 1024;

/// Outcome of one [`Driver::reap`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reap {
    /// Pushed this many frames, at least one.
    Frames(usize),
    /// Ring empty, nothing pushed.
    Idle,
    /// Data pending but no free buffer. Frames pushed before driver ran out
    /// still count.
    Exhausted,
}

/// Driver counters, each monotonic. May start above zero (DPDK port counters
/// count from port start): shell reports only increases since its first read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DriverStats {
    /// Frames lost because no buffer was free.
    pub no_buffer: u64,
    /// Frames NIC dropped before they reached receive ring.
    pub nic_missed: u64,
    /// Frames longer than their buffer, dropped.
    pub truncated: u64,
    /// Syscalls `reap` made (`io_uring_enter`, `AF_XDP` wakeup), so idle-spin
    /// test can assert it stays flat.
    pub syscalls: u64,
}

/// Marker for frame content: picks receive trait [`BypassTransport`] implements.
pub trait Layer {}

/// Frames hold UDP payloads: [`BypassTransport`] implements [`DatagramRecv`].
#[derive(Debug)]
pub struct L4;

/// Frames hold whole Ethernet frames: [`BypassTransport`] implements [`L2Recv`].
#[derive(Debug)]
pub struct L2;

impl Layer for L4 {}
impl Layer for L2 {}

/// Backend ring or poll-mode device under [`BypassTransport`].
///
/// Pooled drivers recycle buffers of dropped frames at start of each `reap`.
pub trait Driver: Send {
    /// Owned received frame. Holds its buffer until dropped.
    type Frame: AsRef<[u8]> + Send + 'static;
    /// [`L4`] or [`L2`].
    type Layer: Layer;
    /// Backend name: [`Transport::name`] and metric label.
    const BACKEND: &'static str;

    /// Move completed frames into `out`.
    ///
    /// Never blocks; only pushes, at most `out.spare()` frames. Keeps consuming
    /// ring until it pushes frame, fills `out` or ring runs empty, so burst of
    /// only truncated or chained packets never reads as idle while more wait.
    ///
    /// # Errors
    ///
    /// Ring or device failure, possibly after frames were pushed this call;
    /// [`BypassTransport`] then returns frames first and error on next call.
    fn reap(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<Reap, TransportError>;

    /// Current counters.
    fn stats(&self) -> DriverStats;

    /// Occupancy of pool backing [`Self::Frame`].
    fn pool_stats(&self) -> PoolStats;
}

/// Receive transport over one [`Driver`]: [`DatagramRecv`] for [`L4`] drivers,
/// [`L2Recv`] for [`L2`] drivers.
///
/// Backends keep it private inside their own type, so users never reach real driver.
#[derive(Debug)]
pub struct BypassTransport<D: Driver> {
    driver: D,
    // met after frames were pushed; returned before next reap
    deferred: Option<TransportError>,
    // driver counters at last drop report
    #[cfg(feature = "observability")]
    last: DriverStats,
    // calls since last drop report, counted only while gate on
    #[cfg(feature = "observability")]
    calls: u32,
}

impl<D: Driver> BypassTransport<D> {
    /// Wrap `driver`.
    pub fn new(driver: D) -> Self {
        Self {
            #[cfg(feature = "observability")]
            last: driver.stats(),
            #[cfg(feature = "observability")]
            calls: 0,
            driver,
            deferred: None,
        }
    }

    /// Driver counters.
    pub fn stats(&self) -> DriverStats {
        self.driver.stats()
    }

    /// Wrapped driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// Wrapped driver, mutably, e.g. to inject into mock.
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }

    fn recv(&mut self, out: &mut FrameBatch<D::Frame>) -> Result<usize, TransportError> {
        debug_assert!(out.spare() > 0, "BypassTransport::recv_burst on full batch");
        #[cfg(feature = "observability")]
        let before = out.len();
        let result = self.reap(out);
        #[cfg(feature = "observability")]
        self.report(&out.frames()[before..]);
        result
    }

    // frames never travel with `Err`: error after pushes waits in `deferred`
    fn reap(&mut self, out: &mut FrameBatch<D::Frame>) -> Result<usize, TransportError> {
        if let Some(error) = self.deferred.take() {
            return Err(error);
        }
        let before = out.len();
        let reaped = self.driver.reap(out);
        let pushed = out.len() - before;
        match reaped {
            Ok(Reap::Exhausted) if pushed == 0 => {
                let PoolStats { in_use, capacity } = self.driver.pool_stats();
                Err(TransportError::PoolExhausted { in_use, capacity })
            }
            Ok(reap) => {
                debug_assert!(
                    match reap {
                        Reap::Frames(n) => n == pushed,
                        Reap::Idle => pushed == 0,
                        Reap::Exhausted => true,
                    },
                    "Driver::reap returned {reap:?} after pushing {pushed} frames"
                );
                Ok(pushed)
            }
            Err(error) if pushed > 0 => {
                self.deferred = Some(error);
                Ok(pushed)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(feature = "observability")]
impl<D: Driver> BypassTransport<D> {
    // gate off: no frame walked, no counter read, no call counted
    fn report(&mut self, pushed: &[D::Frame]) {
        if !observability_core::metrics_enabled() {
            return;
        }
        if !pushed.is_empty() {
            let bytes = pushed.iter().map(|f| f.as_ref().len() as u64).sum();
            crate::telemetry::record_recv_burst(D::BACKEND, pushed.len() as u64, bytes);
        }
        self.calls += 1;
        if self.calls == STATS_EVERY {
            self.calls = 0;
            self.report_drops();
        }
    }

    fn report_drops(&mut self) {
        use crate::telemetry::{DropReason, record_drops};

        let now = self.driver.stats();
        let last = self.last;
        record_drops(
            D::BACKEND,
            DropReason::NoBuffer,
            now.no_buffer.saturating_sub(last.no_buffer),
        );
        record_drops(
            D::BACKEND,
            DropReason::NicMissed,
            now.nic_missed.saturating_sub(last.nic_missed),
        );
        record_drops(
            D::BACKEND,
            DropReason::Truncated,
            now.truncated.saturating_sub(last.truncated),
        );
        self.last = now;
    }
}

impl<D: Driver> Transport for BypassTransport<D> {
    fn name(&self) -> &'static str {
        D::BACKEND
    }
}

impl<D: Driver<Layer = L4>> DatagramRecv for BypassTransport<D> {
    type Frame = D::Frame;

    fn recv_burst(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<usize, TransportError> {
        self.recv(out)
    }

    fn pool_stats(&self) -> PoolStats {
        self.driver.pool_stats()
    }
}

impl<D: Driver<Layer = L2>> L2Recv for BypassTransport<D> {
    type Frame = D::Frame;

    fn recv_burst(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<usize, TransportError> {
        self.recv(out)
    }

    fn pool_stats(&self) -> PoolStats {
        self.driver.pool_stats()
    }
}
