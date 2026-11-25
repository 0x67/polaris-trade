//! Receive-side metrics seam shared by every backend.
//!
//! Metric names and runtime gate live here once, so backends never drift. Each
//! call is no-op while [`observability_core::metrics_enabled`] is false.

/// Metric names emitted on receive path. Prometheus maps `.` to `_` at scrape.
pub mod metric {
    /// Frames received, label `backend`. Monotonic counter.
    pub const RECV_PACKETS: &str = "transport.recv.packets";
    /// Bytes received, label `backend`. Monotonic counter.
    pub const RECV_BYTES: &str = "transport.recv.bytes";
    /// Frames lost before reaching caller, labels `backend` and `reason`. Monotonic counter.
    pub const RECV_DROPS: &str = "transport.recv.drops";
}

/// Why receive path lost frames: `reason` label of [`metric::RECV_DROPS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// Data arrived while no buffer was free.
    NoBuffer,
    /// NIC dropped frame before it reached receive ring.
    NicMissed,
    /// Frame longer than buffer it landed in.
    Truncated,
    /// L2 frame did not match decap filter.
    DecapFiltered,
}

impl DropReason {
    /// Value of `reason` label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::NoBuffer => "no_buffer",
            Self::NicMissed => "nic_missed",
            Self::Truncated => "truncated",
            Self::DecapFiltered => "decap_filtered",
        }
    }
}

/// Record one receive burst of `packets` frames totalling `bytes`.
///
/// Empty burst returns before gate read, so idle spin pays one compare.
#[inline]
pub fn record_recv_burst(backend: &'static str, packets: u64, bytes: u64) {
    if packets == 0 || !observability_core::metrics_enabled() {
        return;
    }
    metrics::counter!(metric::RECV_PACKETS, "backend" => backend).increment(packets);
    metrics::counter!(metric::RECV_BYTES, "backend" => backend).increment(bytes);
}

/// Record `delta` new drops of `reason`: increase of backend's monotonic drop
/// counter since its last read. Zero returns before gate read.
#[inline]
pub fn record_drops(backend: &'static str, reason: DropReason, delta: u64) {
    if delta == 0 || !observability_core::metrics_enabled() {
        return;
    }
    metrics::counter!(metric::RECV_DROPS, "backend" => backend, "reason" => reason.label())
        .increment(delta);
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::DebuggingRecorder;

    use super::*;

    #[test]
    fn zero_counts_record_nothing_nonzero_counts_record() {
        observability_core::set_metrics_enabled(true);
        observability_core::refresh_thread_gate();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            record_recv_burst("mock", 0, 0);
            record_drops("mock", DropReason::NoBuffer, 0);
            let empty = snapshotter.snapshot().into_vec();
            assert!(empty.is_empty(), "zero counts must register no series");

            // control: same gate and recorder record non-zero counts
            record_recv_burst("mock", 3, 192);
            record_drops("mock", DropReason::DecapFiltered, 2);
        });
        let mut series = Vec::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let labels: Vec<_> = key.key().labels().map(|l| (l.key(), l.value())).collect();
            series.push(format!("{} {labels:?} {value:?}", key.key().name()));
        }
        series.sort_unstable();
        assert_eq!(
            series,
            [
                r#"transport.recv.bytes [("backend", "mock")] Counter(192)"#,
                r#"transport.recv.drops [("backend", "mock"), ("reason", "decap_filtered")] Counter(2)"#,
                r#"transport.recv.packets [("backend", "mock")] Counter(3)"#,
            ]
        );
    }
}
