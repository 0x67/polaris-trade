//! Feature-gated observability seam: per-stage pipeline latency timers and a
//! throughput counter. The recorder install and the runtime gate flip live in
//! the binary, so the library only emits through the metrics facade.
//!
//! Every public symbol has a no-op twin under `not(feature = "observability")`
//! so the hot path and wiring compile identically either way. A default build
//! links no observability crate; the timers vanish. With the feature on the
//! runtime gate still defaults off (one atomic load per timer, no clock read)
//! until the host arms `observability_core::set_metrics_enabled`.
//!
//! The runtime seam times four stages: `feed` (parse + normalize, fused in the
//! adapter with no decode-only entry), `book.apply` (core `BookEngine` apply),
//! `book.derive` (`DeriveEngine` reading one MBP10 and BBO off the applied
//! book), and `egress` (stamp + publish). Apply and derive are timed apart so
//! each cost shows on its own. The finer parse-vs-normalize split needs a
//! decode-only replay pass, which `benches/nasdaq_pipeline.rs` runs.

/// Feed-stage latency: `adapter.feed`, i.e. wire parse plus normalization.
pub const STAGE_FEED: &str = "pipeline.feed.latency";
/// Book apply-stage latency: core `BookEngine::apply_mbo` over one MBO segment.
pub const STAGE_BOOK_APPLY: &str = "pipeline.book.apply.latency";
/// Book derive-stage latency: `DeriveEngine::apply_mbo_batch` over one MBO
/// segment, one MBP10 and BBO read off the applied book. Timed apart from apply
/// so the read shows as its own cost, not fused into the core apply.
pub const STAGE_BOOK_DERIVE: &str = "pipeline.book.derive.latency";
/// Egress-stage latency: header stamp and `EgressSequencer::publish`.
pub const STAGE_EGRESS: &str = "pipeline.egress.latency";
/// Throughput counter: routed records per frame. Only the feature-on recorder
/// path names it; a default build routes through the no-op `record_records`.
#[cfg(feature = "observability")]
pub const THROUGHPUT: &str = "pipeline.records";

#[cfg(feature = "observability")]
mod imp {
    /// RAII stage timer wrapping `observability_core::Timer`. Held only for its
    /// drop, which records the elapsed sample; the field is never read.
    pub struct StageTimer(#[allow(dead_code)] observability_core::Timer);

    /// RAII latency timer for one pipeline stage. Reads the clock only when the
    /// gate is on; records elapsed into `metric`'s histogram on drop.
    #[inline]
    pub fn stage_timer(metric: &'static str) -> StageTimer {
        StageTimer(observability_core::timer(metric))
    }

    /// Add `n` to the throughput counter. Facade no-op if no recorder installed.
    #[inline]
    pub fn record_records(n: u64) {
        metrics::counter!(super::THROUGHPUT).increment(n);
    }
}

#[cfg(not(feature = "observability"))]
mod imp {
    /// Zero-sized no-op stage timer; records nothing on drop.
    pub struct StageTimer;

    /// No-op timer: records nothing on drop.
    #[inline]
    pub fn stage_timer(_metric: &'static str) -> StageTimer {
        StageTimer
    }

    /// No-op throughput sink.
    #[inline]
    pub fn record_records(_n: u64) {}
}

pub use imp::{record_records, stage_timer};
