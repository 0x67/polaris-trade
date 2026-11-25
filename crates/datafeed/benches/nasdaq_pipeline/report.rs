//! Result model: the JSON a run writes and `tests/bench_regression.rs` reads.

use serde::Serialize;

use crate::recorder::{StageSamples, WINDOW_SAMPLES};

/// Latency of one stage. `unit` names what one sample covers. Stages sample
/// different populations, so their means do not add up: feed per message, book
/// per MBO segment, egress per published record.
#[derive(Serialize)]
pub struct StageStats {
    pub unit: &'static str,
    pub count: u64,
    pub mean_ns: f64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    /// Samples behind each entry of `window_means_ns`.
    pub window_samples: u64,
    /// Mean of each full window of `window_samples` samples, in arrival order.
    /// A partial last window has no entry. A stage that changes speed partway
    /// through a run shows here as a step, where the percentiles of the whole
    /// run would show one mixed distribution.
    pub window_means_ns: Vec<f64>,
}

impl StageStats {
    pub fn of(samples: StageSamples, unit: &'static str) -> Self {
        let hist = &samples.hist;
        StageStats {
            unit,
            count: hist.len(),
            mean_ns: hist.mean(),
            p50_ns: hist.value_at_quantile(0.50),
            p90_ns: hist.value_at_quantile(0.90),
            p99_ns: hist.value_at_quantile(0.99),
            p999_ns: hist.value_at_quantile(0.999),
            max_ns: hist.max(),
            window_samples: WINDOW_SAMPLES,
            window_means_ns: samples.window_means_ns,
        }
    }
}

/// The low 14 bits of two addresses of the run. A stage can run faster or
/// slower with the position of the process stack, so each report records that
/// position.
#[derive(Serialize)]
pub struct Layout {
    /// A local of `main`, taken as its first statement.
    pub stack_low14: usize,
    /// The boxed adapter of the instrumented pass, a heap address.
    pub adapter_low14: usize,
}

/// Low 14 bits of the address of `place`: its offset in a 16 KiB page.
pub fn low14<T: ?Sized>(place: &T) -> usize {
    std::ptr::from_ref(place).addr() & 0x3fff
}

#[derive(Serialize)]
pub struct HostInfo {
    pub label: String,
    pub cpu: String,
    pub arch: &'static str,
    pub os: &'static str,
    pub logical_cpus: usize,
}

/// The input of a run: the capture file and the message cap applied to it.
/// `messages` is what the passes replayed, at most `max_messages`.
#[derive(Serialize)]
pub struct CaptureInfo {
    pub path: String,
    pub sha256: String,
    pub max_messages: u64,
    pub messages: u64,
}

#[derive(Serialize)]
pub struct Throughput {
    pub records: u64,
    pub wall_ns: u64,
    pub msgs_per_sec: f64,
    pub ns_per_msg: f64,
}

#[derive(Serialize)]
pub struct Stages {
    /// Decode mean over `parse_messages`, timed per batch. One decode is
    /// shorter than a monotonic-clock tick on the baseline host, so percentiles
    /// per message would only show the tick.
    pub parse_mean_ns: f64,
    pub parse_messages: u64,
    pub feed: StageStats,
    /// `BookEngine::apply_mbo` over one MBO segment.
    pub book_apply: StageStats,
    /// `DeriveEngine::apply_mbo_batch` over the same segment: one MBP10 and
    /// BBO read off the applied book. Same denominator as `book_apply`.
    pub book_derive: StageStats,
    pub egress: StageStats,
    /// `feed.mean_ns` minus `parse_mean_ns`, floored at zero.
    pub normalize_mean_ns: f64,
}

#[derive(Serialize)]
pub struct BenchReport {
    pub venue: &'static str,
    pub generated_at_unix_s: u64,
    pub host: HostInfo,
    pub capture: CaptureInfo,
    pub batch: usize,
    pub layout: Layout,
    pub throughput: Throughput,
    pub stages: Stages,
}
