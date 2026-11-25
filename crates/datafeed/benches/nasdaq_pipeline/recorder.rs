//! Capturing `metrics` recorder: routes the session's four stage histograms
//! into local hdr state the bench reads after the run. Samples go straight into
//! hdr and a running window sum, and none are retained. A stage keeps one mean
//! per [`WINDOW_SAMPLES`] samples, so memory stays near flat over a long capture.

use std::sync::{Arc, Mutex};

use hdrhistogram::Histogram;
use metrics::{
    Counter, Gauge, Histogram as MetricsHistogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};

// Stage metric names, repeated from `src/obs.rs` because that module is
// private. `main` fails a run in which one of them recorded nothing.
pub const STAGE_FEED: &str = "pipeline.feed.latency";
pub const STAGE_BOOK_APPLY: &str = "pipeline.book.apply.latency";
pub const STAGE_BOOK_DERIVE: &str = "pipeline.book.derive.latency";
pub const STAGE_EGRESS: &str = "pipeline.egress.latency";

const STAGES: [&str; 4] = [
    STAGE_FEED,
    STAGE_BOOK_APPLY,
    STAGE_BOOK_DERIVE,
    STAGE_EGRESS,
];

const LOW_NS: u64 = 1;
const HIGH_NS: u64 = 60_000_000_000;
const SIGFIGS: u8 = 3;

/// Samples behind each entry of [`StageSamples::window_means_ns`].
pub const WINDOW_SAMPLES: u64 = 1_000_000;

/// What one stage recorded: every sample in `hist`, and the mean of each full
/// window of [`WINDOW_SAMPLES`] samples in arrival order. The samples of a
/// partial last window are in `hist` only.
#[derive(Clone)]
pub struct StageSamples {
    pub hist: Histogram<u64>,
    pub window_means_ns: Vec<f64>,
    window_sum_ns: f64,
    window_len: u64,
}

struct StageSink(Mutex<StageSamples>);

impl StageSink {
    fn new() -> Self {
        let hist = Histogram::new_with_bounds(LOW_NS, HIGH_NS, SIGFIGS)
            .expect("1ns..60s at 3 significant figures is a valid hdr range");
        StageSink(Mutex::new(StageSamples {
            hist,
            window_means_ns: Vec::new(),
            window_sum_ns: 0.0,
            window_len: 0,
        }))
    }
}

impl HistogramFn for StageSink {
    // A stage timer calls this after its end clock read, so the window sum is
    // outside every sample. It allocates only when a window closes.
    fn record(&self, value: f64) {
        // stage timers record elapsed seconds; the hdr range is in ns
        let ns = ((value * 1e9).round() as u64).max(LOW_NS);
        let mut samples = self
            .0
            .lock()
            .expect("the bench records on one thread, so no holder can poison the lock");
        samples.hist.saturating_record(ns);

        samples.window_sum_ns += ns as f64;
        samples.window_len += 1;
        if samples.window_len == WINDOW_SAMPLES {
            let mean = samples.window_sum_ns / WINDOW_SAMPLES as f64;
            samples.window_means_ns.push(mean);
            samples.window_sum_ns = 0.0;
            samples.window_len = 0;
        }
    }
}

/// Recorder for the four stage histograms. Clones share the same sinks, so the
/// bench installs one clone globally and reads the stages off another.
#[derive(Clone)]
pub struct StageRecorder {
    sinks: [Arc<StageSink>; 4],
}

impl StageRecorder {
    pub fn new() -> Self {
        StageRecorder {
            sinks: std::array::from_fn(|_| Arc::new(StageSink::new())),
        }
    }

    /// Copy of what one stage recorded. `name` is one of the `STAGE_*` names.
    pub fn stage(&self, name: &str) -> StageSamples {
        let index = STAGES
            .iter()
            .position(|stage| *stage == name)
            .expect("stage name is one of the STAGE_* constants");
        self.sinks[index]
            .0
            .lock()
            .expect("the bench records on one thread, so no holder can poison the lock")
            .clone()
    }
}

impl Recorder for StageRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    // A stage timer calls this on every drop, before it reads the clock, so the
    // lookup is inside each sample. It takes no lock and allocates nothing.
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> MetricsHistogram {
        match STAGES.iter().position(|stage| *stage == key.name()) {
            Some(index) => MetricsHistogram::from_arc(Arc::clone(&self.sinks[index])),
            None => MetricsHistogram::noop(),
        }
    }
}
