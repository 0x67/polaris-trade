//! Feature-gated observability seam: per-stage pipeline latency timers, a
//! throughput counter, and process init that installs the metrics recorder and
//! flips the runtime gate.
//!
//! Every public symbol has a no-op twin under `not(feature = "observability")`
//! so the hot path and wiring compile identically either way. A default build
//! links no observability crate; the timers vanish. With the feature on the
//! runtime gate still defaults off (one atomic load per timer, no clock read)
//! until a config calls [`init`].
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
    use observability::{
        LogFormat, LogSink, LogSinkKind, LoggingConfig, MetricsConfig, MetricsExporter, ObsConfig,
        OtlpConfig, OtlpProtocol, PipelineConfig,
    };

    use crate::{
        AppResult,
        config::{MetricsExporterConfig, ObservabilityConfig},
    };

    /// Held for the process lifetime; dropping it tears the exporter down.
    pub struct ObsGuard(#[allow(dead_code)] observability::ObsGuard);

    /// RAII stage timer wrapping `observability_core::Timer`. Held only for its
    /// drop, which records the elapsed sample; the field is never read.
    pub struct StageTimer(#[allow(dead_code)] observability_core::Timer);

    /// Install the metrics recorder/exporter, then arm the runtime gate.
    ///
    /// The gate is `observability_core::METRICS_ON`. This crate, the SoupBinTCP
    /// client and `transport-core` take `observability-core` from the root
    /// manifest's `observability-core` entry; `observability` brings its own
    /// from the tag on the root `observability` entry. While both entries carry
    /// one url and tag, cargo links one copy, the `observability::` re-export is
    /// the same static, and `init` arms what `StageTimer` reads. Entries that
    /// differ link two copies, and arming one leaves the other's timers dark.
    pub fn init(cfg: &ObservabilityConfig) -> AppResult<ObsGuard> {
        let (otlp, exporter) = match &cfg.exporter {
            MetricsExporterConfig::Prometheus { bind } => {
                (None, MetricsExporter::Prometheus { bind: *bind })
            }
            MetricsExporterConfig::Otlp { endpoint, grpc } => (
                Some(OtlpConfig {
                    endpoint: endpoint.clone(),
                    protocol: if *grpc {
                        OtlpProtocol::Grpc
                    } else {
                        OtlpProtocol::HttpProtobuf
                    },
                    headers: Vec::new(),
                    resource: Vec::new(),
                    timeout: std::time::Duration::from_secs(10),
                }),
                MetricsExporter::Otlp,
            ),
        };

        let guard = observability::init(ObsConfig {
            pipeline: PipelineConfig {
                service_name: cfg.service_name.clone(),
                level: "info".into(),
                otlp,
                // Service-mode logs: machine-scrapeable Json on stdout, matching
                // the OTLP attribute shape. Per-sink format knob deferred.
                logging: Some(LoggingConfig {
                    sinks: vec![LogSink {
                        kind: LogSinkKind::Stdout,
                        format: LogFormat::Json,
                        level: None,
                    }],
                }),
                tracing: None,
            },
            metrics: Some(MetricsConfig { exporter }),
        })?;

        observability_core::set_metrics_enabled(true);
        observability_core::refresh_thread_gate();
        Ok(ObsGuard(guard))
    }

    /// Install a plain stderr `fmt` subscriber for a run with no observability
    /// pipeline. Honors `RUST_LOG`, defaults to `warn`. `try_init` Err is
    /// ignored: only reachable if a subscriber already exists, which this run
    /// path never installs first.
    pub fn init_fallback_logging() {
        use tracing_subscriber::EnvFilter;
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
            )
            .try_init();
    }

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
    use crate::{AppResult, config::ObservabilityConfig};

    /// Zero-sized guard when observability is compiled out.
    pub struct ObsGuard;

    /// Zero-sized no-op stage timer; records nothing on drop.
    pub struct StageTimer;

    /// A config asking for observability without the feature is a hard error, so
    /// a deployment never silently runs blind against its own request.
    pub fn init(_cfg: &ObservabilityConfig) -> AppResult<ObsGuard> {
        Err(
            "config requests observability but the crate was built without the \
             `observability` feature"
                .into(),
        )
    }

    /// Install a plain stderr `fmt` subscriber for a run with no observability
    /// pipeline. Honors `RUST_LOG`, defaults to `warn`. Identical to the
    /// feature-on twin: subscriber install is independent of the metrics seam.
    pub fn init_fallback_logging() {
        use tracing_subscriber::EnvFilter;
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
            )
            .try_init();
    }

    /// No-op timer: records nothing on drop.
    #[inline]
    pub fn stage_timer(_metric: &'static str) -> StageTimer {
        StageTimer
    }

    /// No-op throughput sink.
    #[inline]
    pub fn record_records(_n: u64) {}
}

pub use imp::{ObsGuard, init, init_fallback_logging, record_records, stage_timer};
