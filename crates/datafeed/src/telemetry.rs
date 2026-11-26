//! Logging and metrics installs for the binary: the one `tracing` subscriber
//! and, when the config asks for it, the metrics recorder and exporter.

use datafeed::{FeedError, config::ObservabilityConfig};
#[cfg(feature = "observability")]
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

/// The exporter guard and the runtime its background tasks run on. Dropping
/// it stops the exporter first, then the runtime.
#[cfg(feature = "observability")]
pub struct Telemetry {
    _guard: observability::ObsGuard,
    _runtime: Runtime,
}

/// Without the feature no exporter exists, so no value is ever built.
#[cfg(not(feature = "observability"))]
pub type Telemetry = std::convert::Infallible;

/// Install the process logging and metrics for one run. With an
/// observability config the recorder, exporter and JSON logs come up on a
/// fresh runtime; without one a stderr `fmt` subscriber honoring `RUST_LOG`
/// (default `warn`) is installed.
pub fn install(cfg: Option<&ObservabilityConfig>) -> Result<Option<Telemetry>, FeedError> {
    let Some(cfg) = cfg else {
        install_fallback_logging();
        return Ok(None);
    };
    install_exporter(cfg).map(Some)
}

/// `try_init` Err is ignored: only reachable if a subscriber already exists,
/// which this run path never installs first.
fn install_fallback_logging() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .try_init();
}

/// A config asking for observability without the feature is a hard error, so
/// a deployment never silently runs blind against its own request.
#[cfg(not(feature = "observability"))]
fn install_exporter(_cfg: &ObservabilityConfig) -> Result<Telemetry, FeedError> {
    Err(datafeed::ConfigError::ObservabilityUnavailable.into())
}

/// Install the metrics recorder/exporter, then arm the runtime gate.
///
/// The gate is `observability_core::METRICS_ON`. This crate, the SoupBinTCP
/// client and `transport-core` take `observability-core` from the root
/// manifest's `observability-core` entry; `observability` brings its own from
/// the tag on the root `observability` entry. While both entries carry one url
/// and tag, cargo links one copy, the `observability::` re-export is the same
/// static, and this arms what the session timers read. Entries that differ link
/// two copies, and arming one leaves the other's timers dark.
#[cfg(feature = "observability")]
fn install_exporter(cfg: &ObservabilityConfig) -> Result<Telemetry, FeedError> {
    use datafeed::config::MetricsExporterConfig;
    use observability::{
        LogFormat, LogSink, LogSinkKind, LoggingConfig, MetricsConfig, MetricsExporter, ObsConfig,
        OtlpConfig, OtlpProtocol, PipelineConfig,
    };

    // Exporter background tasks (prometheus listener / otlp pusher) spawn on
    // this runtime; enter it during init so they attach here and keep serving
    // through the run.
    let runtime = Runtime::new().map_err(FeedError::Runtime)?;
    let _enter = runtime.enter();

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
    drop(_enter);
    Ok(Telemetry {
        _guard: guard,
        _runtime: runtime,
    })
}
