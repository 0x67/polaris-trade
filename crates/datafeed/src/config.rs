//! Deployment config: which venue adapter to run, where frames come from
//! (live socket or replay file), and which capture taps are on. Same shape
//! drives both live and replay deployments; only `SourceConfig` changes.

use std::{net::SocketAddr, path::PathBuf};

use client_soupbintcp::SoupBinClientConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::venue::VenueDescriptor;

/// On-disk replay container format. `Binary`/`BinaryGz` read
/// `md_replay::BinaryFileReplay`; `Capture` reads `md_replay::CaptureReplay`
/// (the only format that carries per-row stream ids, so dual-channel
/// replay needs this one); `Pcap` reads `md_replay::PcapReplay` (packets
/// reach the adapter verbatim, no L2/L3/L4 strip); `Auto` sniffs the
/// container via `md_replay::detect_container` (magic bytes, extension
/// fallback) and refuses startup on unrecognized input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplayFormat {
    #[default]
    Binary,
    BinaryGz,
    Capture,
    Pcap,
    Auto,
}

/// How a replay file is materialized. `Stream` reads incrementally in
/// constant memory; `Eager` slurps (and for gzip fully inflates) the whole
/// file up front, isolating pipeline cost from io and inflate cost.
/// `Eager` is only implemented for `Binary`/`BinaryGz`; combining it with
/// `Capture`/`Pcap` is a wiring-time error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoadMode {
    #[default]
    Stream,
    Eager,
}

/// Replay pacing mode at the config edge; mirrors `md_replay::PacingMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "mode")]
pub enum PacingModeConfig {
    /// Release delay equals the raw `ts_recv` delta between frames.
    RealTime,
    /// `ts_recv` delta divided by `scale`; `2.0` plays back twice as fast.
    Scaled { scale: f64 },
    /// No pacing; frames release back to back.
    #[default]
    Unthrottled,
}

/// One pacing window over `ts_recv`, half-open; mirrors
/// `md_replay::PaceWindow`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct PaceWindowConfig {
    /// Window start `ts_recv`, inclusive, nanoseconds.
    pub from_ts: u64,
    /// Window end `ts_recv`, exclusive, nanoseconds.
    pub to_ts: u64,
    /// Mode governing frames inside the window.
    pub mode: PacingModeConfig,
}

/// Replay release pacing: a default mode plus optional scheduled windows
/// (sorted, non-overlapping; gaps fall back to the default mode). Empty
/// `windows` paces the whole replay at `default_mode`. Pacing only changes
/// release delay; every frame is still processed.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PacingConfig {
    #[serde(default)]
    pub default_mode: PacingModeConfig,
    #[serde(default)]
    pub windows: Vec<PaceWindowConfig>,
}

/// Where a session pulls venue frames from. `Live`/`LiveDual` connect a
/// SoupBinTCP client over TCP; `Replay` reads a capture file. Swapping this
/// is the only wiring difference between a live and a replay deployment
/// (the `IngestSource` seam).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum SourceConfig {
    /// Single-channel live venue: one SoupBinTCP session.
    Live {
        endpoint: SocketAddr,
        #[serde(default)]
        soupbin: SoupBinClientConfig,
    },
    /// Dual-channel live venue: primary + secondary channel, each its own TCP session.
    LiveDual {
        primary_endpoint: SocketAddr,
        secondary_endpoint: SocketAddr,
        #[serde(default)]
        soupbin: SoupBinClientConfig,
    },
    /// Replay from a capture file on disk.
    Replay {
        path: PathBuf,
        #[serde(default)]
        format: ReplayFormat,
        /// Synthetic `ts_recv` of the first message, nanoseconds. Only used
        /// by `Binary`/`BinaryGz`; `Capture` rows carry their own `ts_recv`.
        #[serde(default)]
        base_ts: u64,
        /// Synthetic per-message `ts_recv` step, nanoseconds. See `base_ts`.
        #[serde(default = "default_step_ns")]
        step_ns: u64,
        /// Materialization mode; `Eager` only for `Binary`/`BinaryGz`.
        #[serde(default)]
        load: LoadMode,
        /// Release pacing; defaults to unthrottled with no windows.
        #[serde(default)]
        pacing: PacingConfig,
    },
}

impl SourceConfig {
    /// The `kind` tag this variant carries in config files.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Live { .. } => "live",
            Self::LiveDual { .. } => "live-dual",
            Self::Replay { .. } => "replay",
        }
    }
}

fn default_step_ns() -> u64 {
    1
}

/// Which in-process `OutputSink` a session publishes through, matching
/// `crate::sink::AnySink`'s variants. `Null` discards every record (the
/// binary's default deployment); `Collecting` buffers them in memory
/// (capture-only/test mode; no same-host or cross-host transport sink
/// exists yet).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SinkKind {
    #[default]
    Null,
    Collecting,
}

/// Capture tap output directory: the raw pre-adapter stream. A bucket root, not
/// a file; the tap lays out `<venue>/<version>/<YYYY-MM-DD>/` beneath and derives
/// self-describing leaf names, so a run never names its own output. `raw_dir`
/// drives the tap: unset runs the `NoopTap`, set runs `MdCaptureTap`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TapConfig {
    /// Retired. Normalized capture had no reader and was removed; raw capture is
    /// the replay source of truth and reproduces it. Kept only to reject a stale
    /// config loudly: it used to gate the whole tap, so ignoring it would turn
    /// capture off silently on every config that still sets it.
    #[serde(default)]
    pub normalized_dir: Option<PathBuf>,
    #[serde(default)]
    pub raw_dir: Option<PathBuf>,
    /// Trade date in the derived filename and the container header (`YYYYMMDD`).
    /// Unused when the tap is off. Live leaves it `0`: the tap takes the first
    /// frame's session label, and a set value is a startup error, since it goes
    /// stale once a restart lands on a later day. On MDC1 replay `0` adopts the
    /// capture header's date and a nonzero value that disagrees is a startup
    /// error; any other replay with the tap on must set it.
    #[serde(default)]
    pub capture_date: u32,
    /// Time-partition window as a duration token (`1d` default, `1h`, `30m`, ...),
    /// parsed by [`parse_resolution`] and counted from the venue session's open,
    /// not from UTC. `1d` is one file per session (23h, 24h, or 25h across DST)
    /// and never rolls intra-session; sub-day tokens roll on each boundary and
    /// label windows by their offset from the open, so a 25h session ends `T24`.
    ///
    /// WARNING: sub-day partitions need `ts_recv` monotonic across a boundary.
    /// A dual-channel source polls primary, then secondary, so `ts_recv` can step back
    /// and the tap aborts the run with a window regression. Sub-day partitions
    /// on a dual-channel source are unsupported; the default `1d` is always
    /// safe.
    #[serde(default = "default_partition")]
    pub partition: String,
}

/// Default time-partition window: one file per day.
pub(crate) fn default_partition() -> String {
    "1d".to_string()
}

/// Runtime observability wiring. Present turns the metrics gate on and picks a
/// metrics exporter; absent (default) runs blind, so a normal deployment pays
/// nothing. Only honored when the crate is built `--features observability`; a
/// build without the feature but a config asking for it fails loudly at start
/// rather than running blind. See `crate::obs`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// `service.name` stamped on exported metrics.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    pub exporter: MetricsExporterConfig,
}

/// Which metrics recorder backs the `metrics` facade the pipeline stages record
/// into: a Prometheus pull endpoint or an OTLP push bridge to a collector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum MetricsExporterConfig {
    /// Prometheus pull endpoint; scrape `http://<bind>/metrics`.
    Prometheus { bind: SocketAddr },
    /// OTLP push to a collector `endpoint`. `grpc` picks gRPC transport over
    /// the default HTTP/protobuf.
    Otlp {
        endpoint: String,
        #[serde(default)]
        grpc: bool,
    },
}

fn default_service_name() -> String {
    "datafeed".to_string()
}

/// Full session config: venue, session-anchoring fields the adapters need,
/// source, and tap toggles.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatafeedConfig {
    /// The venue, written by name (`nasdaq`) and resolved
    /// through [`crate::venue::VENUES`] when the config loads.
    #[serde(
        deserialize_with = "crate::venue::deserialize",
        serialize_with = "crate::venue::serialize"
    )]
    pub venue: &'static VenueDescriptor,
    /// Venue registry id stamped on every record's `RecordHeader.venue`. On
    /// MDC1 replay `0` adopts the capture header's venue; a nonzero value that
    /// disagrees with the header is a startup error.
    pub venue_id: u16,
    /// Epoch-ns UTC of the trade-date midnight in the venue's reference
    /// timezone; adapters add each message's within-day offset to this. `0`
    /// leaves it unset: MDC1 replay derives it from the capture date, and a live
    /// session installs each trade date's anchor from frame receive time,
    /// rolling it at every session end. A nonzero value wins on replay; a live
    /// source refuses one at startup, since it would go stale at the first roll.
    pub session_midnight_ns: u64,
    pub source: SourceConfig,
    /// In-process sink selection. Defaults to `SinkKind::Null` (discard).
    #[serde(default)]
    pub sink: SinkKind,
    #[serde(default)]
    pub taps: TapConfig,
    /// OHLCV bar bucket widths as duration tokens (e.g. `"1s"`, `"5m"`, `"1h"`),
    /// resolved to nanoseconds by `resolve_resolutions`. Defaults to the seven
    /// sub-day intervals; `--ohlcv-resolutions` overrides at startup.
    #[serde(default = "default_resolution_tokens")]
    pub ohlcv_resolutions: Vec<String>,
    /// Runtime observability. `None` (default) runs blind.
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
}

/// The seven default OHLCV resolutions, as duration tokens. Day-scale widths are
/// intentionally excluded: epoch-modulo bucketing anchors them to UTC midnight,
/// not the exchange session.
pub(crate) fn default_resolution_tokens() -> Vec<String> {
    ["1s", "1m", "5m", "10m", "15m", "30m", "1h"]
        .into_iter()
        .map(String::from)
        .collect()
}

/// Rejection reasons for an OHLCV resolution token or list.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResolutionParseError {
    #[error("malformed resolution token {token:?} (want <n>{{s|m|h|d}}, e.g. 5m)")]
    BadToken { token: String },
    #[error("resolution token {token:?} has zero width")]
    Zero { token: String },
    #[error("resolution token {token:?} overflows u64 nanoseconds")]
    Overflow { token: String },
    #[error("no OHLCV resolutions configured")]
    Empty,
}

/// Parse a duration token (`^[0-9]+(s|m|h|d)$`) to nanoseconds. `d` is accepted
/// (the engine is width-agnostic) though it is not a default; a zero magnitude,
/// missing/unknown unit, non-digit body, or u64 overflow is rejected.
pub fn parse_resolution(tok: &str) -> Result<u64, ResolutionParseError> {
    let bad = || ResolutionParseError::BadToken {
        token: tok.to_string(),
    };
    let split = tok.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (num, unit) = tok.split_at(split);
    let scale: u64 = match unit {
        "s" => 1_000_000_000,
        "m" => 60_000_000_000,
        "h" => 3_600_000_000_000,
        "d" => 86_400_000_000_000,
        _ => return Err(bad()),
    };
    let value: u64 = num.parse().map_err(|_| bad())?;
    if value == 0 {
        return Err(ResolutionParseError::Zero {
            token: tok.to_string(),
        });
    }
    value
        .checked_mul(scale)
        .ok_or(ResolutionParseError::Overflow {
            token: tok.to_string(),
        })
}

/// Parse every token, then dedup and sort ascending. Rejects an empty result so
/// a session never runs with no resolutions. Shared by config load and the CLI
/// override so both enforce the same well-formed, ordered list.
pub fn resolve_resolutions(tokens: &[String]) -> Result<Vec<u64>, ResolutionParseError> {
    let mut out = tokens
        .iter()
        .map(|t| parse_resolution(t))
        .collect::<Result<Vec<_>, _>>()?;
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err(ResolutionParseError::Empty);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::NASDAQ;

    #[test]
    fn live_source_round_trips() {
        let cfg = DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 1,
            session_midnight_ns: 0,
            source: SourceConfig::Live {
                endpoint: "127.0.0.1:18001".parse().unwrap(),
                soupbin: SoupBinClientConfig::default(),
            },
            sink: SinkKind::Null,
            taps: TapConfig::default(),
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        };
        let json = serde_json::to_string(&cfg).expect("serialize live config");
        let back: DatafeedConfig = serde_json::from_str(&json).expect("deserialize live config");
        assert_eq!(back.venue.name, NASDAQ.name);
        match back.source {
            SourceConfig::Live { endpoint, .. } => {
                assert_eq!(endpoint, "127.0.0.1:18001".parse().unwrap());
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    #[test]
    fn replay_source_round_trips() {
        let cfg = DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 2,
            session_midnight_ns: 1_000,
            source: SourceConfig::Replay {
                path: PathBuf::from("/tmp/sample.bin"),
                format: ReplayFormat::Binary,
                base_ts: 0,
                step_ns: 1,
                load: LoadMode::Eager,
                pacing: PacingConfig {
                    default_mode: PacingModeConfig::Unthrottled,
                    windows: vec![PaceWindowConfig {
                        from_ts: 1_000,
                        to_ts: 2_000,
                        mode: PacingModeConfig::Scaled { scale: 2.0 },
                    }],
                },
            },
            sink: SinkKind::Null,
            taps: TapConfig {
                raw_dir: Some(PathBuf::from("/tmp/raw")),
                normalized_dir: Some(PathBuf::from("/tmp/normalized")),
                capture_date: 20260101,
                partition: default_partition(),
            },
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        };
        let json = serde_json::to_string(&cfg).expect("serialize replay config");
        let back: DatafeedConfig = serde_json::from_str(&json).expect("deserialize replay config");
        assert_eq!(back.venue.name, NASDAQ.name);
        assert!(back.taps.raw_dir.is_some() && back.taps.normalized_dir.is_some());
        match back.source {
            SourceConfig::Replay {
                path,
                format,
                load,
                pacing,
                ..
            } => {
                assert_eq!(path, PathBuf::from("/tmp/sample.bin"));
                assert_eq!(format, ReplayFormat::Binary);
                assert_eq!(load, LoadMode::Eager);
                assert_eq!(pacing.windows.len(), 1);
                assert_eq!(
                    pacing.windows[0].mode,
                    PacingModeConfig::Scaled { scale: 2.0 }
                );
            }
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    // Replay config written before `load`/`pacing` existed must still parse,
    // defaulting to streaming load and unthrottled fixed pacing.
    #[test]
    fn missing_replay_fields_default_stream_unthrottled() {
        let json = r#"{
            "venue": "nasdaq",
            "venue_id": 1,
            "session_midnight_ns": 0,
            "source": {
                "kind": "replay",
                "path": "/tmp/sample.bin"
            }
        }"#;
        let cfg: DatafeedConfig = serde_json::from_str(json).expect("deserialize legacy config");
        match cfg.source {
            SourceConfig::Replay {
                format,
                load,
                pacing,
                ..
            } => {
                assert_eq!(format, ReplayFormat::Binary);
                assert_eq!(load, LoadMode::Stream);
                assert_eq!(pacing, PacingConfig::default());
            }
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    // Config JSON written before `sink` existed has no such key; it must
    // still load and default to `Null` (discard), not fail to parse.
    #[test]
    fn missing_sink_field_defaults_to_null() {
        let json = r#"{
            "venue": "nasdaq",
            "venue_id": 1,
            "session_midnight_ns": 0,
            "source": {
                "kind": "live",
                "endpoint": "127.0.0.1:18001"
            }
        }"#;
        let cfg: DatafeedConfig = serde_json::from_str(json).expect("deserialize legacy config");
        assert_eq!(cfg.sink, SinkKind::Null);
    }

    // Each config venue name resolves to the storage tag, version, and MDC1 feed
    // id the capture tree on disk already uses; an unknown name fails the load.
    #[test]
    fn venue_names_resolve_to_on_disk_identity() {
        let load = |venue: &str| {
            serde_json::from_str::<DatafeedConfig>(&format!(
                r#"{{"venue": "{venue}", "venue_id": 1, "session_midnight_ns": 0,
                    "source": {{"kind": "live", "endpoint": "127.0.0.1:18001"}}}}"#
            ))
            .map(|cfg| (cfg.venue.tag, cfg.venue.version, cfg.venue.feed_id))
        };
        assert_eq!(load("nasdaq").unwrap(), ("nasdaq", "v5", 3));
        let err = load("nasdaq-v6").expect_err("unknown venue must not load");
        assert!(
            err.to_string().contains("unknown venue \"nasdaq-v6\""),
            "{err}"
        );
    }

    // The default resolution set is the seven sub-day intervals, resolving to
    // their nanosecond widths in ascending order.
    #[test]
    fn default_resolutions_are_seven() {
        let tokens = default_resolution_tokens();
        assert_eq!(tokens.len(), 7);
        let ns = resolve_resolutions(&tokens).expect("defaults resolve");
        assert_eq!(
            ns,
            vec![
                1_000_000_000,     // 1s
                60_000_000_000,    // 1m
                300_000_000_000,   // 5m
                600_000_000_000,   // 10m
                900_000_000_000,   // 15m
                1_800_000_000_000, // 30m
                3_600_000_000_000, // 1h
            ]
        );
    }

    // Config JSON written before `ohlcv_resolutions` existed has no such key; it
    // must still load and default to the seven intervals.
    #[test]
    fn missing_ohlcv_resolutions_defaults_to_seven() {
        let json = r#"{
            "venue": "nasdaq",
            "venue_id": 1,
            "session_midnight_ns": 0,
            "source": { "kind": "live", "endpoint": "127.0.0.1:18001" }
        }"#;
        let cfg: DatafeedConfig = serde_json::from_str(json).expect("deserialize legacy config");
        assert_eq!(cfg.ohlcv_resolutions.len(), 7);
    }

    // Each duration unit maps to the right nanosecond width; a bad unit, a bare
    // number, or a unit with no magnitude is rejected.
    #[test]
    fn parse_resolution_units() {
        assert_eq!(parse_resolution("1s").unwrap(), 1_000_000_000);
        assert_eq!(parse_resolution("1m").unwrap(), 60_000_000_000);
        assert_eq!(parse_resolution("1h").unwrap(), 3_600_000_000_000);
        assert_eq!(parse_resolution("2h").unwrap(), 7_200_000_000_000);
        assert_eq!(parse_resolution("1d").unwrap(), 86_400_000_000_000);
        assert!(parse_resolution("5x").is_err());
        assert!(parse_resolution("m").is_err());
        assert!(parse_resolution("100").is_err());
    }

    // A zero-width token is rejected; duplicates collapse and the result is
    // ascending; an empty list is rejected.
    #[test]
    fn resolve_rejects_zero_dedups_sorts() {
        assert!(matches!(
            resolve_resolutions(&["0s".to_string()]),
            Err(ResolutionParseError::Zero { .. })
        ));
        let ns = resolve_resolutions(&[
            "1h".to_string(),
            "1s".to_string(),
            "1s".to_string(),
            "5m".to_string(),
        ])
        .expect("valid tokens resolve");
        assert_eq!(ns, vec![1_000_000_000, 300_000_000_000, 3_600_000_000_000]);
        assert!(matches!(
            resolve_resolutions(&[]),
            Err(ResolutionParseError::Empty)
        ));
    }
}
