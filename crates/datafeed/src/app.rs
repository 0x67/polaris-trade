//! Config loading and top-level venue/source dispatch.

use std::{
    fs,
    path::{Path, PathBuf},
};

use md_pipeline::BundledTzdb;

use crate::{
    AppResult,
    config::{DatafeedConfig, ResolutionParseError, SourceConfig, resolve_resolutions},
    live, replay,
    venue::VenueDescriptor,
    wiring,
};

/// Command-line overrides applied over the loaded config before a session is
/// built. Default is empty, so config values win.
#[derive(Debug, Default)]
pub struct CliOverrides {
    /// `--ohlcv-resolutions=1s,5m,1h` split into tokens; `None` leaves the
    /// config's list untouched.
    pub ohlcv_resolutions: Option<Vec<String>>,
}

impl CliOverrides {
    /// Extract overrides from CLI args (`argv[0]` already skipped). Recognizes
    /// `--ohlcv-resolutions=<csv>`; the config path and any other argument are
    /// resolved by the caller.
    pub fn from_args(args: &[String]) -> Self {
        let ohlcv_resolutions = args
            .iter()
            .find_map(|a| a.strip_prefix("--ohlcv-resolutions="))
            .map(|csv| csv.split(',').map(|t| t.trim().to_string()).collect());
        CliOverrides { ohlcv_resolutions }
    }
}

/// Apply CLI overrides onto a loaded config, validating an overridden
/// resolution list so a malformed token fails at startup, not mid-run.
fn apply_overrides(
    config: &mut DatafeedConfig,
    overrides: &CliOverrides,
) -> Result<(), ResolutionParseError> {
    if let Some(tokens) = &overrides.ohlcv_resolutions {
        resolve_resolutions(tokens)?;
        config.ohlcv_resolutions = tokens.clone();
    }
    Ok(())
}

/// Load one deployment config, apply CLI overrides, and run it to completion.
/// Live sessions reconnect internally and normally do not return.
pub fn run(config_path: PathBuf, overrides: CliOverrides) -> AppResult<()> {
    let raw = fs::read_to_string(&config_path)?;
    let mut config = parse_config(&config_path, &raw)?;
    apply_overrides(&mut config, &overrides)?;
    run_config(config)
}

/// Decode a config body, picking the parser by file extension: `.json` is
/// strict JSON, `.yaml`/`.yml` is YAML. Any other extension falls back to YAML,
/// which is a JSON superset, so a bare or misnamed file still loads.
fn parse_config(path: &Path, raw: &str) -> AppResult<DatafeedConfig> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let config = match ext.as_str() {
        "json" => serde_json::from_str(raw)?,
        _ => serde_norway::from_str(raw)?,
    };
    Ok(config)
}

/// Assemble and run an already-decoded deployment config.
pub fn run_config(mut config: DatafeedConfig) -> AppResult<()> {
    // On MDC1 replay the capture carries its own venue and date; reconcile the
    // config against that header before the tap stamps output with it.
    let capture_meta = match &config.source {
        SourceConfig::Replay { path, .. } => replay::peek_capture_meta(path)?,
        _ => None,
    };
    if let Some((venue, date, feed)) = capture_meta {
        reconcile_capture_meta(&mut config, venue, date, feed)?;
        // header feed is authoritative when set; a legacy (0) capture has none,
        // so it falls back to the best-effort filename check.
        if feed == md_capture::FEED_UNSPECIFIED
            && let SourceConfig::Replay { path, .. } = &config.source
        {
            check_replay_path_feed(path, config.venue)?;
        }
    }

    let tap = wiring::build_tap(&config)?;
    let sink = wiring::build_sink(&config);
    let runtime = tokio::runtime::Runtime::new()?;

    // Exporter background tasks (prometheus listener / otlp pusher) spawn on
    // this runtime; enter it during init so they attach here and keep serving
    // through a sync replay run on the worker pool. Guard lives to run end.
    let _obs: Option<crate::obs::ObsGuard> = match &config.observability {
        Some(obs_cfg) => {
            let _enter = runtime.enter();
            Some(crate::obs::init(obs_cfg)?)
        }
        None => {
            crate::obs::init_fallback_logging();
            None
        }
    };

    match &config.source {
        SourceConfig::Live { .. } | SourceConfig::LiveDual { .. } => {
            live::run(&runtime, &config, sink, tap, crate::shutdown::flag())
        }
        SourceConfig::Replay {
            path,
            format,
            base_ts,
            step_ns,
            load,
            pacing,
        } => {
            let input = replay::ReplayInput {
                path,
                format: *format,
                base_ts: *base_ts,
                step_ns: *step_ns,
                load: *load,
                pacing,
            };
            replay::run(&config, input, sink, tap)
        }
    }
}

/// Reconcile config against a capture container's own header on MDC1 replay: an
/// unset (`0`) `capture_date` or `venue_id` adopts the header value, and a set
/// value that disagrees is a startup error, so a stale config never silently
/// mislabels output. A nonzero header `feed` that disagrees with the configured
/// venue's own feed is rejected too: this is what separates two feeds that
/// share a venue_id. A legacy `0` feed carries no such claim and is left
/// to the caller's path-name fallback. An unset (`0`) `session_midnight_ns`
/// becomes the trade date's anchor under [`VenueDescriptor::session`]; an
/// explicit nonzero offset wins, and a date that does not resolve leaves it
/// manual.
fn reconcile_capture_meta(
    config: &mut DatafeedConfig,
    venue: u16,
    date: u32,
    feed: u8,
) -> AppResult<()> {
    let capture_date = &mut config.taps.capture_date;
    if *capture_date == 0 {
        *capture_date = date;
    } else if *capture_date != date {
        return Err(format!(
            "config taps.capture_date {capture_date} disagrees with capture header {date}"
        )
        .into());
    }

    if config.venue_id == 0 {
        config.venue_id = venue;
    } else if config.venue_id != venue {
        return Err(format!(
            "config venue_id {} disagrees with capture header {venue}",
            config.venue_id
        )
        .into());
    }

    if feed != md_capture::FEED_UNSPECIFIED {
        let want = config.venue.feed_id;
        if feed != want {
            return Err(format!(
                "capture feed id {feed} disagrees with configured venue `{}-{}` (feed {want}): \
                 wrong capture for this venue?",
                config.venue.tag, config.venue.version
            )
            .into());
        }
    }

    // Explicit config offset wins; an unset (0) one derives from the venue
    // session model for the resolved trade date, so the adapter anchors on the
    // real session day without a hand-set value.
    if config.session_midnight_ns == 0
        && let Some(smid) = config
            .venue
            .session
            .resolve_anchor(config.taps.capture_date, &BundledTzdb::new())
    {
        config.session_midnight_ns = smid;
    }
    Ok(())
}

/// Startup guard against a replay capture whose feed does not match the
/// configured venue decoder. The MDC1 header carries no feed-version byte, and
/// two feeds of one venue can share a venue_id, so the header reconcile above
/// cannot tell those feeds apart. The bucket layout names every leaf
/// `<venue>_<version>_<window>...mdc1.zst`, so the filename can: a `<tag>_<ver>`
/// that disagrees with the venue means the wrong capture. Reject fast, before a
/// full replay silently decodes almost nothing (a capture fed to another feed
/// version's adapter drains ~0 records with no error). An off-layout name
/// carries no feed token and runs unguarded. Per-file header check supersedes
/// this once the container gains a feed byte.
fn check_replay_path_feed(path: &Path, venue: &VenueDescriptor) -> AppResult<()> {
    let Some((tag, ver)) = sniff_path_feed(path) else {
        return Ok(());
    };
    let (want_tag, want_ver) = (venue.tag, venue.version);
    if tag != want_tag || ver != want_ver {
        return Err(format!(
            "replay capture `{tag}_{ver}` disagrees with configured venue \
             `{want_tag}_{want_ver}`: wrong capture for this venue?"
        )
        .into());
    }
    Ok(())
}

/// Leading `<venue>_<version>` of a capture filename, or `None` when the name is
/// not a self-describing leaf. Underscore is the field separator (the window
/// uses only hyphen/`T`), so the first two fields are the venue tag and a
/// `v<N>` version. The `v<N>` shape check stops an unrelated underscore
/// filename from reading as a feed token.
fn sniff_path_feed(path: &Path) -> Option<(String, String)> {
    let name = path.file_name()?.to_str()?;
    let mut fields = name.split('_');
    let tag = fields.next()?;
    let ver = fields.next()?;
    let is_version =
        ver.len() >= 2 && ver.starts_with('v') && ver[1..].bytes().all(|b| b.is_ascii_digit());
    if tag.is_empty() || !is_version {
        return None;
    }
    Some((tag.to_string(), ver.to_string()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use md_pipeline::{ClosureModel, RollModel, SessionModel};

    use super::{
        CliOverrides, apply_overrides, check_replay_path_feed, parse_config,
        reconcile_capture_meta, run_config,
    };
    use crate::{
        config::{DatafeedConfig, SinkKind, SourceConfig, TapConfig, default_resolution_tokens},
        venue::{NASDAQ, VenueDescriptor},
    };

    fn live_config() -> DatafeedConfig {
        DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 0,
            session_midnight_ns: 0,
            source: SourceConfig::Live {
                endpoint: "127.0.0.1:1".parse().unwrap(),
                soupbin: Default::default(),
            },
            sink: SinkKind::Null,
            taps: TapConfig::default(),
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        }
    }

    // MDC1 reconcile: an unset (0) capture_date/venue_id adopts the header
    // value; a set value that disagrees fails at startup; a matching value
    // passes untouched.
    #[test]
    fn reconcile_capture_meta_fills_and_validates() {
        use md_capture::FEED_UNSPECIFIED as UNSPECIFIED;
        let mut unset = live_config();
        reconcile_capture_meta(&mut unset, 7, 20260521, UNSPECIFIED).expect("unset adopts header");
        assert_eq!(unset.venue_id, 7);
        assert_eq!(unset.taps.capture_date, 20260521);

        let mut matching = live_config();
        matching.venue_id = 7;
        matching.taps.capture_date = 20260521;
        reconcile_capture_meta(&mut matching, 7, 20260521, UNSPECIFIED)
            .expect("matching values pass");

        let mut bad_date = live_config();
        bad_date.taps.capture_date = 20260101;
        assert!(
            reconcile_capture_meta(&mut bad_date, 7, 20260521, UNSPECIFIED).is_err(),
            "disagreeing date rejected"
        );

        let mut bad_venue = live_config();
        bad_venue.venue_id = 9;
        assert!(
            reconcile_capture_meta(&mut bad_venue, 7, 20260521, UNSPECIFIED).is_err(),
            "disagreeing venue rejected"
        );
    }

    /// A second feed of the NASDAQ venue: same tag and `venue_id`, its own
    /// version and feed id.
    static NASDAQ_NEXT: VenueDescriptor = VenueDescriptor {
        name: "test-nasdaq-next",
        version: "v6",
        feed_id: NASDAQ.feed_id + 1,
        ..NASDAQ
    };

    // Two feeds of one venue can share a venue_id, so the venue_id reconcile
    // cannot separate them; the header feed byte does. A NASDAQ capture under
    // the second feed's config is rejected, a matching feed passes, and a legacy
    // (0) feed skips the check so the path-name fallback can handle it.
    #[test]
    fn reconcile_rejects_feed_venue_mismatch() {
        let next_feed = || {
            let mut c = live_config();
            c.venue = &NASDAQ_NEXT;
            c.venue_id = 2;
            c
        };
        let err = reconcile_capture_meta(&mut next_feed(), 2, 20260521, NASDAQ.feed_id)
            .expect_err("a capture of the first feed under the second must be rejected");
        assert!(err.to_string().contains("disagrees"), "got: {err}");

        reconcile_capture_meta(&mut next_feed(), 2, 20260521, NASDAQ_NEXT.feed_id)
            .expect("matching feed passes");
        reconcile_capture_meta(&mut next_feed(), 2, 20260521, md_capture::FEED_UNSPECIFIED)
            .expect("legacy feed skips the check");
    }

    // A legacy capture header names no feed, so the path-feed guard reads the
    // self-describing filename. A v5 capture under the v6 feed's config is the
    // silent-mismatch bug: reject it before a full replay drains ~0 records.
    // Off-layout names run unguarded.
    #[test]
    fn replay_path_feed_guard_catches_venue_mismatch() {
        let raw_v5 =
            Path::new("../data/capture/nasdaq/v5/2020-01-30/nasdaq_v5_2020-01-30.mdc1.zst");
        check_replay_path_feed(raw_v5, &NASDAQ).expect("v5 capture under the v5 venue passes");
        let err = check_replay_path_feed(raw_v5, &NASDAQ_NEXT)
            .expect_err("v5 capture under the v6 venue must be rejected");
        assert!(err.to_string().contains("disagrees"), "got: {err}");

        // Normalized leaf has a trailing schema field; first two fields still
        // decode to tag+ver, so the mismatch is still caught.
        let norm_v6 = Path::new(
            "/out/normalized/nasdaq/v6/2026-05-21/nasdaq_v6_2026-05-21T1300_mbo.mdc1.zst",
        );
        check_replay_path_feed(norm_v6, &NASDAQ_NEXT).expect("v6 leaf under the v6 venue passes");
        assert!(
            check_replay_path_feed(norm_v6, &NASDAQ).is_err(),
            "v6 leaf under the v5 venue rejected"
        );

        // Same version under another venue tag: the tag alone must reject it.
        let other_tag = Path::new("/out/raw/other/v5/2026-05-21/other_v5_2026-05-21.mdc1.zst");
        assert!(
            check_replay_path_feed(other_tag, &NASDAQ).is_err(),
            "another venue's v5 leaf under nasdaq rejected"
        );

        // No `v<N>` field: carries no feed signal, so it runs rather than
        // rejecting a legitimately off-layout capture.
        let off_layout = Path::new("/tmp/some-capture.bin");
        check_replay_path_feed(off_layout, &NASDAQ_NEXT).expect("off-layout name runs unguarded");
    }

    // UTC civil midnight of a date, epoch ns, resolved independently through
    // jiff's UTC zone so tz-derive expectations do not restate the code under
    // test.
    fn utc_midnight_ns(y: i16, m: i8, d: i8) -> u64 {
        let zoned = jiff::civil::Date::new(y, m, d)
            .expect("valid test date")
            .to_zoned(jiff::tz::TimeZone::UTC)
            .expect("UTC has no gaps");
        u64::try_from(zoned.timestamp().as_nanosecond()).expect("post-epoch")
    }

    const TEST_HOUR_NS: u64 = 3_600_000_000_000;

    // Session midnight the reconcile derives for a NASDAQ capture header dated
    // `date`, with the config offset left unset.
    fn reconciled_nasdaq_midnight(date: u32) -> u64 {
        let mut config = live_config();
        reconcile_capture_meta(&mut config, 2, date, md_capture::FEED_UNSPECIFIED)
            .expect("nasdaq reconcile");
        config.session_midnight_ns
    }

    // ET session midnight is DST-correct: an EST date lands 5h after UTC
    // midnight, an EDT date 4h. A fixed offset would fail one of the two.
    #[test]
    fn et_session_midnight_applies_dst() {
        assert_eq!(
            reconciled_nasdaq_midnight(20260115),
            utc_midnight_ns(2026, 1, 15) + 5 * TEST_HOUR_NS,
            "January is EST (UTC-5)"
        );
        assert_eq!(
            reconciled_nasdaq_midnight(20260615),
            utc_midnight_ns(2026, 6, 15) + 4 * TEST_HOUR_NS,
            "June is EDT (UTC-4)"
        );
        assert_eq!(
            reconciled_nasdaq_midnight(20261301),
            0,
            "malformed month stays manual, no panic"
        );
    }

    // Reconcile derives an unset session midnight from the configured venue's
    // own session model: a fixed UTC+9 venue anchors at 15:00 UTC the prior
    // evening, where the New York zone would give 04:00 UTC. An explicit nonzero
    // offset wins.
    #[test]
    fn reconcile_derives_session_midnight_for_fixed_tz_venue() {
        use md_capture::FEED_UNSPECIFIED as UNSPECIFIED;

        static FIXED_UTC9: VenueDescriptor = VenueDescriptor {
            name: "test-fixed-utc9",
            session: SessionModel {
                roll: RollModel::DailyFixed {
                    offset_ns: 9 * TEST_HOUR_NS as i64,
                },
                closure: ClosureModel::None,
            },
            ..NASDAQ
        };

        let mut fixed = live_config();
        fixed.venue = &FIXED_UTC9;
        reconcile_capture_meta(&mut fixed, 2, 20260521, UNSPECIFIED).expect("fixed-tz reconcile");
        assert_eq!(
            fixed.session_midnight_ns,
            utc_midnight_ns(2026, 5, 21) - 9 * TEST_HOUR_NS,
            "UTC+9 midnight of the trade date"
        );

        let mut explicit = live_config();
        explicit.venue = &FIXED_UTC9;
        explicit.session_midnight_ns = 12_345;
        reconcile_capture_meta(&mut explicit, 2, 20260521, UNSPECIFIED).expect("reconcile");
        assert_eq!(explicit.session_midnight_ns, 12_345, "explicit offset wins");
    }

    // A `.yaml` config decodes through the YAML parser, and a `.json` config
    // with the same shape decodes identically: extension selects the parser.
    #[test]
    fn yaml_and_json_configs_load_equivalently() {
        let yaml = "\
venue: nasdaq
venue_id: 2
session_midnight_ns: 0
source:
  kind: live
  endpoint: 127.0.0.1:18001
";
        let json = r#"{"venue":"nasdaq","venue_id":2,"session_midnight_ns":0,
            "source":{"kind":"live","endpoint":"127.0.0.1:18001"}}"#;

        let from_yaml = parse_config(Path::new("config.yaml"), yaml).expect("yaml config parses");
        let from_json = parse_config(Path::new("config.json"), json).expect("json config parses");

        assert_eq!(from_yaml.venue.name, NASDAQ.name);
        assert_eq!(from_json.venue.name, NASDAQ.name);
        assert!(matches!(from_yaml.source, SourceConfig::Live { .. }));
        assert!(matches!(from_json.source, SourceConfig::Live { .. }));
        // Defaulted list is applied regardless of source format.
        assert_eq!(from_yaml.ohlcv_resolutions.len(), 7);
    }

    // An unknown extension falls back to the YAML parser, which accepts JSON
    // (YAML is a JSON superset), so a misnamed or bare-name file still loads.
    #[test]
    fn unknown_extension_falls_back_to_yaml() {
        let json = r#"{"venue":"nasdaq","venue_id":2,"session_midnight_ns":0,
            "source":{"kind":"replay","path":"/tmp/x.bin"}}"#;
        let cfg = parse_config(Path::new("config"), json).expect("bare-name json loads via yaml");
        assert_eq!(cfg.venue.name, NASDAQ.name);
    }

    #[test]
    fn nasdaq_live_config_rejected() {
        let config = DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 1,
            session_midnight_ns: 0,
            source: SourceConfig::Live {
                endpoint: "127.0.0.1:1".parse().unwrap(),
                soupbin: Default::default(),
            },
            sink: SinkKind::Null,
            taps: TapConfig::default(),
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        };

        let err = run_config(config).expect_err("NASDAQ live must be rejected before dialing");
        assert!(
            err.to_string()
                .contains("unsupported venue/source combination"),
            "got: {err}"
        );
    }

    // `--ohlcv-resolutions` tokens are extracted from argv and, when present,
    // replace the config's list; a malformed token fails at startup.
    #[test]
    fn cli_override_replaces_config() {
        let args = vec![
            "config.json".to_string(),
            "--ohlcv-resolutions=1s,5m".to_string(),
        ];
        let overrides = CliOverrides::from_args(&args);
        assert_eq!(
            overrides.ohlcv_resolutions,
            Some(vec!["1s".to_string(), "5m".to_string()])
        );

        let mut config: DatafeedConfig = serde_json::from_str(
            r#"{"venue":"nasdaq","venue_id":2,"session_midnight_ns":0,
                "source":{"kind":"live","endpoint":"127.0.0.1:1"}}"#,
        )
        .expect("config parses");
        assert_eq!(config.ohlcv_resolutions.len(), 7, "config default");
        apply_overrides(&mut config, &overrides).expect("valid override applies");
        assert_eq!(config.ohlcv_resolutions, vec!["1s", "5m"]);

        let bad = CliOverrides {
            ohlcv_resolutions: Some(vec!["5x".to_string()]),
        };
        assert!(
            apply_overrides(&mut config, &bad).is_err(),
            "malformed CLI token rejected at startup"
        );

        let none = CliOverrides::from_args(&["config.json".to_string()]);
        assert_eq!(
            none.ohlcv_resolutions, None,
            "no flag leaves config untouched"
        );
    }
}
