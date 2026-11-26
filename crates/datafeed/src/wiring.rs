//! Output sink and capture-tap construction from deployment config.

use std::path::{Path, PathBuf};

use crate::{
    ConfigError,
    config::{DatafeedConfig, SinkKind, SourceConfig, parse_resolution},
    sink::{AnySink, CollectingSink, NullSink},
    tap::{AnyTap, MdCaptureTap, NoopTap},
    venue::VenueDescriptor,
};

const NS_PER_DAY: u64 = 86_400_000_000_000;
const NS_PER_HOUR: u64 = 3_600_000_000_000;
const NS_PER_MIN: u64 = 60_000_000_000;
const NS_PER_SEC: u64 = 1_000_000_000;

pub(crate) fn build_tap(config: &DatafeedConfig) -> Result<AnyTap, ConfigError> {
    let taps = &config.taps;
    // It used to gate the whole tap, so silently ignoring it would turn capture
    // off on every config that still sets it. Refuse instead.
    if taps.normalized_dir.is_some() {
        return Err(ConfigError::RetiredNormalizedDir);
    }
    let Some(raw_dir) = &taps.raw_dir else {
        return Ok(AnyTap::Noop(NoopTap));
    };
    // Live dates the capture from its first frame (a set date goes stale across a
    // restart on a later day); replay takes it from the MDC1 header or config.
    match (&config.source, taps.capture_date) {
        (SourceConfig::Replay { .. }, 0) => {
            return Err(ConfigError::ReplayTapDateUnset);
        }
        (SourceConfig::Live { .. } | SourceConfig::LiveDual { .. }, date @ 1..) => {
            return Err(ConfigError::LiveTapDateSet { date });
        }
        _ => {}
    }
    let interval_ns = parse_resolution(&taps.partition)?;
    let metadata = md_capture::CaptureMeta {
        venue: config.venue_id,
        date: taps.capture_date,
        schema: 1,
        feed: config.venue.feed_id,
    };
    Ok(AnyTap::Md(MdCaptureTap::new(
        raw_dir.clone(),
        config.venue,
        metadata,
        interval_ns,
    )))
}

/// The `<bucket_root>/<venue>/<version>/<YYYY-MM-DD>` directory a session's files
/// live in. The day is the trade date `capture_date` (from the MDC1 header), so
/// one capture is one date dir regardless of tz; sub-day windows split by
/// filename, not a deeper dir.
pub(crate) fn capture_dir(
    root: &Path,
    venue: &'static VenueDescriptor,
    capture_date: u32,
) -> PathBuf {
    root.join(venue.tag)
        .join(venue.version)
        .join(fmt_date(capture_date))
}

/// Self-describing leaf: `<venue>_<version>_<window>[_r<part>].mdc1.zst`.
/// Part `0` is the window's first file; a run restarted inside the window
/// writes part `1`, `2`, ... beside it instead of truncating it.
pub(crate) fn capture_filename(
    venue: &'static VenueDescriptor,
    capture_date: u32,
    offset_ns: u64,
    interval_ns: u64,
    part: u32,
) -> String {
    let win = window_label(capture_date, offset_ns, interval_ns);
    let (tag, ver) = (venue.tag, venue.version);
    match part {
        0 => format!("{tag}_{ver}_{win}.mdc1.zst"),
        part => format!("{tag}_{ver}_{win}_r{part}.mdc1.zst"),
    }
}

/// Window token. The day is the trade date `capture_date` (venue-session fact
/// from the header), not an event-time floor, so a non-UTC session is not split
/// across two UTC days. Sub-day partitions append `offset_ns`, the window's
/// offset from the session open: `Thh` (hourly), `ThhMM` (minute), `ThhMMSS`
/// (second). The offset does not wrap at 24h, so a 25h fall-back session labels
/// its last hour `T24` instead of reusing `T00`. Underscore separates fields;
/// hyphen/`T` only here.
pub(crate) fn window_label(capture_date: u32, offset_ns: u64, interval_ns: u64) -> String {
    let day = fmt_date(capture_date);
    if interval_ns >= NS_PER_DAY {
        return day;
    }
    let (hh, mm, ss) = (
        offset_ns / NS_PER_HOUR,
        (offset_ns % NS_PER_HOUR) / NS_PER_MIN,
        (offset_ns % NS_PER_MIN) / NS_PER_SEC,
    );
    if interval_ns >= NS_PER_HOUR {
        format!("{day}T{hh:02}")
    } else if interval_ns >= NS_PER_MIN {
        format!("{day}T{hh:02}{mm:02}")
    } else {
        format!("{day}T{hh:02}{mm:02}{ss:02}")
    }
}

/// `YYYYMMDD` trade date (the MDC1 header field) to `YYYY-MM-DD`.
fn fmt_date(yyyymmdd: u32) -> String {
    let (y, m, d) = (yyyymmdd / 10_000, (yyyymmdd / 100) % 100, yyyymmdd % 100);
    format!("{y:04}-{m:02}-{d:02}")
}

pub(crate) fn build_sink(config: &DatafeedConfig) -> AnySink {
    match config.sink {
        SinkKind::Null => AnySink::Null(NullSink),
        SinkKind::Collecting => AnySink::Collecting(CollectingSink::default()),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        NS_PER_DAY, NS_PER_HOUR, NS_PER_MIN, build_tap, capture_dir, capture_filename, window_label,
    };
    use crate::{
        ConfigError,
        config::{DatafeedConfig, SinkKind, SourceConfig, TapConfig, default_resolution_tokens},
        tap::AnyTap,
        venue::{NASDAQ, VenueDescriptor},
    };

    // Leaf is venue tag + version + window (+ restart part), hyphen/T only in
    // the window, whose day is `capture_date` and whose time is the session offset.
    #[test]
    fn capture_filename_self_describing() {
        // The window sits 13h past the session open: the hour is that offset,
        // the day comes from capture_date, not an event-time floor.
        assert_eq!(
            capture_filename(&NASDAQ, 20260521, 13 * NS_PER_HOUR, NS_PER_HOUR, 0),
            "nasdaq_v5_2026-05-21T13.mdc1.zst"
        );
        // Sub-hour windows carry minute precision, so 13:00 and 13:30 are
        // distinct files (a bare `T13` would truncate one over the other).
        let half = 30 * NS_PER_MIN;
        let base = 13 * NS_PER_HOUR;
        assert_eq!(
            capture_filename(&NASDAQ, 20260521, base, half, 0),
            "nasdaq_v5_2026-05-21T1300.mdc1.zst"
        );
        assert_eq!(
            capture_filename(&NASDAQ, 20260521, base + half, half, 0),
            "nasdaq_v5_2026-05-21T1330.mdc1.zst"
        );
        // A restart part keeps the window token and appends `_r<part>`.
        assert_eq!(
            capture_filename(&NASDAQ, 20260521, base, NS_PER_HOUR, 2),
            "nasdaq_v5_2026-05-21T13_r2.mdc1.zst"
        );
    }

    // A fall-back session's 25th hour is T24: wrapping to T00 would reopen, and
    // truncate, the session's first hourly file.
    #[test]
    fn window_label_does_not_wrap_at_24h() {
        assert_eq!(
            window_label(20261101, 24 * NS_PER_HOUR, NS_PER_HOUR),
            "2026-11-01T24"
        );
    }

    #[test]
    fn capture_dir_nests_venue_version_day() {
        assert_eq!(
            capture_dir(Path::new("/out/normalized"), &NASDAQ, 20260521),
            PathBuf::from("/out/normalized/nasdaq/v5/2026-05-21")
        );
    }

    fn config_with(taps: TapConfig) -> DatafeedConfig {
        DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 1,
            session_midnight_ns: 0,
            source: SourceConfig::Live {
                endpoint: "127.0.0.1:1".parse().unwrap(),
                soupbin: Default::default(),
            },
            sink: SinkKind::Null,
            taps,
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        }
    }

    fn taps(normalized_dir: Option<PathBuf>, raw_dir: Option<PathBuf>) -> TapConfig {
        TapConfig {
            normalized_dir,
            raw_dir,
            ..TapConfig::default()
        }
    }

    // raw_dir builds an Md tap (files open lazily, not at build time); unset
    // is a NoopTap.
    #[test]
    fn build_tap_selects_and_guards() {
        let tap = build_tap(&config_with(taps(None, Some(PathBuf::from("/out/raw")))))
            .expect("raw tap builds");
        assert!(matches!(tap, AnyTap::Md(_)));

        let noop = build_tap(&config_with(TapConfig::default())).expect("no taps builds");
        assert!(matches!(noop, AnyTap::Noop(_)));
    }

    // normalized_dir used to gate the whole tap, so a config still setting it
    // would silently capture nothing. Refuse instead of ignoring.
    #[test]
    fn build_tap_rejects_retired_normalized_dir() {
        let err = match build_tap(&config_with(taps(Some(PathBuf::from("/out/norm")), None))) {
            Ok(_) => panic!("retired normalized_dir must be rejected"),
            Err(e) => e,
        };
        assert!(
            matches!(err, ConfigError::RetiredNormalizedDir),
            "got: {err}"
        );
    }

    #[test]
    fn build_tap_rejects_bad_partition() {
        let mut t = taps(None, Some(PathBuf::from("/out/raw")));
        t.partition = "5x".to_string();
        assert!(
            matches!(build_tap(&config_with(t)), Err(ConfigError::Resolution(_))),
            "malformed partition rejected"
        );
    }

    // Live refuses a set date, which goes stale across a restart; replay refuses
    // an unresolved (0) one instead of writing a 0000-00-00 tree.
    #[test]
    fn build_tap_capture_date_follows_source() {
        let raw = Some(PathBuf::from("/out/raw"));
        let mut live = config_with(taps(None, raw.clone()));
        live.taps.capture_date = 20260521;
        let mut replay = config_with(taps(None, raw));
        replay.source = SourceConfig::Replay {
            path: PathBuf::from("/in/feed.bin"),
            format: Default::default(),
            base_ts: 0,
            step_ns: 1,
            load: Default::default(),
            pacing: Default::default(),
        };
        assert!(matches!(
            build_tap(&live).err(),
            Some(ConfigError::LiveTapDateSet { date: 20260521 })
        ));
        assert!(matches!(
            build_tap(&replay).err(),
            Some(ConfigError::ReplayTapDateUnset)
        ));
    }

    // The producer path: build_tap stamps the configured venue's feed id into
    // the container header, so a capture the datafeed writes identifies its own
    // decoder. Readback of a written file proves the wiring, not just the id
    // table (which the reconcile test covers).
    #[test]
    fn build_tap_stamps_venue_feed_into_header() {
        use md_pipeline::StreamId;

        use crate::tap::CaptureTap;

        // A second feed of the venue `config_with` names: the header can carry
        // its id only if build_tap reads the configured descriptor.
        static OTHER_FEED: VenueDescriptor = VenueDescriptor {
            name: "test-other-feed",
            feed_id: proto_nasdaq::FEED_NASDAQ + 1,
            ..NASDAQ
        };

        let raw = std::env::temp_dir().join("datafeed_wiring_feedstamp_raw");
        let _ = std::fs::remove_dir_all(&raw);

        let mut config = config_with(taps(None, Some(raw.clone())));
        config.venue = &OTHER_FEED;

        let mut tap = build_tap(&config).expect("md tap builds");
        tap.raw(StreamId(0), NS_PER_DAY, b"x")
            .expect("write raw frame");
        tap.finish().expect("finish");

        // Live names the dir after the frame's session: 00:00 UTC on 1970-01-02
        // is 19:00 in New York the day before.
        let raw_path = raw
            .join("nasdaq/v5/1970-01-01")
            .join("nasdaq_v5_1970-01-01.mdc1.zst");
        let (_, meta) = md_capture::RawReader::new(std::io::Cursor::new(
            std::fs::read(&raw_path).expect("read written raw file"),
        ))
        .expect("open raw reader");
        assert_eq!(meta.feed, proto_nasdaq::FEED_NASDAQ + 1);

        let _ = std::fs::remove_dir_all(&raw);
    }
}
