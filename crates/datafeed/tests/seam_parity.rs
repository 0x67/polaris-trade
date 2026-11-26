//! Seam parity gate: replays the fixtures the venue-adapter seam was baselined
//! on through the binary's own path (a `DatafeedConfig`, the venue's adapter,
//! the one session runner over the erased adapter) and compares one sha256 per
//! normalized output stream, plus the session and adapter counters, against
//! `tests/fixtures/seam-parity/`. Each case runs the adapter the descriptor's
//! `make` builds.
//!
//! The digests are per-venue runner output from before the seam. A mismatch is
//! an output change to fix. Rebase a line only for an intended change with a
//! proven cause (only the bytes and records it explains change, every other
//! count and counter holds), in the commit that makes the change, so its diff
//! keeps the old digests. `lat.md/datafeed/tests.md` "Seam parity gate" names
//! each rebase and its proof.
//!
//! Digests hash each record's stamped repr(C) bytes, header then body, with
//! replay assembled like `src/replay.rs`: an unthrottled `PacedSource` driving a
//! `SharedClock`, batch 64, and a zero session anchor.
//!
//! The NASDAQ order-book reference fixture is vendored and runs every time.
//! The NASDAQ prefix is the venue crate's fixture; run it with
//! `cargo nextest run --release -p polaris-datafeed --test seam_parity --run-ignored all`
//! and `NASDAQ_ITCH_PREFIX` set to the path of
//! `crates/venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin`.

mod support;

use std::{
    collections::BTreeMap, fmt::Write as _, io::Cursor, path::PathBuf, sync::atomic::AtomicBool,
};

use datafeed::{
    config::DatafeedConfig,
    restart::RestartState,
    session::{self, SessionState, SessionStats},
    tap::NoopTap,
};
use md_events::rtype;
use md_pipeline::{
    AdapterStats, DynAdapter, EgressSequencer, IngestSource, OutputSink, RecordRef, SinkError,
};
use md_replay::{
    BinaryFileReplay, PacedSource, Pacing, PacingMode, ReplayDriver, SharedClock, ThreadSleepPacer,
};
use sha2::{Digest, Sha256};
use support::{record::header_bytes, reference_parity::binary_dump};

// mirrors BATCH_SIZE in src/replay.rs
const BATCH_SIZE: usize = 64;
// venue id every NASDAQ test uses; a BinaryFILE has no header to adopt one from
const NASDAQ_VENUE: u16 = 2;
const STREAM_HEX: &str = include_str!("fixtures/orderbook-reference/stream.hex");
const DIGESTS: &str = include_str!("fixtures/seam-parity/baseline-nasdaq.sha256");
const COUNTERS: &str = include_str!("fixtures/seam-parity/baseline-counters.txt");

#[derive(Default)]
struct Tally {
    hasher: Sha256,
    count: u64,
}

/// Overall digest plus one per `(stream, rtype)` output stream.
#[derive(Default)]
struct DigestSink {
    all: Tally,
    per_stream: BTreeMap<(u16, u8), Tally>,
}

impl OutputSink for DigestSink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        let header = header_bytes(rec.header);
        let stream = self
            .per_stream
            .entry((rec.stream.0, rec.header.rtype))
            .or_default();
        for tally in [&mut self.all, stream] {
            tally.hasher.update(header);
            tally.hasher.update(rec.body);
            tally.count += 1;
        }
        Ok(())
    }
}

fn rtype_name(rt: u8) -> String {
    let name = match rt {
        rtype::CONTROL => "control",
        rtype::MBO => "mbo",
        rtype::LEVEL => "level",
        rtype::MBP10 => "mbp10",
        rtype::BBO => "bbo",
        rtype::TRADE => "trade",
        rtype::TRADE_BREAK => "trade_break",
        rtype::OHLCV => "ohlcv",
        rtype::IMBALANCE => "imbalance",
        rtype::FILL => "fill",
        rtype::STATS => "stats",
        rtype::STATUS => "status",
        rtype::DEFINITION => "definition",
        other => return format!("rtype0x{other:02x}"),
    };
    name.to_owned()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The sink's digest lines in the committed `<sha256> <fixture>/<stream> <count>` form.
fn digest_lines(fixture: &str, sink: &DigestSink) -> String {
    let digest = |t: &Tally| hex(&t.hasher.clone().finalize());
    let mut lines = format!("{} {fixture}/all {}\n", digest(&sink.all), sink.all.count);
    for ((stream, rt), tally) in &sink.per_stream {
        let _ = writeln!(
            lines,
            "{} {fixture}/stream{stream}.{} {}",
            digest(tally),
            rtype_name(*rt),
            tally.count
        );
    }
    lines
}

/// The committed digest lines for one fixture.
fn expected_digests(fixture: &str) -> String {
    let tag = format!(" {fixture}/");
    DIGESTS
        .lines()
        .filter(|line| line.contains(&tag))
        .map(|line| format!("{line}\n"))
        .collect()
}

/// The raw `value` of a `key=value` from the committed counter block of one fixture.
fn expected_value(fixture: &str, key: &str) -> &'static str {
    let block = COUNTERS
        .split("fixture=")
        .find(|block| block.starts_with(&format!("{fixture}\n")))
        .unwrap_or_else(|| panic!("no counters for {fixture}"));
    block
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("no {key} for {fixture}"))
}

/// A numeric `key=value` from the committed counter block of one fixture.
fn expected_counter(fixture: &str, key: &str) -> u64 {
    expected_value(fixture, key)
        .parse()
        .expect("numeric counter")
}

/// Session counters must match verbatim. Every fixture decoded each frame and
/// dropped none, so `decoded` equals the frame count and both error counters
/// stay zero.
fn assert_counters(fixture: &str, stats: &SessionStats, adapter: &AdapterStats) {
    let frames = expected_counter(fixture, "session.frames");
    assert_eq!(stats.frames, frames, "{fixture} frames");
    assert_eq!(
        stats.records,
        expected_counter(fixture, "session.records"),
        "{fixture} records"
    );
    assert!(
        stats.unknown_order_counts.is_empty(),
        "{fixture} unknown orders"
    );
    assert_eq!(
        (adapter.decoded, adapter.unknown_kind, adapter.malformed),
        (frames, 0, 0),
        "{fixture} adapter counters"
    );
}

/// A replay config parsed the way the binary loads one, so the venue name
/// resolves through the registry and every unset field takes its serde default.
fn config(venue: &str, venue_id: u16, session_midnight_ns: u64) -> DatafeedConfig {
    serde_json::from_str(&format!(
        r#"{{"venue": "{venue}", "venue_id": {venue_id},
            "session_midnight_ns": {session_midnight_ns},
            "source": {{"kind": "replay", "path": "fixture"}}}}"#
    ))
    .expect("parity config")
}

/// Replay `source` through `adapter` on the erased path the binary runs, and
/// check the result against the committed baseline.
fn replay_and_compare<Src: IngestSource>(
    fixture: &str,
    config: &DatafeedConfig,
    adapter: &mut dyn DynAdapter,
    source: Src,
) {
    let clock = SharedClock::new();
    let paced = PacedSource::new(
        ReplayDriver::new(
            source,
            ThreadSleepPacer,
            Pacing::Fixed(PacingMode::Unthrottled),
        ),
        clock.clone(),
    );
    let mut seq = EgressSequencer::new(DigestSink::default(), clock);
    let mut state = SessionState::from_config(config).expect("default resolutions");
    let (stats, _) = session::run(
        paced,
        adapter,
        &mut seq,
        &mut NoopTap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: BATCH_SIZE,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("replay");
    assert_eq!(digest_lines(fixture, seq.sink()), expected_digests(fixture));
    assert_counters(fixture, &stats, adapter.stats());
}

fn env_path(key: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(key).unwrap_or_else(|| panic!("{key} must be set")))
}

/// A NASDAQ source replayed through the adapter the descriptor's `make` builds.
fn nasdaq<Src: IngestSource>(fixture: &str, source: Src) {
    let config = config("nasdaq", NASDAQ_VENUE, 0);
    replay_and_compare(fixture, &config, &mut *(config.venue.make)(&config), source);
}

#[test]
fn nasdaq_orderbook_reference_matches_seam_baseline() {
    let source = BinaryFileReplay::new(Cursor::new(binary_dump(STREAM_HEX)), 0, 1);
    nasdaq("nasdaq-orderbook-reference", source);
}

#[test]
#[ignore = "needs NASDAQ_ITCH_PREFIX; see the module doc"]
fn nasdaq_itch_prefix_matches_seam_baseline() {
    let source =
        BinaryFileReplay::from_path(env_path("NASDAQ_ITCH_PREFIX"), 0, 1).expect("open prefix");
    nasdaq("nasdaq-itch-prefix", source);
}
