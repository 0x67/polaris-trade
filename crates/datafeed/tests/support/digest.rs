//! Per-stream sha256 digests of published records in the committed
//! `<sha256> <fixture>/<stream> <count>` form, and the replay assembly that
//! feeds them. Shared by the seam parity gate and the deployed-binary tests.

use std::{collections::BTreeMap, fmt::Write as _, sync::atomic::AtomicBool};

use datafeed::{
    config::DatafeedConfig,
    restart::RestartState,
    session::{self, SessionState, SessionStats},
    tap::NoopTap,
};
use md_events::rtype;
use md_pipeline::{DynAdapter, EgressSequencer, IngestSource, OutputSink, RecordRef, SinkError};
use md_replay::{PacedSource, Pacing, PacingMode, ReplayDriver, SharedClock, ThreadSleepPacer};
use sha2::{Digest, Sha256};

use super::record::header_bytes;

// mirrors BATCH_SIZE in src/replay.rs
const BATCH_SIZE: usize = 64;
const DIGESTS: &str = include_str!("../fixtures/seam-parity/baseline-nasdaq.sha256");

#[derive(Default)]
pub struct Tally {
    hasher: Sha256,
    count: u64,
}

/// Overall digest plus one per `(stream, rtype)` output stream.
#[derive(Default)]
pub struct DigestSink {
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

/// Lowercase hex of a digest. sha2 0.11 returns an array with no `LowerHex`.
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The sink's digest lines in the committed `<sha256> <fixture>/<stream> <count>` form.
pub fn digest_lines(fixture: &str, sink: &DigestSink) -> String {
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
pub fn expected_digests(fixture: &str) -> String {
    let tag = format!(" {fixture}/");
    DIGESTS
        .lines()
        .filter(|line| line.contains(&tag))
        .map(|line| format!("{line}\n"))
        .collect()
}

/// A replay config parsed the way the binary loads one, so the venue name
/// resolves through the registry and every unset field takes its serde default.
pub fn config(venue: &str, venue_id: u16, session_midnight_ns: u64) -> DatafeedConfig {
    serde_json::from_str(&format!(
        r#"{{"venue": "{venue}", "venue_id": {venue_id},
            "session_midnight_ns": {session_midnight_ns},
            "source": {{"kind": "replay", "path": "fixture"}}}}"#
    ))
    .expect("parity config")
}

/// Replay `source` through `adapter` on the erased path the binary runs and
/// return what the sequencer's sink saw, with the session counters.
pub fn replay_digest<Src: IngestSource>(
    config: &DatafeedConfig,
    adapter: &mut dyn DynAdapter,
    source: Src,
) -> (DigestSink, SessionStats) {
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
    (seq.into_sink(), stats)
}
