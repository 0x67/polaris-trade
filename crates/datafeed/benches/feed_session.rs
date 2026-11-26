//! Cost of the binary's record handoff: [`FeedSession::poll`] against the
//! batch runner, in ns per published record.
//!
//! The binary copies each record into the session's queue as an
//! `OwnedRecord`, again into its own `Vec` on poll, then publishes it from
//! there (`app::forward`). `session::run` publishes each record in place. Both
//! passes replay the same input from memory into a sink that only counts,
//! unthrottled, batch 64, with a fresh session per round, and the rounds
//! alternate which pass goes first. The input is the vendored NASDAQ
//! order-book reference fixture repeated [`REPEATS`] times.
//!
//! `BENCH_ROUNDS` sets the rounds per pass, [`DEFAULT_ROUNDS`] when unset.

#[path = "../tests/support/mod.rs"]
mod support;

use std::{
    fs,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

use datafeed::{
    FeedError, FeedSession,
    config::{DatafeedConfig, LoadMode, ReplayFormat, SourceConfig},
    restart::RestartState,
    session::{self, SessionState},
    tap::NoopTap,
    venue::NASDAQ,
};
use md_pipeline::{EgressSequencer, OutputSink, RecordRef, SinkError};
use md_replay::{BinaryFileReplay, PacedSource, Pacing, PacingMode, ReplayDriver, SharedClock};
use support::reference_parity::binary_dump;

const STREAM_HEX: &str = include_str!("../tests/fixtures/orderbook-reference/stream.hex");
const REPEATS: usize = 1_000;
const DEFAULT_ROUNDS: usize = 31;
// the batch the binary's replay feed reads
const BATCH: usize = 64;
// venue id every NASDAQ test uses; a BinaryFILE has no header to adopt one from
const NASDAQ_VENUE: u16 = 2;

/// Counts what it is handed and keeps nothing, so both passes check they
/// published the same number of records.
struct CountingSink(u64);

impl OutputSink for CountingSink {
    fn publish(&mut self, _rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.0 += 1;
        Ok(())
    }
}

fn rounds() -> usize {
    match std::env::var("BENCH_ROUNDS") {
        Ok(value) => value.parse().expect("BENCH_ROUNDS must be a usize"),
        Err(_) => DEFAULT_ROUNDS,
    }
}

fn source(path: &std::path::Path) -> SourceConfig {
    SourceConfig::Replay {
        path: path.to_path_buf(),
        format: ReplayFormat::Binary,
        base_ts: 0,
        step_ns: 1,
        load: LoadMode::Eager,
        pacing: Default::default(),
    }
}

/// `session::run` publishing in place, assembled as the replay feed assembles
/// its source. Returns the records published and the time of the run alone.
fn runner_pass(path: &std::path::Path) -> (u64, Duration) {
    let config: DatafeedConfig = serde_json::from_str(&format!(
        r#"{{"venue": "nasdaq", "venue_id": {NASDAQ_VENUE}, "session_midnight_ns": 0,
            "source": {{"kind": "replay", "path": "bench"}}}}"#
    ))
    .expect("runner config");
    let clock = SharedClock::new();
    let paced = PacedSource::new(
        ReplayDriver::new(
            BinaryFileReplay::from_path_eager(path, 0, 1).expect("load the bench input"),
            md_replay::ThreadSleepPacer,
            Pacing::Fixed(PacingMode::Unthrottled),
        ),
        clock.clone(),
    );
    let mut adapter = (config.venue.make)(&config);
    let mut state = SessionState::from_config(&config).expect("default resolutions");
    let mut seq = EgressSequencer::new(CountingSink(0), clock);

    let started = Instant::now();
    session::run(
        paced,
        &mut *adapter,
        &mut seq,
        &mut NoopTap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: BATCH,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("a counting sink and no tap cannot fail the run");
    let elapsed = started.elapsed();
    (seq.sink().0, elapsed)
}

/// The binary's loop: poll with no cap, publish each polled record. Returns
/// the records published and the time of the polls and publishes alone.
fn session_pass(path: &std::path::Path) -> (u64, Duration) {
    let mut session = FeedSession::builder(&NASDAQ, source(path))
        .venue_id(NASDAQ_VENUE)
        .build()
        .expect("the bench session builds");
    let mut sink = CountingSink(0);
    let mut records = Vec::new();

    let started = Instant::now();
    loop {
        records.clear();
        match session.poll(&mut records, usize::MAX) {
            Ok(_) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("bench replay failed: {err}"),
        }
        for record in &records {
            sink.publish(record.as_record_ref())
                .expect("a counting sink cannot fail");
        }
    }
    let elapsed = started.elapsed();

    session.close().expect("close with no capture tap");
    (sink.0, elapsed)
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn main() {
    let path = std::env::temp_dir().join("datafeed_feed_session_bench.bin");
    fs::write(&path, binary_dump(STREAM_HEX).repeat(REPEATS)).expect("write the bench input");
    let rounds = rounds();

    let mut runner = Vec::with_capacity(rounds);
    let mut session = Vec::with_capacity(rounds);
    let mut records = 0;
    for round in 0..rounds {
        let (runner_run, session_run) = if round % 2 == 0 {
            let runner_run = runner_pass(&path);
            (runner_run, session_pass(&path))
        } else {
            let session_run = session_pass(&path);
            (runner_pass(&path), session_run)
        };
        assert_eq!(
            runner_run.0, session_run.0,
            "the passes published different records"
        );
        records = runner_run.0;
        runner.push(runner_run.1.as_nanos() as f64 / records as f64);
        session.push(session_run.1.as_nanos() as f64 / records as f64);
    }
    let mut paired: Vec<f64> = session.iter().zip(&runner).map(|(s, r)| s - r).collect();
    let _ = fs::remove_file(&path);

    println!("{records} records per pass, {rounds} rounds, medians in ns per record");
    println!("batch runner:  {:.2}", median(&mut runner));
    println!("feed session:  {:.2}", median(&mut session));
    println!("paired delta:  {:+.2}", median(&mut paired));
}
