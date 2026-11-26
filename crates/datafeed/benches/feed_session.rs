//! Cost of the two feed session handoffs against the batch runner, in ns per
//! published record.
//!
//! The binary's path is a [`SinkSession`]: each poll takes one step and
//! publishes in place into the sink, as `session::run` does. The host's path
//! is a [`FeedSession`]: each poll hands records out as `OwnedRecord`s in the
//! host's `Vec`, and the host then publishes each one. All three passes replay
//! the same input from memory into a sink that only counts, unthrottled,
//! batch 64, with a fresh session per round, and the rounds rotate which pass
//! goes first. The input is the vendored NASDAQ order-book reference fixture
//! repeated [`REPEATS`] times.
//!
//! `BENCH_ROUNDS` sets the rounds per pass, [`DEFAULT_ROUNDS`] when unset.

#[path = "../tests/support/mod.rs"]
mod support;

use std::{
    cell::Cell,
    fs,
    path::Path,
    rc::Rc,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

use datafeed::{
    FeedError, FeedSession, SinkSession,
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
// a multiple of three, so each pass goes first, second and third equally often
const DEFAULT_ROUNDS: usize = 33;
// the batch the binary's replay feed reads
const BATCH: usize = 64;
// venue id every NASDAQ test uses; a BinaryFILE has no header to adopt one from
const NASDAQ_VENUE: u16 = 2;

/// Counts what it is handed and keeps nothing, so every pass checks it
/// published the same number of records. The count is shared because a
/// session keeps its sink until it closes.
#[derive(Clone, Default)]
struct CountingSink(Rc<Cell<u64>>);

impl OutputSink for CountingSink {
    fn publish(&mut self, _rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.0.set(self.0.get() + 1);
        Ok(())
    }
}

fn rounds() -> usize {
    match std::env::var("BENCH_ROUNDS") {
        Ok(value) => value.parse().expect("BENCH_ROUNDS must be a usize"),
        Err(_) => DEFAULT_ROUNDS,
    }
}

fn source(path: &Path) -> SourceConfig {
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
fn runner_pass(path: &Path) -> (u64, Duration) {
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
    let sink = CountingSink::default();
    let mut seq = EgressSequencer::new(sink.clone(), clock);

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
    (sink.0.get(), elapsed)
}

/// The binary's loop: poll until `Closed`, each poll publishing in place.
/// Returns the records published and the time of the polls alone.
fn sink_pass(path: &Path) -> (u64, Duration) {
    let sink = CountingSink::default();
    let mut session: SinkSession<CountingSink> = FeedSession::builder(&NASDAQ, source(path))
        .venue_id(NASDAQ_VENUE)
        .build_with_sink(sink.clone())
        .expect("the bench session builds");

    let started = Instant::now();
    loop {
        match session.poll() {
            Ok(()) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("bench replay failed: {err}"),
        }
    }
    let elapsed = started.elapsed();

    session.close().expect("close with no capture tap");
    (sink.0.get(), elapsed)
}

/// A host's loop: poll with no cap, publish each polled record. Returns the
/// records published and the time of the polls and publishes alone.
fn host_pass(path: &Path) -> (u64, Duration) {
    let mut session = FeedSession::builder(&NASDAQ, source(path))
        .venue_id(NASDAQ_VENUE)
        .build()
        .expect("the bench session builds");
    let mut sink = CountingSink::default();
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
    (sink.0.get(), elapsed)
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// Each round's `pass` minus the batch runner's.
fn paired(pass: &[f64], runner: &[f64]) -> Vec<f64> {
    pass.iter().zip(runner).map(|(p, r)| p - r).collect()
}

/// One timed replay: the records it published and the time they took.
type Pass = fn(&Path) -> (u64, Duration);

fn main() {
    let path = std::env::temp_dir().join("datafeed_feed_session_bench.bin");
    fs::write(&path, binary_dump(STREAM_HEX).repeat(REPEATS)).expect("write the bench input");
    let rounds = rounds();
    let passes: [Pass; 3] = [runner_pass, sink_pass, host_pass];

    let mut ns_per_record = [const { Vec::new() }; 3];
    let mut records = 0;
    for round in 0..rounds {
        let mut timed = [(0, Duration::ZERO); 3];
        for offset in 0..passes.len() {
            let pass = (round + offset) % passes.len();
            timed[pass] = passes[pass](&path);
        }
        records = timed[0].0;
        assert!(
            timed.iter().all(|&(count, _)| count == records),
            "the passes published different records"
        );
        for (samples, (_, elapsed)) in ns_per_record.iter_mut().zip(timed) {
            samples.push(elapsed.as_nanos() as f64 / records as f64);
        }
    }
    let _ = fs::remove_file(&path);

    let [mut runner, mut sink, mut host] = ns_per_record;
    let mut sink_delta = paired(&sink, &runner);
    let mut host_delta = paired(&host, &runner);
    println!("{records} records per pass, {rounds} rounds, medians in ns per record");
    println!("batch runner:  {:.2}", median(&mut runner));
    println!(
        "sink session:  {:.2}  paired delta {:+.2}",
        median(&mut sink),
        median(&mut sink_delta)
    );
    println!(
        "host poll:     {:.2}  paired delta {:+.2}",
        median(&mut host),
        median(&mut host_delta)
    );
}
