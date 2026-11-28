//! Cost of the plugin path against the native erased adapter, in ns per
//! record.
//!
//! The native pass boxes `NasdaqAdapter` as a `dyn DynAdapter`, the shape the
//! datafeed's venue descriptors build. The plugin pass runs the same adapter
//! through `export_adapter!` and the host's `PluginAdapter`, in process: an
//! indirect call into the vtable per feed and per drain, the plugin's copy of
//! each event into its handle, and the host's checks and rebuild of each
//! record. Both feed one frame then drain it into a reused `Vec`, as the
//! session runner does, with a fresh adapter per round, and the rounds
//! alternate which pass goes first. Two inputs run apart: the order-book
//! reference fixture repeated [`REPEATS`] times (order events) and the ITCH
//! prefix fixture (definitions and trading states).
//!
//! `BENCH_ROUNDS` sets the rounds per pass, [`DEFAULT_ROUNDS`] when unset.

#[path = "../tests/support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use md_pipeline::{DynAdapter, MarketEvent, StreamId};

const REPEATS: usize = 1_000;
// the round count of the datafeed `feed_session` bench
const DEFAULT_ROUNDS: usize = 33;

fn rounds() -> usize {
    match std::env::var("BENCH_ROUNDS") {
        Ok(value) => value.parse().expect("BENCH_ROUNDS must be a usize"),
        Err(_) => DEFAULT_ROUNDS,
    }
}

/// Feeds and drains every frame through `adapter`. Returns the events and the
/// time they took.
fn pass(adapter: &mut dyn DynAdapter, frames: &[Vec<u8>]) -> (usize, Duration) {
    let mut events: Vec<MarketEvent> = Vec::new();
    let mut count = 0;
    let started = Instant::now();
    for (seq, payload) in (1..).zip(frames) {
        adapter.feed(StreamId(0), payload, seq, seq);
        adapter.drain_into(&mut events);
        count += events.len();
        events.clear();
    }
    (count, started.elapsed())
}

fn native_pass(frames: &[Vec<u8>]) -> (usize, Duration) {
    let (_, native) = support::configs();
    let mut adapter: Box<dyn DynAdapter> = Box::new(native);
    pass(&mut *adapter, frames)
}

fn plugin_pass(frames: &[Vec<u8>]) -> (usize, Duration) {
    let (json, _) = support::configs();
    let mut adapter = support::plugin().create(&json).expect("the adapter builds");
    let timed = pass(&mut adapter, frames);
    assert_eq!(adapter.fault(), None, "the plugin path faulted");
    timed
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// One timed pass: the events it emitted and the time they took.
type Pass = fn(&[Vec<u8>]) -> (usize, Duration);

#[expect(
    clippy::cast_precision_loss,
    reason = "a pass takes far less than 2^52 ns and emits far fewer than 2^52 records"
)]
fn run(name: &str, frames: &[Vec<u8>], rounds: usize) {
    let passes: [Pass; 2] = [native_pass, plugin_pass];
    let mut ns_per_record = [Vec::new(), Vec::new()];
    let mut records = 0;
    for round in 0..rounds {
        let mut timed = [(0, Duration::ZERO); 2];
        for offset in 0..passes.len() {
            let at = (round + offset) % passes.len();
            timed[at] = passes[at](frames);
        }
        records = timed[0].0;
        assert_eq!(timed[1].0, records, "the passes emitted different records");
        for (samples, (_, elapsed)) in ns_per_record.iter_mut().zip(timed) {
            samples.push(elapsed.as_nanos() as f64 / records as f64);
        }
    }
    let [mut native, mut plugin] = ns_per_record;
    let mut delta: Vec<f64> = plugin.iter().zip(&native).map(|(p, n)| p - n).collect();
    println!("{name}: {records} records per pass, {rounds} rounds, medians in ns per record");
    println!("  native dyn adapter: {:.2}", median(&mut native));
    println!(
        "  plugin path:        {:.2}  paired delta {:+.2}",
        median(&mut plugin),
        median(&mut delta)
    );
}

fn main() {
    let rounds = rounds();
    let book = support::book_frames();
    let repeated: Vec<Vec<u8>> = (0..REPEATS).flat_map(|_| book.iter().cloned()).collect();
    run("order book reference", &repeated, rounds);
    run("ITCH prefix", &support::prefix_frames(), rounds);
}
