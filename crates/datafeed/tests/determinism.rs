//! Determinism gate for the NASDAQ normalized output. Replaying one input
//! twice must yield byte-identical L0-L3 records: the pipeline is a pure
//! function of `(venue bytes, ts_recv)` with no wall-clock, randomness, or env
//! reads. Raw replay and capture-hash storage both depend on this property.
//!
//! Two levels, mirroring `tests/orderbook_reference_parity.rs`: a small
//! synthetic ITCH 5.0 stream runs on every push and covers L3 (`Mbo`), L2
//! (`Mbp10`), L1 (`Bbo`), L0 (`Trade`), status, definition, and the
//! feed-lifecycle `Control` stream; the full-day sample is `#[ignore]`d behind
//! `NASDAQ_ITCH_SAMPLE` and exercises the same families and the L0 aggregates
//! (`Ohlcv`) on real data.
//!
//! The hash is taken over each record's stamped repr(C) bytes, the exact
//! pattern `md_capture::NormalizedWriter` persists verbatim, so a green run is
//! also a check that the stored binary is reproducible. Egress uses
//! `VirtualClock` (logical time): a `SystemClock` wall-clock read anywhere on
//! the path would diverge the two runs and fail this test.

mod support;

use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
};

use datafeed::{
    restart::RestartState,
    session::{self, SessionState},
    sink::NullSink,
    tap::{MdCaptureTap, NoopTap},
    venue::NASDAQ,
};
use md_events::{Ohlcv, decode_prefix, rtype};
use md_pipeline::{
    EgressSequencer, IngestError, IngestFrame, IngestSource, OutputSink, RecordRef, SinkError,
    StreamId, VenueAdapter, VenueId, VirtualClock,
};
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};
use sha2::{Digest, Sha256};
use support::{
    capture_tree::output_tree,
    digest::hex,
    itch::{add, executed, stock_directory, system_event, trading_action},
    record::header_bytes,
    reference_parity::open_binary_sample,
};

// ---------------------------------------------------------------------------
// Minimal in-memory source: closes at end like the real replay containers
// (one `Ok(0)` on the draining call, `Err(Closed)` after), so it can drive the
// session loop to completion.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Frame {
    payload: Vec<u8>,
    ts_recv: u64,
    stream: StreamId,
    venue_seq: u64,
}

impl IngestFrame for Frame {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn ts_recv(&self) -> u64 {
        self.ts_recv
    }
    fn stream(&self) -> StreamId {
        self.stream
    }
    fn venue_seq(&self) -> u64 {
        self.venue_seq
    }
}

struct VecSource {
    frames: VecDeque<Frame>,
    closed: bool,
}

impl VecSource {
    fn new(frames: Vec<Frame>) -> Self {
        VecSource {
            frames: frames.into(),
            closed: false,
        }
    }
}

impl IngestSource for VecSource {
    type Frame = Frame;

    fn recv_batch(&mut self, out: &mut Vec<Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let n = max.min(self.frames.len());
        out.extend(self.frames.drain(..n));
        if n == 0 {
            self.closed = true;
        }
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Tap that hashes every stamped egress record and tallies a per-rtype
// histogram. Hashing the record's own repr(C) byte pattern mirrors what
// `md_capture::NormalizedWriter::write_record` stores, so a stable digest is a
// stable stored file. Memory-flat: retains no records.
// ---------------------------------------------------------------------------

struct DigestTap {
    hasher: Sha256,
    /// Per-rtype hasher + count, so a divergence localizes to a record family.
    per_rtype: BTreeMap<u8, (Sha256, u64)>,
    count: u64,
}

/// One run's fingerprint: overall digest, per-rtype (digest, count).
struct RunDigest {
    overall: String,
    per_rtype: BTreeMap<u8, (String, u64)>,
    count: u64,
}

impl DigestTap {
    fn new() -> Self {
        DigestTap {
            hasher: Sha256::new(),
            per_rtype: BTreeMap::new(),
            count: 0,
        }
    }

    fn finish(&self) -> RunDigest {
        RunDigest {
            overall: hex(&self.hasher.clone().finalize()),
            per_rtype: self
                .per_rtype
                .iter()
                .map(|(rt, (h, n))| (*rt, (hex(&h.clone().finalize()), *n)))
                .collect(),
            count: self.count,
        }
    }
}

// The sink hands header and body separately; a record's on-wire `repr(C)`
// pattern is the two concatenated, so this reproduces exactly what a consumer
// stores.
fn record_bytes(rec: &RecordRef<'_>) -> Vec<u8> {
    let hdr = header_bytes(rec.header);
    let mut out = Vec::with_capacity(hdr.len() + rec.body.len());
    out.extend_from_slice(hdr);
    out.extend_from_slice(rec.body);
    out
}

impl OutputSink for DigestTap {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        let bytes = record_bytes(&rec);
        self.hasher.update(&bytes);
        let slot = self
            .per_rtype
            .entry(rec.header.rtype)
            .or_insert_with(|| (Sha256::new(), 0));
        slot.0.update(&bytes);
        slot.1 += 1;
        self.count += 1;
        Ok(())
    }
}

const SEC: u64 = 1_000_000_000;

/// One synthetic session exercising every L0-L3 level plus the lifecycle and
/// reference streams: feed-start control, directory, status, two book adds
/// (L3/L2/L1), two trades straddling a second boundary (L0 tape, plus a closed
/// 1s bar), feed-end control.
fn synthetic_stream() -> Vec<Frame> {
    let payloads = vec![
        system_event(b'O', SEC),
        stock_directory(SEC),
        trading_action(SEC),
        add(101, b'B', 50, 1_500_000, SEC),
        add(201, b'S', 40, 1_510_000, SEC),
        executed(101, 10, 5_001, SEC),
        executed(101, 5, 5_002, 100 * SEC),
        system_event(b'C', 100 * SEC),
    ];
    payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| Frame {
            payload,
            ts_recv: 1_000 + i as u64,
            stream: StreamId(0),
            venue_seq: i as u64 + 1,
        })
        .collect()
}

// Venue id 2 as in every NASDAQ test here; the zero anchor leaves `ts_event`
// at the wire's nanoseconds since midnight.
fn nasdaq_adapter() -> NasdaqAdapter {
    NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    })
}

fn run<Src: IngestSource>(source: Src, batch: usize) -> RunDigest {
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(DigestTap::new(), VirtualClock::default());
    let mut tap = NoopTap;
    let mut state = SessionState::default();
    let mut restart = RestartState::default();
    session::run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    // a frame the adapter drops emits nothing, so it would pass every digest
    let stats = adapter.stats();
    assert_eq!(
        (stats.malformed, stats.unknown_kind),
        (0, 0),
        "every frame decodes"
    );
    seq.sink().finish()
}

// @lat: [[tests#Output determinism#Synthetic NASDAQ replay is byte-identical]]
#[test]
fn nasdaq_synthetic_output_is_byte_identical_across_replays() {
    let a = run(VecSource::new(synthetic_stream()), 4);
    let b = run(VecSource::new(synthetic_stream()), 4);

    assert!(a.count > 0, "pipeline produced no normalized records");
    assert_eq!(
        a.overall, b.overall,
        "normalized L0-L3 output digest diverged across identical replays: {} vs {}",
        a.overall, b.overall
    );
    assert_eq!(a.per_rtype, b.per_rtype, "per-rtype digest/count diverged");
    assert_eq!(a.count, b.count, "record count diverged");

    for (name, rt) in [
        ("mbo (L3)", rtype::MBO),
        ("mbp10 (L2)", rtype::MBP10),
        ("bbo (L1)", rtype::BBO),
        ("trade (L0)", rtype::TRADE),
        ("status", rtype::STATUS),
        ("definition", rtype::DEFINITION),
        ("control", rtype::CONTROL),
    ] {
        assert!(
            a.per_rtype.contains_key(&rt),
            "L0-L3 coverage gap: no {name} (0x{rt:02x}) record emitted; got {:?}",
            a.per_rtype.keys().collect::<Vec<_>>()
        );
    }
}

// Records raw bytes of the first `CAP` records of one target rtype, so two
// runs can be byte-diffed to localize a divergence to specific field offsets.
struct CollectRtypeTap {
    target: u8,
    cap: usize,
    rows: Vec<Vec<u8>>,
}

impl OutputSink for CollectRtypeTap {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        if rec.header.rtype == self.target && self.rows.len() < self.cap {
            self.rows.push(record_bytes(&rec));
        }
        Ok(())
    }
}

fn nasdaq_sample_path() -> PathBuf {
    PathBuf::from(
        std::env::var_os("NASDAQ_ITCH_SAMPLE")
            .expect("NASDAQ_ITCH_SAMPLE must point to the full-day capture (.bin.zst or .gz)"),
    )
}

fn collect_rtype(path: &Path, target: u8, cap: usize) -> Vec<Vec<u8>> {
    let source = open_binary_sample(path, 0, 1);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(
        CollectRtypeTap {
            target,
            cap,
            rows: Vec::new(),
        },
        VirtualClock::default(),
    );
    let mut tap = NoopTap;
    let mut state = SessionState::default();
    let mut restart = RestartState::default();
    session::run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 512,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run");
    seq.sink().rows.clone()
}

// Localizes the MBP10 divergence to byte offsets. Padding offsets for a
// `RecordHeader` (50 named bytes, repr(C) rounds to 56) are 50..56; for Mbp10
// (hdr 0..56, price 56..64, qty 64..72, action 72, side 73, depth 74, levels
// 80..480) the inner pad is 75..80.
#[test]
#[ignore = "needs NASDAQ_ITCH_SAMPLE; run: NASDAQ_ITCH_SAMPLE=<zst|gz> cargo nextest run --release -p polaris-datafeed --test determinism --run-ignored ignored-only --no-capture diagnose_mbp10_divergence_offsets"]
fn diagnose_mbp10_divergence_offsets() {
    let path = nasdaq_sample_path();
    let cap = 20_000;
    let a = collect_rtype(&path, rtype::MBP10, cap);
    let b = collect_rtype(&path, rtype::MBP10, cap);
    assert_eq!(a.len(), b.len(), "different mbp10 counts");

    let is_pad = |off: usize| (50..56).contains(&off) || (75..80).contains(&off);
    let mut first_diff = None;
    let mut pad_only = true;
    for (i, (ra, rb)) in a.iter().zip(&b).enumerate() {
        if ra != rb {
            let offs: Vec<usize> = (0..ra.len()).filter(|&j| ra[j] != rb[j]).collect();
            if first_diff.is_none() {
                first_diff = Some(i);
                println!("first diverging mbp10 at index {i}, size={}", ra.len());
                for &j in &offs {
                    println!(
                        "  offset {j}: run_a=0x{:02x} run_b=0x{:02x} {}",
                        ra[j],
                        rb[j],
                        if is_pad(j) { "<- PADDING" } else { "<- FIELD" }
                    );
                }
            }
            if offs.iter().any(|&j| !is_pad(j)) {
                pad_only = false;
            }
        }
    }
    match first_diff {
        None => println!("no divergence in first {cap} mbp10 records"),
        Some(_) => {
            println!("all divergences confined to padding offsets: {pad_only}");
            assert!(
                pad_only,
                "some mbp10 divergence is in a real field, not padding"
            );
        }
    }
}

// @lat: [[tests#Output determinism#Full-day capture replay is byte-identical]]
#[test]
#[ignore = "needs NASDAQ_ITCH_SAMPLE; run: NASDAQ_ITCH_SAMPLE=<zst|gz> cargo nextest run --release -p polaris-datafeed --test determinism --run-ignored ignored-only --no-capture nasdaq_full_day_output_is_byte_identical"]
fn nasdaq_full_day_output_is_byte_identical() {
    let path = nasdaq_sample_path();
    let open = || open_binary_sample(&path, 0, 1);

    let a = run(open(), 512);
    let b = run(open(), 512);

    println!("records={}", a.count);
    for (rt, (digest, n)) in &a.per_rtype {
        let (bd, bn) = b.per_rtype.get(rt).expect("same rtype set across runs");
        let ok = digest == bd && n == bn;
        println!(
            "  rtype 0x{rt:02x}: n={n} (b={bn}) {} {digest}",
            if ok { "OK  " } else { "DIFF" }
        );
    }

    assert_eq!(
        a.overall, b.overall,
        "full-day normalized output diverged across identical replays (see per-rtype DIFF lines)"
    );

    // A real NASDAQ day exercises the full family set at scale, including the
    // L0 aggregate (Ohlcv), status, and definition streams.
    for rt in [
        rtype::MBO,
        rtype::MBP10,
        rtype::BBO,
        rtype::TRADE,
        rtype::OHLCV,
        rtype::STATUS,
        rtype::DEFINITION,
    ] {
        assert!(
            a.per_rtype.contains_key(&rt),
            "expected rtype 0x{rt:02x} in full-day output"
        );
    }
}

// The seven default OHLCV bucket widths in nanoseconds (config default set).
fn configured_widths() -> [u64; 7] {
    [
        1_000_000_000,
        60_000_000_000,
        300_000_000_000,
        600_000_000_000,
        900_000_000_000,
        1_800_000_000_000,
        3_600_000_000_000,
    ]
}

// Tap that keeps every emitted Ohlcv record so a test can assert bars close and
// carry their resolution.
struct OhlcvTap {
    bars: Vec<Ohlcv>,
}

impl OutputSink for OhlcvTap {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        if rec.header.rtype == rtype::OHLCV {
            let bytes = record_bytes(&rec);
            let (bar, _) = decode_prefix::<Ohlcv>(&bytes).expect("published Ohlcv decodes");
            self.bars.push(bar);
        }
        Ok(())
    }
}

fn collect_ohlcv(frames: Vec<Frame>, batch: usize) -> Vec<Ohlcv> {
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(OhlcvTap { bars: Vec::new() }, VirtualClock::default());
    let mut tap = NoopTap;
    let mut state = SessionState::default();
    let mut restart = RestartState::default();
    session::run(
        VecSource::new(frames),
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    seq.sink().bars.clone()
}

// A trade opens bars at second 1; a later non-trade book add at second 3 crosses
// the 1-second boundary (closing the second-1 bar mid-stream via the watermark),
// and EOF flushes the rest.
fn flush_stream() -> Vec<Frame> {
    let payloads = vec![
        system_event(b'O', SEC),
        add(101, b'B', 50, 1_500_000, SEC),
        executed(101, 10, 5_001, SEC), // trade at second 1 opens bars
        add(202, b'B', 30, 1_490_000, 3 * SEC), // non-trade at second 3 flushes the 1s bar
        system_event(b'C', 3 * SEC),
    ];
    payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| Frame {
            payload,
            ts_recv: 1_000 + i as u64,
            stream: StreamId(0),
            venue_seq: i as u64 + 1,
        })
        .collect()
}

// @lat: [[tests#Output determinism#OHLCV output is batch-invariant]]
#[test]
fn ohlcv_output_is_batch_invariant() {
    // The OHLCV watermark keys on each record's ts_event, not on frame-batch
    // boundaries, so the emitted record sequence (bars included) is identical at
    // any batch size.
    let d4 = run(VecSource::new(synthetic_stream()), 4);
    let d1 = run(VecSource::new(synthetic_stream()), 1);
    let d7 = run(VecSource::new(synthetic_stream()), 7);

    assert_eq!(d4.overall, d1.overall, "digest changed at batch 1");
    assert_eq!(d4.overall, d7.overall, "digest changed at batch 7");
    assert_eq!(
        d4.per_rtype, d1.per_rtype,
        "per-rtype digest changed at batch 1"
    );
    assert_eq!(d4.count, d1.count, "record count changed with batch size");
    assert!(
        d4.per_rtype.contains_key(&rtype::OHLCV),
        "watermark close plus EOF flush emit Ohlcv even on the synthetic stream"
    );
}

// Records the rtype of every emitted record in emission order, so a test can
// tell a mid-stream flush from the trailing end-of-session flush block.
struct OrderTap {
    rtypes: Vec<u8>,
}

impl OutputSink for OrderTap {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.rtypes.push(rec.header.rtype);
        Ok(())
    }
}

fn collect_order(frames: Vec<Frame>) -> Vec<u8> {
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(OrderTap { rtypes: Vec::new() }, VirtualClock::default());
    let mut tap = NoopTap;
    let mut state = SessionState::default();
    let mut restart = RestartState::default();
    session::run(
        VecSource::new(frames),
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 4,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    seq.sink().rtypes.clone()
}

// @lat: [[tests#Output determinism#Bars carry their resolution and flush at session end]]
#[test]
fn session_bars_carry_resolution_and_flush_at_eof() {
    let bars = collect_ohlcv(flush_stream(), 4);
    assert!(!bars.is_empty(), "session emitted OHLCV bars");

    let configured = configured_widths();
    for b in &bars {
        assert!(
            configured.contains(&b.resolution),
            "bar resolution {} not in the configured set",
            b.resolution
        );
        assert_eq!(
            b.hdr.ts_event % b.resolution,
            0,
            "ts_event is the interval-open bucket_start"
        );
        assert_eq!(b.hdr.rtype, rtype::OHLCV);
    }

    // One trade opens one bar per resolution; the 1s bar closes on the non-trade
    // advance, the coarser six flush at EOF, so every configured width appears.
    for w in configured {
        assert!(
            bars.iter().any(|b| b.resolution == w),
            "no bar emitted for resolution {w}"
        );
    }
}

// @lat: [[tests#Output determinism#A non-trade record flushes a due bar mid-stream]]
#[test]
fn non_trade_flushes_due_bar_mid_stream() {
    let order = collect_order(flush_stream());
    let first = order
        .iter()
        .position(|&rt| rt == rtype::OHLCV)
        .expect("at least one Ohlcv emitted");
    // a non-Ohlcv record follows the first bar, so it closed mid-stream when a
    // non-trade advanced past its boundary, not only in the trailing EOF block.
    assert!(
        order[first + 1..].iter().any(|&rt| rt != rtype::OHLCV),
        "the first bar flushed mid-stream on a non-trade, not only at session end"
    );
    // the remaining coarser bars flush at session end.
    assert!(
        order.iter().filter(|&&rt| rt == rtype::OHLCV).count() >= 2,
        "coarser bars flush at session end"
    );
}

// Replay the synthetic stream into a fresh MdCaptureTap under one partition,
// then finish so every zstd frame and manifest is on disk.
fn replay_to_dir(dir: &Path, interval_ns: u64) {
    let _ = std::fs::remove_dir_all(dir);
    let meta = md_capture::CaptureMeta {
        venue: 2,
        date: 20260521,
        schema: 1,
        feed: NASDAQ.feed_id,
    };
    let mut tap = MdCaptureTap::new(dir.to_path_buf(), &NASDAQ, meta, interval_ns);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(NullSink, VirtualClock::default());
    let mut state = SessionState::default();
    let mut restart = RestartState::default();
    session::run(
        VecSource::new(synthetic_stream()),
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 4,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    tap.finish().expect("finish tap");
}

// @lat: [[tests#Output determinism#Partitioned output is byte-identical across replays]]
#[test]
fn partitioned_output_is_byte_identical_across_replays() {
    let base = std::env::temp_dir().join("datafeed_partition_determinism");
    // Two independent replays of one input, under both a daily and an hourly
    // partition, must each produce a byte-identical output tree (files +
    // per-dir manifest). A wall-clock or randomness read anywhere on the path
    // would diverge the two runs; event-time window floors keep them equal.
    for (label, interval_ns) in [("1d", 86_400_000_000_000u64), ("1h", 3_600_000_000_000u64)] {
        let a = base.join(format!("{label}_a"));
        let b = base.join(format!("{label}_b"));
        replay_to_dir(&a, interval_ns);
        replay_to_dir(&b, interval_ns);

        let ta = output_tree(&a);
        let tb = output_tree(&b);
        assert!(!ta.is_empty(), "{label}: replay wrote no files");
        assert!(
            ta.iter().any(|(p, _)| p.ends_with("manifest.json")),
            "{label}: no manifest.json emitted"
        );
        assert_eq!(
            ta.iter().map(|(p, _)| p).collect::<Vec<_>>(),
            tb.iter().map(|(p, _)| p).collect::<Vec<_>>(),
            "{label}: output file set diverged across replays"
        );
        for ((pa, ba), (_, bb)) in ta.iter().zip(&tb) {
            assert_eq!(ba, bb, "{label}: bytes diverged for {pa}");
        }
    }

    // The hourly partition labels its window differently from the daily one, so
    // the partition token genuinely reaches the derived filenames.
    let daily = output_tree(&base.join("1d_a"));
    let hourly = output_tree(&base.join("1h_a"));
    assert_ne!(
        daily.iter().map(|(p, _)| p).collect::<Vec<_>>(),
        hourly.iter().map(|(p, _)| p).collect::<Vec<_>>(),
        "daily and hourly partitions should name windows differently"
    );

    let _ = std::fs::remove_dir_all(&base);
}
