use core::fmt;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    rc::Rc,
    time::Instant,
};

use datafeed::{
    session::DefaultVenuePolicy,
    tap::{CaptureTap, CaptureTapError},
};
use flate2::read::MultiGzDecoder;
use md_events::{
    Action, Bbo, BidAskPair, Definition, Mbo, Mbp10, Price, Record, RecordHeader, Side,
    decode_prefix,
};
use md_pipeline::{
    IngestError, IngestFrame, IngestSource, OutputSink, RecordRef, SinkError, StreamId,
};
use md_replay::BinaryFileReplay;
use orderbook_engine::{ApplyResult, BookDerive, BookEngine, DeriveEngine, PurposeSet};
use sha2::{Digest, Sha256};

use super::digest::hex;

/// Frame a hex-per-line ITCH dump (one message per line, no framing) as
/// BinaryFILE rows: a `u16` big-endian length, then the message bytes.
pub fn binary_dump(input: &str) -> Vec<u8> {
    let mut dump = Vec::new();
    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        assert_eq!(line.len() % 2, 0);
        let mut message = Vec::with_capacity(line.len() / 2);
        for index in (0..line.len()).step_by(2) {
            message
                .push(u8::from_str_radix(&line[index..index + 2], 16).expect("valid fixture hex"));
        }
        dump.extend_from_slice(&(message.len() as u16).to_be_bytes());
        dump.extend_from_slice(&message);
    }
    dump
}

/// Open a NASDAQ BinaryFILE sample by extension: `.zst` streams through a
/// zstd decoder, anything else inflates as gzip. Frames are byte-identical
/// across both containers; zstd decodes several times faster on replay.
pub fn open_binary_sample(path: &Path, base_ts: u64, step_ns: u64) -> BinaryFileReplay {
    if path.extension().is_some_and(|ext| ext == "zst") {
        let file = File::open(path).expect("open zst sample");
        let decoder = zstd::Decoder::new(file).expect("zstd decoder");
        BinaryFileReplay::new(decoder, base_ts, step_ns)
    } else {
        BinaryFileReplay::from_gz(path, base_ts, step_ns).expect("open gz sample")
    }
}

const PROGRESS_INTERVAL: u64 = 10_000_000;
// divergence diagnostic keeps this many preceding normalized mutations
const HISTORY_BOUND: usize = 16;
// ITCH stock field, 8 bytes space padded; also first 8 bytes of Definition::raw_symbol
const NVDA_STOCK: [u8; 8] = *b"NVDA    ";

/// Caps any [`IngestSource`] at `limit` frames, then closes like
/// `BinaryFileReplay` at end-of-file: one `Ok(0)`, then `Err(Closed)`.
/// Prints one progress line per 10M frames delivered.
pub struct PrefixSource<Src> {
    inner: Src,
    remaining: u64,
    closed: bool,
    frames: u64,
    payload_bytes: u64,
    started: Instant,
}

impl<Src> PrefixSource<Src> {
    pub fn new(inner: Src, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
            closed: false,
            frames: 0,
            payload_bytes: 0,
            started: Instant::now(),
        }
    }

    /// Wrapped source back for post-run checks like `BinaryFileReplay::io_error`.
    pub fn into_inner(self) -> Src {
        self.inner
    }
}

impl<Src: IngestSource> IngestSource for PrefixSource<Src> {
    type Frame = Src::Frame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        if self.remaining == 0 {
            self.closed = true;
            return Ok(0);
        }
        let capped = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(max);
        let n = self.inner.recv_batch(out, capped)?;
        self.remaining -= n as u64;
        for frame in &out[out.len() - n..] {
            self.payload_bytes += frame.payload().len() as u64;
        }
        let before = self.frames;
        self.frames += n as u64;
        if self.frames / PROGRESS_INTERVAL > before / PROGRESS_INTERVAL {
            let secs = self.started.elapsed().as_secs_f64();
            eprintln!(
                "progress: frames={} elapsed={secs:.1}s rate={:.2}M msg/s payload={:.1}MB/s",
                self.frames,
                self.frames as f64 / secs / 1e6,
                self.payload_bytes as f64 / secs / 1e6,
            );
        }
        Ok(n)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParityLayer {
    L1,
    L2,
    L3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParitySubject {
    Book,
    Order(u64),
    Price(i64),
}

#[derive(Clone, Copy, Debug)]
pub struct ParityContext<'a> {
    pub event: u64,
    pub change: u64,
    pub venue_seq: u64,
    pub wire_type: u8,
    pub ts_event: u64,
    pub ts_recv: u64,
    pub instrument: u32,
    pub layer: ParityLayer,
    pub side: Side,
    pub subject: ParitySubject,
    pub prior: &'a [Mbo],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParityDivergence(String);

impl fmt::Display for ParityDivergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub fn expect_exact<T>(
    context: ParityContext<'_>,
    field: &'static str,
    expected: T,
    actual: T,
) -> Result<(), ParityDivergence>
where
    T: fmt::Debug + PartialEq,
{
    if expected == actual {
        return Ok(());
    }
    Err(ParityDivergence(format!(
        "parity divergence: event={} change={} venue_seq={} wire={} ts_event={} ts_recv={} instrument={} layer={:?} side={:?} subject={:?} field={} expected={:?} actual={:?} prior={:?}",
        context.event,
        context.change,
        context.venue_seq,
        char::from(context.wire_type),
        context.ts_event,
        context.ts_recv,
        context.instrument,
        context.layer,
        context.side,
        context.subject,
        field,
        expected,
        actual,
        context.prior,
    )))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L1Row {
    pub event: u64,
    pub venue_seq: u64,
    pub bid_price: i64,
    pub bid_qty: u64,
    pub bid_count: u32,
    pub ask_price: i64,
    pub ask_qty: u64,
    pub ask_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L2Row {
    pub event: u64,
    pub change: u64,
    pub venue_seq: u64,
    pub side: Side,
    pub price: i64,
    pub qty: u64,
    pub count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L3Row {
    pub event: u64,
    pub change: u64,
    pub venue_seq: u64,
    pub action: Action,
    pub order_id: u64,
    pub side: Side,
    pub event_price: i64,
    pub event_qty: u64,
    pub resting_price: i64,
    pub remaining_qty: u64,
    pub participant: [u8; 7],
    pub present: bool,
    pub queue_position: i64,
}

#[derive(Default)]
pub struct ObservedBookTap {
    pub mbos: Vec<Mbo>,
    pub mbp10: Vec<Mbp10>,
    pub bbo: Vec<Bbo>,
    pub book_rtypes: Vec<u8>,
}

impl OutputSink for ObservedBookTap {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        match rec.header.rtype {
            Mbo::RTYPE => {
                self.mbos.push(decode_as::<Mbo>(&rec));
                self.book_rtypes.push(rec.header.rtype);
            }
            Mbp10::RTYPE => {
                self.mbp10.push(decode_as::<Mbp10>(&rec));
                self.book_rtypes.push(rec.header.rtype);
            }
            Bbo::RTYPE => {
                self.bbo.push(decode_as::<Bbo>(&rec));
                self.book_rtypes.push(rec.header.rtype);
            }
            _ => {}
        }
        Ok(())
    }
}

/// Rebuild a typed record from what the sink sees. `publish` hands the header
/// and body separately; a record's `repr(C)` pattern is the two concatenated.
fn decode_as<U: Record>(rec: &RecordRef<'_>) -> U {
    assert_eq!(rec.header.rtype, U::RTYPE);
    // SAFETY: RecordHeader is Copy + repr(C) (md-events contract), so viewing
    // it as its own byte pattern reads nothing beyond the type.
    let hdr = unsafe {
        core::slice::from_raw_parts(
            rec.header as *const RecordHeader as *const u8,
            core::mem::size_of::<RecordHeader>(),
        )
    };
    let mut bytes = Vec::with_capacity(hdr.len() + rec.body.len());
    bytes.extend_from_slice(hdr);
    bytes.extend_from_slice(rec.body);
    decode_prefix::<U>(&bytes)
        .expect("published record decodes")
        .0
}

fn copy_record<T: Record, U: Record>(rec: &T) -> U {
    assert_eq!(T::RTYPE, U::RTYPE);
    assert_eq!(core::mem::size_of::<T>(), core::mem::size_of::<U>());
    // SAFETY: md-events assigns one concrete repr(C), Copy record type to
    // each RTYPE. Matching RTYPE and size therefore identify the same layout.
    unsafe { core::ptr::read_unaligned(rec as *const T as *const U) }
}

pub struct DownstreamMboRebuilder {
    book: BookEngine<DefaultVenuePolicy>,
    derive: DeriveEngine,
    pending: Vec<Mbo>,
    key: Option<(StreamId, u64)>,
    derived: Vec<BookDerive>,
}

impl Default for DownstreamMboRebuilder {
    fn default() -> Self {
        Self {
            book: BookEngine::new(DefaultVenuePolicy),
            derive: DeriveEngine::new(),
            pending: Vec::with_capacity(2),
            key: None,
            derived: Vec::new(),
        }
    }
}

impl DownstreamMboRebuilder {
    pub fn push(&mut self, stream: StreamId, mbo: Mbo) {
        if mbo.hdr.venue_seq == 0 {
            self.flush();
            self.pending.push(mbo);
            self.flush();
            return;
        }
        let key = (stream, mbo.hdr.venue_seq);
        if self.key.is_some_and(|current| current != key) {
            self.flush();
        }
        self.key = Some(key);
        self.pending.push(mbo);
    }

    pub fn derived_count(&self) -> usize {
        self.derived.len()
    }

    pub fn finish(mut self) -> Vec<BookDerive> {
        self.flush();
        self.derived
    }

    fn flush(&mut self) {
        let mut start = 0usize;
        while start < self.pending.len() {
            let instrument = self.pending[start].hdr.instrument_id;
            let mut end = start + 1;
            while end < self.pending.len() && self.pending[end].hdr.instrument_id == instrument {
                end += 1;
            }
            for mbo in &self.pending[start..end] {
                self.book.apply_mbo(mbo);
            }
            self.derived.push(self.derive.apply_mbo_batch(
                &self.pending[start..end],
                &self.book,
                PurposeSet::BOOK,
            ));
            start = end;
        }
        self.pending.clear();
        self.key = None;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferenceAction {
    Add,
    Modify,
    Delete,
}

#[derive(Clone, Copy, Debug)]
struct FullL1 {
    event: u64,
    timestamp: u64,
    wire_type: u8,
    bid_price: i64,
    bid_qty: u64,
    bid_count: u32,
    ask_price: i64,
    ask_qty: u64,
    ask_count: u32,
}

#[derive(Clone, Copy, Debug)]
struct FullL2 {
    event: u64,
    timestamp: u64,
    wire_type: u8,
    change: u64,
    side: Side,
    price: i64,
    qty: u64,
    count: u32,
}

#[derive(Clone, Copy, Debug)]
struct FullL3 {
    event: u64,
    timestamp: u64,
    wire_type: u8,
    change: u64,
    action: ReferenceAction,
    side: Side,
    order_id: u64,
    book_price: i64,
    event_price: i64,
    event_qty: u64,
    remaining_qty: u64,
}

#[derive(Clone, Copy, Debug)]
struct FullStrictL3 {
    event: u64,
    timestamp: u64,
    wire_type: u8,
    change: u64,
    participant: [u8; 7],
    present: bool,
    queue_position: i64,
}

trait EventRow {
    fn event(&self) -> u64;
}

impl EventRow for FullL1 {
    fn event(&self) -> u64 {
        self.event
    }
}

impl EventRow for FullL2 {
    fn event(&self) -> u64 {
        self.event
    }
}

impl EventRow for FullL3 {
    fn event(&self) -> u64 {
        self.event
    }
}

impl EventRow for FullStrictL3 {
    fn event(&self) -> u64 {
        self.event
    }
}

struct ReferenceRows<T> {
    reader: Box<dyn BufRead>,
    buffer: String,
    next: Option<T>,
    parse: fn(&str) -> T,
    consumed: u64,
}

impl<T: EventRow> ReferenceRows<T> {
    fn open(path: &Path, parse: fn(&str) -> T) -> io::Result<Self> {
        let file = File::open(path)?;
        let decoder = MultiGzDecoder::new(BufReader::new(file));
        let mut reader: Box<dyn BufRead> = Box::new(BufReader::new(decoder));
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("empty reference CSV: {}", path.display()),
            ));
        }
        Ok(Self {
            reader,
            buffer: String::new(),
            next: None,
            parse,
            consumed: 0,
        })
    }

    fn fill(&mut self) {
        if self.next.is_some() {
            return;
        }
        self.buffer.clear();
        let bytes = self
            .reader
            .read_line(&mut self.buffer)
            .expect("read reference CSV row");
        if bytes == 0 {
            return;
        }
        let line = self.buffer.trim_end_matches(['\r', '\n']);
        self.next = Some((self.parse)(line));
    }

    fn take_one(&mut self, event: u64) -> T {
        self.fill();
        let row = self.next.take().expect("reference CSV ended early");
        assert_eq!(row.event(), event, "reference event index diverged");
        self.consumed += 1;
        row
    }

    fn take_event(&mut self, event: u64) -> Vec<T> {
        let mut rows = Vec::with_capacity(2);
        loop {
            self.fill();
            if self.next.as_ref().is_none_or(|row| row.event() != event) {
                break;
            }
            rows.push(self.next.take().expect("peeked reference row"));
            self.consumed += 1;
        }
        assert!(!rows.is_empty(), "reference event {event} has no rows");
        rows
    }

    fn assert_exhausted(&mut self, layer: &str) {
        self.fill();
        assert!(
            self.next.is_none(),
            "unconsumed {layer} reference row at event {}",
            self.next.as_ref().map(EventRow::event).unwrap_or_default()
        );
    }
}

/// Verifies consumed reference artifacts against `strict-l3-manifest.json`
/// decompressed sha256 values before any replay starts. First success writes
/// dot-prefixed trust marker `.verified-<sha256-of-manifest>` next to the
/// artifacts; later runs skip re-hashing until the manifest changes.
/// Delete the marker to force re-verification.
fn verify_reference_artifacts(reference_dir: &Path) -> io::Result<()> {
    let manifest_path = reference_dir.join("strict-l3-manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let marker = reference_dir.join(format!(
        ".verified-{}",
        hex(&Sha256::digest(&manifest_bytes))
    ));
    if marker.exists() {
        return Ok(());
    }
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse {}: {error}", manifest_path.display()),
        )
    })?;
    let checks: [(&str, &[&str]); 4] = [
        ("aanrv_nvda_l1.csv.gz", &["baseline", "l1"]),
        ("aanrv_nvda_l2.csv.gz", &["baseline", "l2"]),
        ("aanrv_nvda_l3.csv.gz", &["baseline", "l3"]),
        ("aanrv_nvda_strict_l3_v1.csv.gz", &["strict_l3"]),
    ];
    for (file, section) in checks {
        let mut node = &manifest;
        for key in section {
            node = node.get(key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("manifest missing section {section:?} for {file}"),
                )
            })?;
        }
        let expected = node
            .get("uncompressed_sha256")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("manifest missing uncompressed_sha256 for {file}"),
                )
            })?;
        let actual = decompressed_sha256(&reference_dir.join(file))?;
        if actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reference artifact hash mismatch: {file} expected={expected} actual={actual}; refusing replay"
                ),
            ));
        }
    }
    std::fs::write(
        &marker,
        "harness trust marker; artifacts hash-verified against strict-l3-manifest.json; delete to re-verify\n",
    )?;
    Ok(())
}

fn decompressed_sha256(path: &Path) -> io::Result<String> {
    use std::io::Read as _;

    let file = File::open(path)?;
    let mut decoder = MultiGzDecoder::new(BufReader::new(file));
    let mut hasher = Sha256::new();
    // sha2 0.11 dropped the io::Write impl, so stream through a buffer.
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

struct FullEvent {
    index: u64,
    venue_seq: u64,
    timestamp: u64,
    wire_type: u8,
    l1: FullL1,
    l2: Vec<FullL2>,
    l3: Vec<FullL3>,
    strict: Vec<FullStrictL3>,
    l3_index: usize,
    mbp_count: u8,
    bbo_count: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullDayReport {
    pub input_messages: u64,
    pub target_events: u64,
    pub l1_rows: u64,
    pub l2_rows: u64,
    pub l3_rows: u64,
    pub strict_l3_rows: u64,
    pub target_instrument: u32,
    pub peak_expected_orders: usize,
    pub peak_expected_levels: usize,
    pub peak_actual_orders: usize,
    pub peak_event_mutations: usize,
    pub diagnostic_history_bound: usize,
}

pub struct FullDayComparator {
    l1_rows: ReferenceRows<FullL1>,
    l2_rows: ReferenceRows<FullL2>,
    l3_rows: ReferenceRows<FullL3>,
    strict_rows: ReferenceRows<FullStrictL3>,
    truncated: bool,
    input_messages: u64,
    target_events: u64,
    target_locate: Option<u16>,
    target_instrument: Option<u32>,
    pending: std::collections::VecDeque<FullEvent>,
    expected_bids: BTreeMap<i64, (u64, u32)>,
    expected_asks: BTreeMap<i64, (u64, u32)>,
    expected_orders: BTreeMap<u64, (Side, i64, u64, [u8; 7])>,
    audit: BookEngine<DefaultVenuePolicy>,
    history: Vec<Mbo>,
    peak_expected_orders: usize,
    peak_expected_levels: usize,
    peak_actual_orders: usize,
    peak_event_mutations: usize,
}

impl FullDayComparator {
    pub fn open(reference_dir: &Path) -> io::Result<Self> {
        verify_reference_artifacts(reference_dir)?;
        Ok(Self {
            l1_rows: ReferenceRows::open(
                &reference_dir.join("aanrv_nvda_l1.csv.gz"),
                parse_full_l1,
            )?,
            l2_rows: ReferenceRows::open(
                &reference_dir.join("aanrv_nvda_l2.csv.gz"),
                parse_full_l2,
            )?,
            l3_rows: ReferenceRows::open(
                &reference_dir.join("aanrv_nvda_l3.csv.gz"),
                parse_full_l3,
            )?,
            strict_rows: ReferenceRows::open(
                &reference_dir.join("aanrv_nvda_strict_l3_v1.csv.gz"),
                parse_full_strict_l3,
            )?,
            truncated: false,
            input_messages: 0,
            target_events: 0,
            target_locate: None,
            target_instrument: None,
            pending: std::collections::VecDeque::new(),
            expected_bids: BTreeMap::new(),
            expected_asks: BTreeMap::new(),
            expected_orders: BTreeMap::new(),
            audit: BookEngine::new(DefaultVenuePolicy),
            history: Vec::with_capacity(HISTORY_BOUND),
            peak_expected_orders: 0,
            peak_expected_levels: 0,
            peak_actual_orders: 0,
            peak_event_mutations: 0,
        })
    }

    /// Prefix mode: `finish` skips stream-exhaustion and final-empty-book
    /// asserts and prints a partial report. Per-event asserts stay hard.
    pub fn truncated(mut self) -> Self {
        self.truncated = true;
        self
    }

    pub fn finish(mut self) -> FullDayReport {
        while let Some(event) = self.pending.pop_front() {
            Self::assert_event_complete(&event);
        }
        let target_instrument = if self.truncated {
            self.target_instrument.unwrap_or_default()
        } else {
            self.l1_rows.assert_exhausted("L1");
            self.l2_rows.assert_exhausted("L2");
            self.l3_rows.assert_exhausted("L3");
            self.strict_rows.assert_exhausted("strict L3");
            assert!(self.expected_orders.is_empty(), "expected orders remain");
            assert!(self.expected_bids.is_empty(), "expected bid levels remain");
            assert!(self.expected_asks.is_empty(), "expected ask levels remain");
            let target_instrument = self
                .target_instrument
                .expect("NVDA Definition was observed");
            assert_eq!(
                self.audit.instrument_order_count(target_instrument),
                0,
                "actual target orders remain"
            );
            target_instrument
        };
        let report = FullDayReport {
            input_messages: self.input_messages,
            target_events: self.target_events,
            l1_rows: self.l1_rows.consumed,
            l2_rows: self.l2_rows.consumed,
            l3_rows: self.l3_rows.consumed,
            strict_l3_rows: self.strict_rows.consumed,
            target_instrument,
            peak_expected_orders: self.peak_expected_orders,
            peak_expected_levels: self.peak_expected_levels,
            peak_actual_orders: self.peak_actual_orders,
            peak_event_mutations: self.peak_event_mutations,
            diagnostic_history_bound: HISTORY_BOUND,
        };
        if self.truncated {
            eprintln!(
                "truncated parity: input_messages={} events_compared={} rows consumed l1={} l2={} l3={} strict_l3={}",
                report.input_messages,
                report.target_events,
                report.l1_rows,
                report.l2_rows,
                report.l3_rows,
                report.strict_l3_rows,
            );
        }
        report
    }

    fn start_event(&mut self, venue_seq: u64, timestamp: u64, wire_type: u8) {
        let index = self.target_events + 1;
        let l1 = self.l1_rows.take_one(index);
        let l2 = self.l2_rows.take_event(index);
        let l3 = self.l3_rows.take_event(index);
        let strict = self.strict_rows.take_event(index);
        assert_eq!(l3.len(), strict.len(), "strict L3 join count");
        assert_eq!((l1.timestamp, l1.wire_type), (timestamp, wire_type));
        for (baseline, strict_row) in l3.iter().zip(&strict) {
            assert_eq!(
                (
                    baseline.event,
                    baseline.timestamp,
                    baseline.wire_type,
                    baseline.change,
                ),
                (
                    strict_row.event,
                    strict_row.timestamp,
                    strict_row.wire_type,
                    strict_row.change,
                ),
                "strict L3 key diverged"
            );
        }
        self.peak_event_mutations = self.peak_event_mutations.max(l3.len());
        self.target_events = index;
        self.pending.push_back(FullEvent {
            index,
            venue_seq,
            timestamp,
            wire_type,
            l1,
            l2,
            l3,
            strict,
            l3_index: 0,
            mbp_count: 0,
            bbo_count: 0,
        });
    }

    // Session routing may run several frames behind the raw feed (prefetch
    // lookahead batches feeds before draining records), so expected events
    // queue up FIFO instead of one-at-a-time; an event retires when its BBO
    // lands, and every started event must retire complete.
    fn assert_event_complete(event: &FullEvent) {
        assert_eq!(
            event.l3_index,
            event.l3.len(),
            "event {} did not consume every L3 row",
            event.index
        );
        assert_eq!(event.mbp_count, 1, "event {} MBP10 count", event.index);
        assert_eq!(event.bbo_count, 1, "event {} BBO count", event.index);
    }

    fn consume_mbo(&mut self, actual: Mbo) {
        let event = self.pending.front().expect("target MBO without raw event");
        let expected = *event
            .l3
            .get(event.l3_index)
            .expect("more actual MBO rows than reference L3 rows");
        let strict = event.strict[event.l3_index];
        let context = ParityContext {
            event: event.index,
            change: expected.change,
            venue_seq: event.venue_seq,
            wire_type: event.wire_type,
            ts_event: actual.hdr.ts_event,
            ts_recv: actual.hdr.ts_recv,
            instrument: actual.hdr.instrument_id,
            layer: ParityLayer::L3,
            side: expected.side,
            subject: ParitySubject::Order(expected.order_id),
            prior: &self.history,
        };
        let expected_action = normalized_action(expected.action, event.wire_type);
        expect_exact(
            context,
            "mutation",
            (
                event.timestamp,
                event.venue_seq,
                expected_action,
                expected.order_id,
                expected.side,
                scale_price(expected.event_price),
                expected.event_qty,
                strict.participant,
            ),
            (
                actual.hdr.ts_event,
                actual.hdr.venue_seq,
                actual.action,
                actual.order_id,
                actual.side,
                actual.price,
                actual.qty,
                actual.participant,
            ),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            self.audit.apply_mbo(&actual).result,
            ApplyResult::Applied,
            "comparator audit rejected event {}",
            event.index
        );
        let actual_order =
            self.audit
                .order_state(actual.hdr.instrument_id, expected.side, expected.order_id);
        let actual_state = actual_order.map(|order| {
            (
                order.price,
                order.qty,
                self.audit
                    .queue_position(actual.hdr.instrument_id, expected.side, expected.order_id)
                    .expect("present order has queue position") as i64,
            )
        });
        let expected_state = strict.present.then_some((
            scale_price(expected.book_price),
            expected.remaining_qty,
            strict.queue_position,
        ));
        expect_exact(context, "post_order_state", expected_state, actual_state)
            .unwrap_or_else(|error| panic!("{error}"));
        if strict.present {
            self.expected_orders.insert(
                expected.order_id,
                (
                    expected.side,
                    scale_price(expected.book_price),
                    expected.remaining_qty,
                    strict.participant,
                ),
            );
        } else {
            self.expected_orders.remove(&expected.order_id);
        }
        self.peak_expected_orders = self.peak_expected_orders.max(self.expected_orders.len());
        self.peak_actual_orders = self
            .peak_actual_orders
            .max(self.audit.instrument_order_count(actual.hdr.instrument_id));
        if self.history.len() == HISTORY_BOUND {
            self.history.remove(0);
        }
        self.history.push(actual);
        self.pending.front_mut().expect("current event").l3_index += 1;
    }

    fn consume_mbp10(&mut self, actual: Mbp10) {
        // Apply this event's expected L2 rows to the ledger here, not in
        // raw(): the session may decode frames ahead of routing (prefetch
        // lookahead), so the ledger advances in consume order.
        {
            let event = self
                .pending
                .front()
                .expect("target MBP10 without raw event");
            let (timestamp, wire_type) = (event.timestamp, event.wire_type);
            let rows = core::mem::take(
                &mut self
                    .pending
                    .front_mut()
                    .expect("target MBP10 without raw event")
                    .l2,
            );
            for row in &rows {
                assert_eq!((row.timestamp, row.wire_type), (timestamp, wire_type));
                let levels = match row.side {
                    Side::Bid => &mut self.expected_bids,
                    Side::Ask => &mut self.expected_asks,
                    Side::None => unreachable!("reference level has a concrete side"),
                };
                let price = scale_price(row.price);
                if row.qty == 0 {
                    levels.remove(&price);
                } else {
                    levels.insert(price, (row.qty, row.count));
                }
            }
            self.peak_expected_levels = self
                .peak_expected_levels
                .max(self.expected_bids.len() + self.expected_asks.len());
            self.pending
                .front_mut()
                .expect("target MBP10 without raw event")
                .l2 = rows;
        }
        let event = self
            .pending
            .front()
            .expect("target MBP10 without raw event");
        assert_eq!(
            event.mbp_count, 0,
            "duplicate MBP10 at event {}",
            event.index
        );
        assert_eq!(event.l3_index, event.l3.len(), "MBP10 preceded final MBO");
        let context = ParityContext {
            event: event.index,
            change: 0,
            venue_seq: event.venue_seq,
            wire_type: event.wire_type,
            ts_event: actual.hdr.ts_event,
            ts_recv: actual.hdr.ts_recv,
            instrument: actual.hdr.instrument_id,
            layer: ParityLayer::L2,
            side: Side::None,
            subject: ParitySubject::Book,
            prior: &self.history,
        };
        expect_exact(
            context,
            "header",
            (event.timestamp, event.venue_seq),
            (actual.hdr.ts_event, actual.hdr.venue_seq),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        for expected in &event.l2 {
            let price = scale_price(expected.price);
            let actual_level = self
                .audit
                .level_state(actual.hdr.instrument_id, expected.side, price)
                .map(|level| (level.qty, level.count));
            let expected_level = (expected.qty != 0).then_some((expected.qty, expected.count));
            let level_context = ParityContext {
                layer: ParityLayer::L2,
                side: expected.side,
                subject: ParitySubject::Price(price),
                change: expected.change,
                ..context
            };
            expect_exact(level_context, "level_state", expected_level, actual_level)
                .unwrap_or_else(|error| panic!("{error}"));
        }
        let expected_top = expected_top_ten(&self.expected_bids, &self.expected_asks);
        expect_exact(
            context,
            "mbp10",
            expected_top,
            normalized_levels(actual.levels),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let last = self.history.last().expect("event emitted an MBO");
        expect_exact(
            context,
            "source_mutation",
            (last.price, last.qty, last.action, last.side),
            (actual.price, actual.qty, actual.action, actual.side),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        self.pending.front_mut().expect("current event").mbp_count += 1;
    }

    fn consume_bbo(&mut self, actual: Bbo) {
        let event = self.pending.front().expect("target BBO without raw event");
        assert_eq!(event.bbo_count, 0, "duplicate BBO at event {}", event.index);
        assert_eq!(event.mbp_count, 1, "BBO preceded MBP10");
        let context = ParityContext {
            event: event.index,
            change: 0,
            venue_seq: event.venue_seq,
            wire_type: event.wire_type,
            ts_event: actual.hdr.ts_event,
            ts_recv: actual.hdr.ts_recv,
            instrument: actual.hdr.instrument_id,
            layer: ParityLayer::L1,
            side: Side::None,
            subject: ParitySubject::Book,
            prior: &self.history,
        };
        let expected = BidAskPair {
            bid_px: scale_price(event.l1.bid_price),
            ask_px: scale_price(event.l1.ask_price),
            bid_sz: event.l1.bid_qty,
            ask_sz: event.l1.ask_qty,
            bid_ct: event.l1.bid_count,
            ask_ct: event.l1.ask_count,
        };
        expect_exact(context, "bbo", expected, normalized_pair(actual.pair))
            .unwrap_or_else(|error| panic!("{error}"));
        self.pending.front_mut().expect("current event").bbo_count += 1;
        // BBO is the final record of a book event; retire it now so the
        // queue front is always the next unconsumed expected event.
        let retired = self
            .pending
            .pop_front()
            .expect("retiring the event just consumed");
        Self::assert_event_complete(&retired);
    }
}

impl FullDayComparator {
    /// Consume one raw pre-adapter frame.
    fn consume_raw(&mut self, payload: &[u8]) {
        self.input_messages += 1;
        let Some(&wire_type) = payload.first() else {
            return;
        };
        if wire_type == b'R' && payload.len() >= 19 && payload[11..19] == NVDA_STOCK {
            self.target_locate = Some(u16::from_be_bytes([payload[1], payload[2]]));
            return;
        }
        if payload.len() < 11
            || self.target_locate != Some(u16::from_be_bytes([payload[1], payload[2]]))
            || !matches!(wire_type, b'A' | b'F' | b'E' | b'C' | b'X' | b'D' | b'U')
        {
            return;
        }
        let timestamp = load_be48(&payload[5..11]);
        self.start_event(self.input_messages, timestamp, wire_type);
    }

    /// Consume one published record. Separate from the raw hook because the
    /// two arrive on different seams: raw frames through the capture tap,
    /// published records off the sink.
    pub fn consume_published(&mut self, rec: &RecordRef<'_>) {
        if rec.header.rtype == Definition::RTYPE {
            let definition = decode_as::<Definition>(rec);
            // exact space-padded stock compare, mirrors raw() locate match
            if definition.raw_symbol[..8] == NVDA_STOCK {
                assert_eq!(
                    Some(definition.venue_native_id as u16),
                    self.target_locate,
                    "NVDA locate and Definition disagree"
                );
                self.target_instrument = Some(definition.hdr.instrument_id);
            }
            return;
        }
        if Some(rec.header.instrument_id) != self.target_instrument {
            return;
        }
        match rec.header.rtype {
            Mbo::RTYPE => self.consume_mbo(decode_as::<Mbo>(rec)),
            Mbp10::RTYPE => self.consume_mbp10(decode_as::<Mbp10>(rec)),
            Bbo::RTYPE => self.consume_bbo(decode_as::<Bbo>(rec)),
            _ => {}
        }
    }
}

/// The comparator watches both seams and keeps one shared state across them:
/// raw frames arrive through the capture tap, published records off the sink.
/// One session is one thread, so `Rc<RefCell<..>>` is the same sharing the
/// replay clock already uses.
pub struct ComparatorTap(pub Rc<RefCell<FullDayComparator>>);

impl CaptureTap for ComparatorTap {
    fn raw(
        &mut self,
        _stream: StreamId,
        _ts_recv: u64,
        payload: &[u8],
    ) -> Result<(), CaptureTapError> {
        self.0.borrow_mut().consume_raw(payload);
        Ok(())
    }
}

/// Sink half of the same shared comparator. see [`ComparatorTap`]
pub struct ComparatorSink(pub Rc<RefCell<FullDayComparator>>);

impl OutputSink for ComparatorSink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.0.borrow_mut().consume_published(&rec);
        Ok(())
    }
}

fn parse_full_l1(line: &str) -> FullL1 {
    let cols: Vec<_> = line.split(',').collect();
    FullL1 {
        event: number(cols[0]),
        timestamp: number(cols[1]),
        wire_type: byte(cols[2]),
        bid_price: number(cols[3]),
        bid_qty: number(cols[4]),
        bid_count: number(cols[5]),
        ask_price: number(cols[6]),
        ask_qty: number(cols[7]),
        ask_count: number(cols[8]),
    }
}

fn parse_full_l2(line: &str) -> FullL2 {
    let cols: Vec<_> = line.split(',').collect();
    FullL2 {
        event: number(cols[0]),
        timestamp: number(cols[1]),
        wire_type: byte(cols[2]),
        change: number(cols[3]),
        side: side(cols[4]),
        price: number(cols[5]),
        qty: number(cols[6]),
        count: number(cols[7]),
    }
}

fn parse_full_l3(line: &str) -> FullL3 {
    let cols: Vec<_> = line.split(',').collect();
    FullL3 {
        event: number(cols[0]),
        timestamp: number(cols[1]),
        wire_type: byte(cols[2]),
        change: number(cols[3]),
        action: reference_action(cols[4]),
        side: side(cols[5]),
        order_id: number(cols[6]),
        book_price: number(cols[7]),
        event_price: number(cols[8]),
        event_qty: number(cols[9]),
        remaining_qty: number(cols[10]),
    }
}

fn parse_full_strict_l3(line: &str) -> FullStrictL3 {
    let cols: Vec<_> = line.split(',').collect();
    FullStrictL3 {
        event: number(cols[0]),
        timestamp: number(cols[1]),
        wire_type: byte(cols[2]),
        change: number(cols[3]),
        participant: participant4(cols[4]),
        present: cols[5] == "1",
        queue_position: number(cols[6]),
    }
}

fn byte(value: &str) -> u8 {
    let bytes = value.as_bytes();
    assert_eq!(bytes.len(), 1, "one-byte fixture field");
    bytes[0]
}

fn reference_action(value: &str) -> ReferenceAction {
    match value {
        "ADD" => ReferenceAction::Add,
        "MODIFY" => ReferenceAction::Modify,
        "DELETE" => ReferenceAction::Delete,
        other => panic!("invalid reference action {other}"),
    }
}

fn normalized_action(action: ReferenceAction, wire_type: u8) -> Action {
    match (action, wire_type) {
        (ReferenceAction::Add, _) => Action::Add,
        (ReferenceAction::Modify | ReferenceAction::Delete, b'E') => Action::Fill,
        (ReferenceAction::Modify | ReferenceAction::Delete, b'C') => Action::FillWithPrice,
        (ReferenceAction::Modify | ReferenceAction::Delete, b'X' | b'D' | b'U') => Action::Cancel,
        _ => panic!(
            "reference action {action:?} is invalid for wire type {}",
            char::from(wire_type)
        ),
    }
}

fn participant4(value: &str) -> [u8; 7] {
    assert_eq!(value.len(), 8);
    let mut out = [0u8; 7];
    for (index, byte) in out[..4].iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("valid strict participant hex");
    }
    out
}

fn load_be48(value: &[u8]) -> u64 {
    assert_eq!(value.len(), 6);
    (u64::from(value[0]) << 40)
        | (u64::from(value[1]) << 32)
        | (u64::from(value[2]) << 24)
        | (u64::from(value[3]) << 16)
        | (u64::from(value[4]) << 8)
        | u64::from(value[5])
}

fn scale_price(raw: i64) -> i64 {
    raw * 100_000
}

fn expected_top_ten(
    bids: &BTreeMap<i64, (u64, u32)>,
    asks: &BTreeMap<i64, (u64, u32)>,
) -> [BidAskPair; 10] {
    let mut levels = [BidAskPair::default(); 10];
    let mut bid_iter = bids.iter().rev();
    let mut ask_iter = asks.iter();
    for level in &mut levels {
        if let Some((&price, &(qty, count))) = bid_iter.next() {
            level.bid_px = price;
            level.bid_sz = qty;
            level.bid_ct = count;
        }
        if let Some((&price, &(qty, count))) = ask_iter.next() {
            level.ask_px = price;
            level.ask_sz = qty;
            level.ask_ct = count;
        }
    }
    levels
}

fn normalized_levels(mut levels: [BidAskPair; 10]) -> [BidAskPair; 10] {
    for pair in &mut levels {
        *pair = normalized_pair(*pair);
    }
    levels
}

fn normalized_pair(mut pair: BidAskPair) -> BidAskPair {
    if pair.bid_px == Price::NULL.0 {
        pair.bid_px = 0;
    }
    if pair.ask_px == Price::NULL.0 {
        pair.ask_px = 0;
    }
    pair
}

pub fn parse_l1(input: &str) -> Vec<L1Row> {
    rows(input)
        .map(|cols| L1Row {
            event: number(cols[0]),
            venue_seq: number(cols[1]),
            bid_price: number(cols[2]),
            bid_qty: number(cols[3]),
            bid_count: number(cols[4]),
            ask_price: number(cols[5]),
            ask_qty: number(cols[6]),
            ask_count: number(cols[7]),
        })
        .collect()
}

pub fn parse_l2(input: &str) -> Vec<L2Row> {
    rows(input)
        .map(|cols| L2Row {
            event: number(cols[0]),
            change: number(cols[1]),
            venue_seq: number(cols[2]),
            side: side(cols[3]),
            price: number(cols[4]),
            qty: number(cols[5]),
            count: number(cols[6]),
        })
        .collect()
}

pub fn parse_l3(input: &str) -> Vec<L3Row> {
    rows(input)
        .map(|cols| L3Row {
            event: number(cols[0]),
            change: number(cols[1]),
            venue_seq: number(cols[2]),
            action: action(cols[3]),
            order_id: number(cols[4]),
            side: side(cols[5]),
            event_price: number(cols[6]),
            event_qty: number(cols[7]),
            resting_price: number(cols[8]),
            remaining_qty: number(cols[9]),
            participant: participant(cols[10]),
            present: cols[11] == "1",
            queue_position: number(cols[12]),
        })
        .collect()
}

fn rows(input: &str) -> impl Iterator<Item = Vec<&str>> {
    input
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.split(',').collect())
}

fn number<T: core::str::FromStr>(value: &str) -> T
where
    T::Err: core::fmt::Debug,
{
    value.parse().expect("valid fixture number")
}

fn side(value: &str) -> Side {
    match value {
        "B" => Side::Bid,
        "A" => Side::Ask,
        other => panic!("invalid fixture side {other}"),
    }
}

fn action(value: &str) -> Action {
    match value {
        "ADD" => Action::Add,
        "CANCEL" => Action::Cancel,
        "FILL" => Action::Fill,
        "FILL_WITH_PRICE" => Action::FillWithPrice,
        other => panic!("invalid fixture action {other}"),
    }
}

fn participant(value: &str) -> [u8; 7] {
    assert_eq!(value.len(), 14);
    let mut out = [0u8; 7];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("valid participant hex");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::*;

    fn write_gz(path: &Path, content: &[u8]) {
        let file = File::create(path).expect("create gz artifact");
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(content).expect("write gz artifact");
        encoder.finish().expect("finish gz artifact");
    }

    #[test]
    fn preflight_hashes_artifacts_and_trusts_marker_until_manifest_changes() {
        let dir = std::env::temp_dir().join(format!(
            "parity-preflight-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp reference dir");
        let artifacts: [(&str, &[u8]); 4] = [
            ("aanrv_nvda_l1.csv.gz", b"h\n1,l1\n"),
            ("aanrv_nvda_l2.csv.gz", b"h\n1,l2\n"),
            ("aanrv_nvda_l3.csv.gz", b"h\n1,l3\n"),
            ("aanrv_nvda_strict_l3_v1.csv.gz", b"h\n1,s\n"),
        ];
        for (file, content) in artifacts {
            write_gz(&dir.join(file), content);
        }
        let sha = |content: &[u8]| hex(&Sha256::digest(content));
        let manifest = serde_json::json!({
            "baseline": {
                "l1": { "uncompressed_sha256": sha(artifacts[0].1) },
                "l2": { "uncompressed_sha256": sha(artifacts[1].1) },
                "l3": { "uncompressed_sha256": sha(artifacts[2].1) },
            },
            "strict_l3": { "uncompressed_sha256": sha(artifacts[3].1) },
        });
        let manifest_bytes = serde_json::to_vec(&manifest).expect("encode manifest");
        std::fs::write(dir.join("strict-l3-manifest.json"), &manifest_bytes)
            .expect("write manifest");

        verify_reference_artifacts(&dir).expect("matching hashes pass");
        let marker = dir.join(format!(".verified-{}", sha(&manifest_bytes)));
        assert!(marker.exists(), "first success writes trust marker");

        write_gz(&dir.join("aanrv_nvda_l2.csv.gz"), b"h\n1,tampered\n");
        verify_reference_artifacts(&dir).expect("marker skips re-hash");

        std::fs::remove_file(&marker).expect("remove trust marker");
        let error = verify_reference_artifacts(&dir)
            .expect_err("hash mismatch refuses comparison")
            .to_string();
        assert!(
            error.contains("aanrv_nvda_l2.csv.gz"),
            "mismatch names the file: {error}"
        );
        assert!(!marker.exists(), "failed verification writes no marker");

        std::fs::remove_dir_all(&dir).expect("cleanup temp reference dir");
    }
}
