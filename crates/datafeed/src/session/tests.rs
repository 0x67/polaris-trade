use std::sync::atomic::AtomicBool;

use md_events::{Control, ControlKind, Level, LevelAction, Side, Status, Trade, rtype};
use md_pipeline::{
    AdapterFault, AdapterStats, BundledTzdb, ClosureModel, IngestError, IngestFrame, IngestSource,
    RollModel, SessionModel, SystemClock, VenueAdapter, VenueId, ingest::testing::MockFrame,
};
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};

use super::*;
use crate::{
    sink::{CollectingSink, NullSink},
    tap::{MdCaptureTap, NoopTap},
    venue::{NASDAQ, VenueDescriptor},
};

fn nasdaq_adapter() -> NasdaqAdapter {
    NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(1),
        session_midnight_ns: 0,
    })
}

// Closes at end like the real replay containers (`CaptureReplay`,
// `BinaryFileReplay`): one `Ok(0)` on the call that drains the last
// frame, `Err(Closed)` after. `md_pipeline`'s own conformance `MockSource`
// never closes, so it cannot drive a session loop to completion.
struct VecSource<F> {
    frames: std::collections::VecDeque<F>,
    closed: bool,
}

impl<F> VecSource<F> {
    fn new(frames: Vec<F>) -> Self {
        VecSource {
            frames: frames.into(),
            closed: false,
        }
    }
}

impl<F: IngestFrame> IngestSource for VecSource<F> {
    type Frame = F;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
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

#[derive(Clone)]
struct SequencedFrame {
    payload: Vec<u8>,
    ts_recv: u64,
    stream: StreamId,
    venue_seq: u64,
}

impl IngestFrame for SequencedFrame {
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

// `A` AddOrderNoMpid: 36 bytes, resolves/mints its own registry entry, no
// prior `R` StockDirectory needed. Minimal NASDAQ fixture for tests below.
fn add_order_no_mpid_msg(stock_locate: u16, order_ref: u64, side: u8, shares: u32) -> Vec<u8> {
    let mut buf = vec![b'A'];
    buf.extend_from_slice(&stock_locate.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // tracking_number
    buf.extend_from_slice(&[0u8; 6]); // timestamp
    buf.extend_from_slice(&order_ref.to_be_bytes());
    buf.push(side);
    buf.extend_from_slice(&shares.to_be_bytes());
    buf.extend_from_slice(b"AAPL    ");
    buf.extend_from_slice(&1_500_000u32.to_be_bytes());
    buf
}

fn nasdaq_replace_msg(stock_locate: u16, old_id: u64, new_id: u64, shares: u32) -> Vec<u8> {
    let mut buf = vec![b'U'];
    buf.extend_from_slice(&stock_locate.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&[0u8; 6]);
    buf.extend_from_slice(&old_id.to_be_bytes());
    buf.extend_from_slice(&new_id.to_be_bytes());
    buf.extend_from_slice(&shares.to_be_bytes());
    buf.extend_from_slice(&1_600_000u32.to_be_bytes());
    buf
}

fn assert_atomic_replace_output(records: &[crate::sink::CollectedRecord]) {
    let rtypes: Vec<_> = records.iter().map(|record| record.rtype).collect();
    assert_eq!(
        rtypes,
        [
            rtype::MBO,
            rtype::MBP10,
            rtype::BBO,
            rtype::MBO,
            rtype::MBO,
            rtype::MBP10,
            rtype::BBO,
        ]
    );
    assert!(records[..3].iter().all(|record| record.venue_seq == 1));
    assert!(records[3..].iter().all(|record| record.venue_seq == 2));
    for pair in records.windows(2) {
        assert_eq!(pair[1].egress_seq, pair[0].egress_seq + 1);
    }
}

// A pre-set stop flag must not drop the batch already pulled: the loop
// drains it, tallies it, then ends. The caller's `finish` then closes the
// raw writer and writes the manifest, so the tail capture is complete and
// indexed rather than a truncated zstd stream with no manifest.
#[test]
fn stop_flag_drains_then_finishes() {
    let raw = std::env::temp_dir().join("datafeed_session_stop_raw");
    let _ = std::fs::remove_dir_all(&raw);
    let open = nasdaq_anchor(20260521);

    let frames = vec![
        SequencedFrame {
            payload: add_order_no_mpid_msg(1, 777, b'B', 100),
            ts_recv: open + 10,
            stream: StreamId(0),
            venue_seq: 1,
        },
        SequencedFrame {
            payload: add_order_no_mpid_msg(1, 778, b'B', 50),
            ts_recv: open + 20,
            stream: StreamId(0),
            venue_seq: 2,
        },
    ];
    let source = VecSource::new(frames);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let meta = md_capture::CaptureMeta {
        venue: 1,
        date: 20260521,
        schema: 1,
        feed: md_capture::FEED_UNSPECIFIED,
    };
    let mut tap = MdCaptureTap::new(raw.clone(), &NASDAQ, meta, 24 * HOUR);
    let mut state = SessionState::default();
    let mut restart = RestartState::default();

    // pre-set: the loop still drains the batch it already pulled, then stops.
    let stop = AtomicBool::new(true);

    let (stats, _source) = run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        RunControl {
            batch: 8,
            stop: &stop,
        },
    )
    .expect("run returns on stop");
    assert_eq!(stats.frames, 2, "in-flight batch drained before stop");

    tap.finish()
        .expect("finish closes writers and writes manifest");

    let day_dir = raw.join("nasdaq/v5/2026-05-21");
    let manifest_path = day_dir.join("manifest.json");
    assert!(manifest_path.exists(), "shutdown wrote the raw manifest");

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let files = manifest["files"].as_array().expect("files array");
    assert_eq!(files.len(), 1, "one window file");
    let raw_entry = &files[0];
    assert_eq!(
        raw_entry["records"], 2,
        "raw manifest counts both drained frames"
    );

    // Reading the tail back proves the writer was closed, not left truncated.
    let leaf = raw_entry["path"].as_str().expect("raw leaf name");
    let raw_bytes = std::fs::read(day_dir.join(leaf)).expect("raw tail file exists");
    let (mut reader, _) = md_capture::RawReader::new(std::io::Cursor::new(raw_bytes))
        .expect("open raw reader on closed stream");
    let first = reader.read_row().expect("read").expect("first row");
    assert_eq!(first.payload, add_order_no_mpid_msg(1, 777, b'B', 100));
    let second = reader.read_row().expect("read").expect("second row");
    assert_eq!(second.payload, add_order_no_mpid_msg(1, 778, b'B', 50));

    let _ = std::fs::remove_dir_all(&raw);
}

// Fails on its `fail_at`-th frame, as a full disk would mid-batch.
struct FailingTap {
    fail_at: usize,
    seen: usize,
}

impl CaptureTap for FailingTap {
    fn raw(
        &mut self,
        _stream: StreamId,
        _ts_recv: u64,
        _payload: &[u8],
    ) -> Result<(), CaptureTapError> {
        self.seen += 1;
        if self.seen == self.fail_at {
            return Err(CaptureTapError::Io(std::io::Error::other("disk full")));
        }
        Ok(())
    }
}

// The frames tapped before a fault were captured, so the records they
// decode must publish before the fault ends the session. One batch holds
// all three frames, so routing per batch alone would drop the first two.
#[test]
fn tap_fault_publishes_frames_tapped_before_it() {
    let frame = |order_id: u64, venue_seq: u64| SequencedFrame {
        payload: add_order_no_mpid_msg(1, order_id, b'B', 100),
        ts_recv: venue_seq,
        stream: StreamId(0),
        venue_seq,
    };
    let source = VecSource::new(vec![frame(1, 1), frame(2, 2), frame(3, 3)]);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = FailingTap {
        fail_at: 3,
        seen: 0,
    };

    let Err(err) = run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut SessionState::default(),
        &mut RestartState::default(),
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    ) else {
        panic!("a tap fault must end the session");
    };

    assert!(matches!(err, SessionError::Tap(_)), "got {err:?}");
    let mbo_seqs: Vec<_> = seq
        .sink()
        .records
        .iter()
        .filter(|r| r.rtype == rtype::MBO)
        .map(|r| r.venue_seq)
        .collect();
    assert_eq!(mbo_seqs, [1, 2]);
}

// `DeriveEngine::apply_mbo`'s computed `Mbp10`/`Bbo` must reach the
// sink, not be discarded as a bare statement.
#[test]
fn derived_records_reach_sink() {
    let frames = vec![MockFrame {
        payload: add_order_no_mpid_msg(1, 1001, b'B', 100),
        ts_recv: 1,
        stream: StreamId(0),
    }];
    let source = VecSource::new(frames);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = SessionState::default();

    let (_stats, _source) = run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");

    let records = &seq.sink().records;
    assert!(
        records.iter().any(|r| r.rtype == rtype::MBO),
        "mbo published for the add order"
    );
    assert!(
        records.iter().any(|r| r.rtype == rtype::MBP10),
        "derive engine's Mbp10 output reached the sink, not discarded"
    );
    assert!(
        records.iter().any(|r| r.rtype == rtype::BBO),
        "derive engine's Bbo output reached the sink, not discarded"
    );
}

#[test]
fn nasdaq_replace_publishes_two_mbos_then_one_final_book_view() {
    let frames = vec![
        SequencedFrame {
            payload: add_order_no_mpid_msg(1, 1001, b'B', 100),
            ts_recv: 10,
            stream: StreamId(0),
            venue_seq: 1,
        },
        SequencedFrame {
            payload: nasdaq_replace_msg(1, 1001, 2002, 80),
            ts_recv: 20,
            stream: StreamId(0),
            venue_seq: 2,
        },
    ];
    let source = VecSource::new(frames);
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = SessionState::default();

    let (stats, _source) = run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");

    assert_eq!(stats.records, 3);
    assert!(stats.unknown_order_counts.is_empty());
    assert_atomic_replace_output(&seq.sink().records);
    let instrument = seq.sink().records[0].instrument_id;
    assert!(
        state
            .engines
            .book
            .order_state(instrument, md_events::Side::Bid, 1001)
            .is_none()
    );
    let replacement = state
        .engines
        .book
        .order_state(instrument, md_events::Side::Bid, 2002)
        .expect("replacement order rests");
    assert_eq!(replacement.qty, 80);
    assert_eq!(
        state
            .engines
            .book
            .queue_position(instrument, md_events::Side::Bid, 2002),
        Some(0)
    );
}

// A routed `TradeBreak` must reverse both `OhlcvEngine`'s open bar and
// `DeriveEngine`'s vwap, the latter via the session's own `trade_memo`.
#[test]
fn ohlcv_reversal_end_to_end() {
    const SEC: u64 = 1_000_000_000;

    let mut engines: SessionEngines<DefaultVenuePolicy> =
        SessionEngines::new(DefaultVenuePolicy, default_ohlcv_resolutions());
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();

    let hdr = |ts_event: u64| RecordHeader {
        length: 0,
        rtype: rtype::TRADE,
        venue: 1,
        instrument_id: 7,
        ts_event,
        ts_recv: ts_event,
        ts_egress: 0,
        venue_seq: 0,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    let trade = |ts_event: u64, price: i64, qty: u64, trade_ref: u64| Trade {
        hdr: hdr(ts_event),
        price,
        qty,
        aggressor: md_events::Side::Bid,
        trade_ref,
        _pad: [0u8; 7],
    };

    route_record(
        MarketEvent::Trade(trade(100, 1_000, 10, 1)),
        &mut engines,
        &mut seq,
        &mut restart,
        StreamId(0),
    )
    .expect("route trade 1");
    route_record(
        MarketEvent::Trade(trade(150, 1_100, 5, 2)),
        &mut engines,
        &mut seq,
        &mut restart,
        StreamId(0),
    )
    .expect("route trade 2 (broken later)");

    let before = engines.ohlcv.open_bar(7, SEC).expect("bar open");
    assert_eq!(before.4, 15, "ohlcv volume before break: 10 + 5");
    assert_eq!(before.5, 2, "ohlcv trade count before break");

    // probe vwap without adding a contribution: PurposeSet::NONE takes
    // the read-only early-return branch in DeriveEngine::apply_trade.
    let probe = trade(150, 0, 0, 999);
    let vwap_before = engines.derive.apply_trade(&probe, PurposeSet::NONE).vwap;
    assert_eq!(
        vwap_before,
        Some(1_033),
        "vwap of (1000x10, 1100x5) before the break"
    );

    route_record(
        MarketEvent::TradeBreak(md_events::TradeBreak {
            hdr: hdr(150),
            break_ref: 2,
            ref_kind: BreakRefKind::MatchNumber,
            _pad: [0u8; 7],
        }),
        &mut engines,
        &mut seq,
        &mut restart,
        StreamId(0),
    )
    .expect("route trade break");

    let after = engines.ohlcv.open_bar(7, SEC).expect("bar still open");
    assert_eq!(after.4, 10, "ohlcv volume reversed to just trade 1");
    assert_eq!(after.5, 1, "ohlcv trade count reversed to just trade 1");

    let vwap_after = engines.derive.apply_trade(&probe, PurposeSet::NONE).vwap;
    assert_eq!(
        vwap_after,
        Some(1_000),
        "derive engine vwap reversed via the session's trade_memo lookup"
    );
}

// An `UnknownOrder` fill must be counted per instrument, not silently
// discarded, so a maybe-bad-book instrument stays visible.
#[test]
fn unknown_order_counted_per_instrument() {
    let mut engines: SessionEngines<DefaultVenuePolicy> =
        SessionEngines::new(DefaultVenuePolicy, default_ohlcv_resolutions());
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();

    let mbo = md_events::Mbo {
        hdr: RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: 42,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id: 999,
        price: 100,
        qty: 5,
        action: Action::Fill,
        side: md_events::Side::Bid,
        participant: [0; 7],
        _pad: [0u8; 7],
    };

    route_record(
        MarketEvent::Mbo(mbo),
        &mut engines,
        &mut seq,
        &mut restart,
        StreamId(0),
    )
    .expect("route unknown-order fill");

    assert_eq!(
        engines.stats.unknown_order_counts.get(&42),
        Some(&1),
        "unknown-order fill counted against instrument 42"
    );
}

// The primary `Mbo` is the only book writer: a secondary `Level` at a resting primary price
// neither adds to nor overwrites that depth, and publishes no derived view.
#[test]
fn level_event_publishes_without_writing_the_book() {
    const INSTRUMENT: u32 = 7;
    const PRICE: i64 = 1_000;
    let mut engines: SessionEngines<DefaultVenuePolicy> =
        SessionEngines::new(DefaultVenuePolicy, default_ohlcv_resolutions());
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();
    let hdr = |rtype| RecordHeader {
        length: 0,
        rtype,
        venue: 1,
        instrument_id: INSTRUMENT,
        ts_event: 0,
        ts_recv: 0,
        ts_egress: 0,
        venue_seq: 0,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    let mbo = |action| {
        MarketEvent::Mbo(Mbo {
            hdr: hdr(rtype::MBO),
            order_id: 1,
            price: PRICE,
            qty: 100,
            action,
            side: Side::Bid,
            participant: [0; 7],
            _pad: [0u8; 7],
        })
    };
    let level = |action, qty| {
        MarketEvent::Level(Level {
            hdr: hdr(rtype::LEVEL),
            price: PRICE,
            qty,
            num_deletes: 0,
            side: Side::Bid,
            level: 0,
            action,
            _pad: [0u8; 3],
        })
    };

    let steps = [
        mbo(Action::Add),
        level(LevelAction::New, 100),
        level(LevelAction::Change, 40),
        level(LevelAction::Delete, 0),
        mbo(Action::Cancel),
    ];
    let best_bid: Vec<_> = steps
        .into_iter()
        .map(|event| {
            route_record(event, &mut engines, &mut seq, &mut restart, StreamId(0)).expect("route");
            engines.book.best(INSTRUMENT, Side::Bid)
        })
        .collect();

    let resting = Some((PRICE, 100));
    assert_eq!(
        best_bid,
        [resting, resting, resting, resting, None],
        "best bid after each step: only the primary order sets the depth"
    );
    let rtypes: Vec<_> = seq.sink().records.iter().map(|r| r.rtype).collect();
    assert_eq!(
        rtypes,
        [
            rtype::MBO,
            rtype::MBP10,
            rtype::BBO,
            rtype::LEVEL,
            rtype::LEVEL,
            rtype::LEVEL,
            rtype::MBO,
            rtype::MBP10,
            rtype::BBO,
        ]
    );
}

// A secondary channel reprints primary executions on stream 1 under its own trade ids; those
// prints and their breaks publish but leave OHLCV, VWAP, and the trade memo
// alone.
#[test]
fn second_stream_trades_publish_without_feeding_aggregates() {
    const SEC: u64 = 1_000_000_000;
    const INSTRUMENT: u32 = 7;
    const PRIMARY: StreamId = StreamId(0);
    const SECONDARY: StreamId = StreamId(1);
    let mut engines: SessionEngines<DefaultVenuePolicy> =
        SessionEngines::new(DefaultVenuePolicy, default_ohlcv_resolutions());
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();
    let hdr = |rtype, ts_event| RecordHeader {
        length: 0,
        rtype,
        venue: 1,
        instrument_id: INSTRUMENT,
        ts_event,
        ts_recv: ts_event,
        ts_egress: 0,
        venue_seq: 0,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    let trade = |ts_event, price, qty, trade_ref| Trade {
        hdr: hdr(rtype::TRADE, ts_event),
        price,
        qty,
        aggressor: Side::Bid,
        trade_ref,
        _pad: [0u8; 7],
    };

    let steps = [
        (PRIMARY, MarketEvent::Trade(trade(100, 1_000, 10, 1))),
        (PRIMARY, MarketEvent::Trade(trade(110, 1_100, 5, 2))),
        (SECONDARY, MarketEvent::Trade(trade(120, 1_000, 10, 501))),
        (SECONDARY, MarketEvent::Trade(trade(130, 1_100, 5, 502))),
        // a `SecondaryTradeId` equal to a primary match number names a different print
        (
            SECONDARY,
            MarketEvent::TradeBreak(TradeBreak {
                hdr: hdr(rtype::TRADE_BREAK, 140),
                break_ref: 2,
                ref_kind: BreakRefKind::SecondaryTradeId,
                _pad: [0u8; 7],
            }),
        ),
    ];
    // PurposeSet::NONE reads the VWAP without adding the probe to it
    let probe = trade(140, 0, 0, 999);
    let aggregates: Vec<_> = steps
        .into_iter()
        .map(|(stream, event)| {
            route_record(event, &mut engines, &mut seq, &mut restart, stream).expect("route");
            let bar = engines.ohlcv.open_bar(INSTRUMENT, SEC).expect("bar open");
            let vwap = engines.derive.apply_trade(&probe, PurposeSet::NONE).vwap;
            (bar.4, bar.5, vwap)
        })
        .collect();

    let primary_only = (15, 2, Some(1_033));
    assert_eq!(
        aggregates,
        [
            (10, 1, Some(1_000)),
            primary_only,
            primary_only,
            primary_only,
            primary_only
        ],
        "(1s bar volume, bar trades, vwap) after each step: only stream 0 counts"
    );
    assert_eq!(
        engines.trade_memo.get(501),
        None,
        "a stream 1 print is not remembered for a later break"
    );
    let published: Vec<_> = seq
        .sink()
        .records
        .iter()
        .map(|r| (r.stream, r.rtype))
        .collect();
    assert_eq!(
        published,
        [
            (PRIMARY, rtype::TRADE),
            (PRIMARY, rtype::TRADE),
            (SECONDARY, rtype::TRADE),
            (SECONDARY, rtype::TRADE),
            (SECONDARY, rtype::TRADE_BREAK),
        ]
    );
}

// The watermark advances before routing, so a bar closes on its closer's stream
// ahead of that record and of an earlier event's still-buffered `Mbo` segment.
#[test]
fn closed_bar_publishes_before_its_closer_on_its_stream() {
    const SEC: u64 = 1_000_000_000;
    const PRIMARY: StreamId = StreamId(0);
    const SECONDARY: StreamId = StreamId(1);
    let mut engines: SessionEngines<DefaultVenuePolicy> =
        SessionEngines::new(DefaultVenuePolicy, vec![SEC]);
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();
    let hdr = |rtype, ts_event| RecordHeader {
        length: 0,
        rtype,
        venue: 1,
        instrument_id: 7,
        ts_event,
        ts_recv: ts_event,
        ts_egress: 0,
        venue_seq: 0,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    let trade = |ts_event, trade_ref| {
        MarketEvent::Trade(Trade {
            hdr: hdr(rtype::TRADE, ts_event),
            price: 1_000,
            qty: 10,
            aggressor: Side::Bid,
            trade_ref,
            _pad: [0u8; 7],
        })
    };
    let status = |ts_event| {
        MarketEvent::Status(Status {
            hdr: hdr(rtype::STATUS, ts_event),
            state: TradingState::Continuous,
            venue_reason: [0; 20],
            _pad: [0u8; 3],
        })
    };
    let add = MarketEvent::Mbo(Mbo {
        hdr: hdr(rtype::MBO, 200),
        order_id: 1,
        price: 1_000,
        qty: 100,
        action: Action::Add,
        side: Side::Bid,
        participant: [0; 7],
        _pad: [0u8; 7],
    });

    let events = [
        trade(100, 1),
        add,
        status(SEC + SEC / 2),
        trade(SEC + SEC * 6 / 10, 2),
        status(2 * SEC + SEC / 2),
    ];
    route_events(
        &events,
        &[(PRIMARY, 4), (SECONDARY, 5)],
        &mut engines,
        &mut seq,
        &mut restart,
        &mut Vec::new(),
    )
    .expect("route");

    let published: Vec<_> = seq
        .sink()
        .records
        .iter()
        .map(|r| (r.stream, r.rtype))
        .collect();
    assert_eq!(
        published,
        [
            (PRIMARY, rtype::TRADE),
            (PRIMARY, rtype::OHLCV),
            (PRIMARY, rtype::MBO),
            (PRIMARY, rtype::MBP10),
            (PRIMARY, rtype::BBO),
            (PRIMARY, rtype::STATUS),
            (PRIMARY, rtype::TRADE),
            (SECONDARY, rtype::OHLCV),
            (SECONDARY, rtype::STATUS),
        ]
    );
}

// A session builds its OHLCV engine from the configured resolution widths,
// not a hardcoded default: a single 5-minute config tracks only 5m bars.
#[test]
fn engine_uses_configured_resolutions() {
    const SEC: u64 = 1_000_000_000;
    const FIVE_MIN: u64 = 300 * SEC;
    let mut state = SessionState::new(vec![FIVE_MIN]);
    let trade = Trade {
        hdr: RecordHeader {
            length: 0,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 3,
            ts_event: 100,
            ts_recv: 100,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        price: 1_000,
        qty: 5,
        aggressor: md_events::Side::Bid,
        trade_ref: 1,
        _pad: [0u8; 7],
    };
    state.engines.ohlcv.apply_trade(&trade, PurposeSet::OHLCV);
    assert!(
        state.engines.ohlcv.open_bar(3, FIVE_MIN).is_some(),
        "configured 5m resolution is tracked"
    );
    assert!(
        state.engines.ohlcv.open_bar(3, SEC).is_none(),
        "unconfigured 1s resolution is absent"
    );
}

/// What `run` asked of the adapter, in call order: an anchor install, or a
/// feed of the frame received at that `ts_recv`.
#[derive(Debug, PartialEq, Eq)]
enum AdapterCall {
    Anchor(u64),
    Feed(u64),
}

/// Adapter that decodes nothing and records every anchor install and feed.
#[derive(Default)]
struct SpyAdapter {
    calls: Vec<AdapterCall>,
    stats: AdapterStats,
}

impl DynAdapter for SpyAdapter {
    fn record_types(&self) -> &'static [u8] {
        &[]
    }

    fn session(&self) -> SessionModel {
        NASDAQ.session
    }

    fn feed(&mut self, _stream: StreamId, _payload: &[u8], ts_recv: u64, _venue_seq: u64) {
        self.calls.push(AdapterCall::Feed(ts_recv));
    }

    fn drain_into(&mut self, _out: &mut Vec<MarketEvent>) {}

    fn stats(&self) -> AdapterStats {
        self.stats
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        self.calls.push(AdapterCall::Anchor(anchor_ns));
    }
}

const HOUR: u64 = 3_600_000_000_000;

const LIVE_SOURCE: &str = r#"{"kind":"live","endpoint":"127.0.0.1:1"}"#;

fn nasdaq_config(session_midnight_ns: u64, source: &str) -> DatafeedConfig {
    serde_json::from_str(&format!(
        r#"{{"venue":"nasdaq","venue_id":1,"session_midnight_ns":{session_midnight_ns},
            "source":{source}}}"#
    ))
    .expect("config parses")
}

fn nasdaq_anchor(date: u32) -> u64 {
    NASDAQ
        .session
        .resolve_anchor(date, &BundledTzdb::new())
        .expect("New York anchor resolves")
}

/// Runs one frame per `ts_recv`, all in one batch, through a spy adapter and
/// returns what the runner asked of it plus how the run ended.
fn spy_run(
    state: &mut SessionState,
    ts_recvs: &[u64],
) -> (Vec<AdapterCall>, Result<(), SessionError>) {
    let frames = ts_recvs
        .iter()
        .map(|&ts_recv| SequencedFrame {
            payload: vec![b'A'],
            ts_recv,
            stream: StreamId(0),
            venue_seq: 0,
        })
        .collect();
    let mut adapter = SpyAdapter::default();
    let mut seq = EgressSequencer::new(NullSink, SystemClock);
    let result = run(
        VecSource::new(frames),
        &mut adapter,
        &mut seq,
        &mut NoopTap,
        state,
        &mut RestartState::default(),
        RunControl {
            batch: 64,
            stop: &AtomicBool::new(false),
        },
    )
    .map(drop);
    (adapter.calls, result)
}

// A live wire carries time since the venue's midnight only. With the anchor
// unset, the session start goes in before the first frame feeds, a frame
// inside the session installs nothing, and the next date's anchor goes in
// right before the frame at the session end feeds, mid-batch included.
#[test]
fn live_unset_anchor_installs_at_first_frame_and_rolls_at_session_end() {
    let (start, end) = (nasdaq_anchor(20260424), nasdaq_anchor(20260425));
    let mut state = SessionState::from_config(&nasdaq_config(0, LIVE_SOURCE)).expect("state");
    let (calls, result) = spy_run(&mut state, &[start + 3 * HOUR, end - 1, end, end + 1]);
    result.expect("run");
    assert_eq!(
        calls,
        [
            AdapterCall::Anchor(start),
            AdapterCall::Feed(start + 3 * HOUR),
            AdapterCall::Feed(end - 1),
            AdapterCall::Anchor(end),
            AdapterCall::Feed(end),
            AdapterCall::Feed(end + 1),
        ]
    );
}

// An explicit live anchor wins and never rolls, and replay keeps the anchor
// its capture header resolved since its `ts_recv` can be synthetic: neither
// run touches the adapter's anchor, even across a session end.
#[test]
fn explicit_anchor_and_replay_never_install_one() {
    let (start, end) = (nasdaq_anchor(20260424), nasdaq_anchor(20260425));
    let ts_recvs = [start + 3 * HOUR, end + HOUR];
    let replay = r#"{"kind":"replay","path":"nasdaq_v5.mdc1.zst"}"#;
    for config in [nasdaq_config(start, LIVE_SOURCE), nasdaq_config(0, replay)] {
        let mut state = SessionState::from_config(&config).expect("state");
        let (calls, result) = spy_run(&mut state, &ts_recvs);
        result.expect("run");
        assert_eq!(
            calls,
            ts_recvs.map(AdapterCall::Feed),
            "{:?}",
            config.source
        );
    }
}

// A live frame the venue's session model cannot place (an unknown zone) ends
// the session before it feeds, so nothing decodes against an unset anchor.
#[test]
fn unplaceable_live_frame_ends_the_session_unfed() {
    static UNKNOWN_ZONE: VenueDescriptor = VenueDescriptor {
        name: "test-unknown-zone",
        session: SessionModel {
            roll: RollModel::DailyZoned {
                tz: "Nowhere/Unknown",
                local_ns: 0,
            },
            closure: ClosureModel::None,
        },
        ..NASDAQ
    };
    let mut config = nasdaq_config(0, LIVE_SOURCE);
    config.venue = &UNKNOWN_ZONE;
    let mut state = SessionState::from_config(&config).expect("state");
    let (calls, result) = spy_run(&mut state, &[HOUR]);
    assert!(
        matches!(
            result,
            Err(SessionError::AnchorUnresolved {
                venue: "test-unknown-zone",
                ts_recv: HOUR,
            })
        ),
        "got {result:?}"
    );
    assert!(calls.is_empty(), "adapter saw {calls:?}");
}

/// Streams the test venue below declares.
const TWO_STREAMS: [StreamId; 2] = [StreamId(0), StreamId(1)];

/// Adapter for a test venue with two streams. Its wire frame is one 8-byte
/// big-endian resume sequence and decodes to a `Control::RecoveryComplete`
/// carrying it. A frame on a stream outside `TWO_STREAMS` is ignored and moves
/// no counter, as `VenueAdapter::feed` requires of every adapter. With
/// `fault_after` set it faults on the next frame once that many have decoded,
/// as a plugin adapter does when its plugin panics, and ignores every later one.
#[derive(Default)]
struct TwoStreamAdapter {
    out: Vec<MarketEvent>,
    stats: AdapterStats,
    fault_after: Option<u64>,
    fault: Option<AdapterFault>,
}

impl DynAdapter for TwoStreamAdapter {
    fn record_types(&self) -> &'static [u8] {
        &[rtype::CONTROL]
    }

    fn session(&self) -> SessionModel {
        NASDAQ.session
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if !TWO_STREAMS.contains(&stream) || self.fault.is_some() {
            return;
        }
        if self.fault_after == Some(self.stats.decoded) {
            self.fault = Some(AdapterFault::Panicked);
            return;
        }
        let Ok(resume_seq) = payload.try_into().map(u64::from_be_bytes) else {
            self.stats.malformed += 1;
            return;
        };
        self.stats.decoded += 1;
        self.out.push(MarketEvent::Control(Control {
            hdr: RecordHeader {
                length: (size_of::<Control>() / 4) as u8,
                rtype: rtype::CONTROL,
                venue: 1,
                instrument_id: 0,
                ts_event: ts_recv,
                ts_recv,
                ts_egress: 0,
                venue_seq,
                egress_seq: 0,
                flags: RecordFlags::NONE,
                _pad: [0u8; 6],
            },
            aux: resume_seq,
            kind: ControlKind::RecoveryComplete,
            _pad: [0u8; 7],
        }));
    }

    fn drain_into(&mut self, out: &mut Vec<MarketEvent>) {
        out.append(&mut self.out);
    }

    fn stats(&self) -> AdapterStats {
        self.stats
    }

    fn set_session_anchor(&mut self, _anchor_ns: u64) {}

    fn fault(&self) -> Option<AdapterFault> {
        self.fault
    }
}

fn recovery_frame(stream: u16, venue_seq: u64, resume_seq: u64) -> SequencedFrame {
    SequencedFrame {
        payload: resume_seq.to_be_bytes().to_vec(),
        ts_recv: venue_seq,
        stream: StreamId(stream),
        venue_seq,
    }
}

// The adapter ignores stream 2, so a runner feeding one fixed stream publishes all three frames.
// @lat: [[tests#Two-stream test adapter#Per-stream feed and publish]]
#[test]
fn each_frame_feeds_and_publishes_on_its_own_stream() {
    let source = VecSource::new(vec![
        recovery_frame(0, 1, 9_001),
        recovery_frame(1, 2, 9_002),
        recovery_frame(2, 3, 9_003),
    ]);
    let mut adapter = TwoStreamAdapter::default();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);

    let (stats, _source) = run(
        source,
        &mut adapter,
        &mut seq,
        &mut NoopTap,
        &mut SessionState::default(),
        &mut RestartState::default(),
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("run two streams");

    let published: Vec<_> = seq
        .sink()
        .records
        .iter()
        .map(|r| (r.stream, r.rtype, r.venue_seq))
        .collect();
    assert_eq!(
        published,
        [
            (StreamId(0), rtype::CONTROL, 1),
            (StreamId(1), rtype::CONTROL, 2),
        ]
    );
    assert_eq!(stats.frames, 3, "the ignored frame still counts as pulled");
    assert_eq!(
        adapter.stats.decoded, 2,
        "the stream 2 frame was fed as stream 2"
    );
}

// Routing the decoded `Control` sets the recovery point and leaves the socket resume points unset.
// @lat: [[tests#Two-stream test adapter#Recovery sequence reaches the restart state]]
#[test]
fn decoded_recovery_sequence_reaches_restart_state() {
    let source = VecSource::new(vec![recovery_frame(0, 1, 6_042)]);
    let mut adapter = TwoStreamAdapter::default();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut restart = RestartState::default();

    run(
        source,
        &mut adapter,
        &mut seq,
        &mut NoopTap,
        &mut SessionState::default(),
        &mut restart,
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("run recovery frame");

    assert_eq!(
        restart,
        RestartState {
            glimpse_resume_seq: Some(6_042),
            ..RestartState::default()
        }
    );
}

// One batch: frame 2 faults the adapter, frame 3 meets it faulted, frame 1 still publishes.
// @lat: [[tests#Two-stream test adapter#Adapter fault ends the session]]
#[test]
fn adapter_fault_ends_the_session_after_routing_earlier_frames() {
    let source = VecSource::new(vec![
        recovery_frame(0, 1, 9_001),
        recovery_frame(0, 2, 9_002),
        recovery_frame(0, 3, 9_003),
    ]);
    let mut adapter = TwoStreamAdapter {
        fault_after: Some(1),
        ..TwoStreamAdapter::default()
    };
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);

    let result = run(
        source,
        &mut adapter,
        &mut seq,
        &mut NoopTap,
        &mut SessionState::default(),
        &mut RestartState::default(),
        RunControl {
            batch: 8,
            stop: &AtomicBool::new(false),
        },
    );

    assert!(
        matches!(result, Err(SessionError::Adapter(AdapterFault::Panicked))),
        "got {:?}",
        result.map(|(stats, _)| stats)
    );
    let published: Vec<_> = seq.sink().records.iter().map(|r| r.venue_seq).collect();
    assert_eq!(
        published,
        [1],
        "the frame decoded before the fault publishes"
    );
}
