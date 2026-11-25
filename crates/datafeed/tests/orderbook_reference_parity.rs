mod support;

use std::{cell::RefCell, collections::BTreeMap, io::Cursor, path::PathBuf, rc::Rc, time::Instant};

use datafeed::{
    restart::RestartState,
    session::{self, DefaultVenuePolicy},
    sink::NullSink,
    tap::NoopTap,
};
use md_events::{Action, BidAskPair, Mbo, Price, RecordFlags, RecordHeader, Side, rtype};
use md_pipeline::{
    EgressSequencer, IngestError, IngestFrame, IngestSource, StreamId, SystemClock, VenueAdapter,
    VenueId, VirtualClock,
};
use md_replay::BinaryFileReplay;
use orderbook_engine::{ApplyResult, BookEngine};
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};
use support::reference_parity::{
    ComparatorSink, ComparatorTap, DownstreamMboRebuilder, FullDayComparator, ObservedBookTap,
    ParityContext, ParityLayer, ParitySubject, PrefixSource, binary_dump, expect_exact,
    open_binary_sample, parse_l1, parse_l2, parse_l3,
};

const STREAM_HEX: &str = include_str!("fixtures/orderbook-reference/stream.hex");
const REFERENCE_L1: &str = include_str!("fixtures/orderbook-reference/l1.csv");
const REFERENCE_L2: &str = include_str!("fixtures/orderbook-reference/l2.csv");
const REFERENCE_L3: &str = include_str!("fixtures/orderbook-reference/l3.csv");

#[test]
fn synthetic_nasdaq_replay_matches_exact_l1_l2_l3_fixture() {
    let source = BinaryFileReplay::new(Cursor::new(binary_dump(STREAM_HEX)), 100, 1);
    let mut adapter = NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    });
    let mut sequence = EgressSequencer::new(ObservedBookTap::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();

    let (stats, _source) = session::run(
        source,
        &mut adapter,
        &mut sequence,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: 16,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("reference fixture replay");

    assert_eq!(stats.frames, 34);
    assert_eq!(stats.records, 39);
    assert!(stats.unknown_order_counts.is_empty());
    assert_eq!(adapter.order_count(), 0);
    assert_eq!(adapter.stats().unknown_kind, 0);
    assert_eq!(adapter.stats().malformed, 0);

    let l1 = parse_l1(REFERENCE_L1);
    let l2 = parse_l2(REFERENCE_L2);
    let l3 = parse_l3(REFERENCE_L3);
    assert_eq!(sequence.sink().mbos.len(), l3.len());
    assert_eq!(sequence.sink().mbp10.len(), l1.len());
    assert_eq!(sequence.sink().bbo.len(), l1.len());

    assert_book_record_order(&sequence.sink().book_rtypes);

    let instrument = sequence.sink().mbos[0].hdr.instrument_id;
    let mut audit = BookEngine::new(DefaultVenuePolicy);
    let mut bid_levels = BTreeMap::<i64, (u64, u32)>::new();
    let mut ask_levels = BTreeMap::<i64, (u64, u32)>::new();
    let mut history = Vec::<Mbo>::with_capacity(16);
    let mut mbo_index = 0usize;
    let mut l3_index = 0usize;
    let mut l2_consumed = 0usize;

    for (event_index, expected_l1) in l1.iter().enumerate() {
        assert_eq!(expected_l1.event, event_index as u64 + 1);
        let event_start = mbo_index;
        while mbo_index < sequence.sink().mbos.len()
            && sequence.sink().mbos[mbo_index].hdr.venue_seq == expected_l1.venue_seq
        {
            let actual = sequence.sink().mbos[mbo_index];
            let expected = l3[l3_index];
            let context = parity_context(
                expected.event,
                expected.change,
                &actual,
                ParityLayer::L3,
                expected.side,
                ParitySubject::Order(expected.order_id),
                &history,
            );
            assert_eq!(expected.event, expected_l1.event);
            assert_eq!(expected.change, (mbo_index - event_start) as u64);
            expect_exact(
                context,
                "mutation",
                (
                    expected.venue_seq,
                    expected.action,
                    expected.order_id,
                    expected.side,
                    expected.event_price,
                    expected.event_qty,
                    expected.participant,
                ),
                (
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
            assert_eq!(audit.apply_mbo(&actual).result, ApplyResult::Applied);

            let order = audit.order_state(instrument, expected.side, expected.order_id);
            let actual_state = order.map(|order| {
                (
                    order.price,
                    order.qty,
                    audit
                        .queue_position(instrument, expected.side, expected.order_id)
                        .expect("present order has a queue position") as i64,
                )
            });
            let expected_state = expected.present.then_some((
                expected.resting_price,
                expected.remaining_qty,
                expected.queue_position,
            ));
            expect_exact(context, "post_order_state", expected_state, actual_state)
                .unwrap_or_else(|error| panic!("{error}"));
            push_history(&mut history, actual);
            mbo_index += 1;
            l3_index += 1;
        }
        assert!(
            mbo_index > event_start,
            "reference event has an MBO mutation"
        );

        for expected in l2
            .iter()
            .filter(|row| row.venue_seq == expected_l1.venue_seq)
        {
            l2_consumed += 1;
            assert_eq!(expected.event, expected_l1.event);
            let levels = match expected.side {
                Side::Bid => &mut bid_levels,
                Side::Ask => &mut ask_levels,
                Side::None => unreachable!("fixture has a concrete side"),
            };
            if expected.qty == 0 {
                levels.remove(&expected.price);
            } else {
                levels.insert(expected.price, (expected.qty, expected.count));
            }
            let last_mbo = sequence.sink().mbos[mbo_index - 1];
            let context = parity_context(
                expected.event,
                expected.change,
                &last_mbo,
                ParityLayer::L2,
                expected.side,
                ParitySubject::Price(expected.price),
                &history,
            );
            let actual_level = audit
                .level_state(instrument, expected.side, expected.price)
                .map(|level| (level.qty, level.count));
            let expected_level = (expected.qty != 0).then_some((expected.qty, expected.count));
            expect_exact(context, "level_state", expected_level, actual_level)
                .unwrap_or_else(|error| panic!("{error}"));
        }

        let mbp10 = sequence.sink().mbp10[event_index];
        let bbo = sequence.sink().bbo[event_index];
        assert_eq!(mbp10.hdr.venue_seq, expected_l1.venue_seq);
        assert_eq!(bbo.hdr.venue_seq, expected_l1.venue_seq);
        let expected_levels = top_ten(&bid_levels, &ask_levels);
        let last_mbo = sequence.sink().mbos[mbo_index - 1];
        let l2_context = parity_context(
            expected_l1.event,
            0,
            &last_mbo,
            ParityLayer::L2,
            Side::None,
            ParitySubject::Book,
            &history,
        );
        expect_exact(l2_context, "mbp10", expected_levels, mbp10.levels)
            .unwrap_or_else(|error| panic!("{error}"));
        expect_exact(l2_context, "mbp10_bbo", mbp10.levels[0], bbo.pair)
            .unwrap_or_else(|error| panic!("{error}"));
        let expected_reference_bbo = BidAskPair {
            bid_px: expected_l1.bid_price,
            ask_px: expected_l1.ask_price,
            bid_sz: expected_l1.bid_qty,
            ask_sz: expected_l1.ask_qty,
            bid_ct: expected_l1.bid_count,
            ask_ct: expected_l1.ask_count,
        };
        let l1_context = parity_context(
            expected_l1.event,
            0,
            &last_mbo,
            ParityLayer::L1,
            Side::None,
            ParitySubject::Book,
            &history,
        );
        expect_exact(
            l1_context,
            "bbo",
            expected_reference_bbo,
            reference_pair(bbo.pair),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(bbo.pair, normalized_pair(expected_reference_bbo));
        assert_eq!(mbp10.price, last_mbo.price);
        assert_eq!(mbp10.qty, last_mbo.qty);
        assert_eq!(mbp10.action, last_mbo.action);
        assert_eq!(mbp10.side, last_mbo.side);
    }

    assert_eq!(mbo_index, sequence.sink().mbos.len());
    assert_eq!(l3_index, l3.len());
    assert_eq!(l2_consumed, l2.len(), "unconsumed expected L2 rows remain");
    assert!(history.len() <= 16);
    assert_eq!(audit.instrument_order_count(instrument), 0);
    assert_replace_headers_and_egress(sequence.sink());
    assert_downstream_reconstruction(sequence.sink());
}

fn parity_max_messages() -> Option<u64> {
    let value = std::env::var("PARITY_MAX_MESSAGES").ok()?;
    Some(value.parse().expect("PARITY_MAX_MESSAGES must be a u64"))
}

fn nasdaq_sample_path() -> PathBuf {
    PathBuf::from(
        std::env::var_os("NASDAQ_ITCH_SAMPLE")
            .expect("NASDAQ_ITCH_SAMPLE must point to the full-day capture (.bin.zst or .gz)"),
    )
}

fn parity_batch() -> usize {
    std::env::var("PARITY_BATCH").map_or(512, |value| {
        value.parse().expect("PARITY_BATCH must be a usize")
    })
}

// PARITY_OHLCV_RESOLUTIONS (comma-separated tokens, e.g. "1s,1m") narrows the
// OHLCV engine so probe runs can isolate its cost. Unset = production default
// set. Probe-only: the full-day gate stays pinned to the default.
fn parity_session_state() -> (session::SessionState, String) {
    match std::env::var("PARITY_OHLCV_RESOLUTIONS") {
        Ok(spec) => {
            let tokens: Vec<String> = spec.split(',').map(|t| t.trim().to_string()).collect();
            let resolved = datafeed::config::resolve_resolutions(&tokens)
                .expect("PARITY_OHLCV_RESOLUTIONS tokens must resolve");
            (session::SessionState::new(resolved), spec)
        }
        Err(_) => (session::SessionState::default(), "default".to_string()),
    }
}

#[test]
#[ignore = "needs NASDAQ_ITCH_SAMPLE + ORDERBOOK_REFERENCE_DIR; run: NASDAQ_ITCH_SAMPLE=<zst|gz> ORDERBOOK_REFERENCE_DIR=<dir> [PARITY_MAX_MESSAGES=<n>] [PARITY_BATCH=<n>] cargo nextest run --release --test orderbook_reference_parity --run-ignored ignored-only --no-capture full_day_nasdaq_replay_matches_reference_l1_l2_l3"]
fn full_day_nasdaq_replay_matches_reference_l1_l2_l3() {
    let input = nasdaq_sample_path();
    let reference_dir = PathBuf::from(
        std::env::var_os("ORDERBOOK_REFERENCE_DIR")
            .expect("ORDERBOOK_REFERENCE_DIR must contain the reference CSVs"),
    );
    let limit = parity_max_messages();
    let source = PrefixSource::new(open_binary_sample(&input, 0, 1), limit.unwrap_or(u64::MAX));
    let mut adapter = NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    });
    let mut comparator = FullDayComparator::open(&reference_dir).expect("open reference CSVs");
    if limit.is_some() {
        comparator = comparator.truncated();
    }
    // one comparator, two seams: raw frames reach it through the tap, published
    // records off the sink
    let shared = Rc::new(RefCell::new(comparator));
    let mut sequence =
        EgressSequencer::new(ComparatorSink(Rc::clone(&shared)), VirtualClock::default());
    let mut tap = ComparatorTap(Rc::clone(&shared));
    let mut state = session::SessionState::default();
    let started = Instant::now();

    let (stats, source) = session::run(
        source,
        &mut adapter,
        &mut sequence,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: parity_batch(),
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("full-day NASDAQ replay");
    let source = source.into_inner();
    assert!(source.io_error().is_none(), "full-day gzip replay io error");
    // drop both halves so the shared comparator is uniquely owned again
    drop(tap);
    drop(sequence);
    let report = Rc::try_unwrap(shared)
        .unwrap_or_else(|_| panic!("comparator still shared"))
        .into_inner()
        .finish();

    if limit.is_none() {
        assert_eq!(report.input_messages, 423_285_709);
        assert_eq!(report.target_events, 562_331);
        assert_eq!(report.l1_rows, 562_331);
        assert_eq!(report.l2_rows, 631_267);
        assert_eq!(report.l3_rows, 631_335);
        assert_eq!(report.strict_l3_rows, 631_335);
        assert_eq!(report.peak_event_mutations, 2);
        assert_eq!(report.diagnostic_history_bound, 16);
    }
    assert_eq!(stats.frames, report.input_messages);
    assert_eq!(adapter.stats().decoded, report.input_messages);
    assert_eq!(adapter.stats().unknown_kind, 0);
    assert_eq!(adapter.stats().malformed, 0);
    assert_eq!(
        stats
            .unknown_order_counts
            .get(&report.target_instrument)
            .copied()
            .unwrap_or(0),
        0
    );
    println!(
        "full-day parity: {report:?}, elapsed={:?}",
        started.elapsed()
    );
}

#[test]
#[ignore = "decode throughput probe; needs NASDAQ_ITCH_SAMPLE; run: NASDAQ_ITCH_SAMPLE=<zst|gz> [PARITY_MAX_MESSAGES=<n>] [PARITY_BATCH=<n>] cargo nextest run --release --test orderbook_reference_parity --run-ignored ignored-only --no-capture full_day_nasdaq_decode_only_drain"]
fn full_day_nasdaq_decode_only_drain() {
    let input = nasdaq_sample_path();
    let limit = parity_max_messages().unwrap_or(u64::MAX);
    let mut source = PrefixSource::new(open_binary_sample(&input, 0, 1), limit);
    let batch = parity_batch();
    let started = Instant::now();
    let mut frames = Vec::with_capacity(batch);
    let mut total = 0u64;
    let mut payload_bytes = 0u64;
    loop {
        frames.clear();
        match source.recv_batch(&mut frames, batch) {
            Ok(_) => {}
            Err(IngestError::Closed) => break,
            Err(IngestError::PoolExhausted { .. }) => continue,
        }
        total += frames.len() as u64;
        for frame in &frames {
            payload_bytes += frame.payload().len() as u64;
        }
    }
    let source = source.into_inner();
    assert!(source.io_error().is_none(), "decode drain io error");
    assert!(total > 0, "decode drain yielded no frames");
    println!(
        "decode-only: frames={total} payload_bytes={payload_bytes} elapsed={:?}",
        started.elapsed()
    );
}

#[test]
#[ignore = "session throughput probe (no comparator); needs NASDAQ_ITCH_SAMPLE; run: NASDAQ_ITCH_SAMPLE=<zst|gz> [PARITY_MAX_MESSAGES=<n>] [PARITY_BATCH=<n>] [PARITY_OHLCV_RESOLUTIONS=1s,1m] cargo nextest run --release --test orderbook_reference_parity --run-ignored ignored-only --no-capture full_day_nasdaq_session_noop_tap"]
fn full_day_nasdaq_session_noop_tap() {
    let input = nasdaq_sample_path();
    let limit = parity_max_messages().unwrap_or(u64::MAX);
    let source = PrefixSource::new(open_binary_sample(&input, 0, 1), limit);
    let mut adapter = NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    });
    let mut sequence = EgressSequencer::new(NullSink, VirtualClock::default());
    let mut tap = NoopTap;
    let (mut state, resolutions) = parity_session_state();
    let started = Instant::now();

    let (stats, source) = session::run(
        source,
        &mut adapter,
        &mut sequence,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: parity_batch(),
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("session probe replay");
    let source = source.into_inner();
    assert!(source.io_error().is_none(), "session probe io error");
    assert_eq!(adapter.stats().decoded, stats.frames);
    assert_eq!(adapter.stats().unknown_kind, 0);
    assert_eq!(adapter.stats().malformed, 0);
    println!(
        "session+noop-tap: frames={} records={} ohlcv_resolutions={} elapsed={:?}",
        stats.frames,
        stats.records,
        resolutions,
        started.elapsed()
    );
}

#[test]
fn downstream_rebuilder_splits_streams_and_zero_sequence_records() {
    let mbo = |order_id: u64, venue_seq: u64| Mbo {
        hdr: RecordHeader {
            length: (core::mem::size_of::<Mbo>() / 4) as u8,
            rtype: rtype::MBO,
            venue: 2,
            instrument_id: 1,
            ts_event: order_id,
            ts_recv: order_id,
            ts_egress: 0,
            venue_seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id,
        price: order_id as i64 * 1_000_000_000,
        qty: 10,
        action: Action::Add,
        side: Side::Bid,
        participant: [0; 7],
        _pad: [0u8; 7],
    };
    let mut downstream = DownstreamMboRebuilder::default();
    downstream.push(StreamId(0), mbo(1, 10));
    assert_eq!(downstream.derived_count(), 0);
    downstream.push(StreamId(1), mbo(2, 10));
    assert_eq!(downstream.derived_count(), 1, "stream change flushes");
    downstream.push(StreamId(1), mbo(3, 0));
    assert_eq!(
        downstream.derived_count(),
        3,
        "zero sequence is an ungrouped legacy boundary"
    );
    assert_eq!(downstream.finish().len(), 3);
}

#[test]
fn diagnostic_localizes_injected_l1_divergence() {
    assert_injected_diagnostic(ParityLayer::L1, Side::None, ParitySubject::Book);
}

#[test]
fn diagnostic_localizes_injected_l2_divergence() {
    assert_injected_diagnostic(
        ParityLayer::L2,
        Side::Bid,
        ParitySubject::Price(101_000_000_000),
    );
}

#[test]
fn diagnostic_localizes_injected_l3_divergence() {
    assert_injected_diagnostic(ParityLayer::L3, Side::Bid, ParitySubject::Order(201));
}

fn assert_injected_diagnostic(layer: ParityLayer, side: Side, subject: ParitySubject) {
    let prior = [Mbo {
        hdr: RecordHeader {
            length: (core::mem::size_of::<Mbo>() / 4) as u8,
            rtype: rtype::MBO,
            venue: 2,
            instrument_id: 99,
            ts_event: 700,
            ts_recv: 701,
            ts_egress: 0,
            venue_seq: 6,
            egress_seq: 1,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        order_id: 101,
        price: 100_000_000_000,
        qty: 80,
        action: Action::Cancel,
        side: Side::Bid,
        participant: *b"ABCD\0\0\0",
        _pad: [0u8; 7],
    }];
    let context = ParityContext {
        event: 6,
        change: 1,
        venue_seq: 7,
        wire_type: b'U',
        ts_event: 800,
        ts_recv: 801,
        instrument: 99,
        layer,
        side,
        subject,
        prior: &prior,
    };
    let error = expect_exact(context, "qty", 70_u64, 71_u64)
        .expect_err("injected mismatch must fail")
        .to_string();
    for required in [
        "event=6",
        "change=1",
        "venue_seq=7",
        "wire=U",
        "ts_event=800",
        "ts_recv=801",
        "instrument=99",
        &format!("layer={layer:?}"),
        &format!("side={side:?}"),
        &format!("subject={subject:?}"),
        "field=qty",
        "expected=70",
        "actual=71",
        "prior=[Mbo",
    ] {
        assert!(
            error.contains(required),
            "missing `{required}` in `{error}`"
        );
    }
}

fn parity_context<'a>(
    event: u64,
    change: u64,
    mbo: &Mbo,
    layer: ParityLayer,
    side: Side,
    subject: ParitySubject,
    prior: &'a [Mbo],
) -> ParityContext<'a> {
    ParityContext {
        event,
        change,
        venue_seq: mbo.hdr.venue_seq,
        wire_type: wire_type(event),
        ts_event: mbo.hdr.ts_event,
        ts_recv: mbo.hdr.ts_recv,
        instrument: mbo.hdr.instrument_id,
        layer,
        side,
        subject,
        prior,
    }
}

fn wire_type(event: u64) -> u8 {
    match event {
        1 => b'F',
        2 | 10..=20 | 32 => b'A',
        3 => b'X',
        4 | 33 => b'E',
        5 | 6 => b'U',
        7 => b'C',
        8 | 9 | 21..=31 => b'D',
        _ => b'?',
    }
}

fn push_history(history: &mut Vec<Mbo>, mbo: Mbo) {
    if history.len() == 16 {
        history.remove(0);
    }
    history.push(mbo);
}

fn assert_book_record_order(actual: &[u8]) {
    let mut expected = Vec::new();
    for event in 1..=33 {
        expected.push(rtype::MBO);
        if event == 5 || event == 6 {
            expected.push(rtype::MBO);
        }
        expected.push(rtype::MBP10);
        expected.push(rtype::BBO);
    }
    assert_eq!(actual, expected);
}

fn assert_replace_headers_and_egress(tap: &ObservedBookTap) {
    for venue_seq in [6, 7] {
        let replacement: Vec<_> = tap
            .mbos
            .iter()
            .filter(|mbo| mbo.hdr.venue_seq == venue_seq)
            .collect();
        assert_eq!(replacement.len(), 2);
        assert_eq!(replacement[0].action, Action::Cancel);
        assert_eq!(replacement[1].action, Action::Add);
        assert_eq!(replacement[0].hdr.ts_event, replacement[1].hdr.ts_event);
        assert_eq!(replacement[0].hdr.ts_recv, replacement[1].hdr.ts_recv);
        assert_eq!(
            replacement[1].hdr.egress_seq,
            replacement[0].hdr.egress_seq + 1
        );
        let mbp10 = tap
            .mbp10
            .iter()
            .find(|row| row.hdr.venue_seq == venue_seq)
            .expect("replacement MBP10");
        let bbo = tap
            .bbo
            .iter()
            .find(|row| row.hdr.venue_seq == venue_seq)
            .expect("replacement BBO");
        assert_eq!(mbp10.hdr.egress_seq, replacement[1].hdr.egress_seq + 1);
        assert_eq!(bbo.hdr.egress_seq, mbp10.hdr.egress_seq + 1);
    }
}

fn assert_downstream_reconstruction(tap: &ObservedBookTap) {
    let mut downstream = DownstreamMboRebuilder::default();
    for mbo in tap.mbos.iter().copied() {
        downstream.push(StreamId(0), mbo);
    }
    assert_eq!(
        downstream.derived_count(),
        tap.mbp10.len() - 1,
        "the final non-zero venue sequence remains buffered until EOF"
    );
    let rebuilt = downstream.finish();
    assert_eq!(rebuilt.len(), tap.mbp10.len());
    for (index, derived) in rebuilt.into_iter().enumerate() {
        let mbp10 = derived.mbp10.expect("rebuilt MBP10");
        let bbo = derived.bbo.expect("rebuilt BBO");
        assert_eq!(mbp10.hdr.venue_seq, tap.mbp10[index].hdr.venue_seq);
        assert_eq!(mbp10.price, tap.mbp10[index].price);
        assert_eq!(mbp10.qty, tap.mbp10[index].qty);
        assert_eq!(mbp10.action, tap.mbp10[index].action);
        assert_eq!(mbp10.side, tap.mbp10[index].side);
        assert_eq!(mbp10.levels, tap.mbp10[index].levels);
        assert_eq!(bbo.hdr.venue_seq, tap.bbo[index].hdr.venue_seq);
        assert_eq!(bbo.pair, tap.bbo[index].pair);
    }
}

fn top_ten(bids: &BTreeMap<i64, (u64, u32)>, asks: &BTreeMap<i64, (u64, u32)>) -> [BidAskPair; 10] {
    let mut levels = [BidAskPair::default(); 10];
    let mut bid_iter = bids.iter().rev();
    let mut ask_iter = asks.iter();
    let depth = bids.len().max(asks.len()).min(levels.len());
    for level in levels.iter_mut().take(depth) {
        if let Some((&price, &(qty, count))) = bid_iter.next() {
            level.bid_px = price;
            level.bid_sz = qty;
            level.bid_ct = count;
        } else {
            level.bid_px = Price::NULL.raw();
        }
        if let Some((&price, &(qty, count))) = ask_iter.next() {
            level.ask_px = price;
            level.ask_sz = qty;
            level.ask_ct = count;
        } else {
            level.ask_px = Price::NULL.raw();
        }
    }
    levels
}

fn reference_pair(mut pair: BidAskPair) -> BidAskPair {
    if pair.bid_sz == 0 && pair.bid_ct == 0 && pair.bid_px == Price::NULL.raw() {
        pair.bid_px = 0;
    }
    if pair.ask_sz == 0 && pair.ask_ct == 0 && pair.ask_px == Price::NULL.raw() {
        pair.ask_px = 0;
    }
    pair
}

fn normalized_pair(mut pair: BidAskPair) -> BidAskPair {
    if pair.bid_sz == 0 && pair.bid_ct == 0 && pair.ask_ct > 0 {
        pair.bid_px = Price::NULL.raw();
    }
    if pair.ask_sz == 0 && pair.ask_ct == 0 && pair.bid_ct > 0 {
        pair.ask_px = Price::NULL.raw();
    }
    pair
}
