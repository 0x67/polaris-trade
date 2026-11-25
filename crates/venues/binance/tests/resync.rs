//! Snapshot resync and mapping rules the recorded session does not exercise.

use std::collections::BTreeMap;

use md_events::{
    Action, BreakRefKind, ControlKind, Level, LevelAction, RecordFlags, Side, TradingState,
};
use md_pipeline::{MarketEvent, StreamId, VenueAdapter, VenueId};
use orderbook_engine::{
    BookDepth, BookEngine,
    policy::{BoardId, BreakScope, Gate, PurposeSet, VenuePolicy},
};
use serde_json::{Value, json};
use venue_binance::{
    BinanceAdapter, Config, InvalidSymbol, MARKET_STREAM, SNAPSHOT_STREAM, Symbol,
};

const FRAMES: &str = include_str!("fixtures/session/frames.jsonl");

/// An adapter plus the per-stream `venue_seq` and `ts_recv` a source stamps.
struct Session {
    adapter: BinanceAdapter,
    seq: [u64; 2],
    ts: u64,
}

impl Session {
    fn new() -> Self {
        let cfg = Config {
            venue: VenueId(5),
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
        };
        Session {
            adapter: BinanceAdapter::new(cfg),
            seq: [0; 2],
            ts: 0,
        }
    }

    fn feed(&mut self, stream: StreamId, payload: &str) -> Vec<MarketEvent> {
        let lane = usize::from(stream == SNAPSHOT_STREAM);
        self.seq[lane] += 1;
        self.ts += 1;
        self.adapter
            .feed(stream, payload.as_bytes(), self.ts, self.seq[lane]);
        self.adapter.drain().collect()
    }

    /// Feed a recorded line on the stream it was recorded on.
    fn feed_line(&mut self, line: &str) -> Vec<MarketEvent> {
        self.feed(stream_of(line), line)
    }
}

fn stream_of(line: &str) -> StreamId {
    if line.starts_with(r#"{"lastUpdateId""#) {
        SNAPSHOT_STREAM
    } else {
        MARKET_STREAM
    }
}

fn depth(first: u64, last: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
    json!({"e":"depthUpdate","E":1_795_537_595_914u64,"s":"BTCUSDT","U":first,"u":last,"b":bids,"a":asks})
        .to_string()
}

fn snapshot(id: u64) -> String {
    json!({"lastUpdateId":id,"bids":[["10.00000000","1.00000000"]],"asks":[["11.00000000","2.00000000"]]})
        .to_string()
}

/// A session whose book is live at update id 1: the first diff buffers and
/// the snapshot covers it.
fn live_session() -> Session {
    let mut s = Session::new();
    s.feed(MARKET_STREAM, &depth(1, 1, &[], &[]));
    s.feed(SNAPSHOT_STREAM, &snapshot(1));
    assert!(!s.adapter.needs_snapshot());
    s
}

fn controls(events: &[MarketEvent]) -> Vec<(ControlKind, u64)> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Control(c) => Some((c.kind, c.aux)),
            _ => None,
        })
        .collect()
}

fn levels(events: &[MarketEvent]) -> Vec<&Level> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Level(l) => Some(l),
            _ => None,
        })
        .collect()
}

struct AcceptAll;

impl VenuePolicy for AcceptAll {
    fn book_gate(&self, _state: TradingState, _action: Action) -> Gate {
        Gate::Accept
    }
    fn purposes(&self, _board: BoardId) -> PurposeSet {
        PurposeSet::BOOK
    }
    fn tape_gate(&self, printable: bool, _action: Action) -> bool {
        printable
    }
    fn on_trade_break(&self, _kind: BreakRefKind) -> BreakScope {
        BreakScope::AggregatesOnly
    }
}

/// Every price level of both sides, best first, after applying `events`.
fn book_of(events: &[MarketEvent]) -> [Vec<(i64, u64, u32)>; 2] {
    let mut engine = BookEngine::new(AcceptAll);
    for event in events {
        match event {
            MarketEvent::Level(l) => {
                engine.apply_level(l);
            }
            MarketEvent::Control(c) if c.kind == ControlKind::SnapshotBegin => {
                engine.begin_snapshot(c.hdr.instrument_id);
            }
            MarketEvent::Control(c) if c.kind == ControlKind::SnapshotEnd => {
                engine.end_snapshot(c.hdr.instrument_id);
            }
            _ => {}
        }
    }
    [Side::Bid, Side::Ask].map(|side| {
        let mut rows = vec![(0, 0, 0); 4096];
        let n = engine.top_levels(1, side, &mut rows);
        rows.truncate(n);
        rows
    })
}

/// Book state of the recorded frames kept as decimal strings, so a snapshot
/// at any update id can be written without the adapter under test.
struct Model {
    id: u64,
    bids: BTreeMap<u64, [String; 2]>,
    asks: BTreeMap<u64, [String; 2]>,
}

impl Model {
    fn price_key(price: &str) -> u64 {
        price.replace('.', "").parse().expect("fixed decimals")
    }

    fn set(side: &mut BTreeMap<u64, [String; 2]>, rows: &Value) {
        for row in rows.as_array().expect("level rows") {
            let (price, qty) = (row[0].as_str().unwrap(), row[1].as_str().unwrap());
            if qty.bytes().all(|b| b == b'0' || b == b'.') {
                side.remove(&Self::price_key(price));
            } else {
                side.insert(Self::price_key(price), [price.into(), qty.into()]);
            }
        }
    }

    fn from_snapshot(line: &str) -> Self {
        let v: Value = serde_json::from_str(line).unwrap();
        let mut m = Model {
            id: v["lastUpdateId"].as_u64().unwrap(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        Self::set(&mut m.bids, &v["bids"]);
        Self::set(&mut m.asks, &v["asks"]);
        m
    }

    fn apply(&mut self, line: &str) {
        let v: Value = serde_json::from_str(line).unwrap();
        if v["e"] != "depthUpdate" || v["u"].as_u64().unwrap() <= self.id {
            return;
        }
        Self::set(&mut self.bids, &v["b"]);
        Self::set(&mut self.asks, &v["a"]);
        self.id = v["u"].as_u64().unwrap();
    }

    fn snapshot_json(&self) -> String {
        let bids: Vec<_> = self.bids.values().rev().collect();
        let asks: Vec<_> = self.asks.values().collect();
        json!({"lastUpdateId":self.id,"bids":bids,"asks":asks}).to_string()
    }
}

// @lat: [[binance#Tests#Gap resync matches an uninterrupted book]]
#[test]
fn resync_after_a_gap_builds_the_book_of_an_uninterrupted_session() {
    let lines: Vec<&str> = FRAMES.lines().collect();

    let mut whole = Session::new();
    let uninterrupted: Vec<MarketEvent> = lines[..10]
        .iter()
        .flat_map(|l| whole.feed_line(l))
        .collect();

    // Frame 5 never arrives: frame 6 then skips 21 update ids.
    let mut lossy = Session::new();
    let mut events: Vec<MarketEvent> = Vec::new();
    for line in lines[..5].iter().chain(&lines[6..10]) {
        events.extend(lossy.feed_line(line));
    }
    assert!(lossy.adapter.needs_snapshot());

    // The refetch is current to the end of frame 6, so frames 8 and 9 replay.
    // Synthesized: only one REST body was recorded, and a limit=20 refetch
    // would hold fewer levels than the diffs built.
    let mut model = Model::from_snapshot(lines[2]);
    for line in &lines[3..7] {
        model.apply(line);
    }
    events.extend(lossy.feed(SNAPSHOT_STREAM, &model.snapshot_json()));
    assert!(!lossy.adapter.needs_snapshot());

    assert_eq!(
        controls(&events),
        [
            (ControlKind::SnapshotBegin, 103_620_809_752),
            (ControlKind::SnapshotEnd, 103_620_809_752),
            (ControlKind::GapDetected, 21),
            (ControlKind::SnapshotBegin, model.id),
            (ControlKind::SnapshotEnd, model.id),
        ]
    );
    let trades = events
        .iter()
        .filter(|e| matches!(e, MarketEvent::Trade(_)))
        .count();
    assert_eq!(trades, 1, "a trade during the gap is not held back");
    assert_eq!(book_of(&events), book_of(&uninterrupted));
}

// @lat: [[binance#Tests#Stale snapshot is refused]]
#[test]
fn snapshot_older_than_the_buffered_diffs_is_refused_and_a_fresh_one_joins() {
    let mut s = Session::new();
    s.feed(
        MARKET_STREAM,
        &depth(10, 12, &[("10.00000000", "5.00000000")], &[]),
    );
    s.feed(
        MARKET_STREAM,
        &depth(13, 15, &[("10.00000000", "6.00000000")], &[]),
    );

    assert_eq!(s.feed(SNAPSHOT_STREAM, &snapshot(5)).len(), 0);
    assert!(s.adapter.needs_snapshot());

    let events = s.feed(SNAPSHOT_STREAM, &snapshot(12));
    assert!(!s.adapter.needs_snapshot());
    let rows = levels(&events);
    let last = rows.last().expect("replayed diff");
    assert_eq!(
        (last.action, last.price, last.qty),
        (LevelAction::Change, 10_000_000_000, 600_000_000)
    );
    assert_eq!(
        rows.iter()
            .filter(|l| l.action == LevelAction::Change)
            .count(),
        1
    );
}

// @lat: [[binance#Tests#Snapshot before any diff]]
#[test]
fn snapshot_fed_before_any_diff_waits_for_one_to_join() {
    let mut s = Session::new();
    assert!(!s.adapter.needs_snapshot());
    assert_eq!(s.feed(SNAPSHOT_STREAM, &snapshot(1000)).len(), 0);

    s.feed(
        MARKET_STREAM,
        &depth(1005, 1010, &[("10.00000000", "5.00000000")], &[]),
    );
    assert!(s.adapter.needs_snapshot());

    // 1004 + 1 meets the first diff's start, so the diff joins it
    let events = s.feed(SNAPSHOT_STREAM, &snapshot(1004));
    assert_eq!(
        controls(&events),
        [
            (ControlKind::SnapshotBegin, 1004),
            (ControlKind::SnapshotEnd, 1004)
        ]
    );
    assert_eq!(
        levels(&events).last().map(|l| l.action),
        Some(LevelAction::Change)
    );
    assert!(!s.adapter.needs_snapshot());
}

// @lat: [[binance#Tests#Snapshot inside a diff range]]
#[test]
fn snapshot_inside_a_buffered_diffs_range_replays_that_whole_diff() {
    let mut s = Session::new();
    s.feed(
        MARKET_STREAM,
        &depth(10, 15, &[("10.00000000", "6.00000000")], &[]),
    );
    let events = s.feed(SNAPSHOT_STREAM, &snapshot(12));
    assert!(
        controls(&events)
            .iter()
            .all(|(k, _)| *k != ControlKind::GapDetected)
    );
    let last = *levels(&events).last().expect("replayed diff");
    assert_eq!(
        (last.action, last.price, last.qty),
        (LevelAction::Change, 10_000_000_000, 600_000_000)
    );

    // the book now stands at update id 15
    let next = s.feed(
        MARKET_STREAM,
        &depth(16, 16, &[("10.00000000", "7.00000000")], &[]),
    );
    assert_eq!(levels(&next).len(), 1);
}

// @lat: [[binance#Tests#Old snapshot while live]]
#[test]
fn snapshot_older_than_the_live_book_is_ignored() {
    let mut s = Session::new();
    s.feed(
        MARKET_STREAM,
        &depth(10, 12, &[("10.00000000", "1.00000000")], &[]),
    );
    s.feed(SNAPSHOT_STREAM, &snapshot(11));
    s.feed(
        MARKET_STREAM,
        &depth(13, 20, &[("10.00000000", "2.00000000")], &[]),
    );

    assert_eq!(s.feed(SNAPSHOT_STREAM, &snapshot(9)).len(), 0);
    let next = s.feed(
        MARKET_STREAM,
        &depth(21, 22, &[("10.00000000", "3.00000000")], &[]),
    );
    assert_eq!(controls(&next), []);
    assert_eq!(levels(&next).len(), 1);
    assert!(!s.adapter.needs_snapshot());
}

// @lat: [[binance#Tests#Dropped diff keeps the ingest gap flag]]
#[test]
fn ingest_gap_survives_buffered_diffs_a_snapshot_drops() {
    let mut s = Session::new();
    let feed = |s: &mut Session, stream, payload: &str, seq| {
        s.ts += 1;
        s.adapter.feed(stream, payload.as_bytes(), s.ts, seq);
        s.adapter.drain().collect::<Vec<_>>()
    };
    let diff = |a, b| depth(a, b, &[("10.00000000", "1.00000000")], &[]);
    feed(&mut s, MARKET_STREAM, &diff(10, 12), 1);
    // venue_seq 2 never arrived: it could have been a trade
    feed(&mut s, MARKET_STREAM, &diff(13, 15), 3);
    let snap = feed(&mut s, SNAPSHOT_STREAM, &snapshot(15), 1);
    assert!(snap.iter().all(|e| !is_suspect(e)));

    let trade = r#"{"e":"trade","E":1,"s":"BTCUSDT","t":9,"p":"10.00000000","q":"1.00000000","T":1795537595915,"m":true,"M":true}"#;
    let events = feed(&mut s, MARKET_STREAM, trade, 4);
    assert!(is_suspect(&events[0]), "the skip must reach a record");
    let after = feed(&mut s, MARKET_STREAM, trade, 5);
    assert!(!is_suspect(&after[0]), "a record spends the skip");
}

fn is_suspect(event: &MarketEvent) -> bool {
    let flags = match event {
        MarketEvent::Level(l) => l.hdr.flags,
        MarketEvent::Trade(t) => t.hdr.flags,
        MarketEvent::Control(c) => c.hdr.flags,
        _ => return false,
    };
    flags.contains(RecordFlags::GAP_SUSPECT)
}

// @lat: [[binance#Tests#Buffer is bounded]]
#[test]
fn buffer_drops_the_oldest_diffs_so_an_old_snapshot_cannot_join() {
    let mut s = Session::new();
    // 8200 one-id diffs, ids 1 to 8200, against a buffer of 8192
    for id in 1..=8200 {
        s.feed(
            MARKET_STREAM,
            &depth(id, id, &[("10.00000000", "1.00000000")], &[]),
        );
    }
    // diff 8 is the newest dropped, so a snapshot at 3 leaves a hole
    assert_eq!(s.feed(SNAPSHOT_STREAM, &snapshot(3)).len(), 0);
    assert!(s.adapter.needs_snapshot());
    assert_ne!(s.feed(SNAPSHOT_STREAM, &snapshot(8)).len(), 0);
}

// @lat: [[binance#Tests#Mapping rules]]
#[test]
fn diffs_map_to_change_and_delete_and_trades_take_the_taker_side() {
    let mut s = live_session();
    let events = s.feed(
        MARKET_STREAM,
        &depth(
            2,
            2,
            &[("0.00001234", "0.5"), ("9.00000000", "0")],
            &[("11.00000000", "3.00000001")],
        ),
    );
    let got: Vec<_> = levels(&events)
        .iter()
        .map(|l| (l.side, l.action, l.price, l.qty))
        .collect();
    assert_eq!(
        got,
        [
            (Side::Bid, LevelAction::Change, 12_340, 50_000_000),
            (Side::Bid, LevelAction::Delete, 9_000_000_000, 0),
            (Side::Ask, LevelAction::Change, 11_000_000_000, 300_000_001),
        ]
    );

    let trade = |maker: bool| {
        format!(
            r#"{{"e":"trade","E":1,"s":"BTCUSDT","t":9,"p":"10.00000000","q":"1.00000000","T":1795537595915,"m":{maker},"M":true}}"#
        )
    };
    let prints: Vec<_> = [true, false]
        .map(|maker| match s.feed(MARKET_STREAM, &trade(maker))[0] {
            MarketEvent::Trade(t) => (t.aggressor, t.price, t.qty, t.trade_ref),
            _ => panic!("trade frame did not emit a trade"),
        })
        .to_vec();
    // a resting buyer means the seller took liquidity
    assert_eq!(
        prints,
        [
            (Side::Ask, 10_000_000_000, 100_000_000, 9),
            (Side::Bid, 10_000_000_000, 100_000_000, 9),
        ]
    );
}

// @lat: [[binance#Tests#Bad frames change nothing]]
#[test]
fn malformed_frames_leave_the_sequence_where_it_was() {
    let mut s = live_session();
    let bad = [
        depth(2, 2, &[("1.0000000001", "1")], &[]),
        depth(2, 2, &[("10.00000000", "0.000000001")], &[]),
        depth(2, 2, &[("10.00000000", "-1")], &[]),
        depth(3, 2, &[("10.00000000", "1")], &[]),
        depth(2, 2, &[("10.00000000", "1")], &[]).replace("BTCUSDT", "ETHUSDT"),
        depth(2, 2, &[("9300000000.0", "1")], &[]),
    ];
    for frame in &bad {
        assert!(s.feed(MARKET_STREAM, frame).is_empty(), "{frame}");
    }
    assert_eq!(s.adapter.stats().malformed, bad.len() as u64);

    // update id 2 still applies: none of the bad frames moved the sequence
    let events = s.feed(MARKET_STREAM, &depth(2, 2, &[("10.00000000", "1")], &[]));
    assert_eq!(levels(&events).len(), 1);
}

// @lat: [[binance#Tests#Symbol spelling]]
#[test]
fn symbol_must_be_spelled_as_event_payloads_spell_it() {
    for bad in ["", "btcusdt", "BTC-USDT", "ABCDEFGHIJKLMNOPQRSTU"] {
        assert_eq!(Symbol::new(bad), Err(InvalidSymbol(bad.to_owned())));
    }
    assert!(Symbol::new("1000SATSUSDT").is_ok());
}
