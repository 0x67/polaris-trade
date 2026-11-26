//! A host drives `FeedSession` and `SinkSession` the way an embedding
//! application would: built in code with no config file, polled from the
//! host's own loop.

mod support;

use std::{cell::RefCell, fs, io::Cursor, path::Path, rc::Rc, sync::atomic::AtomicBool};

use datafeed::{
    FeedError, FeedSession,
    config::{DatafeedConfig, ReplayFormat, SourceConfig},
    restart::RestartState,
    session::{self, SessionState},
    source::SourceError,
    tap::NoopTap,
    venue::NASDAQ,
};
use md_events::rtype;
use md_pipeline::{EgressSequencer, OutputSink, RecordRef, SinkError, StreamId};
use md_replay::{
    BinaryFileReplay, PacedSource, Pacing, PacingMode, ReplayDriver, SharedClock, ThreadSleepPacer,
};
use support::{itch, record::header_bytes, reference_parity::binary_dump};

const STREAM_HEX: &str = include_str!("fixtures/orderbook-reference/stream.hex");
// venue id every NASDAQ test uses; a BinaryFILE has no header to adopt one from
const NASDAQ_VENUE: u16 = 2;

type Published = Vec<(StreamId, Vec<u8>)>;

/// Each published record as its stream and bytes: header, then body. Clones
/// share one list, so a test reads it while a session owns the sink.
#[derive(Clone, Default)]
struct BytesSink(Rc<RefCell<Published>>);

impl OutputSink for BytesSink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.0
            .borrow_mut()
            .push((rec.stream, [header_bytes(rec.header), rec.body].concat()));
        Ok(())
    }
}

fn replay_source(path: &Path, format: ReplayFormat) -> SourceConfig {
    SourceConfig::Replay {
        path: path.to_path_buf(),
        format,
        base_ts: 0,
        step_ns: 1,
        load: Default::default(),
        pacing: Default::default(),
    }
}

/// What the batch runner publishes over `dump`, assembled as the binary's
/// replay always was: unthrottled pacing driving a `SharedClock`, batch 64,
/// and the adapter the descriptor's `make` builds.
fn runner_records(dump: Vec<u8>) -> Published {
    let config: DatafeedConfig = serde_json::from_str(&format!(
        r#"{{"venue": "nasdaq", "venue_id": {NASDAQ_VENUE}, "session_midnight_ns": 0,
            "source": {{"kind": "replay", "path": "fixture"}}}}"#
    ))
    .expect("runner config");
    let clock = SharedClock::new();
    let paced = PacedSource::new(
        ReplayDriver::new(
            BinaryFileReplay::new(Cursor::new(dump), 0, 1),
            ThreadSleepPacer,
            Pacing::Fixed(PacingMode::Unthrottled),
        ),
        clock.clone(),
    );
    let sink = BytesSink::default();
    session::run(
        paced,
        &mut *(config.venue.make)(&config),
        &mut EgressSequencer::new(sink.clone(), clock),
        &mut NoopTap,
        &mut SessionState::from_config(&config).expect("default resolutions"),
        &mut RestartState::default(),
        session::RunControl {
            batch: 64,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("runner replay");
    sink.0.take()
}

/// The reference fixture written where a replay source can open it, and what
/// the batch runner publishes over it.
fn reference_replay(name: &str) -> (std::path::PathBuf, Published) {
    let dump = binary_dump(STREAM_HEX);
    let path = std::env::temp_dir().join(name);
    fs::write(&path, &dump).expect("write replay dump");
    (path, runner_records(dump))
}

// Five records per poll into one growing `Vec` must give the host the runner's
// records, byte for byte. The first batch routes far more than five and the
// last step routes the closing OHLCV flush, so a record dropped past `max` or a
// step that wrote over the host's records breaks the match.
#[test]
fn host_poll_receives_the_runner_records_then_closed() {
    let (path, expected) = reference_replay("datafeed_feed_session_runner.bin");
    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Binary))
        .venue_id(NASDAQ_VENUE)
        .build()
        .expect("session builds with no config file");
    let mut out = Vec::new();
    loop {
        let before = out.len();
        match session.poll(&mut out, 5) {
            Ok(n) => assert!(
                n <= 5 && out.len() == before + n,
                "poll appended {n} past max 5"
            ),
            Err(FeedError::Closed) => break,
            Err(err) => panic!("replay failed: {err}"),
        }
    }

    assert!(
        expected.len() > 5,
        "fixture routes more than one poll's worth"
    );
    assert_eq!(
        out.last().map(|r| r.header().rtype),
        Some(rtype::OHLCV),
        "the source ends on the closing flush"
    );
    let polled: Published = out
        .iter()
        .map(|r| (r.stream(), r.bytes().to_vec()))
        .collect();
    assert_eq!(polled, expected);
    for _ in 0..2 {
        assert!(matches!(session.poll(&mut out, 5), Err(FeedError::Closed)));
    }
    session.close().expect("close with no capture tap");
    let _ = fs::remove_file(path);
}

// The binary's path: a sink session publishes the runner's records in place,
// byte for byte and in egress order, then every poll returns `Closed`.
#[test]
fn sink_session_publishes_the_runner_records_then_closed() {
    let (path, expected) = reference_replay("datafeed_sink_session_runner.bin");
    let sink = BytesSink::default();
    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Binary))
        .venue_id(NASDAQ_VENUE)
        .build_with_sink(sink.clone())
        .expect("session builds with no config file");
    loop {
        match session.poll() {
            Ok(()) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("replay failed: {err}"),
        }
    }

    assert_eq!(*sink.0.borrow(), expected);
    for _ in 0..2 {
        assert!(matches!(session.poll(), Err(FeedError::Closed)));
    }
    session.close().expect("close with no capture tap");
    let _ = fs::remove_file(path);
}

/// An MDC1 capture whose last row is on stream 1, which NASDAQ does not
/// declare. The rows before it open the feed, list the security, and trade
/// against a resting order, so the replay ends with an OHLCV bar open.
fn capture_ending_on_an_undeclared_stream(name: &str) -> std::path::PathBuf {
    let meta = md_capture::CaptureMeta {
        venue: 1,
        date: 20260521,
        schema: 1,
        feed: proto_nasdaq::FEED_NASDAQ,
    };
    let open = NASDAQ
        .session
        .resolve_anchor(meta.date, &md_pipeline::BundledTzdb::new())
        .expect("New York anchor resolves");
    let mut writer = md_capture::RawWriter::new(Vec::new(), meta).expect("open capture writer");
    let rows = [
        (0, open + 10, itch::system_event(b'O', 0)),
        (0, open + 11, itch::stock_directory(0)),
        (0, open + 12, itch::add(1001, b'B', 100, 1_500_000, 0)),
        (0, open + 13, itch::executed(1001, 50, 1, 0)),
        (1, open + 20, itch::system_event(b'O', 0)),
    ];
    for (stream, ts_recv, payload) in rows {
        writer
            .write_row(stream, ts_recv, &payload)
            .expect("write capture row");
    }
    let path = std::env::temp_dir().join(name);
    fs::write(&path, writer.finish().expect("finish capture")).expect("write capture");
    path
}

fn is_undeclared_stream_1(end: &Result<impl Sized, FeedError>) -> bool {
    matches!(
        end,
        Err(FeedError::Source(SourceError::UndeclaredStream {
            stream: StreamId(1),
            ..
        }))
    )
}

// A fault that ends the source reaches the host after the records routed
// before it, then `Closed` follows. The faulting step itself routes the
// closing OHLCV flush, so the host first takes the four frames' records, then
// the flushed bars, then the fault.
#[test]
fn poll_returns_a_fault_after_the_records_routed_before_it() {
    let path = capture_ending_on_an_undeclared_stream("datafeed_feed_session_fault.mdc1");
    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Capture))
        .build()
        .expect("session builds");
    let mut out = Vec::new();
    let frames = session.poll(&mut out, usize::MAX).expect("first batch");
    assert!(frames > 0 && out.iter().all(|r| r.header().rtype != rtype::OHLCV));
    out.clear();
    let bars = session.poll(&mut out, usize::MAX).expect("closing flush");
    assert!(bars > 0 && out.iter().all(|r| r.header().rtype == rtype::OHLCV));
    let fault = session.poll(&mut out, usize::MAX);
    assert!(is_undeclared_stream_1(&fault), "got: {fault:?}");
    assert!(matches!(
        session.poll(&mut out, usize::MAX),
        Err(FeedError::Closed)
    ));
    let _ = fs::remove_file(path);
}

// On the binary's path the closing flush is already in the sink when the
// fault returns, once, before `Closed`.
#[test]
fn sink_session_returns_the_fault_once_after_publishing_the_flush() {
    let path = capture_ending_on_an_undeclared_stream("datafeed_sink_session_fault.mdc1");
    let sink = BytesSink::default();
    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Capture))
        .build_with_sink(sink.clone())
        .expect("session builds");
    session.poll().expect("first batch");
    let fault = session.poll();
    assert!(is_undeclared_stream_1(&fault), "got: {fault:?}");
    let rtype_at = core::mem::offset_of!(md_events::RecordHeader, rtype);
    let last_rtype = sink.0.borrow().last().map(|(_, bytes)| bytes[rtype_at]);
    assert_eq!(
        last_rtype,
        Some(rtype::OHLCV),
        "the flush precedes the fault"
    );
    assert!(matches!(session.poll(), Err(FeedError::Closed)));
    let _ = fs::remove_file(path);
}
