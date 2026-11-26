//! A host drives `FeedSession` the way an embedding application would: built
//! in code with no config file, polled from the host's own loop.

mod support;

use std::{fs, io::Cursor, path::Path, sync::atomic::AtomicBool};

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

/// Each published record as its stream and bytes: header, then body.
#[derive(Default)]
struct BytesSink(Vec<(StreamId, Vec<u8>)>);

impl OutputSink for BytesSink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.0
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
fn runner_records(dump: Vec<u8>) -> Vec<(StreamId, Vec<u8>)> {
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
    let mut seq = EgressSequencer::new(BytesSink::default(), clock);
    session::run(
        paced,
        &mut *(config.venue.make)(&config),
        &mut seq,
        &mut NoopTap,
        &mut SessionState::from_config(&config).expect("default resolutions"),
        &mut RestartState::default(),
        session::RunControl {
            batch: 64,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("runner replay");
    seq.into_sink().0
}

// Polling five records at a time must hand the host the runner's records, byte
// for byte and in egress order. The fixture's first batch routes far more than
// five, so any record dropped past `max` instead of carried would break the
// match. Once every record is out, each poll returns `Closed`.
#[test]
fn host_poll_receives_the_runner_records_then_closed() {
    let dump = binary_dump(STREAM_HEX);
    let path = std::env::temp_dir().join("datafeed_feed_session_runner.bin");
    fs::write(&path, &dump).expect("write replay dump");
    let expected = runner_records(dump);

    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Binary))
        .venue_id(NASDAQ_VENUE)
        .build()
        .expect("session builds with no config file");
    let mut out = Vec::new();
    let mut polled = Vec::new();
    loop {
        out.clear();
        match session.poll(&mut out, 5) {
            Ok(n) => {
                assert!(n <= 5 && out.len() == n, "poll appended {n} past max 5");
                polled.extend(out.iter().map(|r| (r.stream(), r.bytes().to_vec())));
            }
            Err(FeedError::Closed) => break,
            Err(err) => panic!("replay failed: {err}"),
        }
    }

    assert!(
        expected.len() > 5,
        "fixture routes more than one poll's worth"
    );
    assert_eq!(polled, expected);
    for _ in 0..2 {
        assert!(matches!(session.poll(&mut out, 5), Err(FeedError::Closed)));
    }
    session.close().expect("close with no capture tap");
    let _ = fs::remove_file(path);
}

// A fault that ends the source reaches the host after the records routed
// before it, then `Closed` follows. A NASDAQ capture whose third row is on an
// undeclared stream ends the replay after two system events.
#[test]
fn poll_returns_a_fault_after_the_records_routed_before_it() {
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
    let event = itch::system_event(b'O', 0);
    for (stream, ts_recv) in [(0, open + 10), (0, open + 15), (1, open + 20)] {
        writer
            .write_row(stream, ts_recv, &event)
            .expect("write capture row");
    }
    let path = std::env::temp_dir().join("datafeed_feed_session_fault.mdc1");
    fs::write(&path, writer.finish().expect("finish capture")).expect("write capture");

    let mut session = FeedSession::builder(&NASDAQ, replay_source(&path, ReplayFormat::Capture))
        .build()
        .expect("session builds");
    let mut out = Vec::new();
    assert_eq!(session.poll(&mut out, usize::MAX).expect("first batch"), 2);
    assert!(out.iter().all(|r| r.header().rtype == rtype::CONTROL));
    let fault = session.poll(&mut out, usize::MAX);
    assert!(
        matches!(
            fault,
            Err(FeedError::Source(SourceError::UndeclaredStream {
                stream: StreamId(1),
                ..
            }))
        ),
        "got: {fault:?}"
    );
    assert!(matches!(
        session.poll(&mut out, usize::MAX),
        Err(FeedError::Closed)
    ));
    let _ = fs::remove_file(path);
}
