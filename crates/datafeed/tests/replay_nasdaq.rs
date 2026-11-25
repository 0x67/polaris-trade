//! Replay e2e: constructs a length-prefixed binary dump from real NASDAQ
//! TotalView-ITCH 5.0 wire bytes in a `Cursor`, runs the same session wiring
//! as live (`session::run`) over the replay `IngestSource`, and
//! asserts normalized output. Proves the live/replay `IngestSource` swap:
//! the session function is identical, only the source construction differs.

mod support;

use std::io::Cursor;

use datafeed::{restart::RestartState, session, sink::CollectingSink, tap::NoopTap};
use md_events::rtype;
use md_pipeline::{EgressSequencer, SystemClock, VenueAdapter, VenueId};
use md_replay::BinaryFileReplay;
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};
use support::itch;

/// Length-prefixed binary dump: `[len:u16 BE][message]` per row, matching
/// `BinaryFileReplay`'s framing.
fn encode_dump(msgs: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for m in msgs {
        out.extend_from_slice(&(m.len() as u16).to_be_bytes());
        out.extend_from_slice(m);
    }
    out
}

#[test]
fn nasdaq_replay_session_produces_normalized_records() {
    let dump = encode_dump(&[
        itch::stock_directory(0),
        itch::system_event(b'O', 0),
        itch::add(1001, b'B', 100, 1_500_000, 0),
    ]);
    let source = BinaryFileReplay::new(Cursor::new(dump), 0, 1);
    let mut adapter = NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    });
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();

    let (stats, _source) = session::run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");

    assert_eq!(
        stats.frames, 3,
        "stock directory + system event + add order frames"
    );
    assert_eq!(adapter.stats().decoded, 3);
    assert_eq!(adapter.stats().unknown_kind, 0, "no unknown-kind frames");
    assert_eq!(adapter.stats().malformed, 0, "no malformed frames");

    let records = &seq.sink().records;
    assert!(
        records.iter().any(|r| r.rtype == rtype::DEFINITION),
        "AddOrderNoMpid mints a Definition for the first-sighted symbol"
    );
    let mbo = records
        .iter()
        .find(|r| r.rtype == rtype::MBO)
        .expect("mbo published for the add order");
    assert!(
        records.iter().any(|r| r.rtype == rtype::CONTROL),
        "system event maps to a Control record"
    );
    assert!(mbo.egress_seq >= 1, "egress sequencer stamped the record");
}

// The same wiring reads a gzip-compressed dump identically: proves the
// container-format swap (`from_gz` vs the plain `Cursor` above) is real, not
// just type-compatible.
#[test]
fn nasdaq_replay_reads_gzip_dump() {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    let dump = encode_dump(&[itch::system_event(b'O', 0)]);
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&dump).expect("gzip write");
    let gz_bytes = enc.finish().expect("gzip finish");

    let source = BinaryFileReplay::new(flate2::read::GzDecoder::new(Cursor::new(gz_bytes)), 0, 1);
    let mut adapter = NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    });
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();

    let (stats, _source) = session::run(
        source,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");

    assert_eq!(stats.frames, 1);
    assert_eq!(adapter.stats().decoded, 1);
}
