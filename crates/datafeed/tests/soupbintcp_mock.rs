//! Localhost SoupBinTCP mock e2e. Binds `127.0.0.1:0` and speaks minimal
//! SoupBinTCP framing in-process; explicitly allowed by the workspace's Unit
//! Test Rules ("Localhost mock servers acceptable"), not a banned
//! external-service test. Proves the live `IngestSource` wiring end to end:
//! connect, login, adapt NASDAQ ITCH 5.0 wire bytes, book, sequence, publish.

mod support;

use std::time::Duration;

use datafeed::{restart::RestartState, session, sink::CollectingSink, source, tap::NoopTap};
use md_events::rtype;
use md_pipeline::{
    EgressSequencer, IngestFrame, IngestSource, StreamId, SystemClock, VenueAdapter, VenueId,
};
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};
use support::{
    itch,
    soupbin::{build_packet, login_accepted_packet},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn read_packet(sock: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut len = [0u8; 2];
    sock.read_exact(&mut len).await.expect("read packet length");
    let len = u16::from_be_bytes(len) as usize;
    let mut body = vec![0u8; len];
    sock.read_exact(&mut body).await.expect("read packet body");
    (body[0], body[1..].to_vec())
}

fn requested_sequence(login: &[u8]) -> u64 {
    std::str::from_utf8(&login[26..46])
        .expect("login sequence ascii")
        .trim()
        .parse()
        .expect("login sequence number")
}

/// Start-of-messages system event at wire time zero. Maps to a `Control`
/// record.
fn system_event_msg() -> Vec<u8> {
    itch::system_event(b'O', 0)
}

/// A 7-share bid at wire time zero.
fn add_order_msg(order_ref: u64) -> Vec<u8> {
    itch::add(order_ref, b'B', 7, 1_500_000, 0)
}

fn delete_order_msg(order_ref: u64) -> Vec<u8> {
    itch::delete(order_ref, 0)
}

fn nasdaq_adapter() -> NasdaqAdapter {
    NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    })
}

fn test_soupbin_config() -> client_soupbintcp::SoupBinClientConfig {
    client_soupbintcp::SoupBinClientConfig {
        username: "user01".into(),
        password: "pass12345".into(),
        requested_session: "sess001".into(),
        requested_sequence_number: 1,
        login_timeout: Duration::from_millis(500),
        heartbeat_interval: Duration::from_secs(30),
        heartbeat_timeout: Duration::from_secs(60),
        max_frame_size: 4096,
        decode_buf_capacity: 4096,
    }
}

// multi_thread: `session::run`'s sync busy-poll loop blocks its worker thread
// (no `.await` inside), so the mock server task needs a second worker thread
// to make progress concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nasdaq_live_session_produces_normalized_records() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        // drain the login request; this mock does not validate credentials
        let mut buf = [0u8; 256];
        let _ = sock.read(&mut buf).await.expect("read login request");
        sock.write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("write login accepted");

        sock.write_all(&build_packet(b'S', &system_event_msg()))
            .await
            .expect("write system event frame");
        sock.write_all(&build_packet(b'S', &add_order_msg(42)))
            .await
            .expect("write add order frame");
        sock.write_all(&build_packet(b'Z', &[]))
            .await
            .expect("write end of session");
    });

    let src = source::connect_tcp(addr, test_soupbin_config(), StreamId(0))
        .await
        .expect("connect_tcp");
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();
    let mut restart = RestartState::default();

    let (stats, _src) = session::run(
        src,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    server.await.expect("mock server task");

    assert_eq!(stats.frames, 2, "system event frame + add order frame");
    assert_eq!(adapter.stats().decoded, 2, "both frames decode");
    assert_eq!(stats.records, 2, "one Control and one Mbo routed");

    let mbo = seq
        .sink()
        .records
        .iter()
        .find(|r| r.rtype == rtype::MBO)
        .expect("mbo record published with correct rtype");
    assert_eq!(mbo.stream, StreamId(0));
    assert_eq!(
        mbo.venue_seq, 2,
        "the real SoupBin session sequence (system event took slot 1, add order took slot 2), \
         not an adapter-internal counter"
    );
    assert!(
        mbo.egress_seq >= 1,
        "egress sequencer stamped a 1-based sequence"
    );
}

// venue_seq flows from the real SoupBin session sequence, never an
// adapter counter (resumed at 50, not 1, so a coincidence cannot hide a bug).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn venue_seq_threaded() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 256];
        let _ = sock.read(&mut buf).await.expect("read login request");
        sock.write_all(&login_accepted_packet("sess001", 50))
            .await
            .expect("write login accepted");

        sock.write_all(&build_packet(b'S', &add_order_msg(42)))
            .await
            .expect("write add order frame");
        sock.write_all(&build_packet(b'Z', &[]))
            .await
            .expect("write end of session");
    });

    let src = source::connect_tcp(addr, test_soupbin_config(), StreamId(0))
        .await
        .expect("connect_tcp");
    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();
    let mut restart = RestartState::default();

    let (_stats, _src) = session::run(
        src,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run nasdaq");
    server.await.expect("mock server task");

    let mbo = seq
        .sink()
        .records
        .iter()
        .find(|r| r.rtype == rtype::MBO)
        .expect("mbo record published");
    assert_eq!(
        mbo.venue_seq, 50,
        "venue_seq is the session's real resume sequence, not an adapter counter restarting at 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_resumes() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");

    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("accept first");
        let (kind, login) = read_packet(&mut first).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 1);
        first
            .write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("accept first login");
        first
            .write_all(&build_packet(b'S', &system_event_msg()))
            .await
            .expect("write system event");
        first
            .write_all(&build_packet(b'S', &add_order_msg(42)))
            .await
            .expect("write add");
        drop(first);

        let (mut second, _) = listener.accept().await.expect("accept second");
        let (kind, login) = read_packet(&mut second).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 3);
        second
            .write_all(&login_accepted_packet("sess001", 3))
            .await
            .expect("accept resumed login");
        second
            .write_all(&build_packet(b'S', &delete_order_msg(42)))
            .await
            .expect("write delete");
        second
            .write_all(&build_packet(b'Z', &[]))
            .await
            .expect("write end of session");
    });

    let mut adapter = nasdaq_adapter();
    let mut seq = EgressSequencer::new(CollectingSink::default(), SystemClock);
    let mut tap = NoopTap;
    let mut state = session::SessionState::default();
    let mut restart = RestartState::default();

    let (first, replay) = source::connect_tcp_resuming(
        addr,
        &test_soupbin_config(),
        StreamId(0),
        &mut restart,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .expect("connect first")
    .expect("connected, not aborted");
    assert!(!replay);
    let (_stats, first) = session::run(
        first,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run first session");
    restart.capture_soupbin_resume(first.next_expected_sequence());

    let (second, replay) = source::connect_tcp_resuming(
        addr,
        &test_soupbin_config(),
        StreamId(0),
        &mut restart,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .expect("connect second")
    .expect("connected, not aborted");
    assert!(!replay);
    let (stats, _second) = session::run(
        second,
        &mut adapter,
        &mut seq,
        &mut tap,
        &mut state,
        &mut restart,
        session::RunControl {
            batch: 8,
            stop: &std::sync::atomic::AtomicBool::new(false),
        },
    )
    .expect("run resumed session");
    server.await.expect("mock server task");

    assert!(
        stats.unknown_order_counts.is_empty(),
        "book state survived reconnect: {:?}",
        stats.unknown_order_counts
    );
    let records = &seq.sink().records;
    assert!(!records.is_empty());
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.egress_seq, index as u64 + 1);
    }
    assert!(
        records
            .iter()
            .any(|record| { record.rtype == rtype::MBO && record.venue_seq == 3 })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_resume_falls_back_to_sequence_one() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");

    let server = tokio::spawn(async move {
        let (mut resumed, _) = listener.accept().await.expect("accept resumed login");
        let (kind, login) = read_packet(&mut resumed).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 99);
        resumed
            .write_all(&build_packet(b'J', b"A"))
            .await
            .expect("reject unavailable resume");
        drop(resumed);

        let (mut replayed, _) = listener.accept().await.expect("accept replay login");
        let (kind, login) = read_packet(&mut replayed).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 1);
        replayed
            .write_all(&login_accepted_packet("sess002", 1))
            .await
            .expect("accept replay login");
    });

    let mut restart = RestartState::default();
    restart.capture_soupbin_resume(99);
    restart.observe_control(md_events::ControlKind::RecoveryComplete, 88);
    let (source, replay) = source::connect_tcp_resuming(
        addr,
        &test_soupbin_config(),
        StreamId(0),
        &mut restart,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .expect("fallback connection")
    .expect("connected, not aborted");

    assert!(replay);
    assert_eq!(source.next_expected_sequence(), 1);
    assert_eq!(restart, RestartState::default());
    server.await.expect("mock server task");
}

// Mixed dual resume: primary accepted at its captured sequence while the venue
// forces secondary back to 1. Keeping that source would lose the primary prefix while
// the engines reset, so both legs must redial requesting sequence 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_dual_resume_reconnects_both_legs_at_sequence_one() {
    let primary_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind primary");
    let secondary_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind secondary");
    let primary_addr = primary_listener.local_addr().expect("primary addr");
    let secondary_addr = secondary_listener.local_addr().expect("secondary addr");

    let primary_server = tokio::spawn(async move {
        let (mut first, _) = primary_listener
            .accept()
            .await
            .expect("primary first accept");
        let (kind, login) = read_packet(&mut first).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 500);
        first
            .write_all(&login_accepted_packet("sess001", 500))
            .await
            .expect("accept primary resume");

        let (mut second, _) = primary_listener
            .accept()
            .await
            .expect("primary second accept");
        let (kind, login) = read_packet(&mut second).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 1);
        second
            .write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("accept primary replay");
    });

    let secondary_server = tokio::spawn(async move {
        let (mut first, _) = secondary_listener
            .accept()
            .await
            .expect("secondary first accept");
        let (kind, login) = read_packet(&mut first).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 700);
        // venue restarted this leg: only sequence 1 on offer
        first
            .write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("accept secondary at sequence 1");

        let (mut second, _) = secondary_listener
            .accept()
            .await
            .expect("secondary second accept");
        let (kind, login) = read_packet(&mut second).await;
        assert_eq!(kind, b'L');
        assert_eq!(requested_sequence(&login), 1);
        second
            .write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("accept secondary replay");
    });

    let mut restart = RestartState::default();
    restart.capture_dual_resume(500, 700);
    let (source, replay) = source::connect_dual_resuming(
        primary_addr,
        secondary_addr,
        [StreamId(0), StreamId(1)],
        &test_soupbin_config(),
        &mut restart,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .expect("dual resume")
    .expect("connected, not aborted");

    assert!(replay, "mixed acceptance must report a full replay");
    assert_eq!(source.next_expected_sequences(), (1, 1));
    assert_eq!(restart, RestartState::default());
    primary_server.await.expect("primary mock server task");
    secondary_server.await.expect("secondary mock server task");
}

// Each dual leg tags frames with the id the caller passes (the venue
// descriptor's declared ids); ids other than 0/1 catch a hardcoded tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dual_legs_tag_frames_with_caller_stream_ids() {
    let primary_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind primary");
    let secondary_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind secondary");
    let primary_addr = primary_listener.local_addr().expect("primary addr");
    let secondary_addr = secondary_listener.local_addr().expect("secondary addr");

    let serve = |listener: TcpListener, payload: &'static [u8]| async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let (kind, _login) = read_packet(&mut sock).await;
        assert_eq!(kind, b'L');
        sock.write_all(&login_accepted_packet("sess001", 1))
            .await
            .expect("accept login");
        sock.write_all(&build_packet(b'S', payload))
            .await
            .expect("write data frame");
        // hold the session open until the client drops it, so one leg closing
        // cannot end the merged source before the other leg's frame drains
        let mut buf = [0u8; 64];
        while sock.read(&mut buf).await.is_ok_and(|n| n > 0) {}
    };
    let primary_server = tokio::spawn(serve(primary_listener, b"primary-leg"));
    let secondary_server = tokio::spawn(serve(secondary_listener, b"secondary-leg"));

    let mut restart = RestartState::default();
    let (mut source, _replay) = source::connect_dual_resuming(
        primary_addr,
        secondary_addr,
        [StreamId(5), StreamId(9)],
        &test_soupbin_config(),
        &mut restart,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .expect("dual connect")
    .expect("connected, not aborted");

    let mut frames = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while frames.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "each leg delivers its frame"
        );
        source
            .recv_batch(&mut frames, 8)
            .expect("both legs stay open");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(source);

    let stream_of = |payload: &[u8]| {
        frames
            .iter()
            .find(|frame| frame.payload() == payload)
            .map(|frame| frame.stream())
    };
    assert_eq!(stream_of(b"primary-leg"), Some(StreamId(5)));
    assert_eq!(stream_of(b"secondary-leg"), Some(StreamId(9)));
    primary_server.await.expect("primary mock server task");
    secondary_server.await.expect("secondary mock server task");
}
