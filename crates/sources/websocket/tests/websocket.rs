//! `WebSocketSource` against a local scripted server, plain and over TLS.

mod support;

use std::{
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use md_pipeline::{
    IngestError, IngestFrame, IngestSource, StreamId,
    ingest::testing::{IngestCase, ScenarioSpec, run_ingest_cases},
};
use source_websocket::{WebSocketConfig, WebSocketFrame, WebSocketSource};
use support::{Server, text_frame};

const DEADLINE: Duration = Duration::from_secs(5);

fn payloads(frames: &[WebSocketFrame]) -> Vec<String> {
    frames
        .iter()
        .map(|f| String::from_utf8(f.payload().to_vec()).expect("utf8 payload"))
        .collect()
}

/// Poll until `want` frames arrived.
fn collect(src: &mut WebSocketSource, want: usize) -> Vec<WebSocketFrame> {
    let mut out = Vec::new();
    let start = Instant::now();
    while out.len() < want {
        src.recv_batch(&mut out, 8).expect("source open");
        assert!(
            start.elapsed() < DEADLINE,
            "got {} of {want} frames",
            out.len()
        );
        thread::sleep(Duration::from_millis(1));
    }
    out
}

/// Poll until the source reports `Closed`; returns the frames read before it.
fn until_closed(src: &mut WebSocketSource) -> Vec<WebSocketFrame> {
    let mut out = Vec::new();
    let start = Instant::now();
    loop {
        match src.recv_batch(&mut out, 8) {
            Ok(_) => {}
            Err(IngestError::Closed) => return out,
            Err(other) => panic!("unexpected {other:?}"),
        }
        assert!(start.elapsed() < DEADLINE, "source never closed");
        thread::sleep(Duration::from_millis(1));
    }
}

// Every preloaded frame must be readable before the source is returned:
// `DrainsInOrder` stops at the first `Ok(0)`. The settle covers loopback
// delivery after the server's last write returned.
fn conformance(tls: bool) {
    let report = run_ingest_cases(
        &[
            IngestCase::EmptyYieldsZero,
            IngestCase::RespectsMax,
            IngestCase::DrainsInOrder,
        ],
        |spec: ScenarioSpec| {
            let (ready_tx, ready_rx) = mpsc::channel();
            let frames = spec.frames;
            let server = Server::start(tls, 1, move |_, conn| {
                for frame in &frames {
                    conn.send_binary(frame.payload());
                }
                ready_tx.send(()).expect("ready");
                conn.hold();
            });
            let src = WebSocketSource::connect(server.config(&[])).expect("connect");
            ready_rx.recv().expect("frames written");
            thread::sleep(Duration::from_millis(50));
            src
        },
    );
    assert!(report.all_passed(), "failures: {:?}", report.results);
}

#[test]
fn conformance_plain() {
    conformance(false);
}

#[test]
fn conformance_tls() {
    conformance(true);
}

fn peer_close(tls: bool) {
    let server = Server::start(tls, 1, |_, conn| {
        conn.send_text("hello");
        conn.close_and_wait()
    });
    let mut src = WebSocketSource::connect(server.config(&[])).expect("connect");
    assert_eq!(payloads(&until_closed(&mut src)), ["hello"]);
    assert_eq!(src.recv_batch(&mut Vec::new(), 8), Err(IngestError::Closed));
    assert_eq!(server.join(), [true], "client must answer the close frame");
}

#[test]
fn peer_close_frame_ends_source_plain() {
    peer_close(false);
}

#[test]
fn peer_close_frame_ends_source_tls() {
    peer_close(true);
}

fn tcp_drop(tls: bool) {
    let server = Server::start(tls, 1, |_, conn| conn.send_text("last"));
    let mut src = WebSocketSource::connect(server.config(&[])).expect("connect");
    assert_eq!(payloads(&until_closed(&mut src)), ["last"]);
    server.join();
}

#[test]
fn tcp_drop_without_close_frame_ends_source_plain() {
    tcp_drop(false);
}

#[test]
fn tcp_drop_without_close_frame_ends_source_tls() {
    tcp_drop(true);
}

fn half_frame(tls: bool) {
    let frame = text_frame(&[b'a'; 40]);
    let (half_tx, half_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let go_rx = Mutex::new(go_rx);
    let server = Server::start(tls, 1, move |_, conn| {
        conn.write_raw(&frame[..20]);
        half_tx.send(()).expect("half");
        go_rx.lock().expect("lock").recv().expect("go");
        conn.write_raw(&frame[20..]);
        conn.hold();
    });
    let mut src = WebSocketSource::connect(server.config(&[])).expect("connect");
    half_rx.recv().expect("first half written");
    thread::sleep(Duration::from_millis(50));
    let mut out = Vec::new();
    assert_eq!(
        src.recv_batch(&mut out, 8),
        Ok(0),
        "half a frame is not ready"
    );
    go_tx.send(()).expect("go");
    let frames = collect(&mut src, 1);
    assert_eq!(frames[0].payload(), [b'a'; 40]);
}

#[test]
fn half_received_frame_reads_as_nothing_ready_plain() {
    half_frame(false);
}

#[test]
fn half_received_frame_reads_as_nothing_ready_tls() {
    half_frame(true);
}

fn reconnect_replays_subscribe(tls: bool) {
    let server = Server::start(tls, 2, |index, conn| {
        let subscribed = vec![conn.read_text(), conn.read_text()];
        conn.send_text(if index == 0 { "first" } else { "second" });
        if index == 1 {
            conn.hold();
        }
        subscribed
    });
    let mut config = server.config(&["sub-a", "sub-b"]);
    config.reconnect_attempts = 3;
    config.backoff_initial = Duration::from_millis(1);
    let mut src = WebSocketSource::connect(config).expect("connect");
    assert_eq!(payloads(&collect(&mut src, 2)), ["first", "second"]);
    drop(src);
    let subscribed = server.join();
    assert_eq!(subscribed, [["sub-a", "sub-b"], ["sub-a", "sub-b"]]);
}

#[test]
fn reconnect_replays_subscribe_messages_plain() {
    reconnect_replays_subscribe(false);
}

#[test]
fn reconnect_replays_subscribe_messages_tls() {
    reconnect_replays_subscribe(true);
}

fn message_size_cap(tls: bool) {
    let server = Server::start(tls, 1, |_, conn| {
        conn.send_text("ok");
        conn.send_text(&"x".repeat(64));
        conn.hold();
    });
    let mut config = server.config(&[]);
    config.max_message_size = 16;
    let mut src = WebSocketSource::connect(config).expect("connect");
    assert_eq!(payloads(&until_closed(&mut src)), ["ok"]);
    drop(src);
    server.join();
}

#[test]
fn message_over_size_cap_drops_connection_plain() {
    message_size_cap(false);
}

#[test]
fn message_over_size_cap_drops_connection_tls() {
    message_size_cap(true);
}

#[test]
fn channels_tag_their_own_stream() {
    let server = Server::start(false, 2, |index, conn| {
        conn.send_text(&format!("c{index}"));
        conn.hold();
    });
    let channels = vec![
        server.channel(StreamId(7), &[]),
        server.channel(StreamId(9), &[]),
    ];
    let mut src = WebSocketSource::connect(WebSocketConfig::new(channels)).expect("connect");
    let mut frames = collect(&mut src, 2);
    frames.sort_by_key(WebSocketFrame::stream);
    let tagged: Vec<_> = frames
        .iter()
        .map(|f| (f.stream(), f.payload().to_vec()))
        .collect();
    assert_eq!(
        tagged,
        [(StreamId(7), b"c0".to_vec()), (StreamId(9), b"c1".to_vec())]
    );
}

#[test]
fn backoff_waits_without_blocking_the_call() {
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let server = Server::start(false, 1, move |_, _| {
        dropped_tx.send(Instant::now()).expect("dropped");
    });
    let mut config = server.config(&[]);
    config.reconnect_attempts = 2;
    config.backoff_initial = Duration::from_millis(150);
    // the reconnects meet a closed port. Windows retries a refused loopback
    // SYN for about two seconds, so the connect budget, not the refusal, ends
    // each attempt there, and both stay under the bound below.
    config.connect_timeout = Duration::from_millis(60);
    let mut src = WebSocketSource::connect(config).expect("connect");

    let dropped_at = dropped_rx.recv().expect("server dropped the connection");
    let mut slowest = Duration::ZERO;
    let mut out = Vec::new();
    let closed_at = loop {
        let call = Instant::now();
        let result = src.recv_batch(&mut out, 8);
        slowest = slowest.max(call.elapsed());
        if result == Err(IngestError::Closed) {
            break Instant::now();
        }
        assert_eq!(result, Ok(0));
        assert!(
            call.duration_since(dropped_at) < DEADLINE,
            "source never closed"
        );
        thread::sleep(Duration::from_millis(2));
    };
    assert!(
        closed_at.duration_since(dropped_at) >= Duration::from_millis(150),
        "second attempt must wait out the backoff"
    );
    assert!(
        slowest < Duration::from_millis(100),
        "a call blocked for {slowest:?}"
    );
}

#[test]
fn flapping_connection_is_rate_limited_and_gives_up() {
    // A venue that accepts the upgrade and closes at once: the reconnect
    // waits out one backoff, and the second dud connection spends the single
    // attempt. Retrying at once, or resetting the count on each reconnect,
    // would end the test far from the 500 ms mark.
    let backoff = Duration::from_millis(500);
    let server = Server::start(false, 2, |_, conn| conn.close_and_wait());
    let mut config = server.config(&[]);
    config.reconnect_attempts = 1;
    config.backoff_initial = backoff;
    config.backoff_max = backoff;
    let start = Instant::now();
    let mut src = WebSocketSource::connect(config).expect("connect");
    assert_eq!(until_closed(&mut src), Vec::<WebSocketFrame>::new());
    let elapsed = start.elapsed();
    assert!(elapsed >= backoff, "reconnected after only {elapsed:?}");
    assert!(
        elapsed < backoff * 2,
        "second backoff was waited out: {elapsed:?}"
    );
    assert_eq!(server.join(), [true, true]);
}
