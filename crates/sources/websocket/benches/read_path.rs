//! Read-path microbench over a local socket: cost of an empty poll and
//! messages per second through `recv_batch`. Custom harness (no criterion).
//! Full run: `cargo bench -p polaris-source-websocket --bench read_path`.
//! Smoke: `cargo bench -p polaris-source-websocket --bench read_path -- --test`.

use std::{io::Write, net::TcpListener, sync::mpsc, thread, time::Instant};

use md_pipeline::{IngestSource, StreamId};
use source_websocket::{ChannelConfig, WebSocketConfig, WebSocketSource};

const PAYLOAD_LEN: u8 = 64;
const PAYLOAD: usize = PAYLOAD_LEN as usize;

/// Server that upgrades one client, then writes `messages` binary frames in
/// one burst once told to. Holds the connection until the client drops.
fn serve(messages: u32) -> (String, mpsc::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("ws://{}/", listener.local_addr().expect("addr"));
    let (go_tx, go_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let (tcp, _) = listener.accept().expect("accept");
        let mut ws = tungstenite::accept(tcp).expect("upgrade");
        let mut burst = Vec::new();
        for _ in 0..messages {
            burst.extend_from_slice(&[0x82, PAYLOAD_LEN]);
            burst.extend_from_slice(&[b'x'; PAYLOAD]);
        }
        if go_rx.recv().is_ok() {
            ws.get_mut().write_all(&burst).expect("burst");
        }
        while ws.read().is_ok() {}
    });
    (url, go_tx)
}

fn source(url: String) -> WebSocketSource {
    let channel = ChannelConfig {
        url,
        stream: StreamId(0),
        subscribe: Vec::new(),
    };
    WebSocketSource::connect(WebSocketConfig::new(vec![channel])).expect("connect")
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--test");
    let (polls, messages) = if smoke {
        (1_000, 1_000)
    } else {
        (200_000, 500_000)
    };

    let (url, _go) = serve(0);
    let mut src = source(url);
    let mut out = Vec::with_capacity(64);
    let t0 = Instant::now();
    for _ in 0..polls {
        let n = src.recv_batch(&mut out, 64).expect("idle poll");
        assert_eq!(n, 0);
    }
    let ns = t0.elapsed().as_secs_f64() * 1e9 / f64::from(polls);
    println!("empty_poll        {ns:>10.1} ns/poll ({polls} polls)");

    let (url, go) = serve(messages);
    let mut src = source(url);
    go.send(()).expect("go");
    let mut got = 0usize;
    let t0 = Instant::now();
    while got < usize::try_from(messages).expect("fits") {
        out.clear();
        got += src.recv_batch(&mut out, 64).expect("burst poll");
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "read_burst        {:>10.1} ns/msg, {:.2} Mmsg/s ({messages} x {PAYLOAD} B)",
        secs * 1e9 / f64::from(messages),
        f64::from(messages) / secs / 1e6,
    );
}
