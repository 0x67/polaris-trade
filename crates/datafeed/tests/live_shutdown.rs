//! A live run told to stop mid-run finishes its capture tap. The run goes
//! through `app::run_config`, which `main` calls with the flag its signal
//! handlers set. The binary cannot be driven live itself: its only venue,
//! NASDAQ, has no live source, so the run uses a test-only descriptor that
//! dials SoupBinTCP, against an in-process server on `127.0.0.1:0` that keeps
//! the connection open.

mod support;

use std::{
    io::Read,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use datafeed::{
    app::run_config,
    config::DatafeedConfig,
    venue::{NASDAQ, VenueDescriptor},
};
use serde_json::json;
use support::{
    capture_tree::{
        assert_manifests_verify, captured_rows, has_capture_file, output_tree, wait_until,
    },
    itch,
    soupbin::{build_packet, login_accepted_packet},
};

static NASDAQ_SOUPBIN: VenueDescriptor = VenueDescriptor {
    name: "test-nasdaq-soupbin",
    soupbin_live: true,
    ..NASDAQ
};

// The stop flag is what the binary's SIGINT and SIGTERM handlers set.
// @lat: [[tests#Live shutdown]]
#[test]
fn stop_mid_run_finishes_the_tap_and_writes_the_manifest() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
    let endpoint = listener.local_addr().expect("local_addr");
    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().expect("accept");
        let mut buf = [0u8; 256];
        let _ = sock.read(&mut buf).expect("read login request");
        let mut reply = login_accepted_packet("sess001", 1);
        reply.extend(build_packet(b'S', &itch::system_event(b'O', 0)));
        reply.extend(build_packet(b'S', &itch::add(42, b'B', 7, 1_500_000, 0)));
        std::io::Write::write_all(&mut sock, &reply).expect("write frames");
        // Hold the session open, with no end-of-session packet, until the client drops it.
        while sock.read(&mut buf).is_ok_and(|n| n > 0) {}
    });

    let out = std::env::temp_dir().join(format!("datafeed_live_shutdown_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    let mut config: DatafeedConfig = serde_json::from_value(json!({
        "venue": "nasdaq",
        "venue_id": 2,
        "session_midnight_ns": 0,
        "source": {"kind": "live", "endpoint": endpoint},
        "taps": {"raw_dir": out},
    }))
    .expect("live config parses");
    config.venue = &NASDAQ_SOUPBIN;

    let stop = Arc::new(AtomicBool::new(false));
    let run = thread::spawn({
        let stop = Arc::clone(&stop);
        move || run_config(config, stop)
    });
    wait_until("the first capture file", || has_capture_file(&out));
    stop.store(true, Ordering::Relaxed);
    let result = run.join().expect("run_config thread");
    server.join().expect("mock server thread");
    result.expect("run_config returns Ok after the stop");

    assert_eq!(assert_manifests_verify(&out), 1, "one listed capture file");
    let leaves: Vec<_> = output_tree(&out)
        .into_iter()
        .filter(|(path, _)| path.ends_with(".mdc1.zst"))
        .collect();
    assert_eq!(
        captured_rows(&out.join(&leaves[0].0)),
        2,
        "both frames captured"
    );
    let _ = std::fs::remove_dir_all(&out);
}
