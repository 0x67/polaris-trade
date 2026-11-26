//! A live session told to stop mid-run finishes its capture tap. The deployed
//! binary cannot show this itself: its only venue, NASDAQ, has no live source,
//! so the run uses a test-only descriptor that dials SoupBinTCP, against an
//! in-process server on `127.0.0.1:0` that keeps the connection open.

mod support;

use std::{
    io::Read,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use datafeed::{
    FeedError, FeedSession,
    config::{SourceConfig, TapConfig},
    venue::{NASDAQ, VenueDescriptor},
};
use md_events::rtype;
use support::{
    capture_tree::{assert_manifests_verify, captured_rows, output_tree},
    itch,
    soupbin::{build_packet, login_accepted_packet},
};

static NASDAQ_SOUPBIN: VenueDescriptor = VenueDescriptor {
    name: "test-nasdaq-soupbin",
    soupbin_live: true,
    ..NASDAQ
};

const DEADLINE: Duration = Duration::from_secs(30);

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
    let stop = Arc::new(AtomicBool::new(false));
    let source = SourceConfig::Live {
        endpoint,
        soupbin: Default::default(),
    };
    let mut session = FeedSession::builder(&NASDAQ_SOUPBIN, source)
        .venue_id(2)
        .taps(TapConfig {
            raw_dir: Some(out.clone()),
            ..TapConfig::default()
        })
        .stop(Arc::clone(&stop))
        .build()
        .expect("live session builds");

    let start = Instant::now();
    let mut records = Vec::new();
    while records.len() < 2 {
        assert!(start.elapsed() < DEADLINE, "frames never arrived");
        session.poll(&mut records, usize::MAX).expect("poll");
    }
    stop.store(true, Ordering::Relaxed);
    loop {
        assert!(
            start.elapsed() < DEADLINE,
            "the session ignored the stop flag"
        );
        match session.poll(&mut records, usize::MAX) {
            Ok(_) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("poll after stop: {err}"),
        }
    }
    session.close().expect("close finishes the tap");
    server.join().expect("mock server thread");

    assert!(
        records.iter().any(|r| r.header().rtype == rtype::MBO),
        "the add reached the book"
    );
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
