//! A host that embeds the library keeps its process: building, polling and
//! closing a session, or running a config through `app::run_config`, installs
//! no signal handler and no `tracing` subscriber.
#![cfg(unix)]

mod support;

use std::{
    fs, io, mem, ptr,
    sync::{Arc, atomic::AtomicBool},
};

use datafeed::{
    FeedError, FeedSession,
    app::run_config,
    config::{DatafeedConfig, ReplayFormat, SourceConfig},
    venue::NASDAQ,
};
use support::reference_parity::binary_dump;

const STREAM_HEX: &str = include_str!("fixtures/orderbook-reference/stream.hex");

fn disposition(signum: libc::c_int) -> usize {
    // SAFETY: a null `act` only reads the current action into `old`, which is
    // a zeroed `sigaction` the call fills.
    let mut old: libc::sigaction = unsafe { mem::zeroed() };
    let rc = unsafe { libc::sigaction(signum, ptr::null(), &mut old) };
    assert_eq!(rc, 0, "sigaction read: {}", io::Error::last_os_error());
    old.sa_sigaction
}

fn assert_process_untouched(before: [usize; 2]) {
    assert_eq!(SIGNALS.map(disposition), before, "SIGINT and SIGTERM moved");
    assert!(
        !tracing::dispatcher::has_been_set(),
        "the library installed a global subscriber"
    );
}

const SIGNALS: [libc::c_int; 2] = [libc::SIGINT, libc::SIGTERM];

#[test]
fn session_leaves_signal_dispositions_and_subscriber_alone() {
    let path = std::env::temp_dir().join("datafeed_host_process.bin");
    fs::write(&path, binary_dump(STREAM_HEX)).expect("write replay dump");
    let before = SIGNALS.map(disposition);

    let source = SourceConfig::Replay {
        path: path.clone(),
        format: ReplayFormat::Binary,
        base_ts: 0,
        step_ns: 1,
        load: Default::default(),
        pacing: Default::default(),
    };
    let mut session = FeedSession::builder(&NASDAQ, source)
        .venue_id(2)
        .build()
        .expect("session builds");
    let mut out = Vec::new();
    loop {
        match session.poll(&mut out, usize::MAX) {
            Ok(_) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("replay failed: {err}"),
        }
    }
    session.close().expect("close with no capture tap");
    let _ = fs::remove_file(path);

    assert!(!out.is_empty(), "the session routed records");
    assert_process_untouched(before);
}

#[test]
fn run_config_leaves_signal_dispositions_and_subscriber_alone() {
    let path = std::env::temp_dir().join("datafeed_host_process_run_config.bin");
    fs::write(&path, binary_dump(STREAM_HEX)).expect("write replay dump");
    let before = SIGNALS.map(disposition);

    let config: DatafeedConfig = serde_json::from_value(serde_json::json!({
        "venue": "nasdaq",
        "venue_id": 2,
        "session_midnight_ns": 0,
        "source": {"kind": "replay", "path": path},
    }))
    .expect("replay config");
    let outcome = run_config(config, Arc::new(AtomicBool::new(false)));
    let _ = fs::remove_file(path);

    outcome.expect("replay config runs to its end");
    assert_process_untouched(before);
}
