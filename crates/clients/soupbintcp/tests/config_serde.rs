//! Config file format: durations write and read as human-readable strings
//! (`"30s"`), why config uses `humantime_serde` over derived `Duration`.

use std::time::Duration;

use client_soupbintcp::SoupBinClientConfig;

#[test]
fn toml_durations_are_human_readable() {
    let cfg = SoupBinClientConfig {
        login_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let text = toml::to_string(&cfg).expect("serialize");
    assert!(text.contains("login_timeout = \"30s\""), "{text}");

    let back: SoupBinClientConfig =
        toml::from_str("heartbeat_timeout = \"250ms\"").expect("deserialize");
    assert_eq!(back.heartbeat_timeout, Duration::from_millis(250));
}
