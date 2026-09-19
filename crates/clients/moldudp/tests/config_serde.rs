//! Config file format: durations are human-readable strings (`"7ms"`), and a
//! file naming one field still loads.

use std::time::Duration;

use client_moldudp::MoldUdpReceiverConfig;

#[test]
fn toml_gap_window_reads_humantime_string() {
    let cfg: MoldUdpReceiverConfig =
        toml::from_str("gap_confirm_window = \"7ms\"").expect("config decodes");
    assert_eq!(cfg.gap_confirm_window, Duration::from_millis(7));
}
