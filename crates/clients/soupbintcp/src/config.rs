//! `SoupBinClientConfig`: login, heartbeat cadence and frame limits.
//! Durations use `humantime_serde`, so config files write `"30s"`, not nanos.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Config for one `SoupBinClient` session. Every field defaults, so config
/// file may omit any subset.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SoupBinClientConfig {
    /// Login user name, up to 6 ASCII bytes.
    pub username: String,
    /// Login password, up to 10 ASCII bytes.
    pub password: String,
    /// Session to join; empty joins current session.
    pub requested_session: String,
    /// Sequence to request first at login. `1` replays session from start
    /// (default); `0` starts at newest message (live tail); on reconnect set
    /// it to `next_expected_sequence()` to resume where dropped socket left off.
    pub requested_sequence_number: u64,

    /// Bound on login handshake. Default 30 s.
    #[serde(with = "humantime_serde")]
    pub login_timeout: Duration,
    /// Client heartbeat sent after this much send silence. Default 1 s.
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Session times out after this much server silence. Default 15 s.
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: Duration,

    /// Max total packet size (2-byte length prefix plus body) before `FrameTooLarge`.
    pub max_frame_size: usize,
    /// Bytes reserved per receive (and, under `compressed`, inflate cap).
    pub decode_buf_capacity: usize,
}

impl Default for SoupBinClientConfig {
    fn default() -> Self {
        Self {
            username: String::new(),
            password: String::new(),
            requested_session: String::new(),
            requested_sequence_number: 1,
            login_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(1),
            heartbeat_timeout: Duration::from_secs(15),
            max_frame_size: 64 * 1024,
            decode_buf_capacity: 64 * 1024,
        }
    }
}
