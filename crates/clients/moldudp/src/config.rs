//! Receiver config: serde-first so deployments load JSON or TOML. Every field
//! defaults, so minimal file suffices. Socket options live on caller-built legs.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// [`MoldUdpReceiver`](crate::MoldUdpReceiver) session, sequence and gap-timing knobs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MoldUdpReceiverConfig {
    /// Re-request rate cap per gap start sequence. Default 4.
    pub max_rerequests_per_gap_per_sec: u32,
    /// Multi-leg only: how long sequence must stay unseen on every leg before
    /// it counts as gap. Default 5 ms.
    #[serde(with = "humantime_serde")]
    pub gap_confirm_window: Duration,
    /// Sequence receiver expects first. `None` adopts first packet's sequence
    /// (cold start at session begin). Set explicitly to resume mid-stream, so
    /// unseen backlog below it is not treated as one giant gap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
}

impl Default for MoldUdpReceiverConfig {
    fn default() -> Self {
        Self {
            max_rerequests_per_gap_per_sec: 4,
            gap_confirm_window: Duration::from_millis(5),
            start_sequence: None,
        }
    }
}
