//! Source and per-channel settings.

use std::{sync::Arc, time::Duration};

use md_pipeline::StreamId;

/// One WebSocket connection: where it connects, which [`StreamId`] its frames
/// carry, and the messages sent after every (re)connect.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// `ws://` or `wss://` URL.
    pub url: String,
    /// Stream every frame from this channel is tagged with.
    pub stream: StreamId,
    /// Text messages sent in order after the upgrade, replayed on reconnect.
    pub subscribe: Vec<String>,
}

/// Settings for a [`WebSocketSource`](crate::WebSocketSource).
#[derive(Clone, Debug)]
pub struct WebSocketConfig {
    /// Connections to hold, polled in turn.
    pub channels: Vec<ChannelConfig>,
    /// Client TLS config for `wss://` channels. `None` uses the platform root store.
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// Largest accepted message in bytes; a bigger one drops the connection.
    pub max_message_size: usize,
    /// Largest accepted frame in bytes; a bigger one drops the connection.
    pub max_frame_size: usize,
    /// Bound on one whole connect: socket connect, TLS and upgrade handshakes
    /// and subscribe. Name resolution is not bounded.
    pub connect_timeout: Duration,
    /// Failed attempts in a row before the source reports `Closed`. A
    /// reconnect that closes before delivering a message counts as failed.
    /// Zero closes the source on the first lost connection.
    pub reconnect_attempts: u32,
    /// Wait after a loss before the next attempt; doubles per failed attempt
    /// up to `backoff_max`.
    pub backoff_initial: Duration,
    /// Upper bound of the wait between attempts.
    pub backoff_max: Duration,
}

impl WebSocketConfig {
    /// Defaults: platform roots, 64 MiB messages, 16 MiB frames, 5 s connect
    /// timeout, 5 attempts with 100 ms to 5 s backoff.
    pub fn new(channels: Vec<ChannelConfig>) -> Self {
        WebSocketConfig {
            channels,
            tls: None,
            max_message_size: 64 << 20,
            max_frame_size: 16 << 20,
            connect_timeout: Duration::from_secs(5),
            reconnect_attempts: 5,
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
        }
    }
}
