//! Errors from standing up a [`WebSocketSource`](crate::WebSocketSource).

/// Failure to start a source. Later connection loss is handled inside
/// `recv_batch` and surfaces only as `IngestError::Closed`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketError {
    /// The configuration lists no channels.
    #[error("websocket source needs at least one channel")]
    NoChannels,
    /// The platform root store gave no usable certificate for `wss://` channels.
    #[error("loading platform root certificates failed")]
    NativeRoots(#[source] std::io::Error),
    /// The first connect, TLS handshake, upgrade or subscribe of a channel failed.
    #[error("websocket connect to {url} failed")]
    Connect {
        /// Channel URL as configured.
        url: String,
        /// Underlying connect or handshake error.
        #[source]
        source: Box<tungstenite::Error>,
    },
}
