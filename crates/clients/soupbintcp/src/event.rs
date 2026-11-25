//! Signals `SoupBinClient` surfaces beside sequenced data.

use crate::frame::Frame;

/// Session lifecycle or liveness signal, distinct from data [`Frame`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoupBinEvent {
    /// Server accepted login (`A`); session now streams.
    LoginAccepted {
        /// Session id as sent, space padded.
        session: [u8; 10],
        /// Sequence of next `Sequenced Data` packet.
        sequence: u64,
    },
    /// Server rejected login (`J`); session closed.
    LoginRejected {
        /// Reject reason code, e.g. `b'A'` not authorized, `b'S'` session unavailable.
        reason: u8,
    },
    /// Server sent `H` (Server Heartbeat).
    HeartbeatReceived,
    /// Client sent `R` (Client Heartbeat).
    HeartbeatSent,
    /// Server silent past `heartbeat_timeout`; session closed.
    HeartbeatTimeout,
    /// Server sent `Z` (End of Session); session closed.
    EndOfSession,
}

/// What `SoupBinClient` hands back: sequenced data or lifecycle event.
#[derive(Debug)]
pub enum SoupBinMessage<'a> {
    /// Sequenced data, borrowed until next call.
    Data(Frame<'a>),
    /// Lifecycle or liveness signal.
    Event(SoupBinEvent),
}
