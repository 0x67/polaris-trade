//! `SoupBinError`: every failure path in this crate resolves to one variant.
//! `Transport` wraps stream failures, rest are protocol level.

use std::time::Duration;

use thiserror::Error;

/// Failure kinds for `SoupBinTCP` session handling.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SoupBinError {
    /// Stream failed; peer close is `Transport(TransportError::PeerClosed)`.
    #[error(transparent)]
    Transport(#[from] transport_core::TransportError),

    /// Server rejected login (async `connect`).
    #[error("login rejected: {code}")]
    LoginRejected {
        /// Reject reason code.
        code: String,
    },

    /// No login response within `login_timeout`.
    #[error("login timeout after {timeout:?}")]
    LoginTimeout {
        /// Configured timeout.
        timeout: Duration,
    },

    /// Inbound packet longer than `max_frame_size`, or outbound payload past
    /// 65534 bytes.
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge {
        /// Packet or payload size.
        size: usize,
        /// Limit crossed.
        max: usize,
    },

    /// Packet valid on wire but wrong for session state.
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    /// Packet type byte outside protocol.
    #[error("unknown packet type: 0x{0:02x}")]
    UnknownPacketType(u8),

    /// Session closed: end of session, logout, rejected login or heartbeat timeout.
    #[error("end of session")]
    EndOfSession,
}
