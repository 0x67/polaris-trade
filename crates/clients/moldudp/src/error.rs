//! `MoldUdpError`: every failure path in this crate returns one of these variants.
//! Transport failures bubble through [`MoldUdpError::Transport`].

use thiserror::Error;
use transport_core::TransportError;

/// Failure kind for `MoldUDP64` wire decode, reassembly, session, and transport paths.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MoldUdpError {
    /// Leg or requester transport failed.
    #[error(transparent)]
    Transport(#[from] TransportError),

    /// Datagram carries session id other than one locked by first datagram.
    #[error("session mismatch: expected {expected:02x?}, got {got:02x?}")]
    SessionMismatch {
        /// Session locked by first datagram.
        expected: [u8; 10],
        /// Session of offending datagram.
        got: [u8; 10],
    },

    /// Sequence gap detected and queued for re-request; receive may continue.
    #[error("gap detected")]
    GapDetected,

    /// Out-of-order message collides with different pending message in reorder ring.
    #[error("reassembly buffer full (capacity {capacity})")]
    ReassemblyBufferFull {
        /// Reorder ring slots.
        capacity: usize,
    },

    /// Datagram or message block shorter than its header claims.
    #[error("packet too short")]
    PacketTooShort,

    /// Datagram larger than any single `MoldUDP64` packet.
    #[error("packet too large")]
    PacketTooLarge,

    /// Leg pool cannot hold reorder window plus one burst.
    #[error("leg {leg}: pool capacity {capacity} below required {required}")]
    PoolTooSmall {
        /// Index of leg in `from_legs` order.
        leg: usize,
        /// Leg's pool capacity.
        capacity: usize,
        /// [`crate::MIN_LEG_POOL_CAPACITY`].
        required: usize,
    },

    /// Leg count outside `1..=255`: stream ids are `u8`, one kept for requester.
    #[error("leg count {count} outside 1..=255")]
    LegCount {
        /// Legs passed to `from_legs`.
        count: usize,
    },
}
