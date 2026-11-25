//! Control events surfaced instead of `Frame` for non-data downstream packets.

/// Non-data downstream packet, classified by [`crate::wire::PacketKind`].
/// No `SessionOpen` variant: session id capture stays internal receiver state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoldUdpEvent {
    /// Server heartbeat. Carries sequence so loss is detected while idle: a
    /// `next_expected` past receiver's own expected sequence means tail was
    /// lost during quiet traffic and gets recorded as gap.
    Heartbeat {
        /// Sequence server sends next.
        next_expected: u64,
    },
    /// Server ended session; tail past receiver's expected sequence is recorded as gap.
    EndOfSession {
        /// Sequence server would have sent next.
        next_expected: u64,
    },
}
