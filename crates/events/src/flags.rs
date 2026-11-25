//! Provenance and quality flags. Signals live in this bitfield, never in forked
//! record types.

/// `u16` provenance bitfield carried on every [`crate::RecordHeader`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct RecordFlags(pub u16);

impl RecordFlags {
    /// Derived from a snapshot rather than a live incremental event.
    pub const SNAPSHOT: RecordFlags = RecordFlags(1 << 0);
    /// A sequence gap is suspected on the stream feeding this record.
    pub const GAP_SUSPECT: RecordFlags = RecordFlags(1 << 1);
    /// Venue timestamp was missing or implausible; `ts_event` is best-effort.
    pub const BAD_TS: RecordFlags = RecordFlags(1 << 2);
    /// Synthesized from an MBP-native level feed, not order-level events.
    pub const MBP_SYNTHESIZED: RecordFlags = RecordFlags(1 << 3);
    /// Venue-specific escape bit; meaning is defined by the emitting adapter.
    pub const VENUE_ESCAPE: RecordFlags = RecordFlags(1 << 4);
    /// Execution mutates the book but is suppressed from tape, volume, and OHLCV
    /// (combination legs). Absent = ordinary printable execution.
    pub const NON_PRINTABLE: RecordFlags = RecordFlags(1 << 5);
    /// Book applied the event but the policy marked it suspect (`Gate::Flag`,
    /// e.g. an order event arriving during a halt). Book state still mutated.
    pub const SUSPECT: RecordFlags = RecordFlags(1 << 6);

    /// Empty flag set.
    pub const NONE: RecordFlags = RecordFlags(0);

    /// True when every bit in `other` is set here.
    pub const fn contains(self, other: RecordFlags) -> bool {
        self.0 & other.0 == other.0
    }

    /// Union of two flag sets.
    pub const fn with(self, other: RecordFlags) -> RecordFlags {
        RecordFlags(self.0 | other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Distinct bits, and containment reads back exactly what was set. The MBP
    // synthesis marker in particular must not alias another provenance bit.
    #[test]
    fn flag_bits_distinct_and_readable() {
        let all = [
            RecordFlags::SNAPSHOT,
            RecordFlags::GAP_SUSPECT,
            RecordFlags::BAD_TS,
            RecordFlags::MBP_SYNTHESIZED,
            RecordFlags::VENUE_ESCAPE,
            RecordFlags::NON_PRINTABLE,
            RecordFlags::SUSPECT,
        ];
        let mut union = RecordFlags::NONE;
        for f in all {
            assert!(!union.contains(f), "flag bit collides: {f:?}");
            union = union.with(f);
        }
        let set = RecordFlags::SNAPSHOT.with(RecordFlags::MBP_SYNTHESIZED);
        assert!(set.contains(RecordFlags::MBP_SYNTHESIZED));
        assert!(!set.contains(RecordFlags::GAP_SUSPECT));
    }
}
