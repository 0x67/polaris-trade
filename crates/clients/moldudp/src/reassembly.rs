//! Sequence reassembler: fixed-capacity slot ring keyed by `seq % capacity`.
//! Slots hold owned, backend-agnostic slab handle `S`, not private copy, so
//! drain never re-copies bytes; it moves ownership out of slot.

use crate::error::MoldUdpError;

/// One ring slot. `seq` meaningful only while `payload` is `Some`.
pub struct Slot<S> {
    /// Sequence of held message.
    pub seq: u64,
    /// Held message; `None` when slot empty.
    pub payload: Option<S>,
}

/// O(1)-insert reassembler over fixed ring of `capacity` slots. Drains
/// contiguous runs from `expected_next`, drops stale duplicates below it,
/// rejects insert that would clobber still-pending slot.
pub struct SequenceReassembler<S> {
    slots: Vec<Slot<S>>,
    capacity: u64,
    expected_next: u64,
}

impl<S> SequenceReassembler<S> {
    /// Ring of `capacity` slots expecting sequence 1 (`MoldUDP64` sessions
    /// start numbering at 1).
    ///
    /// # Panics
    ///
    /// When `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        let slots = (0..capacity)
            .map(|_| Slot {
                seq: 0,
                payload: None,
            })
            .collect();
        Self {
            slots,
            capacity: capacity as u64,
            expected_next: 1,
        }
    }

    /// Next sequence to drain.
    pub fn expected_next(&self) -> u64 {
        self.expected_next
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "remainder below capacity, which came from usize"
    )]
    fn slot_index(&self, seq: u64) -> usize {
        (seq % self.capacity) as usize
    }

    /// Bump `expected_next` past run caller already emitted without
    /// [`insert`](Self::insert): fused in-order path borrows message straight
    /// from source datagram, nothing to store. [`drain_ready`](Self::drain_ready)
    /// picks up from new position.
    pub fn advance_expected(&mut self, n: u64) {
        self.expected_next += n;
    }

    /// Re-anchor `expected_next` for cold start or mid-session join, before
    /// any slab is buffered: joining live feed at seq N sets baseline to N, so
    /// backlog below it is not one giant gap. Later arrivals below drop as stale.
    pub fn reset_expected(&mut self, expected: u64) {
        self.expected_next = expected;
    }

    /// Yield what is already buffered contiguously from `expected_next`,
    /// inserting nothing. Pairs with [`advance_expected`](Self::advance_expected):
    /// fused in-order path advances past emitted run, then cascade-drains older
    /// out-of-order arrivals now contiguous.
    pub fn drain_ready(&mut self) -> DrainCursor<'_, S> {
        DrainCursor { inner: self }
    }
}

impl<S: AsRef<[u8]> + 'static> SequenceReassembler<S> {
    /// Insert `seq`'s slab. Stale (`seq < expected_next`) and exact pending
    /// duplicates drop silently. Insert landing on `expected_next` returns
    /// [`DrainCursor`] over newly contiguous run.
    ///
    /// # Errors
    ///
    /// [`MoldUdpError::ReassemblyBufferFull`] when slot holds different pending
    /// sequence; existing entry is never evicted.
    pub fn insert(
        &mut self,
        seq: u64,
        slab: S,
    ) -> Result<Option<DrainCursor<'_, S>>, MoldUdpError> {
        if seq < self.expected_next {
            return Ok(None);
        }
        let idx = self.slot_index(seq);
        let slot = &self.slots[idx];
        if slot.payload.is_some() {
            if slot.seq == seq {
                return Ok(None);
            }
            return Err(MoldUdpError::ReassemblyBufferFull {
                capacity: self.slots.len(),
            });
        }
        let slot = &mut self.slots[idx];
        slot.seq = seq;
        slot.payload = Some(slab);
        if seq == self.expected_next {
            Ok(Some(DrainCursor { inner: self }))
        } else {
            Ok(None)
        }
    }
}

/// Lazily drains contiguous run from reassembler's `expected_next`. Like
/// `Vec::drain`: dropping cursor early still advances `expected_next` past
/// whole run.
pub struct DrainCursor<'a, S> {
    inner: &'a mut SequenceReassembler<S>,
}

impl<S> Iterator for DrainCursor<'_, S> {
    type Item = S;

    fn next(&mut self) -> Option<S> {
        let idx = self.inner.slot_index(self.inner.expected_next);
        let slot = &mut self.inner.slots[idx];
        if slot.seq != self.inner.expected_next {
            return None;
        }
        let payload = slot.payload.take()?;
        self.inner.expected_next += 1;
        Some(payload)
    }
}

impl<S> Drop for DrainCursor<'_, S> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}
