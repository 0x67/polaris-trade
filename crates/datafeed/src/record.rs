//! Records a [`crate::FeedSession`] hands its host, and the queue its egress
//! sequencer publishes into.
//!
//! An [`OwnedRecord`] holds one stamped record inline, sized to the largest
//! record, so filling the host's `Vec` allocates only when it grows, never
//! per record. During a step the queue lends the host's `Vec` to the
//! sequencer, so each record is written once, straight into it; only records
//! past the host's `max` are copied into the queue to wait for the next poll.

use std::{cell::RefCell, mem, mem::MaybeUninit};

use md_events::RecordHeader;
use md_pipeline::{OutputSink, RecordRef, SinkError, StreamId};

/// Size in bytes of the largest record a session publishes (`Mbp10`), header
/// included. `session.rs` asserts every published record type fits.
pub const MAX_RECORD_LEN: usize = size_of::<md_events::Mbp10>();

const HEADER_LEN: usize = size_of::<RecordHeader>();

const BODY_CAP: usize = MAX_RECORD_LEN - HEADER_LEN;

/// One normalized record copied out of the session: its egress stream, its
/// stamped header, and its body, stored inline.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OwnedRecord {
    header: RecordHeader,
    // Only the first `body_len` bytes are written: zeroing the rest would
    // write the whole buffer per record for bytes nothing reads.
    body: [MaybeUninit<u8>; BODY_CAP],
    body_len: u16,
    stream: StreamId,
}

// `bytes` reads the header and body as one run, so the body must start right
// where the header ends.
const _: () = assert!(core::mem::offset_of!(OwnedRecord, body) == HEADER_LEN);
const _: () = assert!(BODY_CAP <= u16::MAX as usize);

impl OwnedRecord {
    /// Appends `rec` to `records`, writing its body straight into the new
    /// slot: a record built on the stack first would be copied again, all
    /// 488 bytes of it.
    fn push(records: &mut Vec<OwnedRecord>, rec: RecordRef<'_>) {
        let pushed = records.push_mut(OwnedRecord {
            header: *rec.header,
            body: [MaybeUninit::uninit(); BODY_CAP],
            body_len: rec.body.len() as u16,
            stream: rec.stream,
        });
        // Every record type the session publishes is asserted to fit in
        // `MAX_RECORD_LEN`, so this slice is in bounds.
        pushed.body[..rec.body.len()].write_copy_of_slice(rec.body);
    }

    /// Egress stream the record was published on.
    #[inline]
    pub fn stream(&self) -> StreamId {
        self.stream
    }

    /// Stamped header, `ts_egress` and `egress_seq` included.
    #[inline]
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    /// Record bytes after the header.
    #[inline]
    pub fn body(&self) -> &[u8] {
        // SAFETY: `push` wrote the first `body_len` bytes of `body`, and a
        // copy of the record carries them.
        unsafe { self.body[..usize::from(self.body_len)].assume_init_ref() }
    }

    /// The whole record as published: the `repr(C)` header bytes, then the
    /// body.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        let len = HEADER_LEN + usize::from(self.body_len);
        // SAFETY: `OwnedRecord` is `repr(C)` with `body` right after `header`
        // (asserted above), `RecordHeader` has no implicit padding (its `_pad`
        // is explicit and its size is asserted in md-events), and `push` wrote
        // the first `body_len` bytes of `body`, so the first `len` bytes of
        // `self` are initialized and in bounds.
        unsafe { core::slice::from_raw_parts((self as *const Self).cast::<u8>(), len) }
    }

    /// Borrows the record in the shape an [`OutputSink`] takes.
    #[inline]
    pub fn as_record_ref(&self) -> RecordRef<'_> {
        RecordRef {
            stream: self.stream,
            header: &self.header,
            body: self.body(),
        }
    }
}

impl core::fmt::Debug for OwnedRecord {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnedRecord")
            .field("stream", &self.stream)
            .field("header", &self.header)
            .field("body_len", &self.body_len)
            .finish()
    }
}

/// Records published but not yet handed to the host, oldest first.
#[derive(Default)]
struct Pending {
    records: Vec<OwnedRecord>,
    head: usize,
}

/// A [`crate::FeedSession`]'s sink. The egress sequencer owns it and lends
/// it only by shared reference, so the session drains it through a
/// `RefCell`.
#[derive(Default)]
pub(crate) struct RecordQueue(RefCell<Pending>);

impl RecordQueue {
    /// Swaps `out` in as the queue's buffer, so the next step publishes
    /// straight onto the host's `Vec`. The queue must be empty, and
    /// [`reclaim`](Self::reclaim) must follow before any other call.
    pub(crate) fn lend(&self, out: &mut Vec<OwnedRecord>) {
        let mut pending = self.0.borrow_mut();
        debug_assert!(pending.records.is_empty(), "lend needs an empty queue");
        mem::swap(&mut pending.records, out);
    }

    /// Swaps the lent buffer back into `out`, which held `start` records
    /// before the lend, and returns how many the step appended. Records past
    /// the first `max` of them stay queued.
    pub(crate) fn reclaim(&self, out: &mut Vec<OwnedRecord>, start: usize, max: usize) -> usize {
        let mut pending = self.0.borrow_mut();
        mem::swap(&mut pending.records, out);
        let routed = out.len() - start;
        if routed > max {
            pending.records.extend_from_slice(&out[start + max..]);
            out.truncate(start + max);
        }
        routed
    }

    pub(crate) fn is_empty(&self) -> bool {
        let pending = self.0.borrow();
        pending.head == pending.records.len()
    }

    /// Moves up to `max` of the oldest records onto `out`, returning how many.
    pub(crate) fn drain_into(&self, out: &mut Vec<OwnedRecord>, max: usize) -> usize {
        let mut pending = self.0.borrow_mut();
        let Pending { records, head } = &mut *pending;
        let taken = (records.len() - *head).min(max);
        out.extend_from_slice(&records[*head..*head + taken]);
        *head += taken;
        if *head == records.len() {
            records.clear();
            *head = 0;
        }
        taken
    }
}

impl OutputSink for RecordQueue {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        OwnedRecord::push(&mut self.0.get_mut().records, rec);
        Ok(())
    }
}
