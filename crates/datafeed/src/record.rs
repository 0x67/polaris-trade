//! Records a [`crate::FeedSession`] hands its host, and the queue its egress
//! sequencer publishes into until the host takes them.
//!
//! An [`OwnedRecord`] holds one stamped record inline, sized to the largest
//! record, so moving records through the queue and the host's `Vec` allocates
//! only when a buffer grows, never per record.

use std::{cell::RefCell, rc::Rc};

use md_events::RecordHeader;
use md_pipeline::{OutputSink, RecordRef, SinkError, StreamId};

/// Size in bytes of the largest record a session publishes (`Mbp10`), header
/// included. `session.rs` asserts every published record type fits.
pub const MAX_RECORD_LEN: usize = size_of::<md_events::Mbp10>();

const HEADER_LEN: usize = size_of::<RecordHeader>();

/// One normalized record copied out of the session: its egress stream, its
/// stamped header, and its body, stored inline.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OwnedRecord {
    header: RecordHeader,
    body: [u8; MAX_RECORD_LEN - HEADER_LEN],
    body_len: u16,
    stream: StreamId,
}

// `bytes` reads the header and body as one run, so the body must start right
// where the header ends.
const _: () = assert!(core::mem::offset_of!(OwnedRecord, body) == HEADER_LEN);
const _: () = assert!(MAX_RECORD_LEN - HEADER_LEN <= u16::MAX as usize);

impl OwnedRecord {
    fn new(rec: RecordRef<'_>) -> Self {
        let mut body = [0u8; MAX_RECORD_LEN - HEADER_LEN];
        // Every record type the session publishes is asserted to fit in
        // `MAX_RECORD_LEN`, so this slice is in bounds.
        body[..rec.body.len()].copy_from_slice(rec.body);
        OwnedRecord {
            header: *rec.header,
            body,
            body_len: rec.body.len() as u16,
            stream: rec.stream,
        }
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
        &self.body[..usize::from(self.body_len)]
    }

    /// The whole record as published: the `repr(C)` header bytes, then the
    /// body.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        let len = HEADER_LEN + usize::from(self.body_len);
        // SAFETY: `OwnedRecord` is `repr(C)` with `body` right after `header`
        // (asserted above), and `RecordHeader` has no implicit padding (its
        // `_pad` is explicit and its size is asserted in md-events), so the
        // first `len` bytes of `self` are initialized and in bounds.
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

/// The session's output sink. The egress sequencer owns one handle and the
/// session keeps another to drain it, since the sequencer lends its sink only
/// by shared reference (the same sharing `md_replay::SharedClock` uses).
#[derive(Clone, Default)]
pub(crate) struct RecordQueue(Rc<RefCell<Pending>>);

impl RecordQueue {
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
        self.0.borrow_mut().records.push(OwnedRecord::new(rec));
        Ok(())
    }
}
