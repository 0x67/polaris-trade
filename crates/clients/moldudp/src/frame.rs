//! Protocol-side frames handed to consumers. [`Frame`] borrows straight from
//! receiver's still-owned datagram; [`MessageView`] and [`OwnedFrame`] share
//! ownership of it via `Arc` for buffering or cross-thread handoff.

use std::sync::Arc;

/// Datagram receiver holds: leg frame as received, or retransmission copied off
/// requester socket (its frame type is unrelated to legs', slab freed at once).
pub(crate) enum Held<F> {
    Frame(F),
    Copied(Box<[u8]>),
}

impl<F: AsRef<[u8]>> AsRef<[u8]> for Held<F> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Frame(frame) => frame.as_ref(),
            Self::Copied(bytes) => bytes,
        }
    }
}

/// One reassembled, in-order `MoldUDP64` message, borrowed from receiver.
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    /// Message bytes.
    pub payload: &'a [u8],
    /// Message sequence number.
    pub sequence: u64,
    /// Leg index; leg count for retransmission read off requester.
    pub stream_id: u8,
}

impl Frame<'_> {
    /// Message sequence number.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Leg index; leg count for retransmission read off requester.
    #[inline]
    pub fn stream_id(&self) -> u8 {
        self.stream_id
    }
}

impl AsRef<[u8]> for Frame<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.payload
    }
}

/// Message sharing ownership of its backing datagram via `Arc` instead of
/// borrowing it. Built when message must outlive datagram's original owner:
/// reorder buffering or cross-thread handoff. Clone bumps refcount, no byte copy.
pub struct MessageView<F> {
    datagram: Arc<Held<F>>,
    offset: usize,
    len: usize,
}

impl<F> MessageView<F> {
    pub(crate) fn new(datagram: Arc<Held<F>>, offset: usize, len: usize) -> Self {
        Self {
            datagram,
            offset,
            len,
        }
    }
}

impl<F> Clone for MessageView<F> {
    fn clone(&self) -> Self {
        Self {
            datagram: Arc::clone(&self.datagram),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl<F: AsRef<[u8]>> AsRef<[u8]> for MessageView<F> {
    fn as_ref(&self) -> &[u8] {
        &(*self.datagram).as_ref()[self.offset..self.offset + self.len]
    }
}

/// Owned counterpart to [`Frame`]: carries [`MessageView`] instead of borrow,
/// so it is `Send` and crosses threads (e.g. into sharded engine core) without
/// copying message bytes.
pub struct OwnedFrame<F> {
    /// Shared view of message bytes.
    pub view: MessageView<F>,
    /// Message sequence number.
    pub sequence: u64,
    /// Leg index; leg count for retransmission read off requester.
    pub stream_id: u8,
}

impl<F> OwnedFrame<F> {
    /// Message sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Leg index; leg count for retransmission read off requester.
    pub fn stream_id(&self) -> u8 {
        self.stream_id
    }
}

impl<F: AsRef<[u8]>> AsRef<[u8]> for OwnedFrame<F> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}
