//! `MoldUDP64` downstream packet wire codec. Header parse plus message block
//! iteration, both zero-alloc: everything borrows straight from caller's
//! datagram slice.

use crate::error::MoldUdpError;

/// 20-byte Downstream Packet Header: `Session[10]`, `Sequence[8 BE]`, `MessageCount[2 BE]`.
pub const HEADER_LEN: usize = 20;

/// Above this, datagram exceeds any sane single-UDP-packet `MoldUDP64` payload.
const MAX_DOWNSTREAM_DATAGRAM: usize = 64 * 1024;

/// Parsed `MoldUDP64` downstream packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownstreamHeader {
    /// Session id.
    pub session: [u8; 10],
    /// First message's sequence, or next-expected for heartbeat and end of session.
    pub sequence: u64,
    /// Message blocks carried, or heartbeat/end-of-session marker.
    pub message_count: u16,
}

/// What packet's `MessageCount` field classifies it as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketKind {
    /// `MessageCount == 0`; `sequence` carries next-expected, no message blocks.
    Heartbeat,
    /// `MessageCount == 0xFFFF`; `sequence` carries next-expected, no message blocks.
    EndOfSession,
    /// `MessageCount in 1..=0x7FFE`; `sequence` is first message's seq.
    Data,
}

impl DownstreamHeader {
    /// Classify by `message_count` per `MoldUDP64` heartbeat/end-of-session convention.
    pub fn kind(&self) -> PacketKind {
        match self.message_count {
            0 => PacketKind::Heartbeat,
            0xFFFF => PacketKind::EndOfSession,
            _ => PacketKind::Data,
        }
    }

    /// Iterate this header's message blocks out of `datagram` (full packet this
    /// header was parsed from, header bytes included).
    pub fn blocks<'a>(&self, datagram: &'a [u8]) -> MessageBlockIter<'a> {
        let body = datagram.get(HEADER_LEN..).unwrap_or(&[]);
        MessageBlockIter::new(body, self.message_count)
    }
}

/// Parse header, rejecting datagrams shorter than [`HEADER_LEN`] or larger than
/// any sane single UDP datagram before touching byte layout.
///
/// # Errors
///
/// [`MoldUdpError::PacketTooShort`] or [`MoldUdpError::PacketTooLarge`].
pub fn parse_header(buf: &[u8]) -> Result<DownstreamHeader, MoldUdpError> {
    if buf.len() > MAX_DOWNSTREAM_DATAGRAM {
        return Err(MoldUdpError::PacketTooLarge);
    }
    let (session, rest) = buf
        .split_first_chunk::<10>()
        .ok_or(MoldUdpError::PacketTooShort)?;
    let (sequence, rest) = rest
        .split_first_chunk::<8>()
        .ok_or(MoldUdpError::PacketTooShort)?;
    let (message_count, _) = rest
        .split_first_chunk::<2>()
        .ok_or(MoldUdpError::PacketTooShort)?;
    Ok(DownstreamHeader {
        session: *session,
        sequence: u64::from_be_bytes(*sequence),
        message_count: u16::from_be_bytes(*message_count),
    })
}

/// Borrows message block payloads straight from datagram slice; no copy.
/// Each block is `Length[2 BE]` then `Length` payload bytes. Yields
/// `(offset, block)`, `offset` being block's byte position within *full*
/// datagram (header included), so caller builds [`crate::frame::MessageView`]
/// without re-deriving pointer arithmetic.
pub struct MessageBlockIter<'a> {
    remaining: &'a [u8],
    blocks_left: u16,
    truncated: bool,
    next_offset: usize,
}

impl<'a> MessageBlockIter<'a> {
    /// `payload` is packet body after 20-byte header.
    pub fn new(payload: &'a [u8], message_count: u16) -> Self {
        Self {
            remaining: payload,
            blocks_left: message_count,
            truncated: false,
            next_offset: HEADER_LEN,
        }
    }
}

impl<'a> Iterator for MessageBlockIter<'a> {
    type Item = Result<(usize, &'a [u8]), MoldUdpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.truncated || self.blocks_left == 0 {
            return None;
        }
        let Some((len, rest)) = self.remaining.split_first_chunk::<2>() else {
            self.truncated = true;
            return Some(Err(MoldUdpError::PacketTooShort));
        };
        let len = usize::from(u16::from_be_bytes(*len));
        if rest.len() < len {
            self.truncated = true;
            return Some(Err(MoldUdpError::PacketTooShort));
        }
        let (block, rest) = rest.split_at(len);
        let offset = self.next_offset + 2;
        self.next_offset += 2 + len;
        self.remaining = rest;
        self.blocks_left -= 1;
        Some(Ok((offset, block)))
    }
}
