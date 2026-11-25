//! Capture writers and readers.
//!
//! Length-framed zstd for raw venue frames, tapped before the adapter. Every
//! container starts with a 16-byte uncompressed header (magic, venue, date,
//! schema, feed), then a zstd-compressed body. See [[raw]] for the row layout.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod raw;

use std::io::{self, Read, Write};

pub use raw::{RawReader, RawRow, RawWriter};

/// Container magic, first 4 header bytes on every capture file.
pub const MAGIC: [u8; 4] = *b"MDC1";

/// Legacy capture, written before the feed byte existed. Unchecked.
pub const FEED_UNSPECIFIED: u8 = 0;

/// Container header size in bytes, written uncompressed before the zstd body.
pub const HEADER_LEN: usize = 16;

/// Capture container metadata: venue, trading date, schema version. Carried
/// in the 16-byte header outside the zstd body so a reader can route a file
/// without decompressing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureMeta {
    pub venue: u16,
    /// Trading date, `YYYYMMDD` (e.g. `20260712`).
    pub date: u32,
    /// Body schema version, starts at 1.
    pub schema: u16,
    /// Feed discriminator, stamped at header byte 13. Names which venue decoder
    /// can read the body so a reader never silently decodes the wrong feed.
    /// Opaque here: each venue crate owns its own id, and the container only
    /// carries the byte. Ids 1..=127 are public venues, 128..=255 private; `0`
    /// is a legacy capture written before the byte existed. Values are an
    /// on-disk contract across every capture written, so they are never
    /// renumbered. Raw `u8`, not an enum: a header read from disk must never
    /// fail to parse on an id assigned after the reader was built.
    pub feed: u8,
}

/// Capture read/write failure. `Io` wraps the underlying writer/reader error
/// (including zstd stream errors and truncated rows); `BadMagic` and
/// `UnsupportedKind` report container corruption or a retired body kind.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CaptureError {
    #[error("capture io error")]
    Io(#[from] io::Error),
    #[error("bad capture magic: {found:?}")]
    BadMagic { found: [u8; 4] },
    #[error("unsupported capture kind: {kind}")]
    UnsupportedKind { kind: u8 },
}

/// Write the 16-byte uncompressed container header: magic, venue, date,
/// schema, zero kind, feed, then 2 zero pad bytes.
pub(crate) fn write_header<W: Write>(w: &mut W, meta: CaptureMeta) -> Result<(), CaptureError> {
    let mut buf = [0u8; HEADER_LEN];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4..6].copy_from_slice(&meta.venue.to_le_bytes());
    buf[6..10].copy_from_slice(&meta.date.to_le_bytes());
    buf[10..12].copy_from_slice(&meta.schema.to_le_bytes());
    // buf[12] is the retired body-kind byte, always 0. Reserved, not reused:
    // a future body kind can claim it without moving any other field.
    buf[13] = meta.feed;
    w.write_all(&buf)?;
    Ok(())
}

/// Read and validate the 16-byte uncompressed container header. Errors on bad
/// magic, or on a non-zero byte 12: that is a normalized-body container, whose
/// row layout this reader would decode as garbage rather than reject.
pub fn read_header<R: Read>(r: &mut R) -> Result<CaptureMeta, CaptureError> {
    let mut buf = [0u8; HEADER_LEN];
    r.read_exact(&mut buf)?;
    let magic: [u8; 4] = buf[0..4].try_into().expect("4-byte slice");
    if magic != MAGIC {
        return Err(CaptureError::BadMagic { found: magic });
    }
    let kind = buf[12];
    if kind != 0 {
        return Err(CaptureError::UnsupportedKind { kind });
    }
    Ok(CaptureMeta {
        venue: u16::from_le_bytes(buf[4..6].try_into().expect("2-byte slice")),
        date: u32::from_le_bytes(buf[6..10].try_into().expect("4-byte slice")),
        schema: u16::from_le_bytes(buf[10..12].try_into().expect("2-byte slice")),
        feed: buf[13],
    })
}

/// Read exactly `buf.len()` bytes, or report clean EOF (`Ok(false)`) if zero
/// bytes were read before the stream ended. A partial read mid-record is
/// still an IO error, not a clean EOF.
pub(crate) fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool, CaptureError> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..])? {
            0 if filled == 0 => return Ok(false),
            0 => {
                return Err(CaptureError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated capture row",
                )));
            }
            n => filled += n,
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    // The 16-byte header is the container's routing prefix; a reader must
    // recover the exact venue/date/schema/feed a writer stamped, and the feed
    // byte must land at index 13 so the on-disk layout stays pinned.
    #[test]
    fn header_round_trips() {
        let meta = CaptureMeta {
            venue: 7,
            date: 20260712,
            schema: 1,
            feed: 2,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, meta).expect("write header");
        assert_eq!(buf.len(), HEADER_LEN);
        assert_eq!(buf[12], 0, "retired kind byte stays zero");
        assert_eq!(buf[13], 2, "feed byte at index 13");

        let mut cursor = Cursor::new(buf);
        let read_back = read_header(&mut cursor).expect("read header");
        assert_eq!(read_back, meta);
    }

    // A capture written before the feed byte existed leaves a zero pad at index
    // 13; it must read back as UNSPECIFIED, not a wrong feed, so legacy files
    // stay valid and fall through to the caller's own venue check.
    #[test]
    fn legacy_pad_reads_unspecified_feed() {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..6].copy_from_slice(&7u16.to_le_bytes());
        buf[6..10].copy_from_slice(&20260712u32.to_le_bytes());
        buf[10..12].copy_from_slice(&1u16.to_le_bytes());
        // buf[12..16] stay zero, exactly as a pre-feed writer left them.
        let mut cursor = Cursor::new(buf);
        let meta = read_header(&mut cursor).expect("read legacy header");
        assert_eq!(meta.feed, FEED_UNSPECIFIED);
    }

    // An id the container has never heard of must still parse: the feed byte
    // routes a body, it does not gate it, and a reader from disk must never
    // fail on an id assigned after it was built.
    #[test]
    fn unknown_feed_id_parses() {
        let meta = CaptureMeta {
            venue: 9,
            date: 20260712,
            schema: 1,
            feed: 200,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, meta).expect("write header");
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_header(&mut cursor).expect("read header").feed, 200);
    }

    // A container with a mangled magic must fail closed, not silently decode
    // garbage as if it were a valid body.
    #[test]
    fn corrupt_magic_errors() {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(b"NOPE");
        let mut cursor = Cursor::new(buf);
        let err = read_header(&mut cursor).expect_err("bad magic must error");
        assert!(matches!(err, CaptureError::BadMagic { .. }));
    }

    // Retired normalized containers are still on disk. Their rows are records,
    // not raw frames, so accepting one here would hand the caller wrong lengths
    // and wrong timestamps with no error at all. Fail closed on byte 12.
    #[test]
    fn rejects_nonzero_kind_byte() {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[12] = 1;
        let mut cursor = Cursor::new(buf);
        let err = read_header(&mut cursor).expect_err("normalized kind must error");
        assert!(matches!(err, CaptureError::UnsupportedKind { kind: 1 }));
    }
}
