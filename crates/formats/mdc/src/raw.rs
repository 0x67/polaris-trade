//! Raw-stream capture: venue bytes tapped before the adapter, framed with
//! stream id and receive timestamp so raw replay can re-run the adapter and
//! regenerate the normalized stream byte-for-byte.

use std::io::{BufReader, Read, Write};

use crate::{CaptureError, CaptureMeta, read_exact_or_eof, read_header, write_header};

/// One captured raw frame: which stream it arrived on, when it was received,
/// and the venue bytes verbatim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawRow {
    pub stream: u16,
    pub ts_recv: u64,
    pub payload: Vec<u8>,
}

/// Row header size on the wire: `stream:u16 + ts_recv:u64 + len:u32`.
const ROW_HEAD_LEN: usize = 2 + 8 + 4;

/// Writes raw-stream capture rows as a zstd-compressed, DBN-framed container.
/// Call [`RawWriter::write_row`] per frame, then [`RawWriter::finish`] to
/// flush the zstd stream and hand back the underlying writer.
pub struct RawWriter<W: Write> {
    enc: zstd::Encoder<'static, W>,
}

impl<W: Write> RawWriter<W> {
    /// Open a raw capture container: writes the 16-byte header uncompressed,
    /// then starts the zstd body stream.
    pub fn new(mut w: W, meta: CaptureMeta) -> Result<Self, CaptureError> {
        write_header(&mut w, meta)?;
        let enc = zstd::Encoder::new(w, zstd::DEFAULT_COMPRESSION_LEVEL)?;
        Ok(Self { enc })
    }

    /// Append one row: `stream:u16 LE, ts_recv:u64 LE, len:u32 LE, payload`.
    pub fn write_row(
        &mut self,
        stream: u16,
        ts_recv: u64,
        payload: &[u8],
    ) -> Result<(), CaptureError> {
        self.enc.write_all(&stream.to_le_bytes())?;
        self.enc.write_all(&ts_recv.to_le_bytes())?;
        self.enc.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.enc.write_all(payload)?;
        Ok(())
    }

    /// Flush the zstd frame and hand back the underlying writer.
    pub fn finish(self) -> Result<W, CaptureError> {
        Ok(self.enc.finish()?)
    }
}

/// Reads raw-stream capture rows back out of a container written by
/// [`RawWriter`].
pub struct RawReader<R: Read> {
    dec: zstd::Decoder<'static, BufReader<R>>,
}

impl<R: Read> RawReader<R> {
    /// Open a raw capture container: reads and validates the 16-byte header,
    /// then wraps the rest in a zstd decoder. Returns the reader plus the
    /// metadata carried in the header.
    pub fn new(mut r: R) -> Result<(Self, CaptureMeta), CaptureError> {
        let meta = read_header(&mut r)?;
        let dec = zstd::Decoder::new(r)?;
        Ok((Self { dec }, meta))
    }

    /// Read the next row, or `Ok(None)` at clean end of stream.
    pub fn read_row(&mut self) -> Result<Option<RawRow>, CaptureError> {
        let mut head = [0u8; ROW_HEAD_LEN];
        if !read_exact_or_eof(&mut self.dec, &mut head)? {
            return Ok(None);
        }
        let stream = u16::from_le_bytes(head[0..2].try_into().expect("2-byte slice"));
        let ts_recv = u64::from_le_bytes(head[2..10].try_into().expect("8-byte slice"));
        let len = u32::from_le_bytes(head[10..14].try_into().expect("4-byte slice")) as usize;
        let mut payload = vec![0u8; len];
        self.dec.read_exact(&mut payload)?;
        Ok(Some(RawRow {
            stream,
            ts_recv,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    // Round trip through the zstd container: rows written must come back in
    // order, byte-identical, including a zero-length payload edge case.
    #[test]
    fn raw_round_trips_rows() {
        let meta = CaptureMeta {
            venue: 3,
            date: 20260712,
            schema: 1,
            feed: 1,
        };
        let mut writer = RawWriter::new(Vec::new(), meta).expect("open writer");
        writer.write_row(10, 100, b"hello").expect("row 1");
        writer.write_row(11, 200, b"world!!").expect("row 2");
        writer.write_row(12, 300, b"").expect("row 3 empty payload");
        let bytes = writer.finish().expect("finish");

        let (mut reader, read_meta) = RawReader::new(Cursor::new(bytes)).expect("open reader");
        assert_eq!(read_meta, meta);

        let row1 = reader.read_row().expect("read row 1").expect("some row 1");
        assert_eq!(row1.stream, 10);
        assert_eq!(row1.ts_recv, 100);
        assert_eq!(row1.payload, b"hello");

        let row2 = reader.read_row().expect("read row 2").expect("some row 2");
        assert_eq!(row2.stream, 11);
        assert_eq!(row2.ts_recv, 200);
        assert_eq!(row2.payload, b"world!!");

        let row3 = reader.read_row().expect("read row 3").expect("some row 3");
        assert_eq!(row3.stream, 12);
        assert_eq!(row3.ts_recv, 300);
        assert!(row3.payload.is_empty());

        assert!(reader.read_row().expect("read eof").is_none());
    }

    // Reader must report clean end-of-stream, not an error, when there are
    // no more rows after a well-formed container.
    #[test]
    fn raw_reader_reports_clean_eof() {
        let meta = CaptureMeta {
            venue: 1,
            date: 20260101,
            schema: 1,
            feed: crate::FEED_UNSPECIFIED,
        };
        let writer = RawWriter::new(Vec::new(), meta).expect("open writer");
        let bytes = writer.finish().expect("finish empty");

        let (mut reader, _) = RawReader::new(Cursor::new(bytes)).expect("open reader");
        assert!(reader.read_row().expect("read eof").is_none());
    }
}
