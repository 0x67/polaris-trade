//! Open a pbn file, version-gate on the schema major, expose decoded records.

use std::path::Path;

use pbn::{AnyRecord, PbnError, StreamHeader};

use crate::error::MdexError;

/// Schema major this build reads. Hand-bumped alongside the pbn/md-events pin;
/// pbn exposes no CURRENT_SCHEMA_MAJOR const at the tag, so the reader carries
/// its own. `read_stream` fails closed when the stream's major differs.
pub const READER_MAJOR: u8 = 1;

/// Size above which `open` warns. `open` pulls the whole file plus the
/// decompressed body into memory, so a multi-GB capture is costly. Not a hard
/// cap: a diagnostic tool must still open the large files worth inspecting.
const WARN_BYTES: u64 = 1 << 30; // 1 GiB

/// A decoded pbn stream: the version-gated header and the decompressed record
/// body every later stage borrows.
pub struct Reader {
    header: StreamHeader,
    body: Vec<u8>,
}

impl Reader {
    /// Read a whole pbn file, version-gate on the schema major, and hold the
    /// header + decompressed body. Fails closed before the body on bad magic,
    /// unknown proto, or a schema-major mismatch.
    pub fn open(path: &Path) -> Result<Reader, MdexError> {
        let bytes = std::fs::read(path)?;
        if oversized(bytes.len() as u64) {
            tracing::warn!(
                path = %path.display(),
                bytes = bytes.len(),
                "file read whole into memory"
            );
        }
        let (header, body) = pbn::read_stream(&bytes, READER_MAJOR)?;
        Ok(Reader { header, body })
    }

    pub fn header(&self) -> &StreamHeader {
        &self.header
    }

    /// Walk the body, yielding each frame typed. An unknown rtype is yielded as
    /// `AnyRecord::Unknown`, not an error, so a newer stream stays inspectable.
    pub fn records(&self) -> impl Iterator<Item = Result<AnyRecord, PbnError>> + '_ {
        pbn::read_records(&self.body)
    }

    pub(crate) fn body(&self) -> &[u8] {
        &self.body
    }
}

/// Whole-file read crosses the in-memory warning line.
fn oversized(len: u64) -> bool {
    len > WARN_BYTES
}

#[cfg(test)]
mod tests {
    use pbn::{
        AnyRecord, KIND_NORMALIZED, PROTO_VERSION, PbnError, StreamHeader,
        md_events::{RecordFlags, RecordHeader, Side, Trade, rtype},
        write_record, write_stream,
    };
    use tempfile::NamedTempFile;

    use super::*;

    fn trade(seq: u64) -> Trade {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Trade>() / 4) as u8,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 5,
            ts_event: seq,
            ts_recv: seq,
            ts_egress: 0,
            venue_seq: seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        Trade {
            hdr,
            price: 1_000_000_000,
            qty: 50,
            trade_ref: seq,
            aggressor: Side::Bid,
            _pad: [0u8; 7],
        }
    }

    fn header(schema_major: u8) -> StreamHeader {
        StreamHeader {
            proto_version: PROTO_VERSION,
            body_kind: KIND_NORMALIZED,
            schema_major,
            schema_minor: 0,
            venue: 1,
            flags: 0,
            dataset: 0,
            start_ts: 0,
            end_ts: 0,
        }
    }

    /// Write a stream (uncompressed, so the record body maps 1:1 into the file
    /// after the header) to a temp file and return its handle.
    fn stream_file(schema_major: u8, body: &[u8]) -> NamedTempFile {
        let bytes = write_stream(&header(schema_major), body).unwrap();
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), &bytes).unwrap();
        file
    }

    fn trade_body(seqs: &[u64]) -> Vec<u8> {
        let mut body = Vec::new();
        for &s in seqs {
            write_record(&mut body, &trade(s)).unwrap();
        }
        body
    }

    #[test]
    fn open_reads_header_and_body() {
        let file = stream_file(1, &trade_body(&[0, 1]));
        let reader = Reader::open(file.path()).unwrap();
        assert_eq!(reader.header().schema_major, 1);
        assert!(!reader.body().is_empty());
        assert_eq!(reader.records().count(), 2);
    }

    #[test]
    fn records_yield_in_order() {
        let file = stream_file(1, &trade_body(&[0, 1, 2]));
        let reader = Reader::open(file.path()).unwrap();
        let ts: Vec<u64> = reader
            .records()
            .map(|r| r.unwrap().header().unwrap().ts_event)
            .collect();
        assert_eq!(ts, [0, 1, 2]);
    }

    #[test]
    fn unknown_rtype_yielded() {
        let mut body = trade_body(&[0]);
        body[1] = 0x7A; // rtype byte in a reserved band; not a decode error
        let file = stream_file(1, &body);
        let reader = Reader::open(file.path()).unwrap();
        let got: Vec<AnyRecord> = reader.records().map(Result::unwrap).collect();
        assert!(matches!(got[0], AnyRecord::Unknown { rtype: 0x7A, .. }));
    }

    #[test]
    fn version_mismatch_errs_at_open() {
        let file = stream_file(2, &trade_body(&[0]));
        let err = match Reader::open(file.path()) {
            Err(e) => e,
            Ok(_) => panic!("expected a version mismatch"),
        };
        assert!(matches!(
            err,
            MdexError::Pbn(PbnError::VersionMismatch {
                reader: 1,
                stream: 2
            })
        ));
    }

    #[test]
    fn warns_on_large_file() {
        assert!(oversized(WARN_BYTES + 1));
        assert!(!oversized(WARN_BYTES));
        assert!(!oversized(0));
    }
}
