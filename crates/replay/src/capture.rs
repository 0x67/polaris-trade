//! Raw capture container replay. Reads the shared container format md-capture's
//! raw writer produces: 16-byte uncompressed header, then a zstd body of
//! `(stream, ts_recv, len, payload)` rows.

use std::io::Read;

pub use md_capture::CaptureError;
use md_pipeline::{IngestError, IngestFrame, IngestSource, StreamId};

/// One decoded row from a raw capture container: venue bytes plus the capture
/// boundary receive time and stream. `venue_seq` is synthetic: the container
/// carries no real wire sequence, so it is the row's 1-based count within its
/// own stream, like the per-channel session sequence a live feed carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureFrame {
    payload: Vec<u8>,
    ts_recv: u64,
    stream: StreamId,
    venue_seq: u64,
}

impl IngestFrame for CaptureFrame {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn ts_recv(&self) -> u64 {
        self.ts_recv
    }
    fn stream(&self) -> StreamId {
        self.stream
    }
    fn venue_seq(&self) -> u64 {
        self.venue_seq
    }
}

/// Replays a raw capture container as an [`IngestSource`]. Decompresses the
/// whole zstd body up front, then walks rows on each `recv_batch` call.
#[derive(Debug)]
pub struct CaptureReplay {
    body: Vec<u8>,
    cursor: usize,
    closed: bool,
    /// Rows yielded so far, indexed by stream id; seeds the synthetic
    /// per-stream `venue_seq`.
    emitted: Vec<u64>,
    /// Venue id from the container header.
    pub venue: u16,
    /// Capture date, `YYYYMMDD`, from the container header.
    pub date: u32,
    /// Schema id from the container header.
    pub schema: u16,
}

impl CaptureReplay {
    /// Parse the 16-byte header and decompress the zstd body from `reader`.
    pub fn from_reader<R: Read>(mut reader: R) -> Result<Self, CaptureError> {
        let meta = md_capture::read_header(&mut reader)?;
        let body = zstd::decode_all(reader)?;

        Ok(CaptureReplay {
            body,
            cursor: 0,
            closed: false,
            emitted: Vec::new(),
            venue: meta.venue,
            date: meta.date,
            schema: meta.schema,
        })
    }

    // row = stream:u16 LE + ts_recv:u64 LE + len:u32 LE + payload. Bounds
    // check instead of panic so a truncated tail reads as end of container.
    fn next_row(&mut self) -> Option<CaptureFrame> {
        const ROW_HEADER: usize = 2 + 8 + 4;
        if self.cursor + ROW_HEADER > self.body.len() {
            return None;
        }
        let b = &self.body[self.cursor..];
        let stream = u16::from_le_bytes([b[0], b[1]]);
        let ts_recv = u64::from_le_bytes([b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9]]);
        let len = u32::from_le_bytes([b[10], b[11], b[12], b[13]]) as usize;
        let payload_start = self.cursor + ROW_HEADER;
        if payload_start + len > self.body.len() {
            return None;
        }
        let payload = self.body[payload_start..payload_start + len].to_vec();
        self.cursor = payload_start + len;
        let slot = usize::from(stream);
        if slot >= self.emitted.len() {
            self.emitted.resize(slot + 1, 0);
        }
        self.emitted[slot] += 1;
        let venue_seq = self.emitted[slot];
        Some(CaptureFrame {
            payload,
            ts_recv,
            stream: StreamId(stream),
            venue_seq,
        })
    }
}

impl IngestSource for CaptureReplay {
    type Frame = CaptureFrame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let mut n = 0;
        while n < max {
            match self.next_row() {
                Some(frame) => {
                    out.push(frame);
                    n += 1;
                }
                None => break,
            }
        }
        // hit true end this call (not just max=0): next call reports Closed.
        if n == 0 && max > 0 {
            self.closed = true;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use md_pipeline::{
        IngestError, IngestFrame, IngestSource, StreamId,
        ingest::testing::{MockFrame, ScenarioSpec},
    };

    use super::{CaptureError, CaptureReplay};

    fn frame(byte: u8) -> MockFrame {
        MockFrame {
            payload: vec![byte],
            ts_recv: byte as u64,
            stream: StreamId(0),
        }
    }

    // Encode a raw capture container (header + zstd body) from scenario frames.
    fn build(spec: ScenarioSpec) -> CaptureReplay {
        let mut body = Vec::new();
        for f in &spec.frames {
            body.extend_from_slice(&f.stream.0.to_le_bytes());
            body.extend_from_slice(&f.ts_recv.to_le_bytes());
            body.extend_from_slice(&(f.payload.len() as u32).to_le_bytes());
            body.extend_from_slice(&f.payload);
        }
        let compressed = zstd::encode_all(Cursor::new(body), 0).expect("zstd encode");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"MDC1");
        bytes.extend_from_slice(&1u16.to_le_bytes()); // venue
        bytes.extend_from_slice(&20260101u32.to_le_bytes()); // date
        bytes.extend_from_slice(&1u16.to_le_bytes()); // schema
        bytes.push(0); // kind = raw
        bytes.extend_from_slice(&[0, 0, 0]); // pad
        bytes.extend_from_slice(&compressed);

        CaptureReplay::from_reader(Cursor::new(bytes)).expect("build capture container")
    }

    #[test]
    fn empty_yields_zero() {
        let mut s = build(ScenarioSpec::default());
        let mut out = Vec::new();
        assert_eq!(s.recv_batch(&mut out, 8), Ok(0));
        assert!(out.is_empty());
    }

    #[test]
    fn respects_max() {
        let frames: Vec<_> = (0..10).map(frame).collect();
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        let n = s.recv_batch(&mut out, 4).unwrap();
        assert!(n <= 4);
        assert_eq!(out.len(), n);
    }

    #[test]
    fn drains_in_order() {
        let frames: Vec<_> = (0..3).map(frame).collect();
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        while let Ok(n) = s.recv_batch(&mut out, 8) {
            if n == 0 {
                break;
            }
        }
        let seen: Vec<u8> = out.iter().map(|f| f.payload()[0]).collect();
        assert_eq!(seen, vec![0u8, 1, 2]);
    }

    #[test]
    fn closed_at_end() {
        let frames: Vec<_> = (0..2).map(frame).collect();
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        while let Ok(n) = s.recv_batch(&mut out, 8) {
            if n == 0 {
                break;
            }
        }
        assert_eq!(s.recv_batch(&mut out, 8), Err(IngestError::Closed));
    }

    // Stream ids are per-row data in this container, not hardcoded like pcap or
    // the venue dump reader; a multi-stream container must round-trip them.
    #[test]
    fn preserves_stream_id() {
        let frames = vec![
            MockFrame {
                payload: vec![9],
                ts_recv: 1,
                stream: StreamId(7),
            },
            MockFrame {
                payload: vec![9],
                ts_recv: 2,
                stream: StreamId(42),
            },
        ];
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        assert_eq!(out[0].stream(), StreamId(7));
        assert_eq!(out[1].stream(), StreamId(42));
    }

    // venue_seq counts per stream, so an adapter's per-stream gap check never
    // reads an interleaved row of another stream as a skip.
    #[test]
    fn synthesizes_venue_seq_per_stream() {
        let frames: Vec<_> = [0, 1, 0, 0, 1]
            .into_iter()
            .enumerate()
            .map(|(i, stream)| MockFrame {
                payload: vec![i as u8],
                ts_recv: i as u64,
                stream: StreamId(stream),
            })
            .collect();
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        let seqs: Vec<(u16, u64)> = out.iter().map(|f| (f.stream().0, f.venue_seq())).collect();
        assert_eq!(seqs, vec![(0, 1), (1, 1), (0, 2), (0, 3), (1, 2)]);
    }

    // A container that doesn't start with MDC1 is rejected up front, not decoded
    // into garbage rows.
    #[test]
    fn rejects_bad_magic() {
        let bytes = vec![0u8; 16];
        match CaptureReplay::from_reader(Cursor::new(bytes)) {
            Err(CaptureError::BadMagic { .. }) => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }
}
