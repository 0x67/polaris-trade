//! Classic pcap container replay. Hand-rolled parser, no libpcap dependency
//! and no `pcap` crate. Yields captured packet bytes verbatim; L2/L3/L4
//! stripping down to the venue UDP payload is a follow-up for the adapter
//! that consumes these frames, not this container's job.

use std::io::Read;

use md_pipeline::{IngestError, IngestFrame, IngestSource, StreamId};

const MAGIC_LE_USEC: [u8; 4] = [0xd4, 0xc3, 0xb2, 0xa1];
const MAGIC_BE_USEC: [u8; 4] = [0xa1, 0xb2, 0xc3, 0xd4];
const MAGIC_LE_NANOS: [u8; 4] = [0x4d, 0x3c, 0xb2, 0xa1];
const MAGIC_BE_NANOS: [u8; 4] = [0xa1, 0xb2, 0x3c, 0x4d];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endian {
    Little,
    Big,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimeUnit {
    Micros,
    Nanos,
}

fn read_u32(b: &[u8], endian: Endian) -> u32 {
    let bytes = [b[0], b[1], b[2], b[3]];
    match endian {
        Endian::Little => u32::from_le_bytes(bytes),
        Endian::Big => u32::from_be_bytes(bytes),
    }
}

/// One captured packet: raw bytes as recorded, plus the receive time derived
/// from the per-packet header. pcap carries no stream demux, so every frame
/// reports stream 0; the adapter assigns real streams after parsing framing.
/// `venue_seq` is synthetic: pcap carries no wire sequence either, so it is
/// the container's own 1-based emitted count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PcapFrame {
    payload: Vec<u8>,
    ts_recv: u64,
    venue_seq: u64,
}

impl IngestFrame for PcapFrame {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn ts_recv(&self) -> u64 {
        self.ts_recv
    }
    fn stream(&self) -> StreamId {
        StreamId(0)
    }
    fn venue_seq(&self) -> u64 {
        self.venue_seq
    }
}

/// Construction-time failure reading a pcap container.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PcapError {
    /// Global header read failed.
    #[error("pcap container io: {0}")]
    Io(#[from] std::io::Error),
    /// First 4 header bytes matched none of the 4 known classic-pcap magics.
    #[error("bad pcap magic: {found:?}")]
    BadMagic { found: [u8; 4] },
}

/// Replays a classic pcap file as an [`IngestSource`]. Global header (24
/// bytes) plus per-packet headers (16 bytes) only; no pcapng, no libpcap
/// linkage.
#[derive(Debug)]
pub struct PcapReplay {
    body: Vec<u8>,
    cursor: usize,
    closed: bool,
    endian: Endian,
    unit: TimeUnit,
    /// Packets yielded so far; seeds the synthetic per-container `venue_seq`.
    emitted: u64,
    /// Link-layer type from the global header's `network` field.
    pub network: u32,
}

impl PcapReplay {
    /// Parse the 24-byte global header from `reader`, then buffer the rest.
    pub fn from_reader<R: Read>(mut reader: R) -> Result<Self, PcapError> {
        let mut header = [0u8; 24];
        reader.read_exact(&mut header)?;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&header[0..4]);
        let (endian, unit) = match magic {
            MAGIC_LE_USEC => (Endian::Little, TimeUnit::Micros),
            MAGIC_BE_USEC => (Endian::Big, TimeUnit::Micros),
            MAGIC_LE_NANOS => (Endian::Little, TimeUnit::Nanos),
            MAGIC_BE_NANOS => (Endian::Big, TimeUnit::Nanos),
            found => return Err(PcapError::BadMagic { found }),
        };
        let network = read_u32(&header[20..24], endian);

        let mut body = Vec::new();
        reader.read_to_end(&mut body)?;

        Ok(PcapReplay {
            body,
            cursor: 0,
            closed: false,
            endian,
            unit,
            emitted: 0,
            network,
        })
    }

    // record = ts_sec:u32 + ts_frac:u32 + incl_len:u32 + orig_len:u32 (16
    // bytes), then incl_len captured bytes. Bounds check instead of panic so
    // a truncated tail reads as end of container.
    fn next_record(&mut self) -> Option<PcapFrame> {
        const REC_HEADER: usize = 16;
        if self.cursor + REC_HEADER > self.body.len() {
            return None;
        }
        let b = &self.body[self.cursor..];
        let ts_sec = read_u32(&b[0..4], self.endian) as u64;
        let ts_frac = read_u32(&b[4..8], self.endian) as u64;
        let incl_len = read_u32(&b[8..12], self.endian) as usize;
        let payload_start = self.cursor + REC_HEADER;
        if payload_start + incl_len > self.body.len() {
            return None;
        }
        let payload = self.body[payload_start..payload_start + incl_len].to_vec();
        self.cursor = payload_start + incl_len;

        let frac_ns = match self.unit {
            TimeUnit::Micros => ts_frac * 1000,
            TimeUnit::Nanos => ts_frac,
        };
        let ts_recv = ts_sec * 1_000_000_000 + frac_ns;
        let venue_seq = self.emitted + 1;
        self.emitted += 1;
        Some(PcapFrame {
            payload,
            ts_recv,
            venue_seq,
        })
    }
}

impl IngestSource for PcapReplay {
    type Frame = PcapFrame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let mut n = 0;
        while n < max {
            match self.next_record() {
                Some(frame) => {
                    out.push(frame);
                    n += 1;
                }
                None => break,
            }
        }
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

    use super::{PcapError, PcapReplay};

    fn frame(byte: u8) -> MockFrame {
        MockFrame {
            payload: vec![byte],
            ts_recv: byte as u64,
            stream: StreamId(0),
        }
    }

    // Encode a classic pcap file (LE nanos-resolution global header, so tiny
    // ts_recv test values round-trip exactly) from scenario frames.
    fn build(spec: ScenarioSpec) -> PcapReplay {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0x4d, 0x3c, 0xb2, 0xa1]); // LE nanos magic
        bytes.extend_from_slice(&2u16.to_le_bytes()); // version_major
        bytes.extend_from_slice(&4u16.to_le_bytes()); // version_minor
        bytes.extend_from_slice(&0i32.to_le_bytes()); // thiszone
        bytes.extend_from_slice(&0u32.to_le_bytes()); // sigfigs
        bytes.extend_from_slice(&65535u32.to_le_bytes()); // snaplen
        bytes.extend_from_slice(&1u32.to_le_bytes()); // network = ethernet

        for f in &spec.frames {
            let ts_sec = (f.ts_recv / 1_000_000_000) as u32;
            let ts_frac = (f.ts_recv % 1_000_000_000) as u32;
            let len = f.payload.len() as u32;
            bytes.extend_from_slice(&ts_sec.to_le_bytes());
            bytes.extend_from_slice(&ts_frac.to_le_bytes());
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&f.payload);
        }

        PcapReplay::from_reader(Cursor::new(bytes)).expect("build pcap container")
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

    // Big-endian usec-resolution files are the classic-pcap default on
    // network-order-capture tools; ts_frac must scale by 1000 to nanoseconds.
    #[test]
    fn be_usec_header_decodes_correctly() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0xa1, 0xb2, 0xc3, 0xd4]); // BE usec magic
        bytes.extend_from_slice(&2u16.to_be_bytes());
        bytes.extend_from_slice(&4u16.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&65535u32.to_be_bytes());
        bytes.extend_from_slice(&147u32.to_be_bytes()); // network

        bytes.extend_from_slice(&5u32.to_be_bytes()); // ts_sec
        bytes.extend_from_slice(&250u32.to_be_bytes()); // ts_frac (usec)
        bytes.extend_from_slice(&3u32.to_be_bytes()); // incl_len
        bytes.extend_from_slice(&3u32.to_be_bytes()); // orig_len
        bytes.extend_from_slice(&[7, 8, 9]);

        let mut s = PcapReplay::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(s.network, 147);
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        assert_eq!(out[0].payload(), &[7, 8, 9]);
        assert_eq!(out[0].ts_recv(), 5_000_250_000);
    }

    #[test]
    fn rejects_bad_magic() {
        let bytes = vec![0u8; 24];
        match PcapReplay::from_reader(Cursor::new(bytes)) {
            Err(PcapError::BadMagic { .. }) => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    // No real wire sequence in this format: venue_seq is the container's own
    // 1-based emitted count, independent of the per-packet capture timestamp.
    #[test]
    fn synthesizes_venue_seq_from_emitted_count() {
        let frames: Vec<_> = (0..3).map(frame).collect();
        let mut s = build(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        let seqs: Vec<u64> = out.iter().map(|f| f.venue_seq()).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }
}
