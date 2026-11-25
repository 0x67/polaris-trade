//! Published binary-file distribution replay. 2-byte big-endian length
//! prefix per message, no in-file timestamps: the driver supplies pacing
//! metadata instead.
//!
//! Reader-backed and streaming: messages are pulled one at a time from the
//! underlying reader in `recv_batch`, never buffering the whole file, so a
//! multi-gigabyte day replays in constant memory. `from_gz` streams gzip
//! members through the same path, including concatenated multi-member files
//! (gzip is sequential-forward, replay's exact access pattern). Framing
//! verified against a real TotalView-ITCH 5.0
//! sample (2026-05-18): `[len:u16 BE][message]`, message length matching the
//! ITCH message set (S=12, R=39, ...).
//!
//! Throughput: reads go through a 64 KiB `BufReader`, so per-message length
//! and payload reads hit memory not the kernel, and payloads read into fresh
//! capacity, no zeroing. gzip inflate is the ceiling for a `.gz`; when
//! replaying a day repeatedly, decompress once to a `.bin` and use `from_path`.

use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

use flate2::read::MultiGzDecoder;
use md_pipeline::{IngestError, IngestFrame, IngestSource, StreamId};

/// Read-buffer size. Big enough that per-message length + payload reads land in
/// memory, not a syscall each, and that gzip inflate refills in large chunks.
const READ_BUF: usize = 64 * 1024;

/// One message from a length-prefixed binary dump. `ts_recv` is synthetic
/// (constructor base plus per-message step), not read from the file: this
/// format carries no receive timestamp. The message's own venue timestamp
/// is read by the adapter at decode time, not by this container. `venue_seq`
/// is likewise synthetic: no real wire sequence exists in this format, so it
/// is the container's own 1-based emitted count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryFileFrame {
    payload: Vec<u8>,
    ts_recv: u64,
    venue_seq: u64,
}

impl IngestFrame for BinaryFileFrame {
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

/// Failure opening a binary dump path.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BinaryFileError {
    /// Opening the dump file failed.
    #[error("binary dump io: {0}")]
    Io(#[from] std::io::Error),
}

/// Replays a 2-byte-BE-length-prefixed venue binary dump as an
/// [`IngestSource`], streaming messages straight from the reader.
///
/// Construct from any reader with [`new`](Self::new), from an uncompressed
/// `.bin` path with [`from_path`](Self::from_path), or from a gzip `.gz` path
/// with [`from_gz`](Self::from_gz). A real io error mid-stream (short of a
/// clean end-of-file) stops the source and is retained on [`io_error`](Self::io_error).
pub struct BinaryFileReplay {
    reader: Box<dyn Read>,
    closed: bool,
    base_ts: u64,
    step_ns: u64,
    emitted: u64,
    io_error: Option<io::Error>,
}

impl std::fmt::Debug for BinaryFileReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryFileReplay")
            .field("closed", &self.closed)
            .field("emitted", &self.emitted)
            .field("base_ts", &self.base_ts)
            .field("step_ns", &self.step_ns)
            .finish_non_exhaustive()
    }
}

impl BinaryFileReplay {
    /// Stream from any reader. `base_ts` is the synthetic `ts_recv` of the
    /// first message, nanoseconds; `step_ns` is added per subsequent message
    /// since the format carries no real timestamps. The reader is wrapped in a
    /// `BufReader` so the per-message length + payload reads are not a syscall
    /// each.
    pub fn new<R: Read + 'static>(reader: R, base_ts: u64, step_ns: u64) -> Self {
        BinaryFileReplay {
            reader: Box::new(BufReader::with_capacity(READ_BUF, reader)),
            closed: false,
            base_ts,
            step_ns,
            emitted: 0,
            io_error: None,
        }
    }

    /// Stream an uncompressed `.bin` dump from `path`.
    pub fn from_path<P: AsRef<Path>>(
        path: P,
        base_ts: u64,
        step_ns: u64,
    ) -> Result<Self, BinaryFileError> {
        Ok(Self::new(File::open(path)?, base_ts, step_ns))
    }

    /// Stream a gzip `.gz` dump from `path`, inflating on the fly. Constant
    /// memory: gzip members are decoded incrementally, never fully buffered.
    /// Concatenated multi-member archives (rotation-style `cat a.gz b.gz`)
    /// inflate through every member, not only the first.
    pub fn from_gz<P: AsRef<Path>>(
        path: P,
        base_ts: u64,
        step_ns: u64,
    ) -> Result<Self, BinaryFileError> {
        let gz = MultiGzDecoder::new(BufReader::with_capacity(READ_BUF, File::open(path)?));
        Ok(Self::new(gz, base_ts, step_ns))
    }

    /// Materialize a whole uncompressed `.bin` dump in memory, then stream
    /// from the buffer. Frames byte-identical to [`from_path`](Self::from_path);
    /// trades memory for isolating pipeline cost from file io.
    pub fn from_path_eager<P: AsRef<Path>>(
        path: P,
        base_ts: u64,
        step_ns: u64,
    ) -> Result<Self, BinaryFileError> {
        Ok(Self::new(
            io::Cursor::new(std::fs::read(path)?),
            base_ts,
            step_ns,
        ))
    }

    /// Fully inflate a gzip `.gz` dump in memory, then stream from the
    /// inflated buffer. Frames byte-identical to [`from_gz`](Self::from_gz);
    /// a corrupt gzip fails here at construction instead of surfacing on
    /// [`io_error`](Self::io_error) at drain.
    pub fn from_gz_eager<P: AsRef<Path>>(
        path: P,
        base_ts: u64,
        step_ns: u64,
    ) -> Result<Self, BinaryFileError> {
        let mut gz = MultiGzDecoder::new(BufReader::with_capacity(READ_BUF, File::open(path)?));
        let mut bytes = Vec::new();
        gz.read_to_end(&mut bytes)?;
        Ok(Self::new(io::Cursor::new(bytes), base_ts, step_ns))
    }

    /// The io error that ended the stream abnormally, if any. `None` after a
    /// clean end-of-file.
    pub fn io_error(&self) -> Option<&io::Error> {
        self.io_error.as_ref()
    }

    // message = len:u16 BE + that many bytes. A short read at the length prefix
    // or payload is a clean end of container (truncated tail); a real io error
    // (corrupt gzip, permission) is retained and also ends the stream.
    fn next_message(&mut self) -> Option<BinaryFileFrame> {
        let mut len_buf = [0u8; 2];
        match fill(self.reader.as_mut(), &mut len_buf) {
            Ok(2) => {}
            Ok(_) => return None,
            Err(e) => {
                self.io_error = Some(e);
                return None;
            }
        }
        let len = u16::from_be_bytes(len_buf) as usize;
        // Read into fresh capacity, not a zeroed buffer: read_to_end fills the
        // spare bytes directly, skipping the memset a vec![0; len] would pay.
        let mut payload = Vec::with_capacity(len);
        match self
            .reader
            .as_mut()
            .take(len as u64)
            .read_to_end(&mut payload)
        {
            Ok(n) if n == len => {}
            Ok(_) => return None,
            Err(e) => {
                self.io_error = Some(e);
                return None;
            }
        }
        let ts_recv = self.base_ts + self.emitted * self.step_ns;
        let venue_seq = self.emitted + 1;
        self.emitted += 1;
        Some(BinaryFileFrame {
            payload,
            ts_recv,
            venue_seq,
        })
    }
}

// Read exactly buf.len() bytes, or fewer at a clean end-of-stream. Distinct
// from read_exact so a truncated tail is a short count, not an error.
fn fill(r: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(got)
}

impl IngestSource for BinaryFileReplay {
    type Frame = BinaryFileFrame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let mut n = 0;
        while n < max {
            match self.next_message() {
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
    use std::io::{Cursor, Write};

    use flate2::{Compression, write::GzEncoder};
    use md_pipeline::{
        IngestError, IngestFrame, IngestSource,
        ingest::testing::{MockFrame, ScenarioSpec},
    };

    use super::BinaryFileReplay;

    fn frame(byte: u8) -> MockFrame {
        MockFrame {
            payload: vec![byte],
            ts_recv: byte as u64,
            stream: md_pipeline::StreamId(0),
        }
    }

    // Encode a length-prefixed binary dump from scenario frames. Timestamps in
    // the frames are ignored: this format carries none.
    fn encode(spec: &ScenarioSpec) -> Vec<u8> {
        let mut bytes = Vec::new();
        for f in &spec.frames {
            bytes.extend_from_slice(&(f.payload.len() as u16).to_be_bytes());
            bytes.extend_from_slice(&f.payload);
        }
        bytes
    }

    fn build(spec: ScenarioSpec, base_ts: u64, step_ns: u64) -> BinaryFileReplay {
        BinaryFileReplay::new(Cursor::new(encode(&spec)), base_ts, step_ns)
    }

    #[test]
    fn empty_yields_zero() {
        let mut s = build(ScenarioSpec::default(), 0, 1);
        let mut out = Vec::new();
        assert_eq!(s.recv_batch(&mut out, 8), Ok(0));
        assert!(out.is_empty());
    }

    #[test]
    fn respects_max() {
        let frames: Vec<_> = (0..10).map(frame).collect();
        let mut s = build(
            ScenarioSpec {
                frames,
                inject_exhaustion: false,
            },
            0,
            1,
        );
        let mut out = Vec::new();
        let n = s.recv_batch(&mut out, 4).unwrap();
        assert!(n <= 4);
        assert_eq!(out.len(), n);
    }

    #[test]
    fn drains_in_order() {
        let frames: Vec<_> = (0..3).map(frame).collect();
        let mut s = build(
            ScenarioSpec {
                frames,
                inject_exhaustion: false,
            },
            0,
            1,
        );
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
        let mut s = build(
            ScenarioSpec {
                frames,
                inject_exhaustion: false,
            },
            0,
            1,
        );
        let mut out = Vec::new();
        while let Ok(n) = s.recv_batch(&mut out, 8) {
            if n == 0 {
                break;
            }
        }
        assert_eq!(s.recv_batch(&mut out, 8), Err(IngestError::Closed));
    }

    // No in-file timestamps: ts_recv must come from base_ts + emitted*step, not
    // from message content or position alone.
    #[test]
    fn synthesizes_ts_recv_from_base_and_step() {
        let frames: Vec<_> = (0..3).map(frame).collect();
        let mut s = build(
            ScenarioSpec {
                frames,
                inject_exhaustion: false,
            },
            1_000,
            500,
        );
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        let ts: Vec<u64> = out.iter().map(|f| f.ts_recv()).collect();
        assert_eq!(ts, vec![1_000, 1_500, 2_000]);
    }

    // No real wire sequence in this format: venue_seq is the container's own
    // 1-based emitted count, independent of ts_recv's base/step synthesis.
    #[test]
    fn synthesizes_venue_seq_from_emitted_count() {
        let frames: Vec<_> = (0..3).map(frame).collect();
        let mut s = build(
            ScenarioSpec {
                frames,
                inject_exhaustion: false,
            },
            0,
            1,
        );
        let mut out = Vec::new();
        s.recv_batch(&mut out, 8).unwrap();
        let seqs: Vec<u64> = out.iter().map(|f| f.venue_seq()).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    // A truncated payload tail (length says more bytes than remain) reads as a
    // clean end of container, not a panic or error.
    #[test]
    fn truncated_tail_ends_cleanly() {
        // one full message, then a length prefix promising 4 bytes with only 1
        let mut bytes = vec![0x00, 0x01, 0xAA]; // len 1, payload 0xAA
        bytes.extend_from_slice(&[0x00, 0x04, 0xBB]); // len 4, only 1 byte follows
        let mut s = BinaryFileReplay::new(Cursor::new(bytes), 0, 1);
        let mut out = Vec::new();
        while let Ok(n) = s.recv_batch(&mut out, 8) {
            if n == 0 {
                break;
            }
        }
        assert_eq!(out.len(), 1, "only the complete message is yielded");
        assert_eq!(out[0].payload(), &[0xAA]);
        assert!(
            s.io_error().is_none(),
            "a truncated tail is not an io error"
        );
    }

    // Gzip streams through the same reader-backed path: inflate a compressed
    // BinaryFILE on the fly and drain it identically to the plain reader.
    #[test]
    fn streams_gzip_through_decoder() {
        use flate2::read::GzDecoder;

        let spec = ScenarioSpec {
            frames: (0..3).map(frame).collect(),
            inject_exhaustion: false,
        };
        let plain = encode(&spec);
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&plain).unwrap();
        let gz = enc.finish().unwrap();

        let mut s = BinaryFileReplay::new(GzDecoder::new(Cursor::new(gz)), 10, 5);
        let mut out = Vec::new();
        while let Ok(n) = s.recv_batch(&mut out, 8) {
            if n == 0 {
                break;
            }
        }
        let seen: Vec<u8> = out.iter().map(|f| f.payload()[0]).collect();
        let ts: Vec<u64> = out.iter().map(|f| f.ts_recv()).collect();
        assert_eq!(seen, vec![0u8, 1, 2], "gzip inflates to the same messages");
        assert_eq!(ts, vec![10, 15, 20]);
    }
}
