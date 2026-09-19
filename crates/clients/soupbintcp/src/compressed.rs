//! Streaming zlib inflate for Nasdaq compressed `SoupBinTCP` variant.
//! Server to client only: client writes bypass it.

use bytes::BytesMut;
use flate2::{Decompress, FlushDecompress, Status};

use crate::error::SoupBinError;

/// `flate2::Decompress` (zlib framing) over transport read side. `feed`
/// inflates one chunk per call; output holds only latest chunk's bytes.
pub struct CompressedReader {
    inflator: Decompress,
    inflated: BytesMut,
    // cap on one feed's output: zlib bomb hits it instead of growing scratch
    // without bound; sized from client decode budget
    max_inflated: usize,
}

impl CompressedReader {
    /// `inflated_capacity` is both starting buffer size and hard ceiling on one
    /// `feed`'s output: inflate past it errors instead of allocating without
    /// bound, so hostile or corrupt zlib stream cannot exhaust memory.
    pub fn new(inflated_capacity: usize) -> Self {
        Self {
            inflator: Decompress::new(true), // zlib framing (header + adler32), not raw deflate
            inflated: BytesMut::with_capacity(inflated_capacity),
            max_inflated: inflated_capacity,
        }
    }

    /// Inflate `compressed`, returning bytes this call produced; valid until
    /// next `feed`.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::ProtocolViolation`] on corrupt or stuck stream;
    /// [`SoupBinError::FrameTooLarge`] when output passes cap.
    pub fn feed(&mut self, compressed: &[u8]) -> Result<&[u8], SoupBinError> {
        self.inflated.clear();
        let mut scratch = Vec::with_capacity(self.inflated.capacity().max(4096));
        let mut input = compressed;
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 1_000_000 {
                return Err(SoupBinError::ProtocolViolation(
                    "zlib inflate made no progress".into(),
                ));
            }
            if scratch.len() == scratch.capacity() {
                scratch.reserve(scratch.capacity().max(4096));
            }
            let before_in = self.inflator.total_in();
            let status = self
                .inflator
                .decompress_vec(input, &mut scratch, FlushDecompress::None)
                .map_err(|e| {
                    SoupBinError::ProtocolViolation(format!("zlib inflate failed: {e}"))
                })?;
            let consumed = usize::try_from(self.inflator.total_in() - before_in)
                .map_or(input.len(), |n| n.min(input.len()));
            input = &input[consumed..];
            if scratch.len() > self.max_inflated {
                return Err(SoupBinError::FrameTooLarge {
                    size: scratch.len(),
                    max: self.max_inflated,
                });
            }
            if status == Status::StreamEnd || input.is_empty() {
                break;
            }
        }
        self.inflated.extend_from_slice(&scratch);
        Ok(&self.inflated)
    }
}
