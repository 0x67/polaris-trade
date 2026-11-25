//! Streaming zlib inflate for Nasdaq compressed `SoupBinTCP` variant.
//! Server to client only: client writes bypass it.

use flate2::{Decompress, FlushDecompress, Status};

use crate::error::SoupBinError;

/// `flate2::Decompress` (zlib framing) over transport read side. Each `feed`
/// inflates at most cap bytes, so high ratio input (replay backlog, zlib bomb)
/// drains in bounded steps instead of growing memory.
pub struct CompressedReader {
    inflator: Decompress,
    // latest `feed` output; length is per-call cap, allocated once
    out: Box<[u8]>,
    produced: usize,
}

impl CompressedReader {
    /// `inflated_capacity` bounds one `feed`'s output and is allocated once
    /// here, so hostile or corrupt zlib stream cannot exhaust memory.
    pub fn new(inflated_capacity: usize) -> Self {
        Self {
            inflator: Decompress::new(true), // zlib framing (header + adler32), not raw deflate
            out: vec![0; inflated_capacity].into_boxed_slice(),
            produced: 0,
        }
    }

    /// Inflate front of `compressed` until input runs out, stream ends or
    /// output reaches cap. Returns input bytes consumed and bytes produced,
    /// valid until next `feed`. Caller keeps unconsumed tail and feeds it
    /// again once output is drained; bytes past stream end count as consumed.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::ProtocolViolation`] on corrupt stream, or when
    /// non-empty input makes no progress.
    pub fn feed(&mut self, compressed: &[u8]) -> Result<(usize, &[u8]), SoupBinError> {
        let (start_in, start_out) = (self.inflator.total_in(), self.inflator.total_out());
        let mut consumed = 0;
        self.produced = 0;
        while self.produced < self.out.len() {
            let before = (consumed, self.produced);
            let status = self
                .inflator
                .decompress(
                    &compressed[consumed..],
                    &mut self.out[self.produced..],
                    FlushDecompress::None,
                )
                .map_err(|e| {
                    SoupBinError::ProtocolViolation(format!("zlib inflate failed: {e}"))
                })?;
            consumed = moved(self.inflator.total_in(), start_in, compressed.len());
            self.produced = moved(self.inflator.total_out(), start_out, self.out.len());
            if status == Status::StreamEnd {
                // nothing inflates past stream end; dropping tail avoids stall on it
                consumed = compressed.len();
                break;
            }
            // one call may only drain pending output, so loop until it stalls
            if (consumed, self.produced) == before {
                break;
            }
        }
        if consumed == 0 && self.produced == 0 && !compressed.is_empty() {
            return Err(SoupBinError::ProtocolViolation(
                "zlib inflate made no progress".into(),
            ));
        }
        Ok((consumed, &self.out[..self.produced]))
    }

    /// Latest `feed` filled its cap: inflator may still hold output, so feed
    /// again (empty input is fine) before reading more.
    pub fn capped(&self) -> bool {
        self.produced != 0 && self.produced == self.out.len()
    }
}

/// Bytes moved since `start`, clamped to slice bound `limit`.
fn moved(total: u64, start: u64, limit: usize) -> usize {
    usize::try_from(total - start).map_or(limit, |n| n.min(limit))
}
