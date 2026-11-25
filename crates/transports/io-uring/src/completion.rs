//! Pure classification of recv completions: what one CQE means for slot and
//! request. Public only so `benches/classify.rs` reaches it; not API.

use io_uring::cqueue;

/// Meaning of one recv completion.
///
/// `rearm`: request ended with this completion, so driver arms another.
/// Single-shot recv always ends; multishot ends on completion lacking `F_MORE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    /// Datagram of `len` bytes, at most slot size, landed at start of `slot`.
    Data {
        /// Buffer id, equal to pool slot.
        slot: u16,
        /// Datagram length.
        len: u32,
        /// Request ended.
        rearm: bool,
    },
    /// Datagram longer than slot (`MSG_TRUNC` reports real length); `slot`
    /// holds cut copy and goes straight back to kernel.
    Truncated {
        /// Buffer id, equal to pool slot.
        slot: u16,
        /// Request ended.
        rearm: bool,
    },
    /// Empty datagram on kernel 6.0+: kernel recycled buffer itself, so no
    /// buffer id, no slot, no frame. Before 6.0 same datagram is `Data` with
    /// `len` 0.
    Empty {
        /// Request ended.
        rearm: bool,
    },
    /// ENOBUFS: group had no buffer. Request ended; data, if any, stays queued
    /// in socket.
    NoBuffers,
    /// Request failed with this errno and ended. Non-empty success without
    /// buffer id breaks buffer-select contract and reports `EIO`.
    Failed(i32),
}

/// Classify recv completion `res`/`flags` for slots of `slot_size` bytes;
/// `multishot` when request was multishot recv.
#[inline]
pub fn classify(res: i32, flags: u32, slot_size: u32, multishot: bool) -> Completion {
    if res < 0 {
        return if res == -libc::ENOBUFS {
            Completion::NoBuffers
        } else {
            Completion::Failed(-res)
        };
    }
    let rearm = !(multishot && cqueue::more(flags));
    let Some(slot) = cqueue::buffer_select(flags) else {
        return if res == 0 {
            Completion::Empty { rearm }
        } else {
            Completion::Failed(libc::EIO)
        };
    };
    let len = res.unsigned_abs();
    if len > slot_size {
        Completion::Truncated { slot, rearm }
    } else {
        Completion::Data { slot, len, rearm }
    }
}

#[cfg(test)]
mod tests;
