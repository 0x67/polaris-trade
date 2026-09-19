//! Producer side of provided-buffer ring: page-aligned anonymous mapping of
//! `io_uring_buf` entries, filled by [`RingMem::push`], handed to kernel by
//! [`RingMem::publish`].
//!
//! Kernel keeps ring tail in `resv` field of entry 0, so pushes write entry
//! fields one by one and never touch `resv`; only `publish` stores tail.

use std::{
    io,
    mem::size_of,
    ptr::{self, NonNull},
    sync::atomic::{AtomicU16, Ordering},
};

use io_uring::{Submitter, types::BufRingEntry};
use transport_core::TransportError;

/// `struct io_uring_buf`; `resv` of entry 0 is ring tail.
#[repr(C)]
struct Entry {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

// ABI pin: kernel entry is 16 bytes, as io-uring crate's own view of it
const _: () = assert!(size_of::<Entry>() == 16 && size_of::<BufRingEntry>() == 16);

/// Buffer ring of power-of-two entries, unmapped on drop.
#[derive(Debug)]
pub(crate) struct RingMem {
    entries: NonNull<Entry>,
    bytes: usize,
    mask: u16,
    // next free index before masking; kernel sees it only after `publish`
    tail: u16,
}

// SAFETY: `RingMem` owns its mapping outright; no thread affinity, and every
// write goes through `&mut self` or atomic tail store.
unsafe impl Send for RingMem {}

impl RingMem {
    /// Zeroed ring of `count` entries; `count` power of two, at most 32768.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when `mmap` fails.
    pub(crate) fn new(count: u16) -> Result<Self, TransportError> {
        debug_assert!(count.is_power_of_two(), "ring entries not power of two");
        let bytes = usize::from(count) * size_of::<Entry>();
        // SAFETY: fresh private anonymous mapping: no fd, no fixed address, so
        // no existing memory is replaced.
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(TransportError::Io {
                stage: "mmap",
                error: io::Error::last_os_error(),
            });
        }
        let entries = NonNull::new(raw.cast()).ok_or(TransportError::Io {
            stage: "mmap",
            error: io::ErrorKind::OutOfMemory.into(),
        })?;
        Ok(Self {
            entries,
            bytes,
            mask: count - 1,
            tail: 0,
        })
    }

    /// Register ring as buffer group `bgid` of `submitter`'s ring.
    ///
    /// # Safety
    ///
    /// `self` stays alive until group is unregistered or ring is dropped, and
    /// until no request that may select from group is armed.
    pub(crate) unsafe fn register(&self, submitter: &Submitter<'_>, bgid: u16) -> io::Result<()> {
        // SAFETY: mapping page-aligned (mmap) and `mask + 1` entries long;
        // caller keeps it alive past every kernel use.
        unsafe {
            submitter.register_buf_ring_with_flags(
                self.entries.as_ptr().addr() as u64,
                self.mask.wrapping_add(1),
                bgid,
                0,
            )
        }
    }

    /// Queue buffer `bid` of `len` bytes at `addr`. Kernel sees it after
    /// [`publish`](Self::publish).
    ///
    /// Caller never queues more buffers than ring holds: at most `mask + 1`
    /// between kernel head and local tail.
    pub(crate) fn push(&mut self, addr: u64, len: u32, bid: u16) {
        let at = usize::from(self.tail & self.mask);
        // SAFETY: `at <= mask`, inside mapping; kernel reads entries only below
        // published tail, and slot `at` is past it. Field writes leave `resv`
        // alone: on entry 0 it is tail kernel reads concurrently.
        unsafe {
            let entry = self.entries.as_ptr().add(at);
            (&raw mut (*entry).addr).write(addr);
            (&raw mut (*entry).len).write(len);
            (&raw mut (*entry).bid).write(bid);
        }
        self.tail = self.tail.wrapping_add(1);
    }

    /// Hand every pushed entry to kernel: one Release store of tail.
    pub(crate) fn publish(&self) {
        // SAFETY: entry 0 `resv` is 2-aligned inside mapping, lives as long as
        // `self`, and is only ever accessed atomically (kernel loads acquire).
        let tail = unsafe { AtomicU16::from_ptr(&raw mut (*self.entries.as_ptr()).resv) };
        tail.store(self.tail, Ordering::Release);
    }
}

impl Drop for RingMem {
    fn drop(&mut self) {
        // SAFETY: mapping made by `new` with exactly `bytes`; unmapped once.
        // Owner drops ring only once kernel cannot select from it.
        unsafe { libc::munmap(self.entries.as_ptr().cast(), self.bytes) };
    }
}

#[cfg(test)]
mod tests {
    use super::RingMem;

    // (addr, len, bid) of entry `at`
    fn entry(ring: &RingMem, at: usize) -> (u64, u32, u16) {
        // SAFETY: test indexes stay below entry count; no kernel shares mapping
        let e = unsafe { &*ring.entries.as_ptr().add(at) };
        (e.addr, e.len, e.bid)
    }

    fn published(ring: &RingMem) -> u16 {
        // SAFETY: entry 0 lives in mapping; no kernel shares it in this test
        unsafe { (*ring.entries.as_ptr()).resv }
    }

    #[test]
    fn push_and_publish_wrap_u16_tail_without_touching_published_tail() {
        let mut ring = RingMem::new(4).expect("anonymous mapping");
        for i in 0..u32::from(u16::MAX) - 1 {
            ring.push(u64::from(i), 1, 0);
        }
        ring.publish();
        assert_eq!(published(&ring), u16::MAX - 1);

        let pushed = [
            (0xA000, 10, 11),
            (0xB000, 20, 12),
            (0xC000, 30, 13),
            (0xD000, 40, 14),
        ];
        for &(addr, len, bid) in &pushed[..3] {
            ring.push(addr, len, bid);
        }
        // third push landed in entry 0, whose `resv` is tail kernel reads
        assert_eq!(
            published(&ring),
            u16::MAX - 1,
            "push changed published tail"
        );
        let (addr, len, bid) = pushed[3];
        ring.push(addr, len, bid);
        ring.publish();
        assert_eq!(published(&ring), 2, "tail wraps modulo 2^16");
        // tails 65534, 65535, 0, 1 masked by 3
        for (at, want) in [2, 3, 0, 1].into_iter().zip(pushed) {
            assert_eq!(entry(&ring, at), want, "entry {at}");
        }
    }
}
