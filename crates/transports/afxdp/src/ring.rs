//! Single-producer, single-consumer index protocol of `AF_XDP` rings.
//!
//! Kernel and process share producer word, consumer word and descriptor array.
//! Indices run free and wrap at `u32`; entry of index `i` sits at `i & (size - 1)`.
//! Writer publishes entries with Release store of its word; reader Acquire-loads
//! that word before reading entries, then hands them back with Release store of
//! its own. No syscall here, so tests run on heap memory under Miri.

#[cfg(test)]
pub(crate) mod heap;
#[cfg(test)]
mod tests;

use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

/// Addresses of one ring's shared words and descriptor array.
pub(crate) struct RingPtrs<T> {
    pub(crate) producer: NonNull<AtomicU32>,
    pub(crate) consumer: NonNull<AtomicU32>,
    pub(crate) flags: NonNull<AtomicU32>,
    pub(crate) desc: NonNull<T>,
}

// shared memory, built only by unsafe `Ring::new`
struct Ring<T> {
    ptrs: RingPtrs<T>,
    mask: u32,
}

impl<T> Ring<T> {
    /// # Safety
    ///
    /// Contract of [`Producer::new`].
    unsafe fn new(ptrs: RingPtrs<T>, size: u32) -> Self {
        debug_assert!(size.is_power_of_two(), "ring size not a power of two");
        Self {
            ptrs,
            mask: size - 1,
        }
    }

    fn size(&self) -> u32 {
        self.mask + 1
    }

    fn producer(&self) -> &AtomicU32 {
        // SAFETY: `new` contract: live, aligned word while ring lives
        unsafe { self.ptrs.producer.as_ref() }
    }

    fn consumer(&self) -> &AtomicU32 {
        // SAFETY: `new` contract: live, aligned word while ring lives
        unsafe { self.ptrs.consumer.as_ref() }
    }

    fn flags(&self) -> &AtomicU32 {
        // SAFETY: `new` contract: live, aligned word while ring lives
        unsafe { self.ptrs.flags.as_ref() }
    }

    // entry of free-running `index`; masking keeps it inside array
    fn entry(&self, index: u32) -> NonNull<T> {
        // SAFETY: `index & mask < size`, array holds `size` entries (`new` contract)
        unsafe { self.ptrs.desc.add((index & self.mask) as usize) }
    }
}

/// Process side of ring process fills and kernel drains (fill ring).
pub(crate) struct Producer<T> {
    ring: Ring<T>,
    // next index to write; equals producer word after each publish
    prod: u32,
}

/// Process side of ring kernel fills and process drains (receive ring).
pub(crate) struct Consumer<T> {
    ring: Ring<T>,
    // next index to read; equals consumer word after each release
    cons: u32,
}

// SAFETY: ring memory is shared only with kernel, through atomic words and
// entries published by them; `&mut self` keeps process side on one thread at a time.
unsafe impl<T: Send> Send for Producer<T> {}
// SAFETY: as `Producer`.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T: Copy> Producer<T> {
    /// Take over process side of ring at `ptrs`, resuming at its producer word.
    ///
    /// # Safety
    ///
    /// `ptrs` words are aligned, live `AtomicU32`s and `desc` aligned, live array
    /// of `size` entries, all valid until producer drops; peer writes only
    /// consumer and flags words; `size` power of two; no other producer.
    pub(crate) unsafe fn new(ptrs: RingPtrs<T>, size: u32) -> Self {
        // SAFETY: forwarded caller contract
        let ring = unsafe { Ring::new(ptrs, size) };
        let prod = ring.producer().load(Ordering::Relaxed);
        Self { ring, prod }
    }

    /// Write entries from `items` until ring full or `items` ends, then publish
    /// once. Returns count written; items not taken stay in `items`.
    pub(crate) fn produce(&mut self, items: impl Iterator<Item = T>) -> u32 {
        let cons = self.ring.consumer().load(Ordering::Acquire);
        // peer lags at most `size` behind; clamp keeps corrupt word from overwriting
        let free = cons
            .wrapping_add(self.ring.size())
            .wrapping_sub(self.prod)
            .min(self.ring.size());
        let mut written = 0;
        for item in items.take(free as usize) {
            let at = self.ring.entry(self.prod.wrapping_add(written));
            // SAFETY: entry inside array; index lies in `cons..cons + size`, so
            // peer has released it and reads it only after publish below
            unsafe { at.write(item) };
            written += 1;
        }
        if written > 0 {
            self.prod = self.prod.wrapping_add(written);
            self.ring.producer().store(self.prod, Ordering::Release);
        }
        written
    }

    /// Kernel asks for syscall kick before it takes new entries.
    pub(crate) fn needs_wakeup(&self) -> bool {
        self.ring.flags().load(Ordering::Relaxed) & libc::XDP_RING_NEED_WAKEUP != 0
    }
}

impl<T: Copy> Consumer<T> {
    /// Take over process side of ring at `ptrs`, resuming at its consumer word.
    ///
    /// # Safety
    ///
    /// As [`Producer::new`], with peer writing only producer and flags words and
    /// entries it has not yet published.
    pub(crate) unsafe fn new(ptrs: RingPtrs<T>, size: u32) -> Self {
        // SAFETY: forwarded caller contract
        let ring = unsafe { Ring::new(ptrs, size) };
        let cons = ring.consumer().load(Ordering::Relaxed);
        Self { ring, cons }
    }

    /// Pass up to `max` published entries to `f` in ring order, then release
    /// them to peer at once. Returns count passed.
    pub(crate) fn consume(&mut self, max: u32, mut f: impl FnMut(T)) -> u32 {
        let prod = self.ring.producer().load(Ordering::Acquire);
        // clamp keeps corrupt word from replaying stale entries past one lap
        let ready = prod.wrapping_sub(self.cons).min(self.ring.size());
        let taken = ready.min(max);
        for i in 0..taken {
            let at = self.ring.entry(self.cons.wrapping_add(i));
            // SAFETY: entry inside array; peer published it (Acquire above) and
            // rewrites it only after release below
            f(unsafe { at.read() });
        }
        if taken > 0 {
            self.cons = self.cons.wrapping_add(taken);
            self.ring.consumer().store(self.cons, Ordering::Release);
        }
        taken
    }
}
