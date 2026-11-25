//! Heap-backed ring whose methods play kernel side, for syscall-free tests.

use std::{
    cell::UnsafeCell,
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

use super::RingPtrs;

/// Ring memory on heap: words `[producer, consumer, flags]` and entry array.
pub(crate) struct HeapRing<T> {
    words: Box<[AtomicU32; 3]>,
    desc: Box<[UnsafeCell<T>]>,
}

impl<T: Copy> HeapRing<T> {
    /// `size` entries of `blank`, both indices at `start`.
    pub(crate) fn new(size: u32, start: u32, blank: T) -> Self {
        Self {
            words: Box::new([
                AtomicU32::new(start),
                AtomicU32::new(start),
                AtomicU32::new(0),
            ]),
            desc: (0..size).map(|_| UnsafeCell::new(blank)).collect(),
        }
    }

    /// Addresses for [`Producer::new`](super::Producer::new) or
    /// [`Consumer::new`](super::Consumer::new); valid while `self` lives.
    pub(crate) fn ptrs(&self) -> RingPtrs<T> {
        RingPtrs {
            producer: NonNull::from(&self.words[0]),
            consumer: NonNull::from(&self.words[1]),
            flags: NonNull::from(&self.words[2]),
            // from whole slice, so pointer may reach every entry
            desc: NonNull::from(&*self.desc).cast(),
        }
    }

    fn slot(&self, index: u32) -> &UnsafeCell<T> {
        &self.desc[index as usize % self.desc.len()]
    }

    /// Take oldest published entry, as kernel drains fill ring.
    pub(crate) fn pop(&self) -> Option<T> {
        let prod = self.words[0].load(Ordering::Acquire);
        let cons = self.words[1].load(Ordering::Relaxed);
        if prod == cons {
            return None;
        }
        // SAFETY: producer published entry `cons` (Acquire above) and leaves it
        // alone until consumer word passes it
        let entry = unsafe { self.slot(cons).get().read() };
        self.words[1].store(cons.wrapping_add(1), Ordering::Release);
        Some(entry)
    }

    /// Publish one entry, as kernel fills receive ring. False when ring full.
    pub(crate) fn push(&self, entry: T) -> bool {
        let prod = self.words[0].load(Ordering::Relaxed);
        let cons = self.words[1].load(Ordering::Acquire);
        if prod.wrapping_sub(cons) as usize == self.desc.len() {
            return false;
        }
        // SAFETY: entry `prod` released by consumer (Acquire above), unread
        // until producer word passes it
        unsafe { self.slot(prod).get().write(entry) };
        self.words[0].store(prod.wrapping_add(1), Ordering::Release);
        true
    }

    /// Set flags word, as kernel does to ask for wakeup.
    pub(crate) fn set_flags(&self, flags: u32) {
        self.words[2].store(flags, Ordering::Relaxed);
    }

    /// Producer and consumer words.
    pub(crate) fn indices(&self) -> (u32, u32) {
        (
            self.words[0].load(Ordering::Acquire),
            self.words[1].load(Ordering::Acquire),
        )
    }
}
