//! Mock driver: injected bytes land in pool slot at once, as NIC DMA would.

use std::{collections::VecDeque, marker::PhantomData, ptr};

use super::{Driver, DriverStats, Layer, Reap};
use crate::{
    FrameBatch, PoolStats, TransportError,
    pool::{IndexFrame, IndexPool},
};

/// Test driver over [`IndexPool`]: [`inject`](Self::inject) plays NIC, `reap`
/// delivers in injection order.
///
/// Each slot sits in exactly one place: free list, queue or live frame.
/// Injection that finds no free slot counts `no_buffer` and drops bytes, so
/// conformance suite runs mock with `ExhaustionSignal::DropCounter`; `reap`
/// never returns [`Reap::Exhausted`]. `L` is [`L4`](super::L4) for UDP
/// payloads or [`L2`](super::L2) for whole Ethernet frames.
#[derive(Debug)]
pub struct MockDriver<L> {
    pool: IndexPool,
    // slots driver may fill; never above pool capacity
    free: Vec<u32>,
    // `drain_freed` swap target, empty between calls
    freed: Vec<u32>,
    // filled slots awaiting reap: (slot, len)
    queue: VecDeque<(u32, u32)>,
    stats: DriverStats,
    // fn pointer: `Send` never depends on `L`
    layer: PhantomData<fn() -> L>,
}

impl<L> MockDriver<L> {
    /// Take every slot of `pool`, which must hold no live frame. Allocates
    /// here once; `inject` and `reap` never allocate.
    ///
    /// # Panics
    ///
    /// When `pool` has live frame: its slot would be listed free, and safe
    /// `inject` would write bytes that frame still reads.
    pub fn new(pool: IndexPool) -> Self {
        let count = pool.stats().capacity;
        assert_eq!(pool.stats().in_use, 0, "MockDriver::new over live frames");
        let mut freed = Vec::with_capacity(count);
        // slots freed before hand-over are already in `free`; keeping them would list them twice
        pool.drain_freed(&mut freed);
        freed.clear();
        Self {
            free: (0..).take(count).collect(),
            freed,
            queue: VecDeque::with_capacity(count),
            stats: DriverStats::default(),
            layer: PhantomData,
            pool,
        }
    }

    /// Land `bytes` in free slot, queued for next reap.
    ///
    /// Longer than slot stride: counts `truncated`, dropped. No free slot:
    /// counts `no_buffer`, dropped.
    pub fn inject(&mut self, bytes: &[u8]) {
        self.recycle();
        let stride = self.pool.stride();
        let Some(len) = u32::try_from(bytes.len()).ok().filter(|&len| len <= stride) else {
            self.stats.truncated += 1;
            return;
        };
        let Some(slot) = self.free.pop() else {
            self.stats.no_buffer += 1;
            return;
        };
        let at = slot as usize * stride as usize;
        // SAFETY: `slot` came off `free`, so no live frame covers it and no
        // caller slice overlaps it; `slot < count` and `len <= stride` keep
        // `at..at + len` inside region `base` spans.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), self.pool.base().add(at), bytes.len()) };
        self.queue.push_back((slot, len));
    }

    // take back slots of dropped frames; capacities hold, so no allocation
    fn recycle(&mut self) {
        self.pool.drain_freed(&mut self.freed);
        self.free.append(&mut self.freed);
    }
}

impl<L: Layer> Driver for MockDriver<L> {
    type Frame = IndexFrame;
    type Layer = L;
    const BACKEND: &'static str = "mock";

    fn reap(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<Reap, TransportError> {
        self.recycle();
        let mut pushed = 0;
        while out.spare() > 0
            && let Some((slot, len)) = self.queue.pop_front()
        {
            // SAFETY: `slot` left `free` at inject and sat only in `queue`
            // since, so caller owns it and no live frame covers it; inject
            // checked `slot < count` and `len <= stride` and finished writing.
            out.push(unsafe { self.pool.frame(slot, 0, len) });
            pushed += 1;
        }
        Ok(if pushed == 0 {
            Reap::Idle
        } else {
            Reap::Frames(pushed)
        })
    }

    fn stats(&self) -> DriverStats {
        self.stats
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }
}
