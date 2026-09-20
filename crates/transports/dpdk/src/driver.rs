//! Poll-mode driver over one DPDK receive queue, and its pure burst bookkeeping.
//!
//! One shim call per burst takes mbufs and fills each one's data pointer,
//! length and segment count. [`settle`] then turns single-segment mbufs into
//! frames and frees chained ones in one bulk call, counted as truncated: a
//! chained mbuf never turns burst into error, so no received frame is lost.

#[cfg(feature = "driver-dpdk")]
use std::{
    cell::Cell,
    ffi::{c_uint, c_void},
    ptr,
};

#[cfg(feature = "driver-dpdk")]
use transport_core::{
    FrameBatch, PoolStats, TransportError,
    bypass::{Driver, DriverStats, L2, Polled},
};

#[cfg(feature = "driver-dpdk")]
use crate::{config::DpdkConfig, ffi, frame::MbufFrame};

/// Settle one received burst, in arrival order.
///
/// Single-segment mbufs go to `deliver` with their index. Chained ones
/// (`nb_segs > 1`) move to front of `mbufs`, go to `free` in one call and add
/// to `truncated`. Returns frames delivered, never `mbufs.len()`: chained
/// mbufs are not frames.
pub(crate) fn settle<M: Copy>(
    mbufs: &mut [M],
    nb_segs: &[u16],
    truncated: &mut u64,
    mut deliver: impl FnMut(usize, M),
    free: impl FnOnce(&mut [M]),
) -> usize {
    debug_assert_eq!(mbufs.len(), nb_segs.len(), "settle over unequal arrays");
    let mut chained = 0;
    for (i, &segs) in nb_segs.iter().enumerate() {
        let mbuf = mbufs[i];
        if segs > 1 {
            // `chained <= i`: slot already delivered or moved, safe to overwrite
            mbufs[chained] = mbuf;
            chained += 1;
        } else {
            deliver(i, mbuf);
        }
    }
    if chained > 0 {
        free(&mut mbufs[..chained]);
        *truncated += chained as u64;
    }
    mbufs.len() - chained
}

/// [`Driver`] over one caller-configured port queue. Built by
/// [`DpdkL2::attach`](crate::DpdkL2::attach), which holds its contract.
#[cfg(feature = "driver-dpdk")]
#[derive(Debug)]
pub(crate) struct PmdDriver {
    port: u16,
    queue: u16,
    burst: u16,
    // read only by `pool_stats`
    mempool: *mut c_void,
    // rx scratch, `burst` entries each; filled and settled within one burst
    mbufs: Box<[*mut c_void]>,
    data: Box<[*const u8]>,
    lens: Box<[u16]>,
    segs: Box<[u16]>,
    truncated: u64,
    // (imissed, rx_nombuf) of last good read; kept on failed read so counters never fall
    nic: Cell<(u64, u64)>,
}

// SAFETY: `mempool` only read, by thread-safe stats calls; scratch rewritten by each
// burst before read; attach contract allows one poller at a time, so moving to it is sound.
#[cfg(feature = "driver-dpdk")]
unsafe impl Send for PmdDriver {}

#[cfg(feature = "driver-dpdk")]
impl PmdDriver {
    pub(crate) fn new(cfg: &DpdkConfig, mempool: *mut c_void) -> Self {
        let burst = usize::from(cfg.burst.get());
        Self {
            port: cfg.port,
            queue: cfg.queue,
            burst: cfg.burst.get(),
            mempool,
            mbufs: vec![ptr::null_mut(); burst].into_boxed_slice(),
            data: vec![ptr::null(); burst].into_boxed_slice(),
            lens: vec![0; burst].into_boxed_slice(),
            segs: vec![0; burst].into_boxed_slice(),
            truncated: 0,
            nic: Cell::new((0, 0)),
        }
    }
}

#[cfg(feature = "driver-dpdk")]
impl Driver for PmdDriver {
    type Frame = MbufFrame;
    type Layer = L2;
    const BACKEND: &'static str = "dpdk";

    // PMD refills its ring from mempool itself, so no recycle step; DPDK never
    // shows pending data without buffer, so no `Exhausted` (see `rx_nombuf`)
    #[inline]
    fn poll_frames(&mut self, out: &mut FrameBatch<MbufFrame>) -> Result<Polled, TransportError> {
        let want = u16::try_from(out.spare()).map_or(self.burst, |spare| spare.min(self.burst));
        loop {
            // SAFETY: every scratch array holds `burst >= want` entries; port and
            // queue started and polled by this thread only (attach contract).
            let n = unsafe {
                ffi::polaris_dpdk_rx_burst(
                    self.port,
                    self.queue,
                    self.mbufs.as_mut_ptr(),
                    self.data.as_mut_ptr(),
                    self.lens.as_mut_ptr(),
                    self.segs.as_mut_ptr(),
                    want,
                )
            };
            if n == 0 {
                return Ok(Polled::Idle);
            }
            let n = usize::from(n);
            let delivered = settle(
                &mut self.mbufs[..n],
                &self.segs[..n],
                &mut self.truncated,
                // SAFETY: entry `i` is single-segment mbuf rx_burst just handed
                // over, owned by nothing else; `data[i]`, `lens[i]` describe it.
                |i, mbuf| out.push(unsafe { MbufFrame::new(mbuf, self.data[i], self.lens[i]) }),
                |chained| {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "at most one burst, a u16 count"
                    )]
                    let count = chained.len() as c_uint;
                    // SAFETY: chained mbufs were just received and belong to no frame.
                    unsafe { ffi::rte_pktmbuf_free_bulk(chained.as_mut_ptr(), count) }
                },
            );
            // burst of only chained mbufs: ring may hold more, never read as idle
            if delivered > 0 {
                return Ok(Polled::Frames(delivered));
            }
        }
    }

    fn stats(&self) -> DriverStats {
        let (mut imissed, mut rx_nombuf) = self.nic.get();
        // SAFETY: port configured (attach contract); outputs are live locals,
        // left at last good values when read fails.
        unsafe { ffi::polaris_dpdk_rx_drops(self.port, &raw mut imissed, &raw mut rx_nombuf) };
        self.nic.set((imissed, rx_nombuf));
        DriverStats {
            no_buffer: rx_nombuf,
            nic_missed: imissed,
            truncated: self.truncated,
            syscalls: 0,
        }
    }

    // debug only: `rte_mempool_in_use_count` walks every lcore cache
    fn pool_stats(&self) -> PoolStats {
        let (mut capacity, mut in_use): (c_uint, c_uint) = (0, 0);
        // SAFETY: mempool live and non-null (attach contract); outputs are live locals.
        unsafe { ffi::polaris_dpdk_pool_stats(self.mempool, &raw mut capacity, &raw mut in_use) };
        PoolStats {
            capacity: capacity as usize,
            in_use: in_use as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::settle;

    #[test]
    fn chained_mbufs_bulk_freed_counted_truncated_never_returned() {
        let mut mbufs = [10_u32, 11, 12, 13, 14];
        let mut truncated = 7;
        let mut delivered = Vec::new();
        let mut freed = Vec::new();
        let n = settle(
            &mut mbufs,
            &[1, 3, 1, 2, 1],
            &mut truncated,
            |i, m| delivered.push((i, m)),
            |chained| freed.push(chained.to_vec()),
        );
        assert_eq!(n, 3, "returned count must equal frames delivered");
        assert_eq!(delivered, [(0, 10), (2, 12), (4, 14)]);
        assert_eq!(freed, [vec![11, 13]], "chained mbufs freed in one call");
        assert_eq!(truncated, 7 + 2);
    }
}
