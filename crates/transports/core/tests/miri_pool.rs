//! Pool ownership through public API; runs under Miri and plain nextest.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    ptr,
    sync::{Arc, Barrier},
    thread,
};

use transport_core::{
    PoolStats, TransportError,
    pool::{IndexPool, VecPool, backend},
};

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn nz32(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

#[test]
fn acquire_hands_out_every_slab_then_none_then_reuses_dropped_slab() {
    let pool = VecPool::new(nz(3), nz(64)).unwrap();
    let mut slabs: Vec<_> = (0..3).map(|_| backend::acquire(&pool).unwrap()).collect();
    assert_eq!(
        pool.stats(),
        PoolStats {
            capacity: 3,
            in_use: 3
        }
    );
    assert!(backend::acquire(&pool).is_none(), "every slab is out");

    let mut last = slabs.pop().unwrap();
    backend::buf_mut(&mut last)[..3].copy_from_slice(b"abc");
    backend::set_len(&mut last, 3);
    assert_eq!(last.as_ref(), b"abc");
    let reused_buf = backend::buf_mut(&mut last).as_ptr();
    drop(last);
    assert_eq!(pool.stats().in_use, 2);

    let mut again = backend::acquire(&pool).unwrap();
    assert!(again.as_ref().is_empty(), "reacquired slab starts unfilled");
    let buf = backend::buf_mut(&mut again);
    assert_eq!(buf.len(), 64);
    assert_eq!(
        buf.as_ptr(),
        reused_buf,
        "dropped buffer reused, not reallocated"
    );
    assert_eq!(pool.stats().in_use, 3);
}

#[test]
fn index_frames_read_their_own_bytes_at_nonzero_offsets() {
    const STRIDE: u32 = 256;
    let pool = IndexPool::new(nz32(4), nz32(STRIDE)).unwrap();
    assert_eq!(pool.base().addr() % 4096, 0, "region base page-aligned");

    // driver side: fill whole region through base, as kernel DMA would
    let pattern: Vec<u8> = (0..4 * STRIDE as usize)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    // SAFETY: region holds 4 * STRIDE bytes; no frame live yet.
    unsafe { ptr::copy_nonoverlapping(pattern.as_ptr(), pool.base(), pattern.len()) };

    // SAFETY: slots 2 and 3 in bounds, owned by nobody else, writes done.
    let (a, b) = unsafe { (pool.frame(2, 10, 5), pool.frame(3, 200, 56)) };
    let at = |slot: usize, offset: usize, len: usize| {
        let start = slot * STRIDE as usize + offset;
        &pattern[start..start + len]
    };
    assert_eq!(a.as_ref(), at(2, 10, 5));
    assert_eq!(b.as_ref(), at(3, 200, 56));
    assert_eq!(pool.stats().in_use, 2);
    drop(b);
    assert_eq!(pool.stats().in_use, 1);
}

#[test]
fn frame_dropped_on_other_thread_returns_slot_through_drain_freed() {
    let pool = IndexPool::new(nz32(4), nz32(64)).unwrap();
    // SAFETY: slot 3 in bounds and owned by nobody else; region zeroed.
    let frame = unsafe { pool.frame(3, 8, 16) };
    assert_eq!(frame.as_ref(), [0; 16], "unwritten slot reads zeroed");

    let mut freed = Vec::with_capacity(4);
    pool.drain_freed(&mut freed);
    assert!(freed.is_empty(), "live frame not yet freed");

    thread::spawn(move || drop(frame)).join().unwrap();
    assert_eq!(pool.stats().in_use, 0);
    pool.drain_freed(&mut freed);
    assert_eq!(freed, [3]);

    freed.clear();
    pool.drain_freed(&mut freed);
    assert!(freed.is_empty(), "slot handed back once only");
}

#[test]
fn slots_dropped_while_driver_drains_each_return_exactly_once() {
    const SLOTS: u32 = 8;
    let pool = IndexPool::new(nz32(SLOTS), nz32(64)).unwrap();
    // SAFETY: every slot in bounds, each minted once, region zeroed.
    let frames: Vec<_> = (0..SLOTS).map(|s| unsafe { pool.frame(s, 0, 8) }).collect();

    // both sides start together, so drains overlap drops
    let start = Arc::new(Barrier::new(2));
    let dropper = thread::spawn({
        let start = Arc::clone(&start);
        move || {
            start.wait();
            for frame in frames {
                drop(frame);
                thread::yield_now();
            }
        }
    });
    let mut back = Vec::new();
    let mut freed = Vec::with_capacity(SLOTS as usize);
    start.wait();
    while !dropper.is_finished() {
        pool.drain_freed(&mut freed);
        back.append(&mut freed);
        thread::yield_now();
    }
    // join orders every drop before last drain
    dropper.join().unwrap();
    pool.drain_freed(&mut freed);
    back.append(&mut freed);

    back.sort_unstable();
    assert_eq!(
        back,
        (0..SLOTS).collect::<Vec<_>>(),
        "no slot lost or doubled"
    );
    pool.drain_freed(&mut freed);
    assert!(freed.is_empty(), "drained slot handed back again");
    assert_eq!(pool.stats().in_use, 0);
}

#[test]
fn oversized_pools_rejected_as_invalid_config() {
    // u32 inputs cannot overflow 64-bit `usize`; reachable limit is allocation size
    assert!(matches!(
        IndexPool::new(NonZeroU32::MAX, NonZeroU32::MAX),
        Err(TransportError::InvalidConfig {
            field: "count * stride",
            ..
        })
    ));
    assert!(matches!(
        VecPool::new(nz(1), NonZeroUsize::MAX),
        Err(TransportError::InvalidConfig { .. })
    ));
    assert!(matches!(
        VecPool::new(NonZeroUsize::MAX, nz(1)),
        Err(TransportError::InvalidConfig { .. })
    ));
}
