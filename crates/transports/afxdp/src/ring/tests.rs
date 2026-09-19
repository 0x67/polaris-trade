use super::{Consumer, Producer, heap::HeapRing};

// indices start two short of wrap, so every case crosses `u32::MAX -> 0`
const START: u32 = u32::MAX - 1;

#[test]
fn ring_producer_stops_at_full_and_resumes_across_wrap() {
    let heap = HeapRing::new(4, START, 0u64);
    // SAFETY: `heap` outlives `fill` and plays only consumer side
    let mut fill = unsafe { Producer::new(heap.ptrs(), 4) };

    let mut items = 10..17;
    assert_eq!(fill.produce(items.by_ref()), 4, "ring holds size entries");
    assert_eq!(items.next(), Some(14), "items past full ring left untaken");
    assert_eq!(fill.produce(20..22), 0, "full ring takes nothing");

    assert_eq!((heap.pop(), heap.pop()), (Some(10), Some(11)));
    assert_eq!(
        fill.produce(20..30),
        2,
        "consumed entries free exactly their room"
    );
    let drained: Vec<u64> = std::iter::from_fn(|| heap.pop()).collect();
    assert_eq!(
        drained,
        [12, 13, 20, 21],
        "ring order kept across index wrap"
    );
    assert_eq!(
        heap.indices(),
        (START.wrapping_add(6), START.wrapping_add(6))
    );
}

#[test]
fn ring_consumer_takes_at_most_max_and_releases_across_wrap() {
    let heap = HeapRing::new(4, START, 0u64);
    // SAFETY: `heap` outlives `rx` and plays only producer side
    let mut rx = unsafe { Consumer::new(heap.ptrs(), 4) };
    let mut seen = Vec::new();

    assert_eq!(rx.consume(8, |e| seen.push(e)), 0, "empty ring");
    for e in [1, 2, 3] {
        assert!(heap.push(e));
    }
    assert_eq!(rx.consume(2, |e| seen.push(e)), 2);
    assert_eq!(
        heap.indices().1,
        START.wrapping_add(2),
        "consumer word released"
    );
    for e in [4, 5, 6] {
        assert!(heap.push(e), "released entries reusable by producer");
    }
    assert!(!heap.push(7), "unreleased entries still held");
    assert_eq!(rx.consume(8, |e| seen.push(e)), 4);
    assert_eq!(seen, [1, 2, 3, 4, 5, 6]);
}

#[test]
fn ring_producer_reports_wakeup_flag() {
    let heap = HeapRing::new(2, 0, 0u64);
    // SAFETY: `heap` outlives `fill` and plays only consumer side
    let fill = unsafe { Producer::new(heap.ptrs(), 2) };
    assert!(!fill.needs_wakeup());
    heap.set_flags(libc::XDP_RING_NEED_WAKEUP);
    assert!(fill.needs_wakeup());
}
