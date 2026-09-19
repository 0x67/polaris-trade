//! Reassembler: in-order, out-of-order, duplicate and buffer-full paths, with
//! `S = Vec<u8>` as owned slab handle.

use client_moldudp::{MoldUdpError, SequenceReassembler};

#[test]
fn in_order_drains_immediately() {
    let mut r: SequenceReassembler<Vec<u8>> = SequenceReassembler::new(8);
    let drained: Vec<Vec<u8>> = r.insert(1, b"one".to_vec()).unwrap().unwrap().collect();
    assert_eq!(drained, vec![b"one".to_vec()]);
    assert_eq!(r.expected_next(), 2);
}

#[test]
fn out_of_order_buffers_until_gap_fills() {
    let mut r: SequenceReassembler<Vec<u8>> = SequenceReassembler::new(8);
    // seq 2 arrives before seq 1: buffered, no drain yet.
    assert!(r.insert(2, b"two".to_vec()).unwrap().is_none());
    assert_eq!(r.expected_next(), 1);

    // seq 1 arrives: drains both 1 and 2 in order.
    let drained: Vec<Vec<u8>> = r.insert(1, b"one".to_vec()).unwrap().unwrap().collect();
    assert_eq!(drained, vec![b"one".to_vec(), b"two".to_vec()]);
    assert_eq!(r.expected_next(), 3);
}

#[test]
fn stale_duplicate_dropped_silently() {
    let mut r: SequenceReassembler<Vec<u8>> = SequenceReassembler::new(8);
    let _ = r.insert(1, b"one".to_vec()).unwrap();
    assert_eq!(r.expected_next(), 2);
    // seq 1 again, already passed: dropped, no error, no drain
    assert!(r.insert(1, b"stale".to_vec()).unwrap().is_none());
}

#[test]
fn exact_duplicate_pending_slot_dropped_silently() {
    let mut r: SequenceReassembler<Vec<u8>> = SequenceReassembler::new(8);
    assert!(r.insert(5, b"five".to_vec()).unwrap().is_none());
    // same seq re-delivered while pending: dropped, no error
    assert!(r.insert(5, b"five-again".to_vec()).unwrap().is_none());
}

#[test]
fn buffer_full_when_distinct_seq_collides_on_ring_slot() {
    let mut r: SequenceReassembler<Vec<u8>> = SequenceReassembler::new(4);
    // seq 5 occupies ring slot (5 % 4 == 1), still pending (expected_next is 1).
    assert!(r.insert(5, b"five".to_vec()).unwrap().is_none());
    // seq 9 maps to same slot (9 % 4 == 1), distinct message: full
    match r.insert(9, b"nine".to_vec()) {
        Err(err) => assert!(matches!(
            err,
            MoldUdpError::ReassemblyBufferFull { capacity: 4 }
        )),
        Ok(_) => panic!("expected ReassemblyBufferFull"),
    }
}
