//! Allocation proof for receiver's borrowed receive path: in-order messages
//! decode without heap, and gap-buffered datagram promotes to `Arc` once per
//! datagram, not once per message drained from it.

pub mod support;

use client_moldudp::{MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use support::{MockLeg, mold_multi_packet, mold_packet};

const SESSION: [u8; 10] = *b"SESSIONID1";

fn receiver(packets: &[Vec<u8>]) -> MoldUdpReceiver<MockLeg> {
    let mut leg = support::mock_leg();
    for p in packets {
        leg.driver_mut().inject(p);
    }
    MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg]).expect("receiver")
}

#[test]
fn poll_in_order_burst_is_allocation_free() {
    const N: u64 = 8;
    let packets: Vec<_> = (1..=N)
        .map(|seq| mold_packet(&SESSION, seq, format!("msg-{seq}").as_bytes()))
        .collect();
    let mut rx = receiver(&packets);

    let info = allocation_counter::measure(|| {
        for seq in 1..=N {
            match rx.poll() {
                Ok(Some(MoldUdpOutcome::Frame(frame))) => assert_eq!(frame.sequence(), seq),
                other => panic!("expected borrowed frame, got {other:?}"),
            }
        }
    });
    assert_eq!(info.count_total, 0, "in-order poll must not allocate");
}

/// Gap tracking (`GapRequestHandler`'s `BTreeMap`) allocates too, apart from
/// `Arc` promotion under test. So compare single-message out-of-order datagram
/// with same-shaped datagram carrying 3 messages: both record same one gap,
/// so only per-message promotion would make 3-message case cost more.
#[test]
fn gap_fill_promotes_arc_once_per_datagram_not_per_message() {
    // arrival 1, 3, 2: seq 3 buffers ahead of expected 2, then 2 cascade-drains it
    let single = receiver(&[
        mold_packet(&SESSION, 1, b"one"),
        mold_packet(&SESSION, 3, b"three"),
        mold_packet(&SESSION, 2, b"two"),
    ]);
    // same shape, buffered datagram carries 3, 4, 5
    let multi = receiver(&[
        mold_packet(&SESSION, 1, b"one"),
        mold_multi_packet(&SESSION, 3, &[b"three", b"four", b"five"]),
        mold_packet(&SESSION, 2, b"two"),
    ]);

    let allocations = |mut rx: MoldUdpReceiver<MockLeg>, expected: &[u64]| {
        let mut seqs = Vec::with_capacity(expected.len());
        let info = allocation_counter::measure(|| {
            while seqs.len() < expected.len() {
                match rx.poll() {
                    Ok(Some(MoldUdpOutcome::Frame(f))) => seqs.push(f.sequence()),
                    Ok(Some(_)) | Err(MoldUdpError::GapDetected) => {}
                    other => panic!("unexpected poll result {other:?}"),
                }
            }
        });
        assert_eq!(seqs, expected);
        info.count_total
    };
    let single_total = allocations(single, &[1, 2, 3]);
    let multi_total = allocations(multi, &[1, 2, 3, 4, 5]);

    assert!(
        multi_total <= single_total,
        "buffering 3 messages of one datagram ({multi_total} allocs) must cost no more than 1 \
         ({single_total} allocs): slab promotes to `Arc` once per datagram"
    );
}
