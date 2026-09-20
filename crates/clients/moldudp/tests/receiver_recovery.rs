//! Sequence anchoring and gap recording of `MoldUdpReceiver` on one leg (no
//! arbiter): mid-session join, configured start, heartbeat and end-of-session
//! tail gaps, one gap per packet, one gap per discontinuity. Gaps surface via
//! `stats().pending_gaps`.

pub mod support;

use client_moldudp::{
    GapRequest, MoldUdpError, MoldUdpEvent, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig,
};
use smallvec::smallvec;
use support::{MockLeg, mold_end_of_session, mold_heartbeat, mold_multi_packet, mold_packet};

const SESSION: [u8; 10] = *b"SESSIONID1";

fn receiver(cfg: &MoldUdpReceiverConfig, packets: &[Vec<u8>]) -> MoldUdpReceiver<MockLeg> {
    let mut leg = support::mock_leg();
    for p in packets {
        leg.driver_mut().inject(p);
    }
    MoldUdpReceiver::from_legs(cfg, smallvec![leg]).expect("receiver")
}

#[test]
fn cold_start_adopts_first_packet_sequence_no_phantom_gap() {
    // joining live feed at seq 5000 anchors there, not 1..5000 as one gap
    let packets: Vec<_> = (5000u64..=5003)
        .map(|seq| mold_packet(&SESSION, seq, format!("m{seq}").as_bytes()))
        .collect();
    let mut rx = receiver(&MoldUdpReceiverConfig::default(), &packets);

    let got = support::drain(&mut rx, 4);
    assert_eq!(support::sequences(&got), [5000, 5001, 5002, 5003]);
    assert!(rx.stats().pending_gaps.is_empty());
}

#[test]
fn configured_start_sequence_is_honored() {
    // anchored at 100: first packet at 102 buffers behind gap {100, 101},
    // never becomes new anchor
    let cfg = MoldUdpReceiverConfig {
        start_sequence: Some(100),
        ..Default::default()
    };
    let mut rx = receiver(&cfg, &[mold_packet(&SESSION, 102, b"late")]);

    assert!(matches!(rx.poll(), Err(MoldUdpError::GapDetected)));
    assert!(matches!(rx.poll(), Ok(None)));
    assert_eq!(
        rx.stats().pending_gaps,
        [GapRequest {
            start_seq: 100,
            count: 2
        }]
    );
}

#[test]
fn heartbeat_ahead_of_expected_records_tail_gap() {
    // heartbeat saying next is 4 after only seq 1: 2 and 3 lost in quiet traffic
    let mut rx = receiver(
        &MoldUdpReceiverConfig::default(),
        &[
            mold_packet(&SESSION, 1, b"one"),
            mold_heartbeat(&SESSION, 4),
        ],
    );

    assert!(matches!(rx.poll(), Ok(Some(MoldUdpOutcome::Frame(f))) if f.sequence() == 1));
    assert!(matches!(rx.poll(), Err(MoldUdpError::GapDetected)));
    assert!(matches!(
        rx.poll(),
        Ok(Some(MoldUdpOutcome::Event(MoldUdpEvent::Heartbeat {
            next_expected: 4
        })))
    ));
    assert_eq!(
        rx.stats().pending_gaps,
        [GapRequest {
            start_seq: 2,
            count: 2
        }]
    );
}

#[test]
fn end_of_session_ahead_of_expected_records_tail_gap() {
    // end of session is last chance to re-request missing tail
    let mut rx = receiver(
        &MoldUdpReceiverConfig::default(),
        &[
            mold_packet(&SESSION, 1, b"one"),
            mold_end_of_session(&SESSION, 3),
        ],
    );

    assert!(matches!(rx.poll(), Ok(Some(MoldUdpOutcome::Frame(f))) if f.sequence() == 1));
    assert!(matches!(rx.poll(), Err(MoldUdpError::GapDetected)));
    assert!(matches!(
        rx.poll(),
        Ok(Some(MoldUdpOutcome::Event(MoldUdpEvent::EndOfSession {
            next_expected: 3
        })))
    ));
    assert_eq!(
        rx.stats().pending_gaps,
        [GapRequest {
            start_seq: 2,
            count: 1
        }]
    );
}

#[test]
fn multi_block_ahead_records_one_gap_not_per_block() {
    // packet carrying 5, 6, 7 after seq 1: gap is {2, 3, 4} only, never blocks it carries
    let mut rx = receiver(
        &MoldUdpReceiverConfig::default(),
        &[
            mold_packet(&SESSION, 1, b"one"),
            mold_multi_packet(&SESSION, 5, &[b"five", b"six", b"seven"]),
        ],
    );

    assert!(matches!(rx.poll(), Ok(Some(MoldUdpOutcome::Frame(f))) if f.sequence() == 1));
    assert!(matches!(rx.poll(), Err(MoldUdpError::GapDetected)));
    assert_eq!(
        rx.stats().pending_gaps,
        [GapRequest {
            start_seq: 2,
            count: 3
        }]
    );
}

#[test]
fn gap_recorded_once_while_later_packets_keep_arriving() {
    // 2..=4 lost; 6 and heartbeat 7 land behind open gap: already-seen 5 and 6
    // must not rejoin it, and neither re-reports it
    let mut rx = receiver(
        &MoldUdpReceiverConfig::default(),
        &[
            mold_packet(&SESSION, 1, b"one"),
            mold_packet(&SESSION, 5, b"five"),
            mold_packet(&SESSION, 6, b"six"),
            mold_heartbeat(&SESSION, 7),
        ],
    );

    let mut gaps = 0;
    loop {
        match rx.poll() {
            Ok(None) => break,
            Ok(Some(_)) => {}
            Err(MoldUdpError::GapDetected) => gaps += 1,
            Err(e) => panic!("poll: {e}"),
        }
    }
    assert_eq!(gaps, 1, "one discontinuity reported once");
    assert_eq!(
        rx.stats().pending_gaps,
        [GapRequest {
            start_seq: 2,
            count: 3
        }]
    );
}

#[test]
fn poll_reports_nothing_once_legs_are_idle() {
    let mut rx = receiver(
        &MoldUdpReceiverConfig::default(),
        &[mold_packet(&SESSION, 1, b"one")],
    );

    assert!(matches!(rx.poll(), Ok(Some(MoldUdpOutcome::Frame(_)))));
    assert!(matches!(rx.poll(), Ok(None)));
}
