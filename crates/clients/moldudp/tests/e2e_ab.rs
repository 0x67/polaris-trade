//! Two legs share session and sequence space; A/B arbiter merges them so each
//! seq is delivered exactly once, in order, though leg A misses message only
//! leg B carries. Legs are `MioUdp` parked on one `ReadySet`: caller waits only
//! after `poll` reports every leg idle.

pub mod support;

use std::{num::NonZeroUsize, time::Duration};

use client_moldudp::{MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_socket::mio::{MioUdp, ReadySet, ReadyToken};

#[test]
fn each_sequence_delivered_exactly_once_via_ab_merge() {
    let mut set = ReadySet::new(NonZeroUsize::new(4).expect("non-zero")).expect("ready set");
    let mut leg_a = MioUdp::from_socket(support::udp_leg());
    let mut leg_b = MioUdp::from_socket(support::udp_leg());
    set.register(&mut leg_a, ReadyToken(0)).expect("register a");
    set.register(&mut leg_b, ReadyToken(1)).expect("register b");
    let addr_a = leg_a.local_addr().expect("addr a");
    let addr_b = leg_b.local_addr().expect("addr b");
    let mut rx =
        MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg_a, leg_b])
            .expect("receiver");

    let session = *b"SESSIONAB1";
    let tx = support::sender();
    // leg A misses seq 3; leg B has full run
    for seq in [1u64, 2, 4, 5] {
        let packet = support::mold_packet(&session, seq, format!("msg-{seq}").as_bytes());
        tx.send_to(&packet, addr_a).expect("send a");
    }
    for seq in 1u64..=5 {
        let packet = support::mold_packet(&session, seq, format!("msg-{seq}").as_bytes());
        tx.send_to(&packet, addr_b).expect("send b");
    }

    let mut ready = Vec::new();
    let mut received = Vec::new();
    while received.len() < 5 {
        match rx.poll() {
            Ok(Some(MoldUdpOutcome::Frame(f))) => {
                received.push((f.sequence(), f.as_ref().to_vec()));
            }
            Ok(Some(_)) | Err(MoldUdpError::GapDetected) => {}
            Ok(None) => {
                // every datagram was sent up front: timeout means lost wakeup
                set.wait(Some(Duration::from_secs(2)), &mut ready)
                    .expect("wait");
                assert!(
                    !ready.is_empty(),
                    "poll idle but no leg reported; {received:?}"
                );
            }
            Err(e) => panic!("poll: {e}"),
        }
    }

    for (i, (seq, payload)) in (1u64..).zip(&received) {
        assert_eq!(*seq, i);
        assert_eq!(payload, format!("msg-{i}").as_bytes());
    }
}
