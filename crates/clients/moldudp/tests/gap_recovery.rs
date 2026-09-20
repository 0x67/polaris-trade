//! Gap filled end to end: real UDP re-request server answers each request by
//! sending missing packets back to request's source, as `MoldUDP64` servers do.
//! Runs over socket legs and bypass-typed legs, both with `UdpSocket` requester,
//! so requester frame type never matches mock leg's. Retransmissions filling
//! gap from its head must not trigger another request.

pub mod support;

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use client_moldudp::{GapRequest, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_core::DatagramRecv;

const SESSION: [u8; 10] = *b"SESSIONRR1";

/// Re-request server: replies to each Request Packet with stored packets for
/// requested range, sent to request's source address.
struct Server {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Vec<GapRequest>>,
}

impl Server {
    fn spawn(packets: HashMap<u64, Vec<u8>>) -> Self {
        let sock = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind server");
        sock.set_read_timeout(Some(Duration::from_millis(10)))
            .expect("read timeout");
        let addr = sock.local_addr().expect("server addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            let mut buf = [0u8; 64];
            loop {
                let (n, src) = match sock.recv_from(&mut buf) {
                    Ok(got) => got,
                    // stop only on empty socket, so no request sent before stop is missed
                    Err(_) if stopped.load(Ordering::Relaxed) => break,
                    Err(_) => continue,
                };
                assert_eq!(n, 20, "request packet is 20 bytes");
                assert_eq!(buf[..10], SESSION, "request carries session");
                let start_seq = u64::from_be_bytes(buf[10..18].try_into().expect("8 bytes"));
                let count = u16::from_be_bytes(buf[18..20].try_into().expect("2 bytes"));
                requests.push(GapRequest { start_seq, count });
                for seq in start_seq..start_seq + u64::from(count) {
                    if let Some(packet) = packets.get(&seq) {
                        sock.send_to(packet, src).expect("retransmit");
                    }
                }
            }
            requests
        });
        Self { addr, stop, handle }
    }

    fn requests(self) -> Vec<GapRequest> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().expect("server thread")
    }
}

/// Leg receives 1 and 4; server holds 2 and 3. Requester has one slab, so
/// second retransmission lands only if first slab went back at once.
/// Server answers one request for `[2, 4)` with 2 then 3: 2 shrinks gap to
/// `[3, 4)`, which that request already covers, so none follows.
fn assert_gap_recovered<T: DatagramRecv>(mut leg: T, deliver: impl FnOnce(&mut T, &[Vec<u8>])) {
    let packets: Vec<Vec<u8>> = (1..=4)
        .map(|seq| support::mold_packet(&SESSION, seq, format!("m{seq}").as_bytes()))
        .collect();
    deliver(&mut leg, &[packets[0].clone(), packets[3].clone()]);
    let server = Server::spawn(HashMap::from([
        (2, packets[1].clone()),
        (3, packets[2].clone()),
    ]));
    let requester = support::udp_socket(NonZeroUsize::MIN);

    // 1 s rate-limit interval: whole fill lands inside one, no legit retry
    let cfg = MoldUdpReceiverConfig {
        max_rerequests_per_gap_per_sec: 1,
        ..MoldUdpReceiverConfig::default()
    };
    let mut rx = MoldUdpReceiver::from_legs(&cfg, smallvec![leg])
        .expect("receiver")
        .with_requester(requester, server.addr);
    let got = support::drain(&mut rx, 4);

    assert_eq!(support::sequences(&got), [1, 2, 3, 4]);
    for g in &got {
        assert_eq!(g.payload, format!("m{}", g.sequence).as_bytes());
    }
    // retransmissions tagged with requester's stream id, one past last leg
    assert_eq!(got[1].stream_id, 1);
    assert_eq!(got[2].stream_id, 1);
    assert!(rx.stats().pending_gaps.is_empty());
    assert_eq!(
        server.requests(),
        [GapRequest {
            start_seq: 2,
            count: 2
        }]
    );
}

#[test]
fn socket_leg_gap_filled_from_requester() {
    let tx = support::sender();
    assert_gap_recovered(support::udp_leg(), |leg, packets| {
        let to = leg.local_addr().expect("leg addr");
        for p in packets {
            tx.send_to(p, to).expect("send");
        }
    });
}

#[test]
fn bypass_leg_gap_filled_from_socket_requester() {
    assert_gap_recovered(support::mock_leg(), |leg, packets| {
        for p in packets {
            leg.driver_mut().inject(p);
        }
    });
}
