//! `GapRequestEmitter`: Request Packet wire shape, per-gap rate limit, and
//! retry of send that met full socket buffer.

use std::{io, net::SocketAddr, time::Duration};

use client_moldudp::{GapRequest, GapRequestEmitter};
use transport_core::{DatagramSend, Transport, TransportError};

/// Records every datagram; `blocked` makes send fail as full buffer does.
struct Recorder {
    sent: Vec<(Vec<u8>, SocketAddr)>,
    blocked: bool,
}

impl Transport for Recorder {
    fn name(&self) -> &'static str {
        "recorder"
    }
}

impl DatagramSend for Recorder {
    fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> Result<usize, TransportError> {
        if self.blocked {
            return Err(TransportError::Io {
                stage: "send_to",
                error: io::ErrorKind::WouldBlock.into(),
            });
        }
        self.sent.push((buf.to_vec(), to));
        Ok(buf.len())
    }
}

const SESSION: [u8; 10] = *b"SESSIONID1";
const GAP: GapRequest = GapRequest {
    start_seq: 100,
    count: 5,
};

fn server() -> SocketAddr {
    "127.0.0.1:9000".parse().expect("addr")
}

#[test]
fn rate_limits_repeated_requests_for_the_same_gap() {
    let mut sock = Recorder {
        sent: Vec::new(),
        blocked: false,
    };
    // 4 requests/sec/gap => 250 ms minimum spacing
    let mut emitter = GapRequestEmitter::new(server(), 4);

    let mut total_sent = 0usize;
    for _ in 0..10 {
        total_sent += emitter.emit(&[GAP], SESSION, &mut sock).expect("emit");
        std::thread::sleep(Duration::from_millis(111));
    }

    // real clock: ten 111 ms-spaced emits over ~1.1 s at 4/s ideally send 4;
    // coarse timers (Windows ~15 ms) shift it, so assert capped near rate
    assert!(
        (3..=6).contains(&total_sent),
        "rate limiter should cap persistent gap near 4/s, got {total_sent} of 10"
    );
    assert_eq!(sock.sent.len(), total_sent);
    // Session[10], Sequence[8 BE], RequestedMessageCount[2 BE], to server
    let mut packet = SESSION.to_vec();
    packet.extend_from_slice(&100u64.to_be_bytes());
    packet.extend_from_slice(&5u16.to_be_bytes());
    assert_eq!(sock.sent[0], (packet, server()));
}

#[test]
fn blocked_send_is_retried_on_next_emit() {
    let mut sock = Recorder {
        sent: Vec::new(),
        blocked: true,
    };
    let mut emitter = GapRequestEmitter::new(server(), 4);

    assert_eq!(emitter.emit(&[GAP], SESSION, &mut sock).expect("emit"), 0);
    sock.blocked = false;
    // blocked attempt left no rate-limit mark, so immediate retry sends
    assert_eq!(emitter.emit(&[GAP], SESSION, &mut sock).expect("emit"), 1);
    assert_eq!(sock.sent.len(), 1);
}
