//! `GapRequestEmitter`: Request Packet wire shape, per-gap rate limit, and
//! retry of send that met full socket buffer.

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

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

    // driven clock, ten 111 ms steps: a send at 0 admits the next at 333, not
    // 222, so the gap goes out at 0, 333, 666, 999 and nowhere between
    let base = Instant::now();
    let mut total_sent = 0usize;
    for step in 0..10 {
        let now = base + Duration::from_millis(111 * step);
        total_sent += emitter.emit(&[GAP], SESSION, &mut sock, now).expect("emit");
    }

    assert_eq!(
        total_sent, 4,
        "4/s over 999 ms must send at 0, 333, 666, 999"
    );
    assert_eq!(sock.sent.len(), total_sent);
    // Session[10], Sequence[8 BE], RequestedMessageCount[2 BE], to server
    let mut packet = SESSION.to_vec();
    packet.extend_from_slice(&100u64.to_be_bytes());
    packet.extend_from_slice(&5u16.to_be_bytes());
    assert_eq!(sock.sent[0], (packet, server()));
}

/// A gap past `u16::MAX` messages splits into chunks, so a later chunk can sit
/// inside two earlier requests taken together and inside neither alone.
#[test]
fn gap_covered_by_two_earlier_requests_together_is_not_resent() {
    let mut sock = Recorder {
        sent: Vec::new(),
        blocked: false,
    };
    // 1 request/s/gap: whole test runs inside one interval
    let mut emitter = GapRequestEmitter::new(server(), 1);
    let lower = GapRequest {
        start_seq: 100,
        count: 50,
    };
    let upper = GapRequest {
        start_seq: 150,
        count: 50,
    };
    let now = Instant::now();
    assert_eq!(
        emitter
            .emit(&[lower, upper], SESSION, &mut sock, now)
            .expect("emit"),
        2
    );

    let spanning = GapRequest {
        start_seq: 100,
        count: 100,
    };
    assert_eq!(
        emitter
            .emit(&[spanning], SESSION, &mut sock, now)
            .expect("emit"),
        0,
        "range inside the two already requested must not be requested again"
    );
    assert_eq!(sock.sent.len(), 2);
}

#[test]
fn blocked_send_is_retried_on_next_emit() {
    let mut sock = Recorder {
        sent: Vec::new(),
        blocked: true,
    };
    let mut emitter = GapRequestEmitter::new(server(), 4);

    let now = Instant::now();
    assert_eq!(
        emitter.emit(&[GAP], SESSION, &mut sock, now).expect("emit"),
        0
    );
    sock.blocked = false;
    // blocked attempt left no rate-limit mark, so retry at same instant sends
    assert_eq!(
        emitter.emit(&[GAP], SESSION, &mut sock, now).expect("emit"),
        1
    );
    assert_eq!(sock.sent.len(), 1);
}
