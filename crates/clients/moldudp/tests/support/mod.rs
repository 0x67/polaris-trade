//! Packet builders, leg builders and drain loop shared by receiver tests.

use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket},
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use client_moldudp::{
    MIN_LEG_POOL_CAPACITY, MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, Recovery,
};
use transport_core::{
    DatagramRecv, DatagramSend, FrameBatch, PoolStats, Transport, TransportError,
    bypass::{BypassTransport, L4, MockDriver},
    pool::IndexPool,
};
use transport_socket::{UdpConfig, UdpSocket};

/// In-process leg: injected bytes land in pool slots, no socket.
pub type MockLeg = BypassTransport<MockDriver<L4>>;

/// Mock leg whose pool holds exactly `slots` buffers.
///
/// # Panics
///
/// When `slots` is zero or pool cannot be allocated.
pub fn mock_leg_with(slots: u32) -> MockLeg {
    let pool = IndexPool::new(
        NonZeroU32::new(slots).expect("non-zero slots"),
        NonZeroU32::new(512).expect("non-zero stride"),
    )
    .expect("mock pool");
    BypassTransport::new(MockDriver::new(pool))
}

/// Mock leg sized for receiver's reorder window.
///
/// # Panics
///
/// When pool cannot be allocated.
pub fn mock_leg() -> MockLeg {
    mock_leg_with(u32::try_from(MIN_LEG_POOL_CAPACITY).expect("capacity fits u32"))
}

/// Loopback UDP leg sized for receiver's reorder window.
///
/// # Panics
///
/// When bind fails.
pub fn udp_leg() -> UdpSocket {
    udp_socket(NonZeroUsize::new(MIN_LEG_POOL_CAPACITY).expect("non-zero capacity"))
}

/// Loopback UDP socket with `slabs` receive buffers.
///
/// # Panics
///
/// When bind fails.
pub fn udp_socket(slabs: NonZeroUsize) -> UdpSocket {
    let mut cfg = UdpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    cfg.slab_count = slabs;
    UdpSocket::bind(&cfg).expect("bind loopback socket")
}

/// Plain blocking std socket for sending at legs.
///
/// # Panics
///
/// When bind fails.
pub fn sender() -> StdUdpSocket {
    StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind sender")
}

/// One single-message downstream packet.
pub fn mold_packet(session: &[u8; 10], sequence: u64, payload: &[u8]) -> Vec<u8> {
    mold_multi_packet(session, sequence, &[payload])
}

/// Downstream packet carrying `messages`, sequenced from `first_sequence`.
///
/// # Panics
///
/// When count or message length overflows `u16`.
pub fn mold_multi_packet(session: &[u8; 10], first_sequence: u64, messages: &[&[u8]]) -> Vec<u8> {
    let count = u16::try_from(messages.len()).expect("message count fits u16");
    let mut packet = mold_control(session, first_sequence, count);
    for m in messages {
        let len = u16::try_from(m.len()).expect("message fits u16");
        packet.extend_from_slice(&len.to_be_bytes());
        packet.extend_from_slice(m);
    }
    packet
}

/// Heartbeat: header only, `message_count == 0`, next-expected in sequence field.
pub fn mold_heartbeat(session: &[u8; 10], next_expected: u64) -> Vec<u8> {
    mold_control(session, next_expected, 0)
}

/// End of session: header only, `message_count == 0xFFFF`.
pub fn mold_end_of_session(session: &[u8; 10], next_expected: u64) -> Vec<u8> {
    mold_control(session, next_expected, 0xFFFF)
}

fn mold_control(session: &[u8; 10], sequence: u64, message_count: u16) -> Vec<u8> {
    let mut packet = Vec::with_capacity(20);
    packet.extend_from_slice(session);
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&message_count.to_be_bytes());
    packet
}

/// One delivered message.
#[derive(Debug, PartialEq, Eq)]
pub struct Got {
    /// Message sequence.
    pub sequence: u64,
    /// Message bytes.
    pub payload: Vec<u8>,
    /// Leg index, or leg count for requester.
    pub stream_id: u8,
}

/// Poll until `n` data frames arrive, skipping gaps and control events.
///
/// # Panics
///
/// After 5 s, or on any poll error but `GapDetected`.
pub fn drain<T: DatagramRecv, R: Recovery>(rx: &mut MoldUdpReceiver<T, R>, n: usize) -> Vec<Got> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got = Vec::with_capacity(n);
    while got.len() < n {
        assert!(
            Instant::now() < deadline,
            "{} of {n} frames arrived: {got:?}",
            got.len()
        );
        match rx.poll() {
            Ok(Some(MoldUdpOutcome::Frame(f))) => got.push(Got {
                sequence: f.sequence(),
                payload: f.as_ref().to_vec(),
                stream_id: f.stream_id(),
            }),
            Ok(Some(_)) | Err(MoldUdpError::GapDetected) => {}
            Ok(None) => std::thread::yield_now(),
            Err(e) => panic!("poll: {e}"),
        }
    }
    got
}

/// Sequences of `got`, in order.
pub fn sequences(got: &[Got]) -> Vec<u64> {
    got.iter().map(|g| g.sequence).collect()
}

/// Requester socket that accepts every re-request and never has a reply.
pub struct IdleRequester;

impl Transport for IdleRequester {
    fn name(&self) -> &'static str {
        "idle-requester"
    }
}

impl DatagramRecv for IdleRequester {
    type Frame = Vec<u8>;

    fn recv_burst(&mut self, _: &mut FrameBatch<Vec<u8>>) -> Result<usize, TransportError> {
        Ok(0)
    }

    fn pool_stats(&self) -> PoolStats {
        PoolStats {
            capacity: 1,
            in_use: 0,
        }
    }
}

impl DatagramSend for IdleRequester {
    fn send_to(&mut self, buf: &[u8], _: SocketAddr) -> Result<usize, TransportError> {
        Ok(buf.len())
    }
}
