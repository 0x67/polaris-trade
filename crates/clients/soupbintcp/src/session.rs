//! One protocol state machine (login, sequence, outbound buffer, heartbeat
//! deadlines) shared by async and sync drivers, plus sync driver itself.
//!
//! Receive lands bytes straight into decode buffer's spare capacity through
//! [`StreamRecv::recv_into`], so uncompressed stream has one copy; framing
//! (`split_to`) stays refcount-free after. Outbound bytes queue in one buffer
//! whose front is resume point of partial write, so both drivers share it and
//! wire order holds.

use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};
use transport_core::{StreamRecv, StreamTrySend};

#[cfg(feature = "compressed")]
use crate::compressed::CompressedReader;
use crate::{
    client::{ClientState, SoupBinClient},
    config::SoupBinClientConfig,
    error::SoupBinError,
    event::{SoupBinEvent, SoupBinMessage},
    frame::Frame,
    wire::{self, PacketType},
};

/// `protocol` label on every `client.*` metric and log event.
const PROTOCOL: &str = "soupbintcp";

/// Login outcome while authenticating.
pub(crate) enum Login {
    Accepted { session: [u8; 10], sequence: u64 },
    Rejected(u8),
}

/// Streaming outcome, owned so caller borrows `last_frame` only where it returns.
pub(crate) enum Streamed {
    Data(u64),
    Event(SoupBinEvent),
}

#[cfg(feature = "observability")]
fn record_session(event: &'static str) {
    if observability_core::metrics_enabled() {
        metrics::counter!("client.sessions", "protocol" => PROTOCOL, "event" => event).increment(1);
    }
}

#[cfg(not(feature = "observability"))]
fn record_session(_event: &'static str) {}

pub(crate) struct Session {
    pub(crate) state: ClientState,
    cfg: SoupBinClientConfig,
    decode_buf: BytesMut,
    last_frame: BytesMut,
    // queued bytes not yet written; front is where partial write resumes
    out: BytesMut,
    // session id server assigned at login
    assigned_id: String,
    next_expected_sequence: u64,
    last_send: Instant,
    last_recv: Instant,
    login_deadline: Instant,
    #[cfg(feature = "compressed")]
    inflate: CompressedReader,
    // compressed bytes land here first (inflate needs contiguous input, second
    // copy); unconsumed tail waits for next inflate step
    #[cfg(feature = "compressed")]
    recv_staging: BytesMut,
}

impl Session {
    pub(crate) fn new(cfg: SoupBinClientConfig) -> Self {
        let now = Instant::now();
        Self {
            state: ClientState::Disconnected,
            decode_buf: BytesMut::with_capacity(cfg.decode_buf_capacity),
            last_frame: BytesMut::new(),
            out: BytesMut::with_capacity(cfg.max_frame_size),
            assigned_id: String::new(),
            next_expected_sequence: 0,
            last_send: now,
            last_recv: now,
            login_deadline: now,
            #[cfg(feature = "compressed")]
            inflate: CompressedReader::new(cfg.decode_buf_capacity),
            #[cfg(feature = "compressed")]
            recv_staging: BytesMut::with_capacity(cfg.decode_buf_capacity),
            cfg,
        }
    }

    pub(crate) fn next_expected_sequence(&self) -> u64 {
        self.next_expected_sequence
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.assigned_id
    }

    pub(crate) fn login_deadline(&self) -> Instant {
        self.login_deadline
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        match self.state {
            ClientState::Disconnected | ClientState::Authenticating => self.login_deadline,
            ClientState::Streaming | ClientState::Closed => {
                let send_due = self.last_send + self.cfg.heartbeat_interval;
                let recv_due = self.last_recv + self.cfg.heartbeat_timeout;
                send_due.min(recv_due)
            }
        }
    }

    pub(crate) fn frame(&self, sequence: u64) -> Frame<'_> {
        Frame {
            payload: &self.last_frame,
            sequence,
        }
    }

    /// Append one packet to outbound buffer; appends nothing on error.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::FrameTooLarge`] when `payload` passes 65534 bytes:
    /// `u16` length prefix counts type byte too.
    pub(crate) fn queue(&mut self, ty: PacketType, payload: &[u8]) -> Result<(), SoupBinError> {
        let len = u16::try_from(1 + payload.len()).map_err(|_| SoupBinError::FrameTooLarge {
            size: payload.len(),
            max: usize::from(u16::MAX) - 1,
        })?;
        self.out.extend_from_slice(&len.to_be_bytes());
        self.out.put_u8(ty as u8);
        self.out.extend_from_slice(payload);
        Ok(())
    }

    pub(crate) fn queue_login(&mut self, now: Instant) -> Result<(), SoupBinError> {
        let payload = login_request(&self.cfg);
        self.queue(PacketType::LoginRequest, &payload)?;
        self.state = ClientState::Authenticating;
        self.login_deadline = now + self.cfg.login_timeout;
        Ok(())
    }

    pub(crate) fn queue_heartbeat(&mut self) -> Result<(), SoupBinError> {
        self.queue(PacketType::ClientHeartbeat, &[])?;
        // counter only: per-heartbeat log would flood
        #[cfg(feature = "observability")]
        if observability_core::metrics_enabled() {
            metrics::counter!("client.heartbeats", "protocol" => PROTOCOL).increment(1);
        }
        Ok(())
    }

    /// Close after logout was queued (sync) or written (async).
    pub(crate) fn close_logout(&mut self) {
        self.state = ClientState::Closed;
        record_session("logout");
        tracing::info!(protocol = PROTOCOL, session = %self.assigned_id, "soupbintcp logout");
    }

    pub(crate) fn login_timed_out(&mut self) -> SoupBinError {
        self.state = ClientState::Closed;
        tracing::warn!(protocol = PROTOCOL, timeout = ?self.cfg.login_timeout, "soupbintcp login timeout");
        SoupBinError::LoginTimeout {
            timeout: self.cfg.login_timeout,
        }
    }

    pub(crate) fn pending_out(&self) -> &[u8] {
        &self.out
    }

    /// Async driver wrote whole outbound buffer.
    pub(crate) fn sent_all(&mut self, now: Instant) {
        self.out.clear();
        self.last_send = now;
    }

    /// Write queued bytes until empty or socket full, resuming where previous
    /// partial write stopped.
    pub(crate) fn flush<T: StreamTrySend>(
        &mut self,
        transport: &mut T,
        now: Instant,
    ) -> Result<(), SoupBinError> {
        while !self.out.is_empty() {
            let n = transport.try_send(&self.out)?;
            if n == 0 {
                return Ok(());
            }
            self.out.advance(n);
            if self.out.is_empty() {
                self.last_send = now;
            }
        }
        Ok(())
    }

    /// Nothing queued and send silence past `heartbeat_interval`.
    pub(crate) fn heartbeat_due(&self, now: Instant) -> bool {
        self.out.is_empty() && now.duration_since(self.last_send) > self.cfg.heartbeat_interval
    }

    /// Server silent past `heartbeat_timeout`: close and report true.
    pub(crate) fn heartbeat_timed_out(&mut self, now: Instant) -> bool {
        let silent = now.duration_since(self.last_recv);
        if silent <= self.cfg.heartbeat_timeout {
            return false;
        }
        self.state = ClientState::Closed;
        self.out.clear();
        tracing::warn!(protocol = PROTOCOL, session = %self.assigned_id, ?silent, "soupbintcp heartbeat timeout");
        true
    }

    /// Compressed input not yet inflated: next `ingest` reads no socket, so
    /// async driver must not wait on readiness first.
    #[cfg(feature = "compressed")]
    pub(crate) fn staged(&self) -> bool {
        !self.recv_staging.is_empty() || self.inflate.capped()
    }

    /// Land one `recv_into` chunk or inflate one step; `false` once transport
    /// reads nothing, so parked caller may wait. Uncompressed: straight into
    /// decode buffer's spare capacity. Compressed: socket bytes go to staging
    /// only once staging drained, and each call inflates at most
    /// `decode_buf_capacity` bytes for caller to dispatch before next. Refreshes
    /// server liveness only on bytes read.
    pub(crate) fn ingest<T: StreamRecv>(
        &mut self,
        transport: &mut T,
        now: Instant,
    ) -> Result<bool, SoupBinError> {
        #[cfg(feature = "compressed")]
        {
            if !self.staged() {
                let capacity = self.cfg.decode_buf_capacity;
                if land(transport, &mut self.recv_staging, capacity)? == 0 {
                    return Ok(false);
                }
                self.last_recv = now;
            }
            let (consumed, inflated) = self.inflate.feed(&self.recv_staging)?;
            self.decode_buf.extend_from_slice(inflated);
            self.recv_staging.advance(consumed);
            Ok(true)
        }
        #[cfg(not(feature = "compressed"))]
        {
            let capacity = self.cfg.decode_buf_capacity;
            let read = land(transport, &mut self.decode_buf, capacity)? > 0;
            if read {
                self.last_recv = now;
            }
            Ok(read)
        }
    }

    /// Split next whole packet off decode buffer, guarding `max_frame_size`
    /// on length prefix first.
    fn take_packet(&mut self) -> Result<Option<(PacketType, BytesMut)>, SoupBinError> {
        if let Some(prefix) = self.decode_buf.first_chunk::<2>() {
            let total = 2 + usize::from(u16::from_be_bytes(*prefix));
            if total > self.cfg.max_frame_size {
                return Err(SoupBinError::FrameTooLarge {
                    size: total,
                    max: self.cfg.max_frame_size,
                });
            }
        }
        let Some((frame, consumed)) = wire::parse_packet(&self.decode_buf)? else {
            return Ok(None);
        };
        let ty = frame.ty;
        let payload_len = frame.payload.len();
        let mut packet = self.decode_buf.split_to(consumed);
        Ok(Some((ty, packet.split_off(consumed - payload_len))))
    }

    /// Dispatch buffered packets while authenticating. Server heartbeat before
    /// accept is liveness (slow server), debug drops; `None` when no whole
    /// login answer is buffered.
    pub(crate) fn dispatch_login(&mut self, now: Instant) -> Result<Option<Login>, SoupBinError> {
        while let Some((ty, bytes)) = self.take_packet()? {
            match ty {
                PacketType::LoginAccepted => {
                    let (session, sequence) = parse_login_accepted(&bytes)?;
                    parse_ascii_field(&session)?.clone_into(&mut self.assigned_id);
                    self.next_expected_sequence = sequence;
                    self.last_recv = now;
                    self.state = ClientState::Streaming;
                    record_session("login");
                    tracing::info!(
                        protocol = PROTOCOL,
                        session = %self.assigned_id,
                        next_expected_sequence = sequence,
                        "soupbintcp login accepted"
                    );
                    return Ok(Some(Login::Accepted { session, sequence }));
                }
                PacketType::LoginRejected => {
                    let Some(&reason) = bytes.first() else {
                        return Err(SoupBinError::ProtocolViolation(
                            "login rejected without reason code".into(),
                        ));
                    };
                    self.state = ClientState::Closed;
                    tracing::warn!(protocol = PROTOCOL, reason = %char::from(reason), "soupbintcp login rejected");
                    return Ok(Some(Login::Rejected(reason)));
                }
                PacketType::Debug | PacketType::ServerHeartbeat => {}
                other => {
                    return Err(SoupBinError::ProtocolViolation(format!(
                        "unexpected packet type during login: {other:?}"
                    )));
                }
            }
        }
        Ok(None)
    }

    /// Dispatch first non-debug buffered packet while streaming; `None` when no
    /// whole packet is buffered.
    pub(crate) fn dispatch_stream(&mut self) -> Result<Option<Streamed>, SoupBinError> {
        while let Some((ty, bytes)) = self.take_packet()? {
            match ty {
                PacketType::SequencedData => {
                    let sequence = self.next_expected_sequence;
                    self.next_expected_sequence += 1;
                    self.last_frame = bytes;
                    record_message();
                    return Ok(Some(Streamed::Data(sequence)));
                }
                PacketType::ServerHeartbeat => {
                    return Ok(Some(Streamed::Event(SoupBinEvent::HeartbeatReceived)));
                }
                PacketType::EndOfSession => {
                    self.state = ClientState::Closed;
                    // server closes next; queued writes would only meet reset
                    self.out.clear();
                    record_session("eos");
                    tracing::info!(protocol = PROTOCOL, session = %self.assigned_id, "soupbintcp end of session");
                    return Ok(Some(Streamed::Event(SoupBinEvent::EndOfSession)));
                }
                PacketType::Debug => {}
                other => {
                    return Err(SoupBinError::ProtocolViolation(format!(
                        "unexpected packet type in streaming state: {other:?}"
                    )));
                }
            }
        }
        Ok(None)
    }
}

/// One `recv_into` into `buf`'s spare capacity, returning bytes read.
fn land<T: StreamRecv>(
    transport: &mut T,
    buf: &mut BytesMut,
    capacity: usize,
) -> Result<usize, SoupBinError> {
    // reserve first: `split_to` shrinks spare, starved reserve would hand out empty slice
    buf.reserve(capacity);
    let n = transport.recv_into(buf.spare_capacity_mut())?;
    // SAFETY: `StreamRecv` contract: `Ok(n)` only after `recv_into`
    // initialised exactly first `n` bytes of spare slice, `n <= len`.
    unsafe { buf.advance_mut(n) };
    Ok(n)
}

/// Record one sequenced message yielded, from single yield site shared by
/// every receive method, so one gated count never double counts.
#[inline]
fn record_message() {
    #[cfg(feature = "observability")]
    if observability_core::metrics_enabled() {
        observability_core::count_msg();
        if observability_core::should_sample(observability_core::SAMPLE_1_IN_8192) {
            observability_core::merge_local();
        }
    }
}

/// Sync API for transports with partial write: busy-poll on pinned core, or
/// parked on readiness (register `MioTcp` in `ReadySet` before `start`, wait
/// until [`next_deadline`](SoupBinClient::next_deadline) whenever `poll`
/// returns `None`). No runtime.
impl<T: StreamRecv + StreamTrySend> SoupBinClient<T> {
    /// Queue login request over connected `transport` and write what fits.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::Transport`] when first write fails.
    pub fn start(transport: T, cfg: SoupBinClientConfig) -> Result<Self, SoupBinError> {
        let now = Instant::now();
        let mut client = Self::new(transport, cfg);
        client.session.queue_login(now)?;
        client.session.flush(&mut client.transport, now)?;
        Ok(client)
    }

    /// Advance session at `now`: resume pending writes, queue client heartbeat
    /// when due, receive and dispatch until message or drained socket. Yields
    /// data, or [`SoupBinEvent`] for every lifecycle signal. `Ok(None)`: nothing
    /// happened, socket drained, so parked caller may wait.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] once closed and flushed;
    /// [`SoupBinError::LoginTimeout`]; protocol failures; `Transport(PeerClosed)`
    /// when server closes without end of session.
    pub fn poll(&mut self, now: Instant) -> Result<Option<SoupBinMessage<'_>>, SoupBinError> {
        self.session.flush(&mut self.transport, now)?;
        match self.session.state {
            ClientState::Closed if self.session.out.is_empty() => Err(SoupBinError::EndOfSession),
            ClientState::Closed => Ok(None),
            ClientState::Disconnected | ClientState::Authenticating => self.poll_login(now),
            ClientState::Streaming => self.poll_stream(now),
        }
    }

    /// Queue `payload` as `Unsequenced Data (U)`, written by later `poll`s.
    /// Always plain, even under `compressed`.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] once closed;
    /// [`SoupBinError::FrameTooLarge`] when `payload` passes 65534 bytes,
    /// queueing nothing.
    pub fn queue_unsequenced(&mut self, payload: &[u8]) -> Result<(), SoupBinError> {
        if self.session.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        self.session.queue(PacketType::UnsequencedData, payload)
    }

    /// Queue `Logout Request (O)` and close; later `poll`s flush it, then
    /// return [`SoupBinError::EndOfSession`].
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] when already closed.
    pub fn queue_logout(&mut self) -> Result<(), SoupBinError> {
        if self.session.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        self.session.queue(PacketType::LogoutRequest, &[])?;
        self.session.close_logout();
        Ok(())
    }

    fn poll_login(&mut self, now: Instant) -> Result<Option<SoupBinMessage<'_>>, SoupBinError> {
        loop {
            match self.session.dispatch_login(now)? {
                Some(Login::Accepted { session, sequence }) => {
                    return Ok(Some(SoupBinMessage::Event(SoupBinEvent::LoginAccepted {
                        session,
                        sequence,
                    })));
                }
                Some(Login::Rejected(reason)) => {
                    return Ok(Some(SoupBinMessage::Event(SoupBinEvent::LoginRejected {
                        reason,
                    })));
                }
                None => {}
            }
            if !self.session.ingest(&mut self.transport, now)? {
                break;
            }
        }
        if now >= self.session.login_deadline {
            return Err(self.session.login_timed_out());
        }
        Ok(None)
    }

    fn poll_stream(&mut self, now: Instant) -> Result<Option<SoupBinMessage<'_>>, SoupBinError> {
        // before receive, so busy feed never starves client heartbeats
        if self.session.heartbeat_due(now) {
            self.session.queue_heartbeat()?;
            self.session.flush(&mut self.transport, now)?;
            return Ok(Some(SoupBinMessage::Event(SoupBinEvent::HeartbeatSent)));
        }
        loop {
            match self.session.dispatch_stream()? {
                Some(Streamed::Data(sequence)) => {
                    return Ok(Some(SoupBinMessage::Data(self.session.frame(sequence))));
                }
                Some(Streamed::Event(event)) => return Ok(Some(SoupBinMessage::Event(event))),
                None => {}
            }
            // loop until drained: edge-triggered readiness never re-reports leftovers
            if !self.session.ingest(&mut self.transport, now)? {
                break;
            }
        }
        if self.session.heartbeat_timed_out(now) {
            return Ok(Some(SoupBinMessage::Event(SoupBinEvent::HeartbeatTimeout)));
        }
        Ok(None)
    }
}

/// Login payload: user (6), password (10), session (10) left-justified,
/// sequence (20) right-justified, all space padded.
fn login_request(cfg: &SoupBinClientConfig) -> Vec<u8> {
    let mut payload = Vec::with_capacity(6 + 10 + 10 + 20);
    ascii_left_justify(&mut payload, &cfg.username, 6);
    ascii_left_justify(&mut payload, &cfg.password, 10);
    ascii_left_justify(&mut payload, &cfg.requested_session, 10);
    ascii_right_justify(&mut payload, &cfg.requested_sequence_number.to_string(), 20);
    payload
}

/// Append `s` left-justified in `width` bytes, space padded, cut when longer.
fn ascii_left_justify(out: &mut Vec<u8>, s: &str, width: usize) {
    let bytes = &s.as_bytes()[..s.len().min(width)];
    out.extend_from_slice(bytes);
    out.resize(out.len() + width - bytes.len(), b' ');
}

/// Append `s` right-justified in `width` bytes, space padded, keeping its tail.
fn ascii_right_justify(out: &mut Vec<u8>, s: &str, width: usize) {
    let bytes = &s.as_bytes()[s.len().saturating_sub(width)..];
    out.resize(out.len() + width - bytes.len(), b' ');
    out.extend_from_slice(bytes);
}

/// `Login Accepted` payload: session (10) then sequence (20, ASCII).
fn parse_login_accepted(bytes: &[u8]) -> Result<([u8; 10], u64), SoupBinError> {
    let short = || {
        SoupBinError::ProtocolViolation(
            "login accepted payload shorter than Session+SequenceNumber".into(),
        )
    };
    let (session, rest) = bytes.split_first_chunk::<10>().ok_or_else(short)?;
    let sequence = rest.get(..20).ok_or_else(short)?;
    Ok((*session, parse_ascii_numeric(sequence)?))
}

fn parse_ascii_field(bytes: &[u8]) -> Result<&str, SoupBinError> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| SoupBinError::ProtocolViolation("non-ASCII field".into()))
}

fn parse_ascii_numeric(bytes: &[u8]) -> Result<u64, SoupBinError> {
    let s = parse_ascii_field(bytes)?;
    if s.is_empty() {
        return Ok(0);
    }
    s.parse::<u64>()
        .map_err(|_| SoupBinError::ProtocolViolation(format!("bad numeric field: {s:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_rejects_payload_past_length_prefix() {
        let mut session = Session::new(SoupBinClientConfig::default());
        session.queue(PacketType::ClientHeartbeat, &[]).unwrap();
        let before = session.out.clone();
        let max = usize::from(u16::MAX) - 1;

        let result = session.queue(PacketType::UnsequencedData, &vec![7; max + 1]);
        assert!(
            matches!(result, Err(SoupBinError::FrameTooLarge { size, max: m }) if size == max + 1 && m == max),
            "{result:?}"
        );
        assert_eq!(session.out, before, "rejected payload must queue nothing");

        // largest payload still fits: length prefix 0xffff counts type byte
        session
            .queue(PacketType::UnsequencedData, &vec![7; max])
            .unwrap();
        assert_eq!(session.out[before.len()..][..2], [0xff, 0xff]);
    }

    #[test]
    fn login_request_justifies_fields() {
        let payload = login_request(&SoupBinClientConfig {
            username: "user01".into(),
            password: "pass12345".into(),
            requested_session: "toolongsession".into(),
            requested_sequence_number: 42,
            ..Default::default()
        });
        let mut expected = b"user01pass12345 toolongses".to_vec();
        expected.extend_from_slice(&[b' '; 18]);
        expected.extend_from_slice(b"42");
        assert_eq!(payload, expected);
    }

    // one-landing ingest is uncompressed branch only
    #[cfg(not(feature = "compressed"))]
    mod ingest {
        use core::mem::MaybeUninit;

        use transport_core::{Transport, TransportError};

        use super::super::*;

        /// Copies buffered bytes into `dst` on `recv_into`, recording last `dst`
        /// length. Drives ingest without socket.
        struct MockStream {
            pending: Vec<u8>,
            last_dst_len: usize,
        }

        impl Transport for MockStream {
            fn name(&self) -> &'static str {
                "mock-stream"
            }
        }

        // SAFETY: writes `dst[..n]` for returned `n`, and `n <= dst.len()`.
        unsafe impl StreamRecv for MockStream {
            fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
                self.last_dst_len = dst.len();
                let n = self.pending.len().min(dst.len());
                for (slot, byte) in dst[..n].iter_mut().zip(self.pending.drain(..n)) {
                    slot.write(byte);
                }
                Ok(n)
            }
        }

        #[test]
        fn ingest_lands_once_with_no_extra_allocation() {
            let payload = b"steady-state payload, sized well under decode_buf_capacity".to_vec();
            let mut transport = MockStream {
                pending: payload.clone(),
                last_dst_len: 0,
            };
            let mut session = Session::new(SoupBinClientConfig {
                decode_buf_capacity: 4096,
                ..Default::default()
            });
            // warm to steady state so ingest's own reserve is no-op
            session.decode_buf.reserve(session.cfg.decode_buf_capacity);

            let now = Instant::now();
            let info = allocation_counter::measure(|| {
                session.ingest(&mut transport, now).unwrap();
            });
            assert_eq!(info.count_total, 0, "steady-state ingest must not allocate");
            assert_eq!(&session.decode_buf[..], &payload[..]);
        }

        #[test]
        fn ingest_reserve_keeps_spare_available_after_repeated_consume() {
            // regression: without reserve before spare_capacity_mut, split_to
            // eventually starves recv_into to zero-length slice
            let mut transport = MockStream {
                pending: b"0123456789".repeat(20),
                last_dst_len: 0,
            };
            let mut session = Session::new(SoupBinClientConfig {
                decode_buf_capacity: 64,
                ..Default::default()
            });

            for _ in 0..20 {
                session.ingest(&mut transport, Instant::now()).unwrap();
                assert!(
                    transport.last_dst_len > 0,
                    "recv_into starved to empty slice"
                );
                // mimic take_packet consuming front of decode buffer
                let take = session.decode_buf.len().min(10);
                let _ = session.decode_buf.split_to(take);
            }
        }
    }
}
