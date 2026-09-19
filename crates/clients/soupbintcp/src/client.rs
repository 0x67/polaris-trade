//! [`SoupBinClient`]: session over stream transport `T`, plus async API.
//!
//! Protocol state lives in `session`; this file holds async driver for
//! transports with [`StreamSend`] and [`AsyncReady`]. Synchronous driver
//! (`start`, `poll`) sits beside state machine in `session`.

use std::{future::poll_fn, pin::pin, task::Poll, time::Instant};

use transport_core::{AsyncReady, StreamRecv, StreamSend};

use crate::{
    config::SoupBinClientConfig,
    error::SoupBinError,
    event::{SoupBinEvent, SoupBinMessage},
    session::{Login, Session, Streamed},
    wire::PacketType,
};

/// Session lifecycle stage. Only `Streaming` yields data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Built, login not queued yet.
    Disconnected,
    /// Login sent, awaiting accept or reject.
    Authenticating,
    /// Logged in; sequenced data flows.
    Streaming,
    /// Ended: end of session, logout, rejected login or heartbeat timeout.
    Closed,
}

/// `SoupBinTCP` v3.0 session over connected stream transport `T`.
///
/// Sync API ([`start`](Self::start), [`poll`](Self::poll)) needs
/// `T: StreamRecv + StreamTrySend`; async API ([`connect`](Self::connect),
/// [`recv`](Self::recv)) needs `T: StreamRecv + StreamSend + AsyncReady`. Both
/// drive one protocol state machine.
pub struct SoupBinClient<T> {
    pub(crate) transport: T,
    pub(crate) session: Session,
}

impl<T> SoupBinClient<T> {
    pub(crate) fn new(transport: T, cfg: SoupBinClientConfig) -> Self {
        Self {
            transport,
            session: Session::new(cfg),
        }
    }

    /// Sequence server assigns to next `Sequenced Data` packet. Use it as
    /// `requested_sequence_number` on reconnect.
    pub fn next_expected_sequence(&self) -> u64 {
        self.session.next_expected_sequence()
    }

    /// Session id server assigned at login.
    pub fn session(&self) -> &str {
        self.session.session_id()
    }

    /// Current lifecycle stage.
    pub fn state(&self) -> ClientState {
        self.session.state
    }

    /// Earliest instant caller must act on: login deadline while
    /// authenticating, else sooner of next client heartbeat due and
    /// server-silence deadline. Parked loops wait until it when `poll` or
    /// `recv` has nothing.
    pub fn next_deadline(&self) -> Instant {
        self.session.next_deadline()
    }
}

/// Async API: login and receive await [`AsyncReady::ready`], writes go
/// through [`StreamSend::send_all`].
impl<T: StreamRecv + StreamSend + AsyncReady> SoupBinClient<T> {
    /// Run login handshake over connected `transport`.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::LoginRejected`] on `Login Rejected`,
    /// [`SoupBinError::LoginTimeout`] with no answer within `login_timeout`,
    /// [`SoupBinError::Transport`] on stream failure. Transport drops with
    /// client on error, closing socket.
    pub async fn connect(transport: T, cfg: SoupBinClientConfig) -> Result<Self, SoupBinError> {
        let mut client = Self::new(transport, cfg);
        client.session.queue_login(Instant::now());
        client.write_out().await?;
        client.login().await?;
        Ok(client)
    }

    /// Wait for next sequenced data frame or lifecycle event. Debug packets
    /// drop silently. Cancel-safe while waiting.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] once session closed; protocol and
    /// transport failures (peer close is `Transport(PeerClosed)`).
    pub async fn recv(&mut self) -> Result<SoupBinMessage<'_>, SoupBinError> {
        self.ensure_open()?;
        loop {
            match self.session.dispatch_stream()? {
                Some(Streamed::Data(sequence)) => {
                    return Ok(SoupBinMessage::Data(self.session.frame(sequence)));
                }
                Some(Streamed::Event(event)) => return Ok(SoupBinMessage::Event(event)),
                None => {}
            }
            self.await_more_bytes().await?;
        }
    }

    /// [`recv`](Self::recv) with heartbeats handled: selects next message
    /// against heartbeat deadline; when deadline wins, sends client heartbeat
    /// or reports [`SoupBinEvent::HeartbeatTimeout`] on dead link, then
    /// resumes. Data wins ties. Other runtimes drive `recv`, `next_deadline`
    /// and `tick_heartbeat` themselves.
    ///
    /// # Errors
    ///
    /// As [`recv`](Self::recv).
    #[cfg(feature = "tokio")]
    pub async fn recv_managed(&mut self) -> Result<SoupBinMessage<'_>, SoupBinError> {
        self.ensure_open()?;
        loop {
            match self.session.dispatch_stream()? {
                Some(Streamed::Data(sequence)) => {
                    return Ok(SoupBinMessage::Data(self.session.frame(sequence)));
                }
                Some(Streamed::Event(event)) => return Ok(SoupBinMessage::Event(event)),
                None => {}
            }
            // `await_more_bytes` returns unit, so no self borrow escapes select
            let deadline = self.next_deadline();
            tokio::select! {
                biased;
                ready = self.await_more_bytes() => ready?,
                () = tokio::time::sleep_until(deadline.into()) => {
                    if let Some(event @ SoupBinEvent::HeartbeatTimeout) = self.tick_heartbeat().await? {
                        return Ok(SoupBinMessage::Event(event));
                    }
                }
            }
        }
    }

    /// Write `payload` as `Unsequenced Data (U)`. Always plain, even under
    /// `compressed`: upstream never deflates.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] once closed; transport failure.
    pub async fn send_unsequenced(&mut self, payload: &[u8]) -> Result<(), SoupBinError> {
        self.ensure_open()?;
        self.session.queue(PacketType::UnsequencedData, payload);
        self.write_out().await
    }

    /// Send `Logout Request (O)` and close. No-op once closed.
    ///
    /// # Errors
    ///
    /// Transport failure writing logout.
    pub async fn logout(&mut self) -> Result<(), SoupBinError> {
        if self.session.state == ClientState::Closed {
            return Ok(());
        }
        self.session.queue(PacketType::LogoutRequest, &[]);
        self.write_out().await?;
        self.session.close_logout();
        Ok(())
    }

    /// Send `Client Heartbeat (R)` when `heartbeat_interval` passed since last
    /// send; report [`SoupBinEvent::HeartbeatTimeout`] and close when server
    /// was silent past `heartbeat_timeout`. Call on own timer; client runs no
    /// clock task.
    ///
    /// # Errors
    ///
    /// [`SoupBinError::EndOfSession`] once closed; transport failure.
    pub async fn tick_heartbeat(&mut self) -> Result<Option<SoupBinEvent>, SoupBinError> {
        self.ensure_open()?;
        let now = Instant::now();
        if self.session.heartbeat_timed_out(now) {
            return Ok(Some(SoupBinEvent::HeartbeatTimeout));
        }
        if self.session.heartbeat_due(now) {
            self.session.queue_heartbeat();
            self.write_out().await?;
            return Ok(Some(SoupBinEvent::HeartbeatSent));
        }
        Ok(None)
    }

    fn ensure_open(&self) -> Result<(), SoupBinError> {
        if self.session.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        Ok(())
    }

    async fn write_out(&mut self) -> Result<(), SoupBinError> {
        self.transport.send_all(self.session.pending_out()).await?;
        self.session.sent_all(Instant::now());
        Ok(())
    }

    async fn login(&mut self) -> Result<(), SoupBinError> {
        let deadline = self.session.login_deadline();
        loop {
            match self.session.dispatch_login(Instant::now())? {
                Some(Login::Accepted { .. }) => return Ok(()),
                Some(Login::Rejected(reason)) => {
                    return Err(SoupBinError::LoginRejected {
                        code: char::from(reason).to_string(),
                    });
                }
                None => {}
            }
            if Instant::now() >= deadline {
                return Err(self.session.login_timed_out());
            }
            self.await_more_bytes_with_deadline(deadline).await?;
        }
    }

    /// Wait until transport is readable, then land its bytes. No deadline:
    /// heartbeat silence is caught by `tick_heartbeat`.
    async fn await_more_bytes(&mut self) -> Result<(), SoupBinError> {
        self.transport.ready().await?;
        self.session.ingest(&mut self.transport, Instant::now())?;
        Ok(())
    }

    /// As `await_more_bytes`, but self-wakes on `Pending` to recheck clock
    /// each scheduler tick: no timer dependency, so login timeout is polled.
    async fn await_more_bytes_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> Result<(), SoupBinError> {
        // scoped: pinned future holds `&mut self.transport` until dropped
        let ready = {
            let mut ready = pin!(self.transport.ready());
            poll_fn(|cx| match ready.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(Some(result)),
                Poll::Pending if Instant::now() >= deadline => Poll::Ready(None),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        };
        match ready {
            Some(result) => {
                result?;
                self.session.ingest(&mut self.transport, Instant::now())?;
                Ok(())
            }
            None => Err(self.session.login_timed_out()),
        }
    }
}
