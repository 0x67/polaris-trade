//! One blocking poll over many borrowed sockets.

use std::{io, num::NonZeroUsize, time::Duration};

use ::mio::{Events, Poll, Token};
use transport_core::TransportError;

use super::sealed::Sealed;
use crate::io_error;

// far above any leg count; keeps event buffer allocation bounded
const MAX_EVENTS: usize = 1 << 16;

/// Caller-owned poll reporting which registered sockets are ready, by token.
///
/// Edge-triggered: after an event, caller drains that source until it yields
/// nothing (`Ok(0)` from `recv_burst`, `recv_into` or `try_send`) before
/// waiting again; data left behind may never be reported. Registration
/// reports data already queued. Idle source never hides ready one.
#[derive(Debug)]
pub struct ReadySet {
    poll: Poll,
    events: Events,
}

/// Caller-chosen id reported back by [`ReadySet::wait`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReadyToken(pub usize);

/// One readiness report from [`ReadySet::wait`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ready {
    /// Token source was registered with.
    pub token: ReadyToken,
    /// Receive can make progress: data, peer close or pending error.
    pub readable: bool,
    /// Write can make progress (TCP only).
    pub writable: bool,
}

/// Socket [`ReadySet`] can watch: `MioUdp` (read), `MioTcp` (read and write).
/// Sealed.
pub trait ReadySource: Sealed {}

impl ReadySet {
    /// Poll reporting at most `events` sources per [`wait`](Self::wait).
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] when `events` exceeds 65536;
    /// [`TransportError::Io`] when OS poll cannot be created.
    pub fn new(events: NonZeroUsize) -> Result<Self, TransportError> {
        if events.get() > MAX_EVENTS {
            return Err(TransportError::InvalidConfig {
                field: "events",
                reason: "above 65536",
            });
        }
        let poll = Poll::new().map_err(io_error("poll_create"))?;
        Ok(Self {
            poll,
            events: Events::with_capacity(events.get()),
        })
    }

    /// Watch `src` under `token`; `src` stays caller-owned and may move after.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when `src` is already registered or OS refuses.
    pub fn register<S: ReadySource>(
        &mut self,
        src: &mut S,
        token: ReadyToken,
    ) -> Result<(), TransportError> {
        self.poll
            .registry()
            .register(src.source(), Token(token.0), S::INTEREST)
            .map_err(io_error("register"))
    }

    /// Stop watching `src`.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when `src` is not registered here or OS refuses.
    pub fn deregister<S: ReadySource>(&mut self, src: &mut S) -> Result<(), TransportError> {
        self.poll
            .registry()
            .deregister(src.source())
            .map_err(io_error("deregister"))
    }

    /// Block until a registered source is ready or `timeout` passes (`None`:
    /// no bound), then replace `ready`'s contents with every report.
    ///
    /// Empty `ready` after return: timeout, or wait interrupted by signal.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when OS poll fails.
    pub fn wait(
        &mut self,
        timeout: Option<Duration>,
        ready: &mut Vec<Ready>,
    ) -> Result<(), TransportError> {
        ready.clear();
        match self.poll.poll(&mut self.events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(error) => {
                return Err(TransportError::Io {
                    stage: "poll",
                    error,
                });
            }
        }
        ready.extend(self.events.iter().map(|event| Ready {
            token: ReadyToken(event.token().0),
            readable: event.is_readable() || event.is_read_closed() || event.is_error(),
            writable: event.is_writable() || event.is_write_closed(),
        }));
        Ok(())
    }
}
