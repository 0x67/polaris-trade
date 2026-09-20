//! Gap recovery type state: [`NoRecovery`] carries nothing, [`Requester`]
//! owns unicast socket that sends re-requests and reads retransmissions.

use std::{future, net::SocketAddr, num::NonZeroUsize};

use transport_core::{AsyncReady, DatagramRecv, DatagramSend, FrameBatch, TransportError};

use crate::gap::{GapRequest, GapRequestEmitter, GapRequestHandler};

// only this crate's recovery modes plug into receiver
pub(crate) mod sealed {
    use transport_core::TransportError;

    use crate::gap::GapRequestHandler;

    pub trait Sealed {
        /// Send re-requests for `gaps` not rate-limited now; reads clock. Send
        /// failure is logged, never returned, so it can't stall leg receive.
        fn send_due(&mut self, session: [u8; 10], gaps: &GapRequestHandler);

        /// Take one burst, handing each datagram's bytes to `copy` before its
        /// buffer returns to pool. Returns whether anything arrived.
        fn poll_frames(&mut self, copy: impl FnMut(&[u8])) -> Result<bool, TransportError>;
    }

    pub trait SealedReady {
        /// Resolve once recovery socket has data.
        fn ready(&mut self) -> impl Future<Output = Result<(), TransportError>> + Send;
    }
}

/// Recovery mode of [`MoldUdpReceiver`](super::MoldUdpReceiver). Sealed.
pub trait Recovery: sealed::Sealed {}

/// Recovery mode whose socket has async readiness; gates `recv` and
/// `recv_owned`. Sealed.
pub trait AsyncRecovery: Recovery + sealed::SealedReady {}

/// No gap recovery: gaps are reported and tracked, never re-requested.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoRecovery;

impl sealed::Sealed for NoRecovery {
    fn send_due(&mut self, _: [u8; 10], _: &GapRequestHandler) {}

    fn poll_frames(&mut self, _: impl FnMut(&[u8])) -> Result<bool, TransportError> {
        Ok(false)
    }
}

impl sealed::SealedReady for NoRecovery {
    fn ready(&mut self) -> impl Future<Output = Result<(), TransportError>> + Send {
        future::pending()
    }
}

impl Recovery for NoRecovery {}
impl AsyncRecovery for NoRecovery {}

/// Gap recovery over requester socket `Q`: sends rate-limited re-requests to
/// server and reads retransmissions server unicasts back.
pub struct Requester<Q: DatagramRecv> {
    sock: Q,
    emitter: GapRequestEmitter,
    // requester frames are copied and dropped at once, never held
    scratch: FrameBatch<Q::Frame>,
    // refilled every send, so open gap costs no allocation per poll
    due: Vec<GapRequest>,
}

impl<Q: DatagramRecv> Requester<Q> {
    pub(super) fn new(
        sock: Q,
        server: SocketAddr,
        max_per_gap_per_sec: u32,
        burst: NonZeroUsize,
    ) -> Self {
        Self {
            sock,
            emitter: GapRequestEmitter::new(server, max_per_gap_per_sec),
            scratch: FrameBatch::with_capacity(burst),
            due: Vec::new(),
        }
    }
}

impl<Q: DatagramRecv + DatagramSend> sealed::Sealed for Requester<Q> {
    fn send_due(&mut self, session: [u8; 10], gaps: &GapRequestHandler) {
        gaps.pending_gaps_into(&mut self.due);
        match self.emitter.emit(&self.due, session, &mut self.sock) {
            Ok(0) => {}
            Ok(sent) => tracing::debug!(sent, "gap re-requests sent"),
            // emitter already backed failed range off one interval
            Err(error) => tracing::warn!(
                server = %self.emitter.server_addr,
                %error,
                "gap re-request send failed on requester socket; retrying next interval"
            ),
        }
    }

    fn poll_frames(&mut self, mut copy: impl FnMut(&[u8])) -> Result<bool, TransportError> {
        let n = self.sock.recv_burst(&mut self.scratch)?;
        for frame in self.scratch.drain() {
            copy(frame.as_ref());
        }
        Ok(n > 0)
    }
}

impl<Q: DatagramRecv + DatagramSend + AsyncReady> sealed::SealedReady for Requester<Q> {
    fn ready(&mut self) -> impl Future<Output = Result<(), TransportError>> + Send {
        self.sock.ready()
    }
}

impl<Q: DatagramRecv + DatagramSend> Recovery for Requester<Q> {}
impl<Q: DatagramRecv + DatagramSend + AsyncReady> AsyncRecovery for Requester<Q> {}
