//! Synchronous receive: send due re-requests, poll every source, decode.

use std::time::Instant;

use transport_core::DatagramRecv;

use super::{MoldUdpOutcome, MoldUdpReceiver, Recovery};
use crate::{error::MoldUdpError, frame::Held};

impl<T: DatagramRecv, R: Recovery> MoldUdpReceiver<T, R> {
    /// Next data frame or control event, borrowed until next call. Never waits.
    /// `Ok(None)` only after every leg (and requester, while gap pending)
    /// returned nothing this call, so parked caller may wait; with gaps pending,
    /// wait with timeout, since re-requests go out only from `poll`. A/B gap
    /// candidate confirms only when later datagram lands after confirm window.
    ///
    /// # Errors
    ///
    /// [`MoldUdpError::GapDetected`] for this call only: keep calling. Other
    /// variants are leg or packet failures; failed re-request send is `warn` log.
    pub fn poll(&mut self) -> Result<Option<MoldUdpOutcome<'_, T::Frame>>, MoldUdpError> {
        self.inner.retire_current();
        self.pump()?;
        match self.inner.ready.pop_front() {
            Some(item) => self.inner.yield_item(item).map(Some),
            None => Ok(None),
        }
    }

    /// Send due re-requests, then poll and decode until `ready` holds item or
    /// every source (legs, plus requester while gap pending) returned nothing.
    pub(super) fn pump(&mut self) -> Result<(), MoldUdpError> {
        let inner = &mut self.inner;
        if inner.gap_handler.has_pending()
            && let Some(session) = inner.session
        {
            self.recovery.send_due(session, &inner.gap_handler);
        }
        loop {
            if !inner.ready.is_empty() {
                return Ok(());
            }
            if inner.pending_datagrams.is_empty() {
                let mut landed = inner.poll_legs()?;
                if inner.gap_handler.has_pending() {
                    let stream = inner.recovery_stream;
                    let pending = &mut inner.pending_datagrams;
                    landed |= self.recovery.poll_frames(|bytes| {
                        pending.push_back((stream, Held::Copied(bytes.into())));
                    })?;
                }
                if !landed {
                    return Ok(());
                }
            }
            // arbiter clock: one read per datagram, none on one leg
            let now = inner.arbiter.is_some().then(Instant::now);
            inner.process_next_pending(now)?;
            if let Some(now) = now {
                inner.drain_confirmed_gaps(now);
            }
        }
    }
}
