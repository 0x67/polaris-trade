//! Synchronous receive: send due re-requests, reap every source, decode.

use transport_core::DatagramRecv;

use super::{MoldUdpOutcome, MoldUdpReceiver, Recovery};
use crate::{error::MoldUdpError, frame::Held};

impl<T: DatagramRecv, R: Recovery> MoldUdpReceiver<T, R> {
    /// Next data frame or control event, borrowed until next call. Never waits.
    /// `Ok(None)` only after every leg (and requester, while gap pending)
    /// returned nothing this call, so parked caller may wait; with gaps pending,
    /// wait with timeout, since re-requests go out only from `poll`.
    ///
    /// # Errors
    ///
    /// [`MoldUdpError::GapDetected`] for this call only: gap recorded, keep
    /// calling. Session mismatch, malformed packet, reorder overflow and
    /// transport failure surface as own variants.
    pub fn poll(&mut self) -> Result<Option<MoldUdpOutcome<'_, T::Frame>>, MoldUdpError> {
        self.inner.retire_current();
        self.pump()?;
        match self.inner.ready.pop_front() {
            Some(item) => self.inner.yield_item(item).map(Some),
            None => Ok(None),
        }
    }

    /// Send due re-requests, then reap and decode until `ready` holds item or
    /// every source (legs, plus requester while gap pending) returned nothing.
    pub(super) fn pump(&mut self) -> Result<(), MoldUdpError> {
        let inner = &mut self.inner;
        if inner.gap_handler.has_pending()
            && let Some(session) = inner.session
        {
            self.recovery.send_due(session, &inner.gap_handler)?;
        }
        loop {
            if !inner.ready.is_empty() {
                return Ok(());
            }
            if inner.pending_datagrams.is_empty() {
                let mut reaped = inner.reap_legs()?;
                if inner.gap_handler.has_pending() {
                    let stream = inner.recovery_stream;
                    let pending = &mut inner.pending_datagrams;
                    reaped |= self.recovery.reap(|bytes| {
                        pending.push_back((stream, Held::Copied(bytes.into())));
                    })?;
                }
                if !reaped {
                    return Ok(());
                }
            }
            inner.process_next_pending()?;
            inner.drain_confirmed_gaps();
        }
    }
}
