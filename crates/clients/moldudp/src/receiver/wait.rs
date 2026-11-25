//! Async receive for legs (and requester) with async readiness.

use std::{future::poll_fn, pin::pin, task::Poll};

use transport_core::{AsyncReady, DatagramRecv};

use super::{AsyncRecovery, MoldUdpOutcome, MoldUdpReceiver, ReadyItem};
use crate::error::MoldUdpError;

impl<T: DatagramRecv + AsyncReady, R: AsyncRecovery> MoldUdpReceiver<T, R> {
    /// Next data frame or control event, borrowed until next call. Spins
    /// [`poll`](Self::poll)'s path, then waits on every leg and, while gap is
    /// pending, requester. No timer: pending gap is re-requested again only on
    /// next wake, so on quiet feed wrap call in timeout (cancel-safe) and repeat.
    ///
    /// # Errors
    ///
    /// As [`poll`](Self::poll); [`MoldUdpError::GapDetected`] is for this call only.
    pub async fn recv(&mut self) -> Result<MoldUdpOutcome<'_, T::Frame>, MoldUdpError> {
        self.inner.retire_current();
        let item = self.next_item().await?;
        self.inner.yield_item(item)
    }

    /// Owned counterpart to [`recv`](Self::recv) for cross-thread handoff (e.g.
    /// sharded engine core): message shares its datagram through `Arc`.
    ///
    /// # Errors
    ///
    /// As [`recv`](Self::recv).
    pub async fn recv_owned(&mut self) -> Result<MoldUdpOutcome<'static, T::Frame>, MoldUdpError> {
        self.inner.retire_current();
        let item = self.next_item().await?;
        self.inner.yield_owned(item)
    }

    async fn next_item(&mut self) -> Result<ReadyItem<T::Frame>, MoldUdpError> {
        loop {
            self.pump()?;
            if let Some(item) = self.inner.ready.pop_front() {
                return Ok(item);
            }
            self.wait_ready().await?;
        }
    }

    /// Wait until any leg, or requester while gap is pending, is readable.
    /// Entered only after sync spin found nothing, so boxed per-leg futures
    /// never touch hot path.
    async fn wait_ready(&mut self) -> Result<(), MoldUdpError> {
        let gap_pending = self.inner.gap_handler.has_pending();
        let mut legs: Vec<_> = self
            .inner
            .legs
            .iter_mut()
            .map(|leg| Box::pin(leg.ready()))
            .collect();
        // built unpolled; polled only while gap pending
        let mut requester = pin!(self.recovery.ready());
        poll_fn(|cx| {
            for leg in &mut legs {
                if let Poll::Ready(result) = leg.as_mut().poll(cx) {
                    return Poll::Ready(result);
                }
            }
            if gap_pending {
                requester.as_mut().poll(cx)
            } else {
                Poll::Pending
            }
        })
        .await?;
        Ok(())
    }
}
