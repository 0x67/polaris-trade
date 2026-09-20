//! Receiver assembly over caller-built legs, and requester attachment.

use std::{collections::VecDeque, net::SocketAddr};

use smallvec::SmallVec;
use transport_core::{DatagramRecv, DatagramSend, FrameBatch};

use super::{
    BLOCKS_PER_DATAGRAM, Backing, Inner, MAX_INFLIGHT_BURST, MIN_LEG_POOL_CAPACITY,
    MoldUdpReceiver, NoRecovery, RING_CAPACITY, Requester,
};
use crate::{
    ab::AbArbiter, config::MoldUdpReceiverConfig, error::MoldUdpError, gap::GapRequestHandler,
    reassembly::SequenceReassembler,
};

impl<T: DatagramRecv> MoldUdpReceiver<T> {
    /// Receiver over `legs`, each bound and joined to its group by caller.
    /// Two or more legs share one session and sequence space and run A/B
    /// arbitration. No gap recovery until [`with_requester`](Self::with_requester).
    ///
    /// # Errors
    ///
    /// [`MoldUdpError::LegCount`] for zero or more than 255 legs;
    /// [`MoldUdpError::PoolTooSmall`] when leg's pool holds fewer than
    /// [`MIN_LEG_POOL_CAPACITY`] buffers, so undersized pool fails here, not
    /// as live stall once reorder window fills.
    pub fn from_legs(
        cfg: &MoldUdpReceiverConfig,
        legs: SmallVec<[T; 2]>,
    ) -> Result<Self, MoldUdpError> {
        let count = legs.len();
        let recovery_stream = u8::try_from(count)
            .ok()
            .filter(|&n| n > 0)
            .ok_or(MoldUdpError::LegCount { count })?;
        for (leg, t) in legs.iter().enumerate() {
            let capacity = t.pool_stats().capacity;
            if capacity < MIN_LEG_POOL_CAPACITY {
                return Err(MoldUdpError::PoolTooSmall {
                    leg,
                    capacity,
                    required: MIN_LEG_POOL_CAPACITY,
                });
            }
        }
        let mut arbiter = (count > 1).then(|| {
            let window_ms = u64::try_from(cfg.gap_confirm_window.as_millis()).unwrap_or(u64::MAX);
            AbArbiter::new(count, RING_CAPACITY, window_ms)
        });
        let mut reassembler = SequenceReassembler::new(RING_CAPACITY);
        // configured start anchors now; otherwise first packet anchors in decode
        if let Some(start) = cfg.start_sequence {
            reassembler.reset_expected(start);
            if let Some(arb) = arbiter.as_mut() {
                arb.rebase(start);
            }
        }
        Ok(Self {
            inner: Inner {
                legs,
                recovery_stream,
                session: None,
                reassembler,
                gap_handler: GapRequestHandler::new(),
                arbiter,
                max_rerequests_per_gap_per_sec: cfg.max_rerequests_per_gap_per_sec,
                ready: VecDeque::with_capacity(MAX_INFLIGHT_BURST.get()),
                current: None,
                pending_datagrams: VecDeque::with_capacity(MAX_INFLIGHT_BURST.get()),
                backing: Backing::empty(),
                inline_pending: 0,
                seq_anchored: cfg.start_sequence.is_some(),
                recv_batch: FrameBatch::with_capacity(MAX_INFLIGHT_BURST),
                blocks: Vec::with_capacity(BLOCKS_PER_DATAGRAM),
            },
            recovery: NoRecovery,
        })
    }

    /// Enable gap recovery: re-requests go from `requester` to `server`, and
    /// `requester` is read while any gap is pending, since `MoldUDP64` unicasts
    /// retransmission back to request's source. Retransmitted datagrams are
    /// copied into receiver memory and their buffers returned at once, so
    /// `Q`'s frame type need not match legs'. Parked callers register
    /// `requester` beside legs before handing it over.
    pub fn with_requester<Q: DatagramRecv + DatagramSend>(
        self,
        requester: Q,
        server: SocketAddr,
    ) -> MoldUdpReceiver<T, Requester<Q>> {
        let recovery = Requester::new(
            requester,
            server,
            self.inner.max_rerequests_per_gap_per_sec,
            MAX_INFLIGHT_BURST,
        );
        MoldUdpReceiver {
            inner: self.inner,
            recovery,
        }
    }
}
