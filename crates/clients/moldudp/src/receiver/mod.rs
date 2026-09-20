//! Wire codec, reassembler, gap tracking and optional A/B arbiter assembled
//! into one receiver over caller-built legs of any [`DatagramRecv`].
//!
//! [`MoldUdpReceiver::poll`] reaps owned frames by burst. Datagram whose leading
//! sequence is already `expected_next` drains inline, borrowed straight from
//! still-owned frame (no allocation); datagram ahead of `expected_next`
//! promotes its frame to one `Arc` and buffers [`MessageView`]s in reassembler
//! until gap fills. Recovery is type state: [`NoRecovery`] carries nothing,
//! [`Requester`] sends re-requests and reads retransmissions.
//!
//! ```no_run
//! # use client_moldudp::{MIN_LEG_POOL_CAPACITY, MoldUdpError, MoldUdpReceiver, MoldUdpReceiverConfig};
//! # use smallvec::smallvec;
//! # use transport_socket::{UdpConfig, UdpSocket};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut cfg = UdpConfig::new("0.0.0.0:30001".parse()?);
//! cfg.slab_count = MIN_LEG_POOL_CAPACITY.try_into()?;
//! let leg = UdpSocket::bind(&cfg)?; // join multicast group here
//! let requester = UdpSocket::bind(&UdpConfig::new("0.0.0.0:0".parse()?))?;
//! let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])?
//!     .with_requester(requester, "10.0.0.2:40000".parse()?);
//! loop {
//!     match rx.poll() {
//!         Ok(Some(outcome)) => drop(outcome), // borrowed until next poll
//!         Ok(None) | Err(MoldUdpError::GapDetected) => {} // gap: this call only
//!         Err(e) => return Err(e.into()),
//!     }
//! }
//! # }
//! ```

mod construct;
mod decode;
mod recovery;
mod recv;
mod wait;

use std::{collections::VecDeque, fmt, num::NonZeroUsize, sync::Arc};

pub use recovery::{AsyncRecovery, NoRecovery, Recovery, Requester};
use smallvec::SmallVec;
use transport_core::{DatagramRecv, FrameBatch};

use crate::{
    ab::{AbArbiter, ArbiterStats},
    event::MoldUdpEvent,
    frame::{Frame, Held, MessageView, OwnedFrame},
    gap::{GapRequest, GapRequestHandler},
    reassembly::SequenceReassembler,
    wire::HEADER_LEN,
};

/// Ring capacity for both sequence reassembler and A/B arbiter window.
const RING_CAPACITY: usize = 4096;

/// Burst depth per `recv_burst` call, also pool headroom on top of
/// [`RING_CAPACITY`]: full reorder window may pin buffered slabs while fresh
/// burst lands.
const MAX_INFLIGHT_BURST: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Smallest leg pool [`MoldUdpReceiver::from_legs`] accepts: reorder window
/// plus one burst. Size each leg's receive pool at least this large.
pub const MIN_LEG_POOL_CAPACITY: usize = RING_CAPACITY + MAX_INFLIGHT_BURST.get();

/// Block scratch capacity: every block of 1500-byte MTU datagram, each at
/// least its 2-byte length prefix. Bigger datagram grows it once.
const BLOCKS_PER_DATAGRAM: usize = (1500 - HEADER_LEN) / 2;

/// Record one message yielded to caller: gated thread-local count plus
/// 1-in-8192 sampled merge, so flusher on another thread sees total without
/// caller wiring merge tick. Single `Cell` read when gate off.
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

/// Record one client-visible gap, once per [`ReadyItem::Gap`] pushed, so
/// counter matches `GapDetected` results caller observes.
#[inline]
fn record_gap() {
    #[cfg(feature = "observability")]
    if observability_core::metrics_enabled() {
        metrics::counter!("client.gaps", "protocol" => "moldudp").increment(1);
    }
}

/// Drained item waiting for caller. `Inline` borrows from receiver's
/// [`Backing`] (zero-alloc in-order path); `View` carries own `Arc`
/// (gap-buffered or cascade-drained).
enum ReadyItem<F> {
    Inline {
        sequence: u64,
        stream_id: u8,
        offset: usize,
        len: usize,
    },
    View {
        view: MessageView<F>,
        sequence: u64,
        stream_id: u8,
    },
    Event(MoldUdpEvent),
    Gap,
}

/// Datagram backing outstanding `Inline` items: as reaped, or shared once a
/// message inside it needed buffering. At most one `Arc::new` per datagram.
enum Backing<F> {
    Owned(Held<F>),
    Shared(Arc<Held<F>>),
}

impl<F> Backing<F> {
    // no datagram: empty bytes, `Box<[u8]>` of len 0 does not allocate
    fn empty() -> Self {
        Self::Owned(Held::Copied(Box::default()))
    }

    fn share(self) -> (Self, Arc<Held<F>>) {
        let arc = match self {
            Self::Owned(held) => Arc::new(held),
            Self::Shared(arc) => arc,
        };
        (Self::Shared(Arc::clone(&arc)), arc)
    }
}

impl<F: AsRef<[u8]>> Backing<F> {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Owned(held) => held.as_ref(),
            Self::Shared(arc) => (**arc).as_ref(),
        }
    }
}

/// What [`MoldUdpReceiver::poll`] and `recv` hand back on data or control
/// packet. `recv_owned` uses [`Owned`](Self::Owned) so message can move to
/// another thread.
pub enum MoldUdpOutcome<'a, F> {
    /// Message borrowed from receiver until next call.
    Frame(Frame<'a>),
    /// Message owning share of its datagram.
    Owned(OwnedFrame<F>),
    /// Heartbeat or end of session.
    Event(MoldUdpEvent),
}

// manual: derive would demand `F: Debug`, which backend frames need not carry
impl<F> fmt::Debug for MoldUdpOutcome<'_, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MoldUdpOutcome::Frame(frame) => f.debug_tuple("Frame").field(frame).finish(),
            MoldUdpOutcome::Owned(owned) => f
                .debug_struct("Owned")
                .field("sequence", &owned.sequence)
                .field("stream_id", &owned.stream_id)
                .finish(),
            MoldUdpOutcome::Event(ev) => f.debug_tuple("Event").field(ev).finish(),
        }
    }
}

/// Receiver health: arbiter stats (multi-leg only) and outstanding gaps.
#[derive(Debug, Clone, Default)]
pub struct ReceiverStats {
    /// Per-leg race counters; `None` with one leg.
    pub arbiter: Option<ArbiterStats>,
    /// Gaps not yet filled.
    pub pending_gaps: Vec<GapRequest>,
}

/// `MoldUDP64` receiver over caller-built legs of any [`DatagramRecv`], with
/// recovery mode `R`: [`NoRecovery`] or [`Requester`].
pub struct MoldUdpReceiver<T: DatagramRecv, R = NoRecovery> {
    inner: Inner<T>,
    recovery: R,
}

/// Everything but recovery, so [`MoldUdpReceiver::with_requester`] moves it whole.
struct Inner<T: DatagramRecv> {
    legs: SmallVec<[T; 2]>,
    // stream id tagging datagrams read off requester: leg count
    recovery_stream: u8,
    session: Option<[u8; 10]>,
    reassembler: SequenceReassembler<MessageView<T::Frame>>,
    // one past highest sequence seen on any source; gaps open only beyond it
    next_unseen: u64,
    gap_handler: GapRequestHandler,
    arbiter: Option<AbArbiter>,
    max_rerequests_per_gap_per_sec: u32,
    ready: VecDeque<ReadyItem<T::Frame>>,
    // item last handed out; its borrow ends when caller calls again
    current: Option<ReadyItem<T::Frame>>,
    // reaped, not yet decoded into `ready`
    pending_datagrams: VecDeque<(u8, Held<T::Frame>)>,
    backing: Backing<T::Frame>,
    // outstanding `Inline` items; `backing` resets (slab reclaimed) at zero
    inline_pending: usize,
    // configured `start_sequence` anchors at construction, else first packet
    seq_anchored: bool,
    // preallocated, reused every burst
    recv_batch: FrameBatch<T::Frame>,
    // (seq, offset, len) per block of datagram being decoded; reused, never shrunk
    blocks: Vec<(u64, usize, usize)>,
}

impl<T: DatagramRecv, R> MoldUdpReceiver<T, R> {
    /// Legs, in `from_legs` order, e.g. for local address.
    pub fn transports(&self) -> &[T] {
        &self.inner.legs
    }

    /// Arbiter counters and outstanding gaps.
    pub fn stats(&self) -> ReceiverStats {
        ReceiverStats {
            arbiter: self.inner.arbiter.as_ref().map(AbArbiter::stats),
            pending_gaps: self.inner.gap_handler.pending_gaps(),
        }
    }
}
