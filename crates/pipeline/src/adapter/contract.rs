//! The adapter traits and the counters every adapter reports.

use super::{event::MarketEvent, session::SessionModel};
use crate::{coverage::CoverageMatrix, ingest::StreamId};

/// Per-adapter decode counters. `decoded` and the error counters count whole
/// wire frames, one per `feed` call, not emitted events: one frame can emit
/// several records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct AdapterStats {
    /// Frames dispatched to a decoder.
    pub decoded: u64,
    /// Frames whose message type the adapter does not know.
    pub unknown_kind: u64,
    /// Frames of a known type with an invalid length or body.
    pub malformed: u64,
}

/// Turns one venue's wire bytes into normalized [`MarketEvent`]s.
///
/// The static, monomorphized seam the hot path runs on. It is not object-safe
/// (`drain` returns `impl Iterator`); [`DynAdapter`] is the erased form, and
/// every implementor gets it from a blanket impl.
pub trait VenueAdapter {
    /// Venue-shaped construction input: tier tables, session anchors, venue id.
    type Config;

    /// Build an adapter for one session.
    fn new(config: Self::Config) -> Self;

    /// Ingest streams this adapter consumes; a single-channel venue has one.
    fn streams() -> &'static [StreamId];

    /// Decode coverage for one stream, checked against the dispatch arms.
    fn coverage(stream: StreamId) -> CoverageMatrix;

    /// The record types this venue builds from the wire, as `md_events::rtype`
    /// bytes. A consumer reads it to predict the output without naming the
    /// venue. It is the set of rtypes of every event [`Self::drain`] can yield,
    /// no more and no fewer. Records the derive engine builds (`Mbp10`, `Bbo`,
    /// `Ohlcv`) are not listed, and bands follow from `Category::of_rtype`.
    /// [`Self::coverage`] answers a different question, which wire kinds decode
    /// and what they feed.
    fn record_types() -> &'static [u8];

    /// The trading-time model this venue's sessions follow.
    fn session(&self) -> SessionModel;

    /// Decode one wire frame from `stream` received at `ts_recv`. Every record
    /// the frame emits carries that `ts_recv` and the wire `venue_seq`
    /// verbatim, except a `Definition` the adapter mints from its own registry.
    ///
    /// Malformed and unknown frames, empty ones included, are counted in
    /// [`AdapterStats`], never a panic; frames on streams outside
    /// [`Self::streams`] move no counter. A dropped frame emits nothing and
    /// leaves pending events and later decoding as they were, except that a
    /// `venue_seq` skip it carried flags [`GAP_SUSPECT`](md_events::RecordFlags::GAP_SUSPECT)
    /// on each non-`Definition` record of the stream's next frame that emits one.
    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64);

    /// Take every event emitted since the last drain, in emission order. A
    /// drain never moves a counter, and what the adapter emits does not depend
    /// on when the caller drains.
    fn drain(&mut self) -> impl Iterator<Item = MarketEvent> + '_;

    /// Counters since construction.
    fn stats(&self) -> &AdapterStats;

    /// Replace the session anchor, the epoch-ns UTC instant that wire
    /// time-of-day offsets count from, for every frame fed after the call. A
    /// runner calls it with [`SessionModel::resolve_anchor`] at session open
    /// and at each trade-date roll, never per frame. The default ignores it,
    /// right for a venue whose wire carries absolute time.
    fn set_session_anchor(&mut self, _anchor_ns: u64) {}
}

/// Object-safe [`VenueAdapter`] for a venue registry or a plugin loader.
///
/// Implemented for every `VenueAdapter`, so an adapter author writes only the
/// static trait. Venue selection stays per session: the erased path costs one
/// indirect call per frame and per drain, not per event.
///
/// Both traits name `feed`, `stats`, `record_types`, `session`, and
/// `set_session_anchor`. With both in scope, such a call on a concrete adapter
/// is ambiguous (E0034): import only the trait you call through, or name it, as
/// in `VenueAdapter::feed(&mut adapter, ..)` or
/// `<A as VenueAdapter>::record_types()`.
pub trait DynAdapter {
    /// See [`VenueAdapter::record_types`].
    fn record_types(&self) -> &'static [u8];

    /// See [`VenueAdapter::session`].
    fn session(&self) -> SessionModel;

    /// See [`VenueAdapter::feed`].
    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64);

    /// Append every pending event to `out`. The caller owns and reuses `out`,
    /// so the erased path allocates once per session rather than per drain.
    fn drain_into(&mut self, out: &mut Vec<MarketEvent>);

    /// See [`VenueAdapter::stats`].
    fn stats(&self) -> &AdapterStats;

    /// See [`VenueAdapter::set_session_anchor`].
    fn set_session_anchor(&mut self, anchor_ns: u64);
}

impl<T: VenueAdapter> DynAdapter for T {
    fn record_types(&self) -> &'static [u8] {
        T::record_types()
    }

    fn session(&self) -> SessionModel {
        VenueAdapter::session(self)
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        VenueAdapter::feed(self, stream, payload, ts_recv, venue_seq);
    }

    fn drain_into(&mut self, out: &mut Vec<MarketEvent>) {
        out.extend(VenueAdapter::drain(self));
    }

    fn stats(&self) -> &AdapterStats {
        VenueAdapter::stats(self)
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        VenueAdapter::set_session_anchor(self, anchor_ns);
    }
}
