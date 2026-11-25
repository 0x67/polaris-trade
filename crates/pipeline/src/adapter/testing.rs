//! Conformance suite for [`VenueAdapter`] implementations, callable from an
//! in-tree venue crate or from an adapter whose source this repository never
//! sees. Mirrors [`crate::ingest::testing`].
//!
//! The caller supplies a config and [`ContractFrames`]: decodable samples
//! labelled with their wire kind, plus frames of a kind the adapter does not
//! know. The suite derives the rest (truncated frames, undeclared streams),
//! builds fresh adapters per case, and reports a panic inside a case as that
//! case's failure.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    panic::{self, AssertUnwindSafe},
};

use md_events::{
    Control, Definition, Fill, Imbalance, Level, Mbo, Record, RecordFlags, RecordHeader, Stats,
    Status, Trade, TradeBreak,
};

use super::{AdapterStats, MarketEvent, VenueAdapter};
use crate::ingest::StreamId;

mod parity;
#[cfg(test)]
mod parity_tests;
#[cfg(test)]
mod tests;

pub use parity::assert_fixture_parity;

/// One wire frame on one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SampleFrame {
    /// Stream the frame arrives on.
    pub stream: StreamId,
    /// Wire kind the frame carries, as the stream's `CoverageMatrix` keys it.
    pub kind: u8,
    /// The whole wire message.
    pub payload: Vec<u8>,
}

/// What the suite feeds the adapter under test.
#[derive(Clone, Debug, Default)]
pub struct ContractFrames {
    /// Frames the adapter decodes, fed in this order. They must include every
    /// kind the adapter dispatches on each stream, whether or not its coverage
    /// marks it decoded, and emit at least one event between them. At least
    /// one should emit two or more events, or a `decoded` that counts events
    /// instead of frames goes unnoticed; so should the first on each stream to
    /// emit anything but a `Definition`, or the gap case misses a flag absent
    /// from some of its records. Each one, cut by its last byte, must count as
    /// malformed. A sample must not also be a frame another declared stream
    /// decodes, unless that stream's coverage marks the sample's kind: the
    /// suite feeds each sample there and expects it dropped.
    pub decodable: Vec<SampleFrame>,
    /// Frames of a kind the adapter does not know, at least one per declared
    /// stream.
    pub unknown: Vec<SampleFrame>,
}

/// One rule of the adapter contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdapterCase {
    /// A drain takes every pending event and a second drain yields none.
    DrainEmpties,
    /// On one adapter drained after every frame, a decodable frame adds one to
    /// `decoded` and moves no other counter. Its truncated copy adds one to
    /// `malformed`, an unknown-kind frame adds one to `unknown_kind`, an empty
    /// frame adds one to either, and a frame on an undeclared stream moves
    /// nothing. No drain moves a counter. Every drained record's header names
    /// its own rtype and size and, unless it is a `Definition`, carries the
    /// `ts_recv` and `venue_seq` of its frame.
    CountersExact,
    /// An unknown-kind frame, fed after the decodable frames and before any
    /// drain, adds one to `unknown_kind`, moves no other counter, leaves the
    /// pending events as they were, emits nothing, and does not panic.
    UnknownKindCounted,
    /// A decodable frame missing its last byte, fed the same way, adds one to
    /// `malformed`, moves no other counter, leaves the pending events as they
    /// were, emits nothing, and does not panic. An empty frame on each declared
    /// stream does the same, adding one to `malformed` or to `unknown_kind`.
    TruncatedMalformed,
    /// Each declared stream's `coverage` decodes exactly the kinds the adapter
    /// decoded there, and no undeclared stream decoded anything. A sample fed,
    /// after the decodable frames and before any drain, on another declared
    /// stream whose coverage does not mark its kind adds one to `unknown_kind`
    /// or to `malformed`, moves no other counter, leaves the pending events as
    /// they were, and emits nothing.
    CoverageMatchesDispatch,
    /// `streams()` is non-empty, and frames on undeclared streams (the lowest
    /// and highest free ids), fed after the decodable frames and before any
    /// drain, move no counter, leave the pending events as they were, and emit
    /// nothing.
    UndeclaredStreamIgnored,
    /// Dropped frames on a gap-free stream leave no state behind and emit
    /// nothing: with a truncated, an unknown-kind, or an undeclared-stream frame
    /// right before each decodable frame, the decodable frames decode to the
    /// same records as on an adapter that saw no drops, `venue_seq` aside.
    DecodeAfterDrops,
    /// Drained events are final: copying them out after every frame, as a
    /// plugin host does before its next call, gives the same events as one
    /// drain at the end.
    RecordLifetime,
    /// A `venue_seq` skip carried by an unknown-kind frame outlives the drop:
    /// the next frame on that stream emitting anything but a `Definition`
    /// flags each such record `GAP_SUSPECT`. Apart from that bit, every frame
    /// after the skip, on any stream, decodes to the same records as on an
    /// adapter that saw no skip, `venue_seq` aside.
    GapSurvivesDrops,
    /// `record_types()` equals the set of record types of every event the
    /// decodable frames emit: none missing, none extra.
    RecordTypesMatchEmitted,
}

/// Outcome of a conformance run.
#[derive(Debug)]
pub struct AdapterConformanceReport {
    /// Per-case pass (`Ok`) or failure with a reason (`Err`), in suite order.
    pub results: Vec<(AdapterCase, Result<(), String>)>,
}

impl AdapterConformanceReport {
    /// The failed cases with their reasons, in suite order.
    pub fn failures(&self) -> impl Iterator<Item = (AdapterCase, &str)> {
        self.results
            .iter()
            .filter_map(|(case, result)| result.as_ref().err().map(|why| (*case, why.as_str())))
    }
}

type Check<A> = fn(&<A as VenueAdapter>::Config, &ContractFrames) -> Result<(), String>;

/// Run every case against `A`, each on fresh adapters built from `cfg`.
#[must_use]
pub fn run_adapter_contract<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> AdapterConformanceReport {
    let cases: [(AdapterCase, Check<A>); 10] = [
        (AdapterCase::DrainEmpties, drain_empties::<A>),
        (AdapterCase::CountersExact, counters_exact::<A>),
        (AdapterCase::UnknownKindCounted, unknown_kind_counted::<A>),
        (AdapterCase::TruncatedMalformed, truncated_malformed::<A>),
        (
            AdapterCase::CoverageMatchesDispatch,
            coverage_matches_dispatch::<A>,
        ),
        (
            AdapterCase::UndeclaredStreamIgnored,
            undeclared_stream_ignored::<A>,
        ),
        (AdapterCase::DecodeAfterDrops, decode_after_drops::<A>),
        (AdapterCase::RecordLifetime, record_lifetime::<A>),
        (AdapterCase::GapSurvivesDrops, gap_survives_drops::<A>),
        (
            AdapterCase::RecordTypesMatchEmitted,
            record_types_match_emitted::<A>,
        ),
    ];
    AdapterConformanceReport {
        results: cases
            .into_iter()
            .map(|(case, check)| (case, catch(|| check(cfg, frames))))
            .collect(),
    }
}

/// Fail unless `A` passes every case of [`run_adapter_contract`].
///
/// # Panics
///
/// When any case fails; the message lists each failed case and its reason.
#[track_caller]
pub fn assert_adapter_contract<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) {
    let report = run_adapter_contract::<A>(cfg, frames);
    let failures: Vec<_> = report.failures().collect();
    assert!(
        failures.is_empty(),
        "adapter contract failures: {failures:?}"
    );
}

fn catch(check: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
    panic::catch_unwind(AssertUnwindSafe(check))
        .unwrap_or_else(|payload| Err(format!("panicked: {}", panic_text(&*payload))))
}

fn panic_text(payload: &(dyn Any + Send)) -> &str {
    if let Some(text) = payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text
    } else {
        "non-string panic payload"
    }
}

/// Drives one adapter with a rising `ts_recv` and a `venue_seq` per stream
/// that gaps only where a case calls [`Feeder::skip_seq`], so no other case
/// trips a venue's sequence-gap logic.
struct Feeder<A> {
    adapter: A,
    ts_recv: u64,
    venue_seq: BTreeMap<StreamId, u64>,
}

impl<A: VenueAdapter<Config: Clone>> Feeder<A> {
    fn new(cfg: &A::Config) -> Self {
        Feeder {
            adapter: A::new(cfg.clone()),
            ts_recv: 0,
            venue_seq: BTreeMap::new(),
        }
    }

    /// Feed one frame; returns the `ts_recv` and `venue_seq` it was given.
    fn feed(&mut self, stream: StreamId, payload: &[u8]) -> (u64, u64) {
        self.ts_recv += 1;
        let seq = self.venue_seq.entry(stream).or_insert(0);
        *seq += 1;
        self.adapter.feed(stream, payload, self.ts_recv, *seq);
        (self.ts_recv, *seq)
    }

    /// Use up one `venue_seq` on `stream` without feeding it, so the next frame
    /// there arrives after a gap.
    fn skip_seq(&mut self, stream: StreamId) {
        *self.venue_seq.entry(stream).or_insert(0) += 1;
    }

    fn feed_all(&mut self, frames: &[SampleFrame]) {
        for frame in frames {
            self.feed(frame.stream, &frame.payload);
        }
    }

    fn stats(&self) -> AdapterStats {
        *self.adapter.stats()
    }

    fn drain_count(&mut self) -> usize {
        self.adapter.drain().count()
    }

    /// Feed one frame, then drain. Fails unless the feed moved the counters as
    /// `rule` says, every drained record has a sound header for that frame,
    /// and the drain moved no counter.
    fn exact_step(
        &mut self,
        stream: StreamId,
        payload: &[u8],
        rule: &CountRule,
    ) -> Result<(), String> {
        let before = self.stats();
        let (ts_recv, venue_seq) = self.feed(stream, payload);
        let fed = self.stats();
        if !(rule.counted)(&before, &fed) {
            return Err(format!(
                "feed moved counters {before:?} -> {fed:?} (want {})",
                rule.wording
            ));
        }
        let events: Vec<MarketEvent> = self.adapter.drain().collect();
        for event in &events {
            check_header(event, stream, ts_recv, venue_seq)?;
        }
        let after = self.stats();
        if after == fed {
            Ok(())
        } else {
            Err(format!("drain moved counters {fed:?} -> {after:?}"))
        }
    }
}

/// How one frame must move the counters, from `before` to `after`, with the
/// rule's wording for failure messages.
struct CountRule {
    counted: fn(&AdapterStats, &AdapterStats) -> bool,
    wording: &'static str,
}

const COUNTED_DECODED: CountRule = CountRule {
    counted: |before, after| {
        *after
            == AdapterStats {
                decoded: before.decoded + 1,
                ..*before
            }
    },
    wording: "decoded + 1 only",
};

const NOT_COUNTED: CountRule = CountRule {
    counted: |before, after| after == before,
    wording: "no counter moves",
};

const COUNTED_UNKNOWN: CountRule = CountRule {
    counted: |before, after| *after == plus_unknown(before),
    wording: "unknown_kind + 1 only",
};

const COUNTED_MALFORMED: CountRule = CountRule {
    counted: |before, after| *after == plus_malformed(before),
    wording: "malformed + 1 only",
};

const COUNTED_EITHER: CountRule = CountRule {
    counted: |before, after| *after == plus_unknown(before) || *after == plus_malformed(before),
    wording: "unknown_kind + 1 or malformed + 1 only",
};

fn plus_unknown(stats: &AdapterStats) -> AdapterStats {
    AdapterStats {
        unknown_kind: stats.unknown_kind + 1,
        ..*stats
    }
}

fn plus_malformed(stats: &AdapterStats) -> AdapterStats {
    AdapterStats {
        malformed: stats.malformed + 1,
        ..*stats
    }
}

/// Feed every decodable frame without draining, then each probe, which the
/// adapter must drop. Fails unless each probe moves the counters as `rule`
/// says and the final drain equals that of an adapter fed only the decodable
/// frames. Probing a primed adapter is what exposes a drop path that resets
/// counters or discards, edits, or adds pending events.
fn expect_dropped<'a, A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
    probes: impl Iterator<Item = (String, StreamId, &'a [u8])>,
    rule: &CountRule,
) -> Result<(), String> {
    need_decodable(frames)?;
    let mut reference = Feeder::<A>::new(cfg);
    reference.feed_all(&frames.decodable);
    let pending: Vec<MarketEvent> = reference.adapter.drain().collect();
    let mut f = Feeder::<A>::new(cfg);
    f.feed_all(&frames.decodable);
    for (label, stream, payload) in probes {
        let before = f.stats();
        f.feed(stream, payload);
        let after = f.stats();
        if !(rule.counted)(&before, &after) {
            return Err(format!(
                "{label}: moved counters {before:?} -> {after:?} (want {})",
                rule.wording
            ));
        }
    }
    let drained: Vec<MarketEvent> = f.adapter.drain().collect();
    if drained == pending {
        return Ok(());
    }
    let at = drained
        .iter()
        .zip(&pending)
        .position(|(a, b)| a != b)
        .unwrap_or(drained.len().min(pending.len()));
    Err(format!(
        "dropped frames changed the pending events: drained {} where an adapter fed only the \
         decodable frames drains {}; first difference at event {at}",
        drained.len(),
        pending.len()
    ))
}

fn need_decodable(frames: &ContractFrames) -> Result<(), String> {
    if frames.decodable.is_empty() {
        Err("no decodable frame supplied".into())
    } else {
        Ok(())
    }
}

fn cut_last_byte(payload: &[u8]) -> &[u8] {
    &payload[..payload.len().saturating_sub(1)]
}

/// The lowest and highest stream ids `streams()` leaves free: one id when
/// they coincide, none when every id is declared.
fn undeclared_streams<A: VenueAdapter>() -> Vec<StreamId> {
    let declared = A::streams();
    let low = (0..=u16::MAX).map(StreamId).find(|s| !declared.contains(s));
    let high = (0..=u16::MAX)
        .rev()
        .map(StreamId)
        .find(|s| !declared.contains(s));
    let mut ids: Vec<StreamId> = low.into_iter().chain(high).collect();
    ids.dedup();
    ids
}

/// Header and body of a drained record, with `venue_seq` cleared: the dropped
/// frames shift it on the probed adapter, and nothing else.
fn record_key(stream: StreamId, event: &MarketEvent) -> (RecordHeader, Vec<u8>) {
    let mut header = *event.header();
    header.venue_seq = 0;
    (header, event.as_record_ref(stream).body.to_vec())
}

/// The rtype a record of this variant must carry.
fn variant_rtype(event: &MarketEvent) -> u8 {
    match event {
        MarketEvent::Mbo(_) => Mbo::RTYPE,
        MarketEvent::Level(_) => Level::RTYPE,
        MarketEvent::Trade(_) => Trade::RTYPE,
        MarketEvent::TradeBreak(_) => TradeBreak::RTYPE,
        MarketEvent::Imbalance(_) => Imbalance::RTYPE,
        MarketEvent::Fill(_) => Fill::RTYPE,
        MarketEvent::Stats(_) => Stats::RTYPE,
        MarketEvent::Status(_) => Status::RTYPE,
        MarketEvent::Control(_) => Control::RTYPE,
        MarketEvent::Definition(_) => Definition::RTYPE,
    }
}

/// Fails unless the header names the record's own type and size and, except on
/// a `Definition` the adapter mints from its registry, carries the `ts_recv`
/// and `venue_seq` of the frame it came from.
fn check_header(
    event: &MarketEvent,
    stream: StreamId,
    ts_recv: u64,
    venue_seq: u64,
) -> Result<(), String> {
    let header = event.header();
    let rtype = variant_rtype(event);
    if header.rtype != rtype {
        return Err(format!(
            "a record of rtype {rtype:#04x} carries rtype {:#04x} in its header",
            header.rtype
        ));
    }
    let size = size_of::<RecordHeader>() + event.as_record_ref(stream).body.len();
    if usize::from(header.length) * 4 != size {
        return Err(format!(
            "rtype {rtype:#04x}: header length {} (x4) does not match the record's {size} bytes",
            header.length
        ));
    }
    let stamped = (header.ts_recv, header.venue_seq) == (ts_recv, venue_seq);
    if stamped || matches!(event, MarketEvent::Definition(_)) {
        Ok(())
    } else {
        Err(format!(
            "rtype {rtype:#04x} carries ts_recv {} and venue_seq {}; its frame arrived with \
             {ts_recv} and {venue_seq}",
            header.ts_recv, header.venue_seq
        ))
    }
}

/// A reference adapter and a probed one fed the same decodable frames at the
/// same `ts_recv`; only the probed one takes the dropped frames in between.
struct Twin<A> {
    reference: Feeder<A>,
    probed: Feeder<A>,
    want: Vec<(RecordHeader, Vec<u8>)>,
    got: Vec<(RecordHeader, Vec<u8>)>,
}

impl<A: VenueAdapter<Config: Clone>> Twin<A> {
    fn new(cfg: &A::Config) -> Self {
        Twin {
            reference: Feeder::new(cfg),
            probed: Feeder::new(cfg),
            want: Vec::new(),
            got: Vec::new(),
        }
    }

    /// Feed a decodable frame to both adapters and keep what each drains.
    fn decode(&mut self, frame: &SampleFrame) {
        self.reference.feed(frame.stream, &frame.payload);
        self.want.extend(
            self.reference
                .adapter
                .drain()
                .map(|e| record_key(frame.stream, &e)),
        );
        self.probed.feed(frame.stream, &frame.payload);
        self.got.extend(
            self.probed
                .adapter
                .drain()
                .map(|e| record_key(frame.stream, &e)),
        );
    }

    /// Feed a frame the probed adapter must drop; the reference skips that
    /// `ts_recv` so both stay aligned.
    fn drop_frame(&mut self, stream: StreamId, payload: &[u8]) {
        self.probed.feed(stream, payload);
        self.reference.ts_recv += 1;
    }
}

fn drain_empties<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    let mut f = Feeder::<A>::new(cfg);
    f.feed_all(&frames.decodable);
    let first = f.drain_count();
    if first == 0 {
        return Err("decodable frames emitted no event to drain".into());
    }
    match f.drain_count() {
        0 => Ok(()),
        again => Err(format!(
            "second drain yielded {again} events after the first took {first}"
        )),
    }
}

fn counters_exact<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    need_decodable(frames)?;
    let undeclared = undeclared_streams::<A>();
    let mut f = Feeder::<A>::new(cfg);
    for (i, frame) in frames.decodable.iter().enumerate() {
        f.exact_step(frame.stream, &frame.payload, &COUNTED_DECODED)
            .map_err(|why| format!("decodable frame {i}: {why}"))?;
        f.exact_step(
            frame.stream,
            cut_last_byte(&frame.payload),
            &COUNTED_MALFORMED,
        )
        .map_err(|why| format!("decodable frame {i} cut by its last byte: {why}"))?;
        for &other in &undeclared {
            f.exact_step(other, &frame.payload, &NOT_COUNTED)
                .map_err(|why| {
                    format!(
                        "decodable frame {i} on undeclared stream {}: {why}",
                        other.0
                    )
                })?;
        }
    }
    for (i, frame) in frames.unknown.iter().enumerate() {
        f.exact_step(frame.stream, &frame.payload, &COUNTED_UNKNOWN)
            .map_err(|why| format!("unknown frame {i}: {why}"))?;
    }
    for &stream in A::streams() {
        f.exact_step(stream, &[], &COUNTED_EITHER)
            .map_err(|why| format!("empty frame on stream {}: {why}", stream.0))?;
    }
    Ok(())
}

fn unknown_kind_counted<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    if let Some(stream) = A::streams()
        .iter()
        .find(|s| frames.unknown.iter().all(|f| f.stream != **s))
    {
        return Err(format!(
            "no unknown-kind frame supplied for stream {}",
            stream.0
        ));
    }
    let probes = frames.unknown.iter().map(|frame| {
        (
            format!(
                "unknown kind {:#04x} on stream {}",
                frame.kind, frame.stream.0
            ),
            frame.stream,
            frame.payload.as_slice(),
        )
    });
    expect_dropped::<A>(cfg, frames, probes, &COUNTED_UNKNOWN)
}

fn truncated_malformed<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    let probes = frames.decodable.iter().map(|frame| {
        let cut = cut_last_byte(&frame.payload);
        (
            format!(
                "kind {:#04x} on stream {} cut to {} bytes",
                frame.kind,
                frame.stream.0,
                cut.len()
            ),
            frame.stream,
            cut,
        )
    });
    expect_dropped::<A>(cfg, frames, probes, &COUNTED_MALFORMED)?;
    let empty = A::streams().iter().map(|&stream| {
        (
            format!("empty frame on stream {}", stream.0),
            stream,
            &[][..],
        )
    });
    expect_dropped::<A>(cfg, frames, empty, &COUNTED_EITHER)
}

fn coverage_matches_dispatch<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    let mut dispatched: BTreeMap<StreamId, Vec<u8>> = BTreeMap::new();
    let mut f = Feeder::<A>::new(cfg);
    for frame in &frames.decodable {
        let before = f.stats();
        f.feed(frame.stream, &frame.payload);
        let after = f.stats();
        // A dispatch arm that only counts the frame as unknown or malformed
        // does not decode it, even when `decoded` moved first.
        if after.decoded > before.decoded
            && after.unknown_kind == before.unknown_kind
            && after.malformed == before.malformed
        {
            dispatched.entry(frame.stream).or_default().push(frame.kind);
        }
    }
    let declared = A::streams();
    if let Some(stream) = dispatched.keys().find(|s| !declared.contains(s)) {
        return Err(format!(
            "stream {} decoded frames but streams() does not declare it",
            stream.0
        ));
    }
    for &stream in declared {
        let kinds = dispatched.get(&stream).map_or(&[][..], Vec::as_slice);
        A::coverage(stream)
            .matches_dispatch(kinds)
            .map_err(|e| format!("stream {}: {e}", stream.0))?;
    }
    let probes = frames.decodable.iter().flat_map(|frame| {
        declared
            .iter()
            .filter(|&&s| {
                s != frame.stream && !A::coverage(s).get(frame.kind).is_some_and(|c| c.decoded)
            })
            .map(move |&stream| {
                (
                    format!(
                        "kind {:#04x} from stream {} on stream {}, whose coverage does not \
                         mark it",
                        frame.kind, frame.stream.0, stream.0
                    ),
                    stream,
                    frame.payload.as_slice(),
                )
            })
    });
    expect_dropped::<A>(cfg, frames, probes, &COUNTED_EITHER)
}

fn record_types_match_emitted<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    need_decodable(frames)?;
    let mut f = Feeder::<A>::new(cfg);
    f.feed_all(&frames.decodable);
    let emitted: BTreeSet<u8> = f.adapter.drain().map(|event| event.rtype()).collect();
    let declared: BTreeSet<u8> = A::record_types().iter().copied().collect();
    if let Some(missing) = emitted.difference(&declared).next() {
        return Err(format!(
            "rtype {missing:#04x} emitted but not declared in record_types"
        ));
    }
    match declared.difference(&emitted).next() {
        Some(extra) => Err(format!("rtype {extra:#04x} declared but never emitted")),
        None => Ok(()),
    }
}

fn undeclared_stream_ignored<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    if A::streams().is_empty() {
        return Err("streams() declares no stream".into());
    }
    let undeclared = undeclared_streams::<A>();
    if undeclared.is_empty() {
        return Err("streams() declares every stream id, none left to probe".into());
    }
    let probes = undeclared.iter().flat_map(|&other| {
        frames.decodable.iter().map(move |frame| {
            (
                format!("kind {:#04x} on undeclared stream {}", frame.kind, other.0),
                other,
                frame.payload.as_slice(),
            )
        })
    });
    expect_dropped::<A>(cfg, frames, probes, &NOT_COUNTED)
}

fn decode_after_drops<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    need_decodable(frames)?;
    if frames.unknown.is_empty() {
        return Err("no unknown-kind frame supplied".into());
    }
    let undeclared = undeclared_streams::<A>();
    let mut twin = Twin::<A>::new(cfg);
    for frame in &frames.decodable {
        twin.decode(frame);
    }
    // Each kind of drop sits right before a decodable frame, so state a drop
    // leaves for the next feed reaches a frame that decodes.
    for frame in &frames.decodable {
        twin.drop_frame(frame.stream, cut_last_byte(&frame.payload));
        twin.decode(frame);
    }
    for (frame, unknown) in frames.decodable.iter().zip(frames.unknown.iter().cycle()) {
        twin.drop_frame(unknown.stream, &unknown.payload);
        twin.decode(frame);
    }
    for frame in &frames.decodable {
        for &other in &undeclared {
            twin.drop_frame(other, &frame.payload);
        }
        twin.decode(frame);
    }
    if twin.got == twin.want {
        return Ok(());
    }
    let at = twin
        .got
        .iter()
        .zip(&twin.want)
        .position(|(a, b)| a != b)
        .unwrap_or(twin.got.len().min(twin.want.len()));
    Err(format!(
        "after dropped frames the decodable frames decoded to {} records, {} on an adapter \
         that saw no drops; first difference at record {at}",
        twin.got.len(),
        twin.want.len()
    ))
}

fn gap_survives_drops<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    need_decodable(frames)?;
    let streams: BTreeSet<StreamId> = frames.decodable.iter().map(|f| f.stream).collect();
    for stream in streams {
        let unknown = frames
            .unknown
            .iter()
            .find(|f| f.stream == stream)
            .ok_or_else(|| format!("no unknown-kind frame supplied for stream {}", stream.0))?;
        gap_after_drop::<A>(cfg, frames, unknown).map_err(|why| {
            format!(
                "skip before the unknown frame on stream {}: {why}",
                stream.0
            )
        })?;
    }
    Ok(())
}

/// Prime a reference and a probed adapter with every decodable frame, feed the
/// probed one `unknown` after a skipped `venue_seq`, then feed both every
/// decodable frame again and compare them frame by frame.
fn gap_after_drop<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
    unknown: &SampleFrame,
) -> Result<(), String> {
    let mut reference = Feeder::<A>::new(cfg);
    let mut probed = Feeder::<A>::new(cfg);
    for feeder in [&mut reference, &mut probed] {
        feeder.feed_all(&frames.decodable);
        feeder.drain_count();
    }
    probed.skip_seq(unknown.stream);
    probed.feed(unknown.stream, &unknown.payload);
    reference.ts_recv += 1;
    let mut pending = true;
    for (i, frame) in frames.decodable.iter().enumerate() {
        reference.feed(frame.stream, &frame.payload);
        probed.feed(frame.stream, &frame.payload);
        let want: Vec<MarketEvent> = reference.adapter.drain().collect();
        let got: Vec<MarketEvent> = probed.adapter.drain().collect();
        let flagged = pending && frame.stream == unknown.stream && want.iter().any(stamped);
        if flagged {
            pending = false;
            let unflagged = got
                .iter()
                .any(|e| stamped(e) && !e.header().flags.contains(RecordFlags::GAP_SUSPECT));
            if unflagged {
                return Err(format!(
                    "decodable frame {i}, the first on the stream to emit after the skip, \
                     emitted a record without GAP_SUSPECT"
                ));
            }
        }
        let key = |event: &MarketEvent| {
            let (mut header, body) = record_key(frame.stream, event);
            if flagged {
                header.flags.0 &= !RecordFlags::GAP_SUSPECT.0;
            }
            (header, body)
        };
        if !got.iter().map(key).eq(want.iter().map(key)) {
            let aside = if flagged { ", GAP_SUSPECT aside" } else { "" };
            return Err(format!(
                "decodable frame {i} decoded to {} records unlike the {} of an adapter that saw \
                 no skip{aside}",
                got.len(),
                want.len()
            ));
        }
    }
    if pending {
        Err("no decodable frame on the stream emitted a record after the skip".into())
    } else {
        Ok(())
    }
}

/// A record whose header carries its frame's `venue_seq`: anything but a
/// `Definition` the adapter mints from its registry.
fn stamped(event: &MarketEvent) -> bool {
    !matches!(event, MarketEvent::Definition(_))
}

fn record_lifetime<A: VenueAdapter<Config: Clone>>(
    cfg: &A::Config,
    frames: &ContractFrames,
) -> Result<(), String> {
    let mut stepped = Feeder::<A>::new(cfg);
    let mut copied: Vec<MarketEvent> = Vec::new();
    for frame in &frames.decodable {
        stepped.feed(frame.stream, &frame.payload);
        copied.extend(stepped.adapter.drain());
    }
    let mut batched = Feeder::<A>::new(cfg);
    batched.feed_all(&frames.decodable);
    let at_end: Vec<MarketEvent> = batched.adapter.drain().collect();
    if at_end.is_empty() {
        return Err("decodable frames emitted no event to compare".into());
    }
    if copied == at_end {
        return Ok(());
    }
    let at = copied
        .iter()
        .zip(&at_end)
        .position(|(a, b)| a != b)
        .unwrap_or(copied.len().min(at_end.len()));
    Err(format!(
        "draining after every frame copied {} events, one drain at the end took {}; \
         first difference at event {at}",
        copied.len(),
        at_end.len()
    ))
}
