//! Venue session hot loop: pull an `IngestFrame` batch, tap and feed each
//! frame through the venue adapter, route every drained
//! [`md_pipeline::MarketEvent`] to `BookEngine`/`DeriveEngine`/`OhlcvEngine`,
//! and publish through `EgressSequencer`. One runner serves every venue, over a
//! concrete adapter or a `dyn DynAdapter` alike. One venue session is one
//! single-threaded loop; generic over `S: OutputSink` so the hot path never
//! boxes the sink.

use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicBool, Ordering},
};

use md_events::{
    Action, BreakRefKind, Mbo, Record, RecordFlags, RecordHeader, TradeBreak, TradingState,
};
use md_pipeline::{
    DynAdapter, EgressClock, EgressSequencer, IngestError, IngestFrame, IngestSource, MarketEvent,
    OutputSink, StreamId,
};
use orderbook_engine::{
    ApplyResult, BoardId, BookEngine, BreakScope, DeriveEngine, Gate, OhlcvEngine, PurposeSet,
    VenuePolicy,
};

use crate::{
    anchor_roll::AnchorRoll,
    config::{
        DatafeedConfig, ResolutionParseError, SourceConfig, default_resolution_tokens,
        resolve_resolutions,
    },
    restart::RestartState,
    tap::{CaptureTap, CaptureTapError},
};

/// Permissive policy: accepts every book event, every board gets the full
/// purpose set, printable executions reach the tape. Board-to-purpose
/// routing and per-venue book gating are policy-authoring concerns outside
/// this task's wiring scope; this is the one policy every venue session in
/// this binary uses today.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultVenuePolicy;

const DEFAULT_PURPOSES: PurposeSet = PurposeSet::BOOK
    .with(PurposeSet::OHLCV)
    .with(PurposeSet::TAPE);

impl VenuePolicy for DefaultVenuePolicy {
    fn book_gate(&self, _state: TradingState, _action: Action) -> Gate {
        Gate::Accept
    }
    fn purposes(&self, _board: BoardId) -> PurposeSet {
        DEFAULT_PURPOSES
    }
    fn tape_gate(&self, printable: bool, _action: Action) -> bool {
        printable
    }
    fn on_trade_break(&self, _kind: BreakRefKind) -> BreakScope {
        BreakScope::AggregatesOnly
    }
}

/// The seven default OHLCV bucket widths in nanoseconds, resolved from the
/// config default tokens (the single source of truth for the default set).
fn default_ohlcv_resolutions() -> Vec<u64> {
    resolve_resolutions(&default_resolution_tokens())
        .expect("built-in default resolution tokens are valid")
}

/// The stream whose trades feed OHLCV, the `DeriveEngine` VWAP, and
/// `trade_memo`: a single-channel venue's only stream, and a dual-channel
/// venue's primary channel. Another stream's `Trade` or `TradeBreak` publishes and
/// nothing more, since a secondary channel may reprint executions under its own
/// trade id, an id space apart from the primary feed's match number. Every built-in
/// descriptor declares it first; a `venue.rs` test pins that.
pub(crate) const SOURCE_STREAM: StreamId = StreamId(0);

/// How many recent trades' `(price, qty, instrument)` a session retains for
/// `TradeBreak` lookups. Capped since a venue trade bust corrects a recent
/// print, not one from hours earlier; bounded FIFO eviction keeps a
/// full-day session from leaking one entry per trade forever.
const TRADE_MEMO_CAP: usize = 1024;

/// Bounded `trade_ref -> (price, qty, instrument)` lookup backing
/// `DeriveEngine::apply_trade_break`'s caller-supplied values. `DeriveEngine`
/// keeps no memo of its own (design decision: only `OhlcvEngine` does), so
/// the session records each [`SOURCE_STREAM`] `Trade` here and consults it
/// when the matching `TradeBreak` arrives.
#[derive(Default)]
struct TradeMemo {
    values: HashMap<u64, (i64, u64, u32)>,
    order: VecDeque<u64>,
}

impl TradeMemo {
    fn record(&mut self, trade_ref: u64, price: i64, qty: u64, instrument: u32) {
        if self
            .values
            .insert(trade_ref, (price, qty, instrument))
            .is_some()
        {
            return;
        }
        self.order.push_back(trade_ref);
        if self.order.len() > TRADE_MEMO_CAP
            && let Some(oldest) = self.order.pop_front()
        {
            self.values.remove(&oldest);
        }
    }

    fn get(&self, trade_ref: u64) -> Option<(i64, u64, u32)> {
        self.values.get(&trade_ref).copied()
    }
}

/// Failure publishing or tapping one record. `IngestError` is handled inside
/// the session loop directly (`Closed` ends it, `PoolExhausted` retries), so
/// it never needs to bubble up as a `SessionError`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    #[error("output sink: {0}")]
    Sink(#[from] md_pipeline::SinkError),
    #[error("capture tap: {0}")]
    Tap(#[from] CaptureTapError),
    /// A live frame's `ts_recv` falls in no session the venue's model can
    /// resolve (an unknown zone), so the adapter has no anchor to decode it on.
    #[error("venue {venue} session anchor does not resolve for ts_recv {ts_recv}")]
    AnchorUnresolved { venue: &'static str, ts_recv: u64 },
}

/// Running counters for one session run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionStats {
    pub frames: u64,
    pub records: u64,
    /// Per-instrument count of `ApplyResult::UnknownOrder` from
    /// `BookEngine::apply_mbo`. A non-empty entry flags that instrument's
    /// book as possibly desynced (design's "count + flag maybe-bad-book").
    pub unknown_order_counts: HashMap<u32, u64>,
}

/// Mutable per-session engine state threaded through record routing: order
/// book, derive engine, OHLCV aggregator, broken-trade lookup, and running
/// telemetry. Grouped so `route_*_record` takes one param instead of five.
struct SessionEngines<P: VenuePolicy> {
    book: BookEngine<P>,
    derive: DeriveEngine,
    ohlcv: OhlcvEngine,
    trade_memo: TradeMemo,
    stats: SessionStats,
}

/// Engine and telemetry state retained while a live source reconnects.
/// Keeping this separate from the transport source prevents a resumed
/// execution or cancel from losing the resting book it refers to.
pub struct SessionState {
    engines: SessionEngines<DefaultVenuePolicy>,
    /// Live anchor roll, set only for a live source with an unset anchor.
    anchor_roll: Option<AnchorRoll>,
}

impl SessionState {
    /// Build session state whose OHLCV engine tracks the given bucket widths.
    pub fn new(resolutions: Vec<u64>) -> Self {
        SessionState {
            engines: SessionEngines::new(DefaultVenuePolicy, resolutions),
            anchor_roll: None,
        }
    }

    /// Build session state from a config, resolving its OHLCV resolution
    /// tokens. Fails before any records are processed if a token is malformed.
    ///
    /// A live source with `session_midnight_ns == 0` also rolls the adapter
    /// anchor: [`run`] installs each trade date's anchor before the first frame
    /// received in that session feeds. An explicit anchor is left alone and never
    /// rolls; the binary's live runner refuses one before dialing. Replay never
    /// rolls, since its `ts_recv` can be synthetic.
    pub fn from_config(config: &DatafeedConfig) -> Result<Self, ResolutionParseError> {
        let mut state = Self::new(resolve_resolutions(&config.ohlcv_resolutions)?);
        let replay = matches!(config.source, SourceConfig::Replay { .. });
        if !replay && config.session_midnight_ns == 0 {
            state.anchor_roll = Some(AnchorRoll::new(config.venue));
        }
        Ok(state)
    }

    /// Counters accumulated since this state was built.
    pub fn stats(&self) -> &SessionStats {
        &self.engines.stats
    }
}

impl Default for SessionState {
    fn default() -> Self {
        SessionState::new(default_ohlcv_resolutions())
    }
}

impl<P: VenuePolicy> SessionEngines<P> {
    fn new(policy: P, resolutions: Vec<u64>) -> Self {
        SessionEngines {
            book: BookEngine::new(policy),
            derive: DeriveEngine::new(),
            ohlcv: OhlcvEngine::new(resolutions),
            trade_memo: TradeMemo::default(),
            stats: SessionStats::default(),
        }
    }
}

/// Gives `publish_record` mutable access to a record's header without every
/// call site repeating the `&mut rec.hdr` field access. Local trait over
/// foreign `md_events` types: allowed under the orphan rule (local trait,
/// foreign type).
trait RecordMut: Record {
    fn header_mut(&mut self) -> &mut RecordHeader;
}

macro_rules! impl_record_mut {
    ($t:ty) => {
        impl RecordMut for $t {
            fn header_mut(&mut self) -> &mut RecordHeader {
                &mut self.hdr
            }
        }
        // `OwnedRecord` stores any published record inline.
        const _: () = assert!(size_of::<$t>() <= crate::record::MAX_RECORD_LEN);
    };
}

impl_record_mut!(md_events::Definition);
impl_record_mut!(md_events::Mbo);
impl_record_mut!(md_events::Level);
impl_record_mut!(md_events::Fill);
impl_record_mut!(md_events::Trade);
impl_record_mut!(md_events::Status);
impl_record_mut!(md_events::Stats);
impl_record_mut!(md_events::TradeBreak);
impl_record_mut!(md_events::Control);
impl_record_mut!(md_events::Imbalance);
impl_record_mut!(md_events::Mbp10);
impl_record_mut!(md_events::Bbo);
impl_record_mut!(md_events::Ohlcv);

/// Stamps egress ts/seq on `rec`'s header in place and publishes its
/// post-header bytes through `seq`.
fn publish_record<T, S, C>(
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
    mut rec: T,
) -> Result<T, SessionError>
where
    T: RecordMut,
    S: OutputSink,
    C: EgressClock,
{
    let hdr_len = core::mem::size_of::<RecordHeader>();
    let total = core::mem::size_of::<T>();
    // body bytes read from a pre-stamp copy: header stamping below takes
    // `&mut rec`, and a slice borrowed out of `rec` itself would alias it.
    // Stamping never touches body bytes, so the copy is exact.
    let body_src = rec;
    // SAFETY: T: Record requires Copy + repr(C) (md-events contract, the same
    // precondition md_events::decode_prefix and md_capture::NormalizedWriter
    // rely on), so the bytes after the header are the record body. `body_src`
    // is an immutable local that outlives `body`.
    let body = unsafe {
        core::slice::from_raw_parts(
            (&body_src as *const T as *const u8).add(hdr_len),
            total - hdr_len,
        )
    };
    {
        let _egress = crate::obs::stage_timer(crate::obs::STAGE_EGRESS);
        seq.publish(stream, rec.header_mut(), body)?;
    }
    Ok(rec)
}

/// Publishes a derived `Mbp10`/`Bbo` pair when the book purposes produced
/// one (both are `None` together when the instrument's board excludes BOOK
/// participation).
fn publish_book_derive<S, C>(
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
    derived: orderbook_engine::BookDerive,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    if let Some(mbp10) = derived.mbp10 {
        publish_record(seq, stream, mbp10)?;
    }
    if let Some(bbo) = derived.bbo {
        publish_record(seq, stream, bbo)?;
    }
    Ok(())
}

fn same_mbo_source_event(first: &Mbo, next: &Mbo) -> bool {
    first.hdr.venue == next.hdr.venue
        && first.hdr.instrument_id == next.hdr.instrument_id
        && first.hdr.ts_event == next.hdr.ts_event
        && first.hdr.ts_recv == next.hdr.ts_recv
        && first.hdr.venue_seq == next.hdr.venue_seq
}

/// Applies one contiguous source-event MBO segment in adapter order, publishes
/// every L3 mutation, then publishes only the final derived book view.
fn route_mbo_segment<S, C>(
    mbos: &mut [Mbo],
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    if mbos.is_empty() {
        return Ok(());
    }
    {
        let _apply = crate::obs::stage_timer(crate::obs::STAGE_BOOK_APPLY);
        for mbo in mbos.iter_mut() {
            let outcome = engines.book.apply_mbo(mbo);
            if outcome.result == ApplyResult::UnknownOrder {
                *engines
                    .stats
                    .unknown_order_counts
                    .entry(mbo.hdr.instrument_id)
                    .or_insert(0) += 1;
            }
            if outcome.suspect {
                mbo.hdr.flags = mbo.hdr.flags.with(RecordFlags::SUSPECT);
            }
        }
    }
    // Timed apart from apply, not nested inside it: nesting would fold the derive
    // timer's own clock reads into the apply sample. Both fire once per segment,
    // so their means share a denominator and stay additive.
    let derived = {
        let _derive = crate::obs::stage_timer(crate::obs::STAGE_BOOK_DERIVE);
        engines
            .derive
            .apply_mbo_batch(mbos, &engines.book, DEFAULT_PURPOSES)
    };
    for mbo in mbos.iter().copied() {
        publish_record(seq, stream, mbo)?;
    }
    publish_book_derive(seq, stream, derived)
}

fn flush_mbo_batch<S, C>(
    batch: &mut Vec<Mbo>,
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    route_mbo_segment(batch.as_mut_slice(), engines, seq, stream)?;
    batch.clear();
    Ok(())
}

/// How many events sit between a book prefetch probe and its apply. Wide
/// enough to cover an L2 miss behind routing work, narrow enough that the
/// probed lines survive in cache.
const PREFETCH_DISTANCE: usize = 8;

/// How [`run`] pulls frames and when it ends: up to `batch` frames per pull,
/// stopping after the batch in flight once `stop` is set.
#[derive(Clone, Copy, Debug)]
pub struct RunControl<'a> {
    pub batch: usize,
    pub stop: &'a AtomicBool,
}

/// Runs one venue session: pull a frame batch, tap and feed every frame, then
/// route the batch's events and publish them. `adapter` is a concrete
/// `VenueAdapter` on the static path or a `dyn DynAdapter` on the registry
/// path; both run this body. Each frame feeds on its own stream and its events
/// publish on that stream, so one dual-channel source drives a two-stream
/// session. Ends cleanly when `source` closes (replay exhausted, or a live
/// session's `EndOfSession`) or after the batch in flight when `control.stop`
/// is set; the caller decides whether to reconnect and finishes the tap.
/// Returns the exhausted source so callers can inspect its state (e.g.
/// `BinaryFileReplay::io_error`).
///
/// # Errors
///
/// A sink or tap failure ends the session, as does a live frame whose anchor
/// does not resolve ([`SessionError::AnchorUnresolved`]). Frames fed before
/// such a fault in the same batch still route and publish first.
pub fn run<A, Src, S, C, Tap>(
    mut source: Src,
    adapter: &mut A,
    seq: &mut EgressSequencer<S, C>,
    tap: &mut Tap,
    state: &mut SessionState,
    restart: &mut RestartState,
    control: RunControl<'_>,
) -> Result<(SessionStats, Src), SessionError>
where
    A: DynAdapter + ?Sized,
    Src: IngestSource,
    S: OutputSink,
    C: EgressClock,
    Tap: CaptureTap,
{
    let mut buffers = StepBuffers::new(control.batch);
    loop {
        match step(&mut source, adapter, seq, tap, state, restart, &mut buffers)? {
            Step::Closed => break,
            Step::Retry => continue,
            Step::Routed => {}
        }
        // batch boundary: a stop signal ends the loop only after the pulled
        // batch is fully routed, so no frame is half-processed.
        if control.stop.load(Ordering::Relaxed) {
            break;
        }
    }
    flush_bars(state, seq)?;
    Ok((state.engines.stats.clone(), source))
}

/// What one [`step`] did with the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// A batch was pulled, possibly empty, and every frame in it routed.
    Routed,
    /// The source's frame pool was exhausted; nothing was pulled.
    Retry,
    /// The source closed; nothing was pulled.
    Closed,
}

/// Buffers one [`step`] fills and clears, allocated once per source.
pub(crate) struct StepBuffers<F> {
    batch: usize,
    frames: Vec<F>,
    events: Vec<MarketEvent>,
    /// (stream, end index into `events`) per frame, in frame order.
    spans: Vec<(StreamId, usize)>,
    mbo_batch: Vec<Mbo>,
}

impl<F> StepBuffers<F> {
    /// Buffers for steps that pull up to `batch` frames each.
    pub(crate) fn new(batch: usize) -> Self {
        StepBuffers {
            batch,
            frames: Vec::with_capacity(batch),
            events: Vec::new(),
            spans: Vec::with_capacity(batch),
            mbo_batch: Vec::with_capacity(2),
        }
    }
}

/// Pulls one frame batch, taps and feeds every frame, then routes the batch's
/// events and publishes them: the body [`run`] loops over.
///
/// # Errors
///
/// As [`run`]: a sink or tap failure, or a live frame whose anchor does not
/// resolve. Frames fed before the fault in the batch still route and publish.
pub(crate) fn step<A, Src, S, C, Tap>(
    source: &mut Src,
    adapter: &mut A,
    seq: &mut EgressSequencer<S, C>,
    tap: &mut Tap,
    state: &mut SessionState,
    restart: &mut RestartState,
    buffers: &mut StepBuffers<Src::Frame>,
) -> Result<Step, SessionError>
where
    A: DynAdapter + ?Sized,
    Src: IngestSource,
    S: OutputSink,
    C: EgressClock,
    Tap: CaptureTap,
{
    let StepBuffers {
        batch,
        frames,
        events,
        spans,
        mbo_batch,
    } = buffers;
    frames.clear();
    match source.recv_batch(frames, *batch) {
        Ok(_) => {}
        Err(IngestError::Closed) => return Ok(Step::Closed),
        Err(IngestError::PoolExhausted { .. }) => return Ok(Step::Retry),
    }
    events.clear();
    spans.clear();
    let mut fault = None;
    // Feed the whole batch before routing so the prefetch window crosses
    // frames; an adapter's output does not depend on when it is drained.
    for frame in frames.iter() {
        if let Err(err) = tap.raw(frame.stream(), frame.ts_recv(), frame.payload()) {
            fault = Some(err.into());
            break;
        }
        if let Some(roll) = state.anchor_roll.as_mut()
            && roll.due(frame.ts_recv())
        {
            match roll.roll(frame.ts_recv()) {
                Ok(anchor) => adapter.set_session_anchor(anchor),
                Err(err) => {
                    fault = Some(err);
                    break;
                }
            }
        }
        {
            let _feed = crate::obs::stage_timer(crate::obs::STAGE_FEED);
            adapter.feed(
                frame.stream(),
                frame.payload(),
                frame.ts_recv(),
                frame.venue_seq(),
            );
        }
        adapter.drain_into(events);
        spans.push((frame.stream(), events.len()));
    }
    route_events(events, spans, &mut state.engines, seq, restart, mbo_batch)?;
    let routed = events.len() as u64;
    state.engines.stats.records += routed;
    // An idle live source polls empty batches in a tight loop; skip the
    // counter lookup when there is nothing to add.
    if routed > 0 {
        crate::obs::record_records(routed);
    }
    state.engines.stats.frames += spans.len() as u64;
    match fault {
        Some(err) => Err(err),
        None => Ok(Step::Routed),
    }
}

/// Publishes every still-open OHLCV bar on [`SOURCE_STREAM`], as a session
/// does once its source ends.
pub(crate) fn flush_bars<S, C>(
    state: &mut SessionState,
    seq: &mut EgressSequencer<S, C>,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    for bar in state.engines.ohlcv.flush_all() {
        publish_record(seq, SOURCE_STREAM, bar)?;
    }
    Ok(())
}

/// Routes one batch's events in emission order, each on the stream of the
/// frame that emitted it. An MBO segment never spans frames: it flushes at
/// every frame's end. Each MBO's book lines are probed `PREFETCH_DISTANCE`
/// events before it routes, so those cache misses resolve behind routing
/// work; the probe is read-only, so order and output are unchanged.
fn route_events<S, C>(
    events: &[MarketEvent],
    spans: &[(StreamId, usize)],
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    restart: &mut RestartState,
    mbo_batch: &mut Vec<Mbo>,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    for event in events.iter().take(PREFETCH_DISTANCE) {
        prefetch(&engines.book, event);
    }
    let mut start = 0;
    for &(stream, end) in spans {
        for (i, &event) in (start..end).zip(&events[start..end]) {
            if let Some(ahead) = events.get(i + PREFETCH_DISTANCE) {
                prefetch(&engines.book, ahead);
            }
            route_event(event, engines, seq, restart, stream, mbo_batch)?;
        }
        flush_mbo_batch(mbo_batch, engines, seq, stream)?;
        start = end;
    }
    Ok(())
}

fn prefetch(book: &BookEngine<DefaultVenuePolicy>, event: &MarketEvent) {
    if let MarketEvent::Mbo(mbo) = event {
        book.prefetch_mbo(mbo);
    }
}

/// Advances the OHLCV watermark to the event's time, so non-trade traffic
/// closes due bars on time, then routes it. Consecutive MBOs from one source
/// event collect in `mbo_batch` and apply as one segment.
fn route_event<S, C>(
    event: MarketEvent,
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    restart: &mut RestartState,
    stream: StreamId,
    mbo_batch: &mut Vec<Mbo>,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    for bar in engines.ohlcv.advance(event.header().ts_event) {
        publish_record(seq, stream, bar)?;
    }
    match event {
        MarketEvent::Mbo(mbo) => {
            let starts_new_event = mbo_batch
                .first()
                .is_some_and(|first| !same_mbo_source_event(first, &mbo));
            if starts_new_event {
                flush_mbo_batch(mbo_batch, engines, seq, stream)?;
            }
            mbo_batch.push(mbo);
        }
        other => {
            flush_mbo_batch(mbo_batch, engines, seq, stream)?;
            route_record(other, engines, seq, restart, stream)?;
        }
    }
    Ok(())
}

/// Routes one event on its own, an MBO as a one-record segment.
fn route_record<S, C>(
    event: MarketEvent,
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    restart: &mut RestartState,
    stream: StreamId,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    match event {
        MarketEvent::Mbo(mut mbo) => {
            route_mbo_segment(core::slice::from_mut(&mut mbo), engines, seq, stream)?;
        }
        // The primary `Mbo` is the book's only writer. Secondary price levels are
        // enrichment: applying one would double-count or overwrite primary depth.
        MarketEvent::Level(level) => {
            publish_record(seq, stream, level)?;
        }
        MarketEvent::Trade(trade) => route_trade(engines, seq, stream, trade)?,
        MarketEvent::TradeBreak(tb) => route_trade_break(engines, seq, stream, tb)?,
        MarketEvent::Control(ctl) => {
            restart.observe_control(ctl.kind, ctl.aux);
            publish_record(seq, stream, ctl)?;
        }
        MarketEvent::Definition(def) => {
            publish_record(seq, stream, def)?;
        }
        MarketEvent::Fill(fill) => {
            publish_record(seq, stream, fill)?;
        }
        MarketEvent::Status(status) => {
            publish_record(seq, stream, status)?;
        }
        MarketEvent::Stats(stats) => {
            publish_record(seq, stream, stats)?;
        }
        MarketEvent::Imbalance(imb) => {
            publish_record(seq, stream, imb)?;
        }
    }
    Ok(())
}

/// `Trade` routing: on [`SOURCE_STREAM`], updates the derive-engine VWAP
/// accumulator and OHLCV bars and remembers the print for a later
/// `TradeBreak`. On any stream, publishes the trade plus any bars it closed.
fn route_trade<S, C>(
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
    trade: md_events::Trade,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    let closed_bars = if stream == SOURCE_STREAM {
        engines.derive.apply_trade(&trade, DEFAULT_PURPOSES);
        engines.trade_memo.record(
            trade.trade_ref,
            trade.price,
            trade.qty,
            trade.hdr.instrument_id,
        );
        engines.ohlcv.apply_trade(&trade, DEFAULT_PURPOSES)
    } else {
        Vec::new()
    };
    publish_record(seq, stream, trade)?;
    for bar in closed_bars {
        publish_record(seq, stream, bar)?;
    }
    Ok(())
}

/// `TradeBreak` routing: on [`SOURCE_STREAM`], reverses both `OhlcvEngine`'s
/// still-open bar aggregates and, when the broken trade's values are still in
/// `trade_memo`, `DeriveEngine`'s VWAP accumulator. Publishes on any stream.
fn route_trade_break<S, C>(
    engines: &mut SessionEngines<DefaultVenuePolicy>,
    seq: &mut EgressSequencer<S, C>,
    stream: StreamId,
    tb: TradeBreak,
) -> Result<(), SessionError>
where
    S: OutputSink,
    C: EgressClock,
{
    if stream == SOURCE_STREAM {
        engines.ohlcv.apply_trade_break(&tb);
        if let Some((price, qty, instrument)) = engines.trade_memo.get(tb.break_ref) {
            engines.derive.apply_trade_break(price, qty, instrument);
        }
    }
    publish_record(seq, stream, tb)?;
    Ok(())
}

#[cfg(test)]
mod tests;
