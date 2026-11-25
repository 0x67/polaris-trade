//! NASDAQ TotalView-ITCH 5.0 venue adapter, a [`VenueAdapter`] on one ingest
//! stream. `HotVisitor` methods return nothing, so decoded records buffer as
//! [`MarketEvent`]s in an internal `Vec` the pipeline drains after each `feed`
//! call. Decoupled from any sink, so the whole 23-kind mapping is unit-testable
//! without a pipeline running.

use std::{collections::HashMap, mem::size_of};

use md_events::{
    Action, AssetClass, BreakRefKind, Control, ControlKind, Imbalance, Mbo, Price, RecordFlags,
    RecordHeader, SentinelScheme, Side, Status, Trade, TradeBreak, TradingState, rtype,
};
use md_pipeline::{
    AdapterStats, ClosureModel, Coverage, CoverageMatrix, DefinitionEvent, InstrumentRegistry,
    MarketEvent, RollModel, SessionModel, StreamId, VenueAdapter, VenueId,
};
use orderbook_engine::StatusMachine;

use crate::itch_v5::{
    self, AddOrderMpidAttributionView, AddOrderNoMpidView, BrokenTradeView, CrossTradeView,
    DirectListingCapitalRaisePriceDiscoveryView, HotVisitor, IpoQuotingPeriodUpdateView,
    LuldAuctionCollarView, MarketParticipantPositionView, MwcbDeclineLevelView, MwcbStatusView,
    NetOrderImbalanceIndicatorView, OperationalHaltView, OrderCancelView, OrderDeleteView,
    OrderExecutedView, OrderExecutedWithPriceView, OrderReplaceView, RegShoRestrictionView,
    RetailPriceImprovementIndicatorView, StockDirectoryView, StockTradingActionView,
    SystemEventView, TradeView,
};

/// Single-board venue: NASDAQ carries no board distinction on the wire, so
/// every minted instrument key uses the zeroed default.
const DEFAULT_BOARD: [u8; 4] = [0u8; 4];

/// The one ingest stream ITCH frames arrive on.
const ITCH_STREAM: StreamId = StreamId(0);

/// Construction input for one [`NasdaqAdapter`] session.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Venue id stamped on every record header and minted instrument key.
    pub venue: VenueId,
    /// Epoch-ns UTC of the trade-date midnight in America/New_York, the
    /// anchor [`SessionModel::resolve_anchor`] gives for that date. Initial
    /// only: [`VenueAdapter::set_session_anchor`] replaces it.
    pub session_midnight_ns: u64,
}

/// Why `feed` could not dispatch a frame to `HotVisitor`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NasdaqAdapterError {
    /// Wire byte matched a known kind but the frame length did not match
    /// `message_type::BYTE_LEN`; never panics, just drops and counts.
    Malformed,
    /// Wire byte matches no ITCH 5.0 kind this schema declares.
    UnknownKind,
}

/// Resting order state the adapter must track itself: NASDAQ's execution,
/// cancel, delete, and replace messages carry only the order reference
/// number, never side or symbol (`OrderReplaceView`'s own doc: "Side, stock
/// symbol, and attribution cannot change and are not resent"). Downstream
/// `BookEngine::apply_mbo` keys resting orders on `(instrument, side,
/// order_id)`, so a wrong side on a later `Mbo` silently orphans the fill.
/// `remaining_qty` tracks displayed shares left so a full fill can evict the
/// entry: ITCH 5.0 sends no Delete after a full execution.
#[derive(Clone, Copy)]
struct OrderState {
    instrument_id: u32,
    side: Side,
    price: i64,
    remaining_qty: u64,
    participant: [u8; 7],
}

/// NASDAQ TotalView-ITCH 5.0 adapter: `itch_v5::HotVisitor` impl that maps
/// all 23 kinds to normalized records and buffers them for the pipeline to
/// drain. Holds its own instrument registry, trading-state machine, and
/// resting-order table so it can run standalone against a replay source with
/// no book engine attached.
pub struct NasdaqAdapter {
    venue: VenueId,
    /// Epoch-ns UTC of the trade-date midnight in the venue reference
    /// timezone (America/New_York), the base the wire's
    /// nanoseconds-since-midnight add to. The config value until a runner
    /// installs another through `set_session_anchor`.
    session_midnight_ns: u64,
    registry: InstrumentRegistry,
    status: StatusMachine,
    orders: HashMap<u64, OrderState>,
    out: Vec<MarketEvent>,
    stats: AdapterStats,
    /// Wire venue sequence of the message currently being decoded, stamped
    /// verbatim from `feed`'s `venue_seq` argument.
    cur_venue_seq: u64,
    /// A `venue_seq` skip no record has carried yet: set when a feed skips past
    /// `cur_venue_seq + 1` (the session's first message excepted), cleared
    /// when `next_header` flags a record, so a skip on a dropped or
    /// non-emitting frame still reaches the next frame that emits.
    gap_pending: bool,
    /// Whether this frame's records carry `GAP_SUSPECT`: `gap_pending` as the
    /// frame arrived.
    frame_gap_suspect: bool,
    cur_ts_recv: u64,
}

impl VenueAdapter for NasdaqAdapter {
    type Config = Config;

    /// New adapter for one venue id and trade-date session.
    fn new(config: Config) -> Self {
        NasdaqAdapter {
            venue: config.venue,
            session_midnight_ns: config.session_midnight_ns,
            registry: InstrumentRegistry::new(),
            status: StatusMachine::new(),
            orders: HashMap::new(),
            out: Vec::new(),
            stats: AdapterStats::default(),
            cur_venue_seq: 0,
            gap_pending: false,
            frame_gap_suspect: false,
            cur_ts_recv: 0,
        }
    }

    fn streams() -> &'static [StreamId] {
        &[ITCH_STREAM]
    }

    /// Declared wire coverage on stream 0: which of the 23 kinds this adapter
    /// decodes, and whether each feeds `BookEngine`/`DeriveEngine` downstream.
    /// `A/F` (add), `E/C` (fill), `X/D` (cancel/delete), `U` (replace) emit
    /// `Mbo`, which both mutates the book and feeds mbp10/bbo derivation.
    /// `E/C/P/Q` also emit `Trade`, which feeds the tape/vwap derivation; `B`
    /// reverses an already-derived OHLCV bar. Everything else (status family,
    /// `Definition`, `Control`, `Imbalance`) is decoded but not book/derive
    /// applied by any engine surface that exists today. Any other stream gets
    /// an empty matrix.
    fn coverage(stream: StreamId) -> CoverageMatrix {
        if stream != ITCH_STREAM {
            return CoverageMatrix::new();
        }
        let mut m = CoverageMatrix::new();
        m.set(b'S', Coverage::REFERENCE)
            .set(b'R', Coverage::REFERENCE)
            .set(b'H', Coverage::REFERENCE)
            .set(b'Y', Coverage::REFERENCE)
            .set(b'L', Coverage::REFERENCE)
            .set(b'V', Coverage::REFERENCE)
            .set(b'W', Coverage::REFERENCE)
            .set(b'K', Coverage::REFERENCE)
            .set(b'J', Coverage::REFERENCE)
            .set(b'h', Coverage::REFERENCE)
            .set(b'A', Coverage::BOOK_AND_DERIVED)
            .set(b'F', Coverage::BOOK_AND_DERIVED)
            .set(b'E', Coverage::BOOK_AND_DERIVED)
            .set(b'C', Coverage::BOOK_AND_DERIVED)
            .set(b'X', Coverage::BOOK_AND_DERIVED)
            .set(b'D', Coverage::BOOK_AND_DERIVED)
            .set(b'U', Coverage::BOOK_AND_DERIVED)
            .set(b'P', Coverage::TAPE_ONLY)
            .set(b'Q', Coverage::TAPE_ONLY)
            .set(b'B', Coverage::TAPE_ONLY)
            .set(b'I', Coverage::REFERENCE)
            .set(b'N', Coverage::REFERENCE)
            .set(b'O', Coverage::REFERENCE);
        m
    }

    /// `Control` from system events, `Mbo`, `Trade` and `TradeBreak` from
    /// orders and trades, `Imbalance` from the net order imbalance
    /// indicator, `Status` from the trading-state family, and the minted
    /// `Definition`.
    fn record_types() -> &'static [u8] {
        &[
            rtype::CONTROL,
            rtype::MBO,
            rtype::TRADE,
            rtype::TRADE_BREAK,
            rtype::IMBALANCE,
            rtype::STATUS,
            rtype::DEFINITION,
        ]
    }

    /// NASDAQ rolls at New York midnight. The zone's DST rule moves that
    /// anchor in UTC, so it resolves per trade date through a
    /// [`md_pipeline::TzResolver`], never once for the adapter's lifetime.
    fn session(&self) -> SessionModel {
        SessionModel {
            roll: RollModel::DailyZoned {
                tz: "America/New_York",
                local_ns: 0,
            },
            closure: ClosureModel::None,
        }
    }

    /// Feed one wire message (type byte at `payload[0]`, whole message
    /// including that byte) received at `ts_recv`, carrying `venue_seq`
    /// verbatim from the wire (session sequence live, per-stream
    /// synthesized count in replay). Malformed or unknown frames are dropped
    /// and counted, never panic. A frame on any stream but 0 is ignored
    /// before any state moves.
    #[inline]
    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if stream != ITCH_STREAM {
            return;
        }
        self.cur_ts_recv = ts_recv;
        self.gap_pending |= self.cur_venue_seq != 0 && venue_seq != self.cur_venue_seq + 1;
        self.frame_gap_suspect = self.gap_pending;
        self.cur_venue_seq = venue_seq;
        match classify(payload) {
            Ok(msg_type) => {
                self.stats.decoded += 1;
                itch_v5::parse_match(msg_type, payload, self);
            }
            Err(NasdaqAdapterError::Malformed) => self.stats.malformed += 1,
            Err(NasdaqAdapterError::UnknownKind) => self.stats.unknown_kind += 1,
        }
    }

    #[inline]
    fn drain(&mut self) -> impl Iterator<Item = MarketEvent> + '_ {
        self.out.drain(..)
    }

    fn stats(&self) -> &AdapterStats {
        &self.stats
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        self.session_midnight_ns = anchor_ns;
    }
}

impl NasdaqAdapter {
    /// Resting order count. Mirrors `orderbook_engine::BookEngine::order_count`;
    /// stays bounded across a full session since fills evict on zero remaining.
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    // venue_seq is per-message, not per-record: E and C each emit two
    // records from one frame, and both carry that frame's one wire sequence.
    fn header(&mut self, rt: u8, size: usize, instrument_id: u32, ts_event: u64) -> RecordHeader {
        self.next_header(rt, size, instrument_id, ts_event, RecordFlags::NONE)
    }

    fn next_header(
        &mut self,
        rt: u8,
        size: usize,
        instrument_id: u32,
        ts_event: u64,
        flags: RecordFlags,
    ) -> RecordHeader {
        let flags = if self.frame_gap_suspect {
            self.gap_pending = false;
            flags.with(RecordFlags::GAP_SUSPECT)
        } else {
            flags
        };
        RecordHeader {
            length: (size / 4) as u8,
            rtype: rt,
            venue: self.venue.0,
            instrument_id,
            ts_event,
            ts_recv: self.cur_ts_recv,
            ts_egress: 0,
            venue_seq: self.cur_venue_seq,
            egress_seq: 0,
            flags,
            _pad: [0u8; 6],
        }
    }

    // resolve-or-mint by (venue, symbol, board): idempotent, re-sighting an
    // already-minted symbol just adds an alias interval, never a second Definition.
    fn resolve_or_mint(&mut self, stock: &[u8], stock_locate: u16, ts_event: u64) -> u32 {
        let ev = DefinitionEvent {
            venue: self.venue,
            raw_symbol: pad_symbol(stock),
            board: DEFAULT_BOARD,
            venue_native_id: stock_locate as u64,
            ts: ts_event,
            asset_class: AssetClass::Equity,
            qty_scale: 0,
            lot_size: 0,
        };
        self.registry.upsert(&ev).0
    }

    // fallback to the raw locate code when no directory alias has been
    // seen yet: isolated unit tests skip `R`, a full-day replay never does.
    fn resolve_instrument(&self, stock_locate: u16, ts_event: u64) -> u32 {
        self.registry
            .resolve(self.venue, stock_locate as u64, ts_event)
            .map(|id| id.0)
            .unwrap_or(stock_locate as u32)
    }

    // instrument, side, resting price for a message that references an order by
    // id. Falls back to the locate-code instrument and unknown side when the
    // order was never tracked: isolated tests skip the Add, a real feed never does.
    fn order_fields(
        &self,
        order_id: u64,
        stock_locate: u16,
        ts_event: u64,
    ) -> (u32, Side, i64, u64, [u8; 7]) {
        match self.orders.get(&order_id) {
            Some(s) => (
                s.instrument_id,
                s.side,
                s.price,
                s.remaining_qty,
                s.participant,
            ),
            None => (
                self.resolve_instrument(stock_locate, ts_event),
                Side::None,
                0,
                0,
                [0; 7],
            ),
        }
    }

    // like order_fields but retires the order id (delete and replace remove it).
    fn take_order_fields(
        &mut self,
        order_id: u64,
        stock_locate: u16,
        ts_event: u64,
    ) -> (u32, Side, i64, u64, [u8; 7]) {
        match self.orders.remove(&order_id) {
            Some(s) => (
                s.instrument_id,
                s.side,
                s.price,
                s.remaining_qty,
                s.participant,
            ),
            None => (
                self.resolve_instrument(stock_locate, ts_event),
                Side::None,
                0,
                0,
                [0; 7],
            ),
        }
    }

    fn ts_event(&self, wire_ts: u64) -> u64 {
        self.session_midnight_ns.wrapping_add(wire_ts)
    }

    // subtract executed qty, evict once remaining hits 0: ITCH 5.0 sends no
    // Delete after a full fill. mirrors BookEngine::apply_mbo's Fill arm.
    fn apply_reduction(&mut self, order_id: u64, qty: u64) {
        if let Some(state) = self.orders.get_mut(&order_id) {
            state.remaining_qty = state.remaining_qty.saturating_sub(qty);
            if state.remaining_qty == 0 {
                self.orders.remove(&order_id);
            }
        }
    }
}

fn classify(payload: &[u8]) -> Result<u8, NasdaqAdapterError> {
    let &msg_type = payload.first().ok_or(NasdaqAdapterError::Malformed)?;
    match itch_v5::BYTE_LEN[msg_type as usize] {
        Some(len) if len.get() as usize == payload.len() => Ok(msg_type),
        Some(_) => Err(NasdaqAdapterError::Malformed),
        None => Err(NasdaqAdapterError::UnknownKind),
    }
}

fn pad_symbol(stock: &[u8]) -> [u8; 20] {
    let mut sym = [0u8; 20];
    let n = stock.len().min(20);
    sym[..n].copy_from_slice(&stock[..n]);
    sym
}

fn map_side(b: u8) -> Side {
    match b {
        b'B' => Side::Bid,
        b'S' => Side::Ask,
        _ => Side::None,
    }
}

// aggressor (taker) side for Executed-family fills: opposite of resting
// order's side; unknown resting side stays None.
fn aggressor_of_resting(resting: Side) -> Side {
    match resting {
        Side::Bid => Side::Ask,
        Side::Ask => Side::Bid,
        Side::None => Side::None,
    }
}

fn map_imbalance_side(b: u8) -> Side {
    match b {
        b'B' => Side::Bid,
        b'S' => Side::Ask,
        // N = no imbalance, O = insufficient orders, P = paused
        _ => Side::None,
    }
}

fn map_system_event(event_code: u8) -> ControlKind {
    match event_code {
        b'O' => ControlKind::FeedStart,
        b'C' => ControlKind::FeedEnd,
        // S/Q/M/E: system/market hours milestones, no dedicated ControlKind
        _ => ControlKind::Heartbeat,
    }
}

fn map_trading_state(b: u8) -> TradingState {
    match b {
        b'H' => TradingState::Halt,
        b'P' => TradingState::Pause,
        b'Q' => TradingState::IntradayAuction,
        b'T' => TradingState::Continuous,
        _ => TradingState::Unknown,
    }
}

fn map_operational_halt(b: u8) -> TradingState {
    match b {
        b'H' => TradingState::Halt,
        b'T' => TradingState::Continuous,
        _ => TradingState::Unknown,
    }
}

fn attribution_participant(a: &[u8]) -> [u8; 7] {
    let mut p = [0u8; 7];
    let n = a.len().min(7);
    p[..n].copy_from_slice(&a[..n]);
    p
}

impl HotVisitor for NasdaqAdapter {
    fn system_event(&mut self, v: SystemEventView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let kind = map_system_event(v.event_code());
        let hdr = self.header(rtype::CONTROL, size_of::<Control>(), 0, ts_event);
        self.out.push(MarketEvent::Control(Control {
            hdr,
            kind,
            aux: v.event_code() as u64,
            _pad: [0u8; 7],
        }));
    }

    fn stock_directory(&mut self, v: StockDirectoryView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let ev = DefinitionEvent {
            venue: self.venue,
            raw_symbol: pad_symbol(v.stock()),
            board: DEFAULT_BOARD,
            venue_native_id: v.stock_locate() as u64,
            ts: ts_event,
            asset_class: AssetClass::Equity,
            qty_scale: 0,
            lot_size: v.round_lot_size(),
        };
        let id = self.registry.upsert(&ev);
        let def = InstrumentRegistry::definition_record(&ev, id);
        self.out.push(MarketEvent::Definition(def));
    }

    fn stock_trading_action(&mut self, v: StockTradingActionView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = map_trading_state(v.trading_state());
        self.status.apply(instrument_id, state);
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.trading_state();
        venue_reason[1..5].copy_from_slice(v.reason());
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn reg_sho_restriction(&mut self, v: RegShoRestrictionView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.locate_code(), ts_event);
        let state = self.status.state(instrument_id);
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.reg_sho_action();
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn market_participant_position(&mut self, v: MarketParticipantPositionView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = self.status.state(instrument_id);
        let mut venue_reason = [0u8; 20];
        venue_reason[0..4].copy_from_slice(v.mpid());
        venue_reason[4] = v.primary_market_maker();
        venue_reason[5] = v.market_maker_mode();
        venue_reason[6] = v.market_participant_state();
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn mwcb_decline_level(&mut self, v: MwcbDeclineLevelView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = 0; // market-wide
        let state = self.status.state(instrument_id);
        // NOTE: level precision undocumented upstream; low 4 bytes of each
        // kept as a raw hint, pending correction against a real sample.
        let mut venue_reason = [0u8; 20];
        venue_reason[0..4].copy_from_slice(&(v.level_1() as u32).to_be_bytes());
        venue_reason[4..8].copy_from_slice(&(v.level_2() as u32).to_be_bytes());
        venue_reason[8..12].copy_from_slice(&(v.level_3() as u32).to_be_bytes());
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn mwcb_status(&mut self, v: MwcbStatusView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = 0; // market-wide
        // NOTE: ITCH has no resumption message for a MWCB breach. State
        // stays Halt for the rest of the session; only a new session resets it.
        self.status.apply(instrument_id, TradingState::Halt);
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.breached_level();
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state: TradingState::Halt,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn ipo_quoting_period_update(&mut self, v: IpoQuotingPeriodUpdateView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = self.status.state(instrument_id);
        let mut venue_reason = [0u8; 20];
        venue_reason[0..4].copy_from_slice(&v.ipo_quotation_release_time().to_be_bytes());
        venue_reason[4] = v.ipo_quotation_release_qualifier();
        venue_reason[5..9].copy_from_slice(&v.ipo_price().to_be_bytes());
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn luld_auction_collar(&mut self, v: LuldAuctionCollarView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = self.status.state(instrument_id);
        let mut venue_reason = [0u8; 20];
        venue_reason[0..4].copy_from_slice(&v.auction_collar_reference_price().to_be_bytes());
        venue_reason[4..8].copy_from_slice(&v.upper_auction_collar_price().to_be_bytes());
        venue_reason[8..12].copy_from_slice(&v.lower_auction_collar_price().to_be_bytes());
        venue_reason[12..16].copy_from_slice(&v.auction_collar_extension().to_be_bytes());
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn operational_halt(&mut self, v: OperationalHaltView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = map_operational_halt(v.operational_halt_action());
        self.status.apply(instrument_id, state);
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.market_code();
        venue_reason[1] = v.operational_halt_action();
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn add_order_no_mpid(&mut self, v: AddOrderNoMpidView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let side = map_side(v.buy_sell_indicator());
        let price = Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
        self.orders.insert(
            v.order_reference_number(),
            OrderState {
                instrument_id,
                side,
                price,
                remaining_qty: v.shares() as u64,
                participant: [0; 7],
            },
        );
        let hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id: v.order_reference_number(),
            price,
            qty: v.shares() as u64,
            action: Action::Add,
            side,
            participant: [0u8; 7],
            _pad: [0u8; 7],
        }));
    }

    fn add_order_mpid_attribution(&mut self, v: AddOrderMpidAttributionView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let side = map_side(v.buy_sell_indicator());
        let price = Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let participant = attribution_participant(v.attribution());
        self.orders.insert(
            v.order_reference_number(),
            OrderState {
                instrument_id,
                side,
                price,
                remaining_qty: v.shares() as u64,
                participant,
            },
        );
        let hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id: v.order_reference_number(),
            price,
            qty: v.shares() as u64,
            action: Action::Add,
            side,
            participant,
            _pad: [0u8; 7],
        }));
    }

    fn order_executed(&mut self, v: OrderExecutedView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let order_id = v.order_reference_number();
        let (instrument_id, side, price, _, participant) =
            self.order_fields(order_id, v.stock_locate(), ts_event);
        let qty = v.executed_shares() as u64;
        self.apply_reduction(order_id, qty);
        let mbo_hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr: mbo_hdr,
            order_id,
            price,
            qty,
            action: Action::Fill,
            side,
            participant,
            _pad: [0u8; 7],
        }));
        let trade_hdr = self.header(rtype::TRADE, size_of::<Trade>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Trade(Trade {
            hdr: trade_hdr,
            price,
            qty,
            // side is the resting order's; taker sits opposite
            aggressor: aggressor_of_resting(side),
            trade_ref: v.match_number(),
            _pad: [0u8; 7],
        }));
    }

    fn order_executed_with_price(&mut self, v: OrderExecutedWithPriceView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let order_id = v.order_reference_number();
        let (instrument_id, side, _, _, participant) =
            self.order_fields(order_id, v.stock_locate(), ts_event);
        let qty = v.executed_shares() as u64;
        self.apply_reduction(order_id, qty);
        let price =
            Price::from_venue(v.execution_price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let flags = if v.printable() == b'N' {
            RecordFlags::NON_PRINTABLE
        } else {
            RecordFlags::NONE
        };
        let mbo_hdr =
            self.next_header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event, flags);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr: mbo_hdr,
            order_id,
            price,
            qty,
            action: Action::FillWithPrice,
            side,
            participant,
            _pad: [0u8; 7],
        }));
        let trade_hdr = self.next_header(
            rtype::TRADE,
            size_of::<Trade>(),
            instrument_id,
            ts_event,
            flags,
        );
        self.out.push(MarketEvent::Trade(Trade {
            hdr: trade_hdr,
            price,
            qty,
            // side is the resting order's; taker sits opposite
            aggressor: aggressor_of_resting(side),
            trade_ref: v.match_number(),
            _pad: [0u8; 7],
        }));
    }

    fn order_cancel(&mut self, v: OrderCancelView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let order_id = v.order_reference_number();
        let (instrument_id, side, price, _, participant) =
            self.order_fields(order_id, v.stock_locate(), ts_event);
        let qty = v.cancelled_shares() as u64;
        self.apply_reduction(order_id, qty);
        let hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id,
            price,
            qty,
            action: Action::Cancel,
            side,
            participant,
            _pad: [0u8; 7],
        }));
    }

    fn order_delete(&mut self, v: OrderDeleteView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let order_id = v.order_reference_number();
        let (instrument_id, side, price, remaining_qty, participant) =
            self.take_order_fields(order_id, v.stock_locate(), ts_event);
        let hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id,
            price,
            qty: remaining_qty,
            action: Action::Cancel,
            side,
            participant,
            _pad: [0u8; 7],
        }));
    }

    fn order_replace(&mut self, v: OrderReplaceView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let old_id = v.original_order_reference_number();
        let new_id = v.new_order_reference_number();
        let (instrument_id, side, old_price, old_remaining, participant) =
            self.take_order_fields(old_id, v.stock_locate(), ts_event);
        let price = Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
        self.orders.insert(
            new_id,
            OrderState {
                instrument_id,
                side,
                price,
                remaining_qty: v.shares() as u64,
                participant,
            },
        );
        let hdr = self.header(rtype::MBO, size_of::<Mbo>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id: old_id,
            price: old_price,
            qty: old_remaining,
            action: Action::Cancel,
            side,
            participant,
            _pad: [0u8; 7],
        }));
        self.out.push(MarketEvent::Mbo(Mbo {
            hdr,
            order_id: new_id,
            price,
            qty: v.shares() as u64,
            action: Action::Add,
            side,
            participant,
            _pad: [0u8; 7],
        }));
    }

    fn trade(&mut self, v: TradeView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let price = Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let hdr = self.header(rtype::TRADE, size_of::<Trade>(), instrument_id, ts_event);
        // buy_sell_indicator is frozen at 'B' since 2014: not a real aggressor signal.
        self.out.push(MarketEvent::Trade(Trade {
            hdr,
            price,
            qty: v.shares() as u64,
            aggressor: Side::None,
            trade_ref: v.match_number(),
            _pad: [0u8; 7],
        }));
    }

    fn cross_trade(&mut self, v: CrossTradeView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let price = Price::from_venue(v.cross_price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let hdr = self.header(rtype::TRADE, size_of::<Trade>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Trade(Trade {
            hdr,
            price,
            qty: v.shares(),
            aggressor: Side::None,
            trade_ref: v.match_number(),
            _pad: [0u8; 7],
        }));
    }

    fn broken_trade(&mut self, v: BrokenTradeView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_instrument(v.stock_locate(), ts_event);
        let hdr = self.header(
            rtype::TRADE_BREAK,
            size_of::<TradeBreak>(),
            instrument_id,
            ts_event,
        );
        self.out.push(MarketEvent::TradeBreak(TradeBreak {
            hdr,
            break_ref: v.match_number(),
            ref_kind: BreakRefKind::MatchNumber,
            _pad: [0u8; 7],
        }));
    }

    fn net_order_imbalance_indicator(&mut self, v: NetOrderImbalanceIndicatorView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let ref_px = Price::from_venue(
            v.current_reference_price() as i64,
            4,
            SentinelScheme::NoSentinel,
        )
        .raw();
        let far_px = Price::from_venue(v.far_price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let near_px = Price::from_venue(v.near_price() as i64, 4, SentinelScheme::NoSentinel).raw();
        let imbalance_side = map_imbalance_side(v.imbalance_direction());
        let hdr = self.header(
            rtype::IMBALANCE,
            size_of::<Imbalance>(),
            instrument_id,
            ts_event,
        );
        // NASDAQ has no equilibrium price/qty; eq_px is the null sentinel.
        self.out.push(MarketEvent::Imbalance(Imbalance {
            hdr,
            ref_px,
            far_px,
            near_px,
            paired_qty: v.paired_shares(),
            imbalance_qty: v.imbalance_shares(),
            imbalance_side,
            cross_type: v.cross_type(),
            eq_px: Price::NULL.raw(),
            eq_qty: 0,
            _pad: [0u8; 6],
        }));
    }

    fn retail_price_improvement_indicator(&mut self, v: RetailPriceImprovementIndicatorView<'_>) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = self.status.state(instrument_id);
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.interest_flag();
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn direct_listing_capital_raise_price_discovery(
        &mut self,
        v: DirectListingCapitalRaisePriceDiscoveryView<'_>,
    ) {
        let ts_event = self.ts_event(v.timestamp());
        let instrument_id = self.resolve_or_mint(v.stock(), v.stock_locate(), ts_event);
        let state = self.status.state(instrument_id);
        // NOTE: near_execution_time and lower/upper price-range collar decoded
        // by view but unread here; no free slot in the 20-byte venue_reason.
        let mut venue_reason = [0u8; 20];
        venue_reason[0] = v.open_eligibility_status();
        venue_reason[1..5].copy_from_slice(&v.minimum_allowable_price().to_be_bytes());
        venue_reason[5..9].copy_from_slice(&v.maximum_allowable_price().to_be_bytes());
        venue_reason[9..13].copy_from_slice(&v.near_execution_price().to_be_bytes());
        let hdr = self.header(rtype::STATUS, size_of::<Status>(), instrument_id, ts_event);
        self.out.push(MarketEvent::Status(Status {
            hdr,
            state,
            venue_reason,
            _pad: [0u8; 3],
        }));
    }

    fn cold(&mut self, _msg_type: u8, _data: &[u8]) {
        self.stats.unknown_kind += 1;
    }
}
