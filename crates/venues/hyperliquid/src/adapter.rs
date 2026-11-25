//! The Hyperliquid [`VenueAdapter`]: JSON frames in, normalized records out.

use core::mem::size_of;

use md_events::{
    AssetClass, Level, LevelAction, Price, Record, RecordFlags, RecordHeader, Side, StatType,
    Stats, Trade, rtype,
};
use md_pipeline::{
    AdapterStats, ClosureModel, Coverage, CoverageMatrix, DefinitionEvent, InstrumentRegistry,
    MarketEvent, RollModel, SessionModel, StreamId, VenueAdapter, VenueId,
};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::wire::{
    Coin, Envelope, EpochNs, QTY_SCALE, Qty, Rate, WireSide, WsActiveAssetCtx, WsBook, WsLevel,
    WsTrade,
};

/// The one ingest stream every channel of the connection arrives on.
pub const STREAM: StreamId = StreamId(0);

/// Coverage key of the `trades` channel.
pub const KIND_TRADES: u8 = b't';
/// Coverage key of the `l2Book` channel.
pub const KIND_L2_BOOK: u8 = b'l';
/// Coverage key of the `activeAssetCtx` channel.
pub const KIND_ACTIVE_ASSET_CTX: u8 = b'c';

/// Construction input for one [`HyperliquidAdapter`] session.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Venue id stamped on every record header and minted instrument key.
    pub venue: VenueId,
}

/// The `Level` body fields a book push varies; the header is built per record.
#[derive(Clone, Copy)]
struct LevelRow {
    price: i64,
    qty: u64,
    side: Side,
    depth: u8,
    action: LevelAction,
}

/// Decodes the Hyperliquid websocket feed. Instruments mint by coin name on
/// first sight, with a `Definition` record ahead of the first record on them.
pub struct HyperliquidAdapter {
    venue: VenueId,
    registry: InstrumentRegistry,
    out: Vec<MarketEvent>,
    stats: AdapterStats,
    cur_venue_seq: u64,
    /// A `venue_seq` skip no record has carried yet, kept across dropped
    /// frames until the next frame that emits a non-`Definition` record.
    gap_pending: bool,
    frame_gap_suspect: bool,
    cur_ts_recv: u64,
}

impl VenueAdapter for HyperliquidAdapter {
    type Config = Config;

    fn new(config: Config) -> Self {
        HyperliquidAdapter {
            venue: config.venue,
            registry: InstrumentRegistry::new(),
            out: Vec::new(),
            stats: AdapterStats::default(),
            cur_venue_seq: 0,
            gap_pending: false,
            frame_gap_suspect: false,
            cur_ts_recv: 0,
        }
    }

    fn streams() -> &'static [StreamId] {
        &[STREAM]
    }

    fn coverage(stream: StreamId) -> CoverageMatrix {
        let mut m = CoverageMatrix::new();
        if stream == STREAM {
            m.set(KIND_TRADES, Coverage::TAPE_ONLY)
                .set(KIND_L2_BOOK, Coverage::BOOK_AND_DERIVED)
                .set(KIND_ACTIVE_ASSET_CTX, Coverage::REFERENCE);
        }
        m
    }

    /// `Trade` from trades, `Level` from book pushes, `Stats` from asset
    /// contexts and the minted `Definition`. No public channel carries
    /// liquidations or order-level data, so there is no `Mbo`.
    fn record_types() -> &'static [u8] {
        &[rtype::TRADE, rtype::LEVEL, rtype::STATS, rtype::DEFINITION]
    }

    /// 24/7 perpetuals: the session rolls on UTC midnight and never closes.
    fn session(&self) -> SessionModel {
        SessionModel {
            roll: RollModel::Continuous,
            closure: ClosureModel::None,
        }
    }

    /// Decode one websocket text frame. A frame that is not JSON, has the
    /// wrong shape or carries a value outside its fixed-point range counts as
    /// malformed and emits nothing; an unlisted channel counts as unknown.
    #[inline]
    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if stream != STREAM {
            return;
        }
        self.cur_ts_recv = ts_recv;
        self.gap_pending |= self.cur_venue_seq != 0 && venue_seq != self.cur_venue_seq + 1;
        self.frame_gap_suspect = self.gap_pending;
        self.cur_venue_seq = venue_seq;
        let Ok(envelope) = serde_json::from_slice::<Envelope<'_>>(payload) else {
            self.stats.malformed += 1;
            return;
        };
        let decoded = match envelope.channel {
            "trades" => body::<Vec<WsTrade<'_>>>(envelope.data).map(|t| self.trades(&t)),
            "l2Book" => body::<WsBook<'_>>(envelope.data).map(|b| self.book(&b)),
            "activeAssetCtx" => {
                body::<WsActiveAssetCtx<'_>>(envelope.data).map(|c| self.asset_ctx(&c))
            }
            _ => {
                self.stats.unknown_kind += 1;
                return;
            }
        };
        match decoded {
            Some(()) => self.stats.decoded += 1,
            None => self.stats.malformed += 1,
        }
    }

    #[inline]
    fn drain(&mut self) -> impl Iterator<Item = MarketEvent> + '_ {
        self.out.drain(..)
    }

    fn stats(&self) -> &AdapterStats {
        &self.stats
    }
}

/// Read a channel payload as `T`; a missing or ill-shaped payload is `None`.
fn body<'a, T: Deserialize<'a>>(data: Option<&'a RawValue>) -> Option<T> {
    serde_json::from_str(data?.get()).ok()
}

impl HyperliquidAdapter {
    fn trades(&mut self, trades: &[WsTrade<'_>]) {
        for t in trades {
            let id = self.instrument(t.coin, t.time);
            let hdr = self.header::<Trade>(id, t.time, RecordFlags::NONE);
            self.out.push(MarketEvent::Trade(Trade {
                hdr,
                price: t.px.0,
                qty: t.sz.0,
                trade_ref: t.tid,
                aggressor: match t.side {
                    WireSide::Buy => Side::Bid,
                    WireSide::Sell => Side::Ask,
                },
                _pad: [0u8; 7],
            }));
        }
    }

    /// Each push replaces the whole visible book, so it emits `Clear` on both
    /// sides and every level as `New`, flagged `SNAPSHOT`. The wire has no
    /// sequence number, so a diff against the previous push could not notice a
    /// lost frame and would keep the damage; a full replacement heals on the
    /// next push and holds no per-coin state.
    fn book(&mut self, book: &WsBook<'_>) {
        let id = self.instrument(book.coin, book.time);
        let clear = LevelRow {
            price: Price::NULL.raw(),
            qty: 0,
            side: Side::None,
            depth: 0,
            action: LevelAction::Clear,
        };
        self.level(id, book.time, RecordFlags::NONE, clear);
        for (side, levels) in [(Side::Bid, &book.levels.0), (Side::Ask, &book.levels.1)] {
            // a zero size is an absent level; depth counts the levels kept
            let present = levels.iter().filter(|l| l.sz.0 != 0);
            for (depth, WsLevel { px, sz }) in present.enumerate() {
                let row = LevelRow {
                    price: px.0,
                    qty: sz.0,
                    side,
                    depth: u8::try_from(depth).unwrap_or(u8::MAX),
                    action: LevelAction::New,
                };
                self.level(id, book.time, RecordFlags::SNAPSHOT, row);
            }
        }
    }

    fn level(&mut self, id: u32, ts: EpochNs, flags: RecordFlags, row: LevelRow) {
        let hdr = self.header::<Level>(id, ts, flags);
        self.out.push(MarketEvent::Level(Level {
            hdr,
            price: row.price,
            qty: row.qty,
            num_deletes: 0,
            side: row.side,
            level: row.depth,
            action: row.action,
            _pad: [0u8; 3],
        }));
    }

    /// The context carries no time, so its records take the receive time.
    fn asset_ctx(&mut self, msg: &WsActiveAssetCtx<'_>) {
        let ts = EpochNs(self.cur_ts_recv);
        let id = self.instrument(msg.coin, ts);
        let Rate(funding) = msg.ctx.funding;
        let Qty(open_interest) = msg.ctx.open_interest;
        let null = Price::NULL.raw();
        self.stat(id, ts, StatType::FundingRate, null, 0, funding);
        self.stat(id, ts, StatType::OpenInterest, null, open_interest, 0);
        self.stat(id, ts, StatType::MarkPrice, msg.ctx.mark_px.0, 0, 0);
        self.stat(id, ts, StatType::IndexPrice, msg.ctx.oracle_px.0, 0, 0);
    }

    fn stat(&mut self, id: u32, ts: EpochNs, kind: StatType, price: i64, qty: u64, aux: i64) {
        let hdr = self.header::<Stats>(id, ts, RecordFlags::NONE);
        self.out.push(MarketEvent::Stats(Stats {
            hdr,
            price,
            qty,
            aux,
            stat_type: kind,
            category: 0,
            _pad: [0u8; 5],
        }));
    }

    /// Resolve a coin to its instrument, minting it and emitting its
    /// `Definition` on first sight. The venue has no numeric id, so the
    /// symbol index is the only key.
    fn instrument(&mut self, coin: Coin<'_>, ts: EpochNs) -> u32 {
        let raw_symbol = coin.padded();
        if let Some(id) = self.registry.resolve_symbol(self.venue, &raw_symbol) {
            return id.0;
        }
        let ev = DefinitionEvent {
            venue: self.venue,
            raw_symbol,
            board: [0u8; 4],
            venue_native_id: 0,
            ts: ts.0,
            asset_class: AssetClass::Perpetual,
            qty_scale: QTY_SCALE,
            lot_size: 0,
        };
        let id = self.registry.upsert(&ev);
        self.out.push(MarketEvent::Definition(
            InstrumentRegistry::definition_record(&ev, id),
        ));
        id.0
    }

    fn header<R: Record>(
        &mut self,
        instrument_id: u32,
        ts_event: EpochNs,
        flags: RecordFlags,
    ) -> RecordHeader {
        let flags = if self.frame_gap_suspect {
            self.gap_pending = false;
            flags.with(RecordFlags::GAP_SUSPECT)
        } else {
            flags
        };
        RecordHeader {
            // every record is at most 176 bytes, so the word count fits
            length: u8::try_from(size_of::<R>() / 4).unwrap_or(u8::MAX),
            rtype: R::RTYPE,
            venue: self.venue.0,
            instrument_id,
            ts_event: ts_event.0,
            ts_recv: self.cur_ts_recv,
            ts_egress: 0,
            venue_seq: self.cur_venue_seq,
            egress_seq: 0,
            flags,
            _pad: [0u8; 6],
        }
    }
}
