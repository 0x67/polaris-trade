//! The Binance Spot [`VenueAdapter`]: JSON frames in, normalized records out.

use core::mem::size_of;
use std::collections::VecDeque;

use md_events::{
    AssetClass, Control, ControlKind, Level, LevelAction, Record, RecordFlags, RecordHeader, Side,
    Trade, rtype,
};
use md_pipeline::{
    AdapterStats, ClosureModel, Coverage, CoverageMatrix, DefinitionEvent, InstrumentRegistry,
    MarketEvent, RollModel, SessionModel, StreamId, VenueAdapter, VenueId,
};
use thiserror::Error;

use crate::wire::{EventKind, QTY_SCALE, RestDepth, WsDepth, WsTrade};

mod resync;

use resync::{Delta, Sync};

/// Ingest stream of the websocket market data: `depthUpdate` and `trade`
/// events of the one symbol, as raw (not combined-stream) payloads.
pub const MARKET_STREAM: StreamId = StreamId(0);

/// Ingest stream of REST depth snapshot bodies, fetched by the caller.
pub const SNAPSHOT_STREAM: StreamId = StreamId(1);

/// Coverage key of the `depthUpdate` event.
pub const KIND_DEPTH_UPDATE: u8 = b'd';
/// Coverage key of the `trade` event.
pub const KIND_TRADE: u8 = b't';
/// Coverage key of the REST depth snapshot.
pub const KIND_SNAPSHOT: u8 = b's';

/// Width of `Definition::raw_symbol`.
const SYMBOL_LEN: usize = 20;

/// The symbol an adapter serves, as Binance spells it in event payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Symbol {
    padded: [u8; SYMBOL_LEN],
    len: usize,
}

/// A symbol name that Binance Spot does not use.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("binance symbol must be 1 to 20 uppercase ASCII letters or digits: {0:?}")]
pub struct InvalidSymbol(pub String);

impl Symbol {
    /// Check `name` against the spelling of event payloads, for example
    /// `BTCUSDT`.
    ///
    /// # Errors
    ///
    /// [`InvalidSymbol`] when `name` is empty, longer than 20 bytes, or holds
    /// anything but uppercase ASCII letters and digits.
    pub fn new(name: &str) -> Result<Self, InvalidSymbol> {
        let valid = (1..=SYMBOL_LEN).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit());
        if !valid {
            return Err(InvalidSymbol(name.to_owned()));
        }
        let mut padded = [0u8; SYMBOL_LEN];
        padded[..name.len()].copy_from_slice(name.as_bytes());
        Ok(Symbol {
            padded,
            len: name.len(),
        })
    }

    fn matches(&self, wire: &str) -> bool {
        wire.as_bytes() == &self.padded[..self.len]
    }
}

/// Construction input for one [`BinanceAdapter`] session.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Venue id stamped on every record header and the minted instrument.
    pub venue: VenueId,
    /// The one symbol this adapter serves.
    pub symbol: Symbol,
}

/// Arrival data of the frame being decoded, stamped on every record it emits.
#[derive(Clone, Copy)]
struct Stamp {
    ts_recv: u64,
    venue_seq: u64,
    /// A `venue_seq` skip no record has carried yet.
    suspect: bool,
}

/// How one frame counts in [`AdapterStats`].
enum Outcome {
    Decoded,
    Unknown,
    Malformed,
}

/// The `Level` body fields a book row varies; the header is built per record.
#[derive(Clone, Copy)]
struct LevelRow {
    price: i64,
    qty: u64,
    side: Side,
    depth: u8,
    action: LevelAction,
}

/// Decodes Binance Spot depth, trades and depth snapshots of one symbol. The
/// instrument mints on the first record, with a `Definition` ahead of it.
pub struct BinanceAdapter {
    venue: VenueId,
    symbol: Symbol,
    registry: InstrumentRegistry,
    instrument: Option<u32>,
    out: Vec<MarketEvent>,
    stats: AdapterStats,
    sync: Sync,
    /// Diffs held while `sync` waits for a snapshot, oldest first.
    buffer: VecDeque<Delta>,
    last_seq: [u64; 2],
    /// A `venue_seq` skip per stream that no frame has carried yet, kept
    /// across dropped frames until a frame emits a record.
    gap_pending: [bool; 2],
    stamp: Stamp,
    /// The current frame emitted a record, so it carried the skip.
    gap_spent: bool,
}

impl VenueAdapter for BinanceAdapter {
    type Config = Config;

    fn new(config: Config) -> Self {
        BinanceAdapter {
            venue: config.venue,
            symbol: config.symbol,
            registry: InstrumentRegistry::new(),
            instrument: None,
            out: Vec::new(),
            stats: AdapterStats::default(),
            sync: Sync::Awaiting,
            buffer: VecDeque::new(),
            last_seq: [0; 2],
            gap_pending: [false; 2],
            stamp: Stamp {
                ts_recv: 0,
                venue_seq: 0,
                suspect: false,
            },
            gap_spent: false,
        }
    }

    fn streams() -> &'static [StreamId] {
        &[MARKET_STREAM, SNAPSHOT_STREAM]
    }

    fn coverage(stream: StreamId) -> CoverageMatrix {
        let mut m = CoverageMatrix::new();
        if stream == MARKET_STREAM {
            m.set(KIND_DEPTH_UPDATE, Coverage::BOOK_AND_DERIVED)
                .set(KIND_TRADE, Coverage::TAPE_ONLY);
        } else if stream == SNAPSHOT_STREAM {
            m.set(KIND_SNAPSHOT, Coverage::BOOK_AND_DERIVED);
        }
        m
    }

    /// `Level` from depth and snapshots, `Trade` from trades, `Control` for
    /// snapshot framing and gaps, and the minted `Definition`. The public
    /// feed has no order-level data, so there is no `Mbo`.
    fn record_types() -> &'static [u8] {
        &[
            rtype::LEVEL,
            rtype::TRADE,
            rtype::CONTROL,
            rtype::DEFINITION,
        ]
    }

    /// 24/7 spot market: the session rolls on UTC midnight and never closes.
    fn session(&self) -> SessionModel {
        SessionModel {
            roll: RollModel::Continuous,
            closure: ClosureModel::None,
        }
    }

    /// Decode one websocket text frame or one REST snapshot body. A frame that
    /// is not JSON, has the wrong shape, names another symbol or carries a
    /// value outside its fixed-point range counts as malformed and changes
    /// nothing; an unlisted event type counts as unknown.
    #[inline]
    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        let lane = if stream == MARKET_STREAM {
            0
        } else if stream == SNAPSHOT_STREAM {
            1
        } else {
            return;
        };
        self.gap_pending[lane] |= self.last_seq[lane] != 0 && venue_seq != self.last_seq[lane] + 1;
        self.last_seq[lane] = venue_seq;
        self.stamp = Stamp {
            ts_recv,
            venue_seq,
            suspect: self.gap_pending[lane],
        };
        self.gap_spent = false;
        let outcome = if lane == 0 {
            self.market_frame(payload)
        } else {
            self.snapshot_frame(payload)
        };
        match outcome {
            Outcome::Decoded => self.stats.decoded += 1,
            Outcome::Unknown => self.stats.unknown_kind += 1,
            Outcome::Malformed => self.stats.malformed += 1,
        }
        if self.gap_spent {
            self.gap_pending[lane] = false;
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

impl BinanceAdapter {
    /// Whether the adapter holds diffs back for lack of a snapshot. The caller
    /// fetches a depth snapshot and feeds it on [`SNAPSHOT_STREAM`] while this
    /// is true: once the first diff is buffered, after a gap, and after a
    /// snapshot too old to join the buffered diffs. It stays false until a
    /// diff is buffered, because a snapshot fetched earlier cannot be checked
    /// against the stream.
    pub fn needs_snapshot(&self) -> bool {
        matches!(self.sync, Sync::Awaiting) && !self.buffer.is_empty()
    }

    fn market_frame(&mut self, payload: &[u8]) -> Outcome {
        let Ok(EventKind { kind }) = serde_json::from_slice(payload) else {
            return Outcome::Malformed;
        };
        match kind {
            Some("depthUpdate") => self.depth_frame(payload),
            Some("trade") => self.trade_frame(payload),
            _ => Outcome::Unknown,
        }
    }

    fn depth_frame(&mut self, payload: &[u8]) -> Outcome {
        let Ok(d) = serde_json::from_slice::<WsDepth<'_>>(payload) else {
            return Outcome::Malformed;
        };
        if !self.symbol.matches(d.symbol) || d.first > d.last {
            return Outcome::Malformed;
        }
        self.on_delta(Delta {
            stamp: self.stamp,
            first: d.first,
            last: d.last,
            time: d.time.0,
            bids: d.bids,
            asks: d.asks,
        });
        Outcome::Decoded
    }

    fn trade_frame(&mut self, payload: &[u8]) -> Outcome {
        let Ok(t) = serde_json::from_slice::<WsTrade<'_>>(payload) else {
            return Outcome::Malformed;
        };
        if !self.symbol.matches(t.symbol) {
            return Outcome::Malformed;
        }
        let id = self.instrument(t.time.0);
        let hdr = self.header::<Trade>(self.stamp, id, t.time.0, RecordFlags::NONE);
        self.out.push(MarketEvent::Trade(Trade {
            hdr,
            price: t.price.0,
            qty: t.qty.0,
            trade_ref: t.id,
            aggressor: if t.buyer_is_maker {
                Side::Ask
            } else {
                Side::Bid
            },
            _pad: [0u8; 7],
        }));
        Outcome::Decoded
    }

    fn snapshot_frame(&mut self, payload: &[u8]) -> Outcome {
        let Ok(snap) = serde_json::from_slice::<RestDepth>(payload) else {
            return Outcome::Malformed;
        };
        let Some(last_update_id) = snap.last_update_id else {
            return Outcome::Unknown;
        };
        self.on_snapshot(last_update_id, &snap);
        Outcome::Decoded
    }

    fn level(&mut self, stamp: Stamp, id: u32, ts: u64, flags: RecordFlags, row: LevelRow) {
        let hdr = self.header::<Level>(stamp, id, ts, flags);
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

    fn control(&mut self, stamp: Stamp, id: u32, ts: u64, kind: ControlKind, aux: u64) {
        let hdr = self.header::<Control>(stamp, id, ts, RecordFlags::NONE);
        self.out.push(MarketEvent::Control(Control {
            hdr,
            aux,
            kind,
            _pad: [0u8; 7],
        }));
    }

    /// The instrument of the served symbol, minted with its `Definition` on
    /// first use.
    fn instrument(&mut self, ts: u64) -> u32 {
        if let Some(id) = self.instrument {
            return id;
        }
        let ev = DefinitionEvent {
            venue: self.venue,
            raw_symbol: self.symbol.padded,
            board: [0u8; 4],
            venue_native_id: 0,
            ts,
            asset_class: AssetClass::Spot,
            qty_scale: QTY_SCALE,
            lot_size: 0,
        };
        let id = self.registry.upsert(&ev);
        self.out.push(MarketEvent::Definition(
            InstrumentRegistry::definition_record(&ev, id),
        ));
        self.instrument = Some(id.0);
        id.0
    }

    /// Header of a non-`Definition` record. Building one spends the frame's
    /// pending `venue_seq` skip.
    fn header<R: Record>(
        &mut self,
        stamp: Stamp,
        instrument_id: u32,
        ts_event: u64,
        flags: RecordFlags,
    ) -> RecordHeader {
        self.gap_spent = true;
        RecordHeader {
            // every record is at most 176 bytes, so the word count fits
            length: u8::try_from(size_of::<R>() / 4).unwrap_or(u8::MAX),
            rtype: R::RTYPE,
            venue: self.venue.0,
            instrument_id,
            ts_event,
            ts_recv: stamp.ts_recv,
            ts_egress: 0,
            venue_seq: stamp.venue_seq,
            egress_seq: 0,
            flags: if stamp.suspect {
                flags.with(RecordFlags::GAP_SUSPECT)
            } else {
                flags
            },
            _pad: [0u8; 6],
        }
    }
}
