//! Multi-resolution OHLCV bar engine. Bars close on an event-time watermark:
//! the max `ts_event` seen across the stream drives closes, so a bar whose
//! window elapsed closes even with no following trade, and a close can fire on
//! a foreign instrument's event. Never wall clock, so replay is deterministic.
//! Trade-break reversal is keyed by `break_ref` against per-trade contribution
//! history retained for every still-open bar; an already-closed, already-emitted
//! bar is immutable (a correction would live on a later stream, out of scope
//! here).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use md_events::{Ohlcv, RecordFlags, RecordHeader, Trade, TradeBreak, rtype};

use crate::policy::PurposeSet;

/// One bucket width in nanoseconds. Multiple resolutions run independently
/// over the same trade stream (e.g. 1-second and 1-minute bars at once).
pub type ResolutionNanos = u64;

/// In-progress bar for one (instrument, resolution, bucket). Owns its
/// `instrument`/`venue`/`resolution` because a close can fire on a foreign
/// instrument's event, so the closed record cannot borrow the triggering
/// header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Bar {
    bucket_start: u64,
    resolution: ResolutionNanos,
    instrument: u32,
    venue: u16,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    value: i128,
    trades: u32,
}

impl Bar {
    fn open_at(
        bucket_start: u64,
        resolution: ResolutionNanos,
        instrument: u32,
        venue: u16,
        price: i64,
    ) -> Bar {
        Bar {
            bucket_start,
            resolution,
            instrument,
            venue,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0,
            value: 0,
            trades: 0,
        }
    }
}

/// One trade's contribution to every resolution's bar, retained so a later
/// `TradeBreak` can find and reverse exactly what that print added. Retired
/// from `history` once every bucket it fed has closed naturally (see
/// `OhlcvEngine::by_bucket`), so a full session does not leak one entry per
/// trade forever.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TradeMemo {
    instrument: u32,
    price: i64,
    qty: u64,
    buckets: Vec<(ResolutionNanos, u64)>,
    /// Count of buckets in `buckets` still open. Decremented at each of that
    /// trade's buckets closing; the memo is pruned from `history` at 0.
    open_buckets: u8,
}

fn bucket_start(ts_event: u64, resolution: ResolutionNanos) -> u64 {
    debug_assert!(resolution > 0, "resolution must be positive");
    ts_event - (ts_event % resolution)
}

/// Build the closed record from bar-owned identity. `ts_event` is the interval
/// open (`bucket_start`); `ts_recv` is the event-time watermark at close. The
/// header is built directly, not borrowed from a trigger, so a foreign-event
/// close still stamps this bar's own instrument/venue.
fn finish_bar(bar: Bar, close_ts: u64) -> Ohlcv {
    Ohlcv {
        hdr: RecordHeader {
            length: (core::mem::size_of::<Ohlcv>() / 4) as u8,
            rtype: rtype::OHLCV,
            venue: bar.venue,
            instrument_id: bar.instrument,
            ts_event: bar.bucket_start,
            ts_recv: close_ts,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        // wider accumulator avoids overflow mid-bar; realistic notional*qty
        // fits i64 at bar close
        value: bar.value as i64,
        resolution: bar.resolution,
        trades: bar.trades,
        _pad: [0u8; 4],
    }
}

/// Multi-resolution OHLCV bars from the normalized trade stream. Only trades
/// whose board purposes include `OHLCV` feed bars (board-to-purpose routing).
/// Closes are driven by a monotonic event-time watermark; `open_boundaries`
/// indexes open bars by close time so a close touches only due bars.
#[derive(Default)]
pub struct OhlcvEngine {
    resolutions: Vec<ResolutionNanos>,
    /// Max `ts_event` seen; only rises. A bar closes when its boundary is at or
    /// below it.
    watermark: u64,
    bars: HashMap<(u32, ResolutionNanos), Bar>,
    /// Open bars keyed by close boundary (`bucket_start + resolution`) so
    /// `advance` pops only due bars instead of scanning `bars`. Inner set holds
    /// every `(resolution, instrument)` sharing that boundary, iterated in order
    /// for a deterministic emit sequence.
    open_boundaries: BTreeMap<u64, BTreeSet<(ResolutionNanos, u32)>>,
    history: HashMap<u64, TradeMemo>,
    /// Reverse index: which trades fed a given (instrument, resolution,
    /// bucket start), so a bucket close can find and decrement them without
    /// scanning `history`.
    by_bucket: HashMap<(u32, ResolutionNanos, u64), Vec<u64>>,
}

impl OhlcvEngine {
    /// New engine tracking the given bucket widths (nanoseconds).
    pub fn new(resolutions: Vec<ResolutionNanos>) -> Self {
        OhlcvEngine {
            resolutions,
            watermark: 0,
            bars: HashMap::new(),
            open_boundaries: BTreeMap::new(),
            history: HashMap::new(),
            by_bucket: HashMap::new(),
        }
    }

    /// Raise the watermark to `now` and close every bar whose boundary is at or
    /// below it. Bars that had no trade never opened, so they never emit.
    #[inline]
    pub fn advance(&mut self, now: u64) -> Vec<Ohlcv> {
        if now > self.watermark {
            self.watermark = now;
        }
        self.close_boundaries_through(self.watermark)
    }

    /// Close every still-open bar regardless of the watermark (stream end), so
    /// the last window of each traded instrument is not lost.
    pub fn flush_all(&mut self) -> Vec<Ohlcv> {
        self.close_boundaries_through(u64::MAX)
    }

    /// Pop boundaries at or below `through`, closing each `(resolution,
    /// instrument)` bar in set order; emit sequence is ascending boundary then
    /// `(resolution, instrument)`. `ts_recv` on every closed record is the
    /// current watermark.
    fn close_boundaries_through(&mut self, through: u64) -> Vec<Ohlcv> {
        let close_ts = self.watermark;
        let mut closed = Vec::new();
        while let Some(&boundary) = self.open_boundaries.keys().next() {
            if boundary > through {
                break;
            }
            let set = self
                .open_boundaries
                .remove(&boundary)
                .expect("boundary key just read");
            for (res, inst) in set {
                if let Some(bar) = self.bars.remove(&(inst, res)) {
                    self.prune_bucket(inst, res, bar.bucket_start);
                    closed.push(finish_bar(bar, close_ts));
                }
            }
        }
        closed
    }

    /// Decrement the trade memos feeding a closed bucket, pruning any memo whose
    /// last open bucket just closed. Keeps `history` bounded over a session.
    fn prune_bucket(&mut self, inst: u32, res: ResolutionNanos, bucket_start: u64) {
        if let Some(refs) = self.by_bucket.remove(&(inst, res, bucket_start)) {
            for trade_ref in refs {
                if let Some(memo) = self.history.get_mut(&trade_ref) {
                    memo.open_buckets = memo.open_buckets.saturating_sub(1);
                    if memo.open_buckets == 0 {
                        self.history.remove(&trade_ref);
                    }
                }
            }
        }
    }

    /// Apply one trade print. Advances the watermark to the trade's `ts_event`
    /// first (closing any bucket that elapsed), then folds the trade into its
    /// bucket for every resolution, opening a fresh bar where needed. Returns
    /// the bars this event closed.
    #[inline]
    pub fn apply_trade(&mut self, ev: &Trade, purposes: PurposeSet) -> Vec<Ohlcv> {
        if !purposes.contains(PurposeSet::OHLCV) {
            return Vec::new();
        }
        let closed = self.advance(ev.hdr.ts_event);
        let inst = ev.hdr.instrument_id;
        let venue = ev.hdr.venue;
        let mut buckets = Vec::with_capacity(self.resolutions.len());
        for &res in &self.resolutions {
            let start = bucket_start(ev.hdr.ts_event, res);
            let key = (inst, res);
            // direct field access (not a `&mut self` helper): the loop holds an
            // immutable borrow of `self.resolutions`, so a method taking `&mut
            // self` would conflict. register the close boundary only when the
            // bar is first opened.
            if !self.bars.contains_key(&key) {
                self.open_boundaries
                    .entry(start + res)
                    .or_default()
                    .insert((res, inst));
            }
            let bar = self
                .bars
                .entry(key)
                .or_insert_with(|| Bar::open_at(start, res, inst, venue, ev.price));
            bar.high = bar.high.max(ev.price);
            bar.low = bar.low.min(ev.price);
            bar.close = ev.price;
            bar.volume += ev.qty;
            bar.value += ev.price as i128 * ev.qty as i128;
            bar.trades += 1;
            buckets.push((res, start));
        }
        for &(res, start) in &buckets {
            self.by_bucket
                .entry((inst, res, start))
                .or_default()
                .push(ev.trade_ref);
        }
        let open_buckets = buckets.len() as u8;
        self.history.insert(
            ev.trade_ref,
            TradeMemo {
                instrument: inst,
                price: ev.price,
                qty: ev.qty,
                buckets,
                open_buckets,
            },
        );
        closed
    }

    /// Reverse a broken trade's volume/value/frequency from every
    /// still-open bar it fed; open/high/low/close stay untouched. A break for
    /// a trade whose bar already closed is a no-op: an emitted bar is
    /// append-only.
    pub fn apply_trade_break(&mut self, brk: &TradeBreak) {
        let Some(memo) = self.history.remove(&brk.break_ref) else {
            return;
        };
        for (res, start) in memo.buckets {
            let key = (memo.instrument, res);
            if let Some(bar) = self.bars.get_mut(&key)
                && bar.bucket_start == start
            {
                bar.volume = bar.volume.saturating_sub(memo.qty);
                bar.value -= memo.price as i128 * memo.qty as i128;
                bar.trades = bar.trades.saturating_sub(1);
            }
        }
    }

    /// Snapshot of the still-open bar for `(instrument, resolution)`, as
    /// `(open, high, low, close, volume, trades)`.
    pub fn open_bar(
        &self,
        instrument: u32,
        resolution: ResolutionNanos,
    ) -> Option<(i64, i64, i64, i64, u64, u32)> {
        self.bars
            .get(&(instrument, resolution))
            .map(|b| (b.open, b.high, b.low, b.close, b.volume, b.trades))
    }
}

#[cfg(test)]
mod tests {
    use md_events::{RecordFlags, Side};

    use super::*;
    use crate::policy::{BoardId, BoardPurposeMap};

    const SEC: ResolutionNanos = 1_000_000_000;
    const MIN: ResolutionNanos = 60 * SEC;

    fn hdr(inst: u32, ts_event: u64, venue_seq: u64) -> RecordHeader {
        RecordHeader {
            length: 0,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: inst,
            ts_event,
            ts_recv: ts_event,
            ts_egress: 0,
            venue_seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        }
    }

    fn trade(inst: u32, ts_event: u64, price: i64, qty: u64, trade_ref: u64) -> Trade {
        Trade {
            hdr: hdr(inst, ts_event, trade_ref),
            price,
            qty,
            aggressor: Side::Bid,
            trade_ref,
            _pad: [0u8; 7],
        }
    }

    // Trades in-bucket update high/low/close/volume, no close; a boundary-crossing
    // trade advances the watermark and closes the prior bucket. The closed record
    // carries the interval-open ts_event (bucket_start), its bucket width, the
    // watermark as ts_recv, and its own instrument.
    #[test]
    fn bucket_closes_on_event_time_crossing() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        assert!(
            e.apply_trade(&trade(1, 100, 1000, 5, 1), PurposeSet::OHLCV)
                .is_empty()
        );
        assert!(
            e.apply_trade(&trade(1, 200, 1010, 3, 2), PurposeSet::OHLCV)
                .is_empty()
        );
        let bar = e.open_bar(1, SEC).expect("bar open");
        assert_eq!(bar, (1000, 1010, 1000, 1010, 8, 2));

        // crosses into the next 1-second bucket
        let closed = e.apply_trade(&trade(1, SEC + 50, 990, 1, 3), PurposeSet::OHLCV);
        assert_eq!(closed.len(), 1);
        let c = closed[0];
        assert_eq!(
            (c.open, c.high, c.low, c.close, c.volume, c.trades),
            (1000, 1010, 1000, 1010, 8, 2)
        );
        assert_eq!(
            c.hdr.ts_event, 0,
            "closed record carries the interval-open time (bucket_start)"
        );
        assert_eq!(c.resolution, SEC, "closed record carries its bucket width");
        assert_eq!(c.hdr.ts_recv, SEC + 50, "ts_recv is the watermark at close");
        assert_eq!(c.hdr.instrument_id, 1);

        let next = e.open_bar(1, SEC).expect("next bucket open");
        assert_eq!(next, (990, 990, 990, 990, 1, 1));
    }

    // A bar closes once the watermark passes its boundary, even with no trade
    // following in that bucket.
    #[test]
    fn advance_closes_trade_less_bar() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        assert!(
            e.apply_trade(&trade(1, 100, 1000, 5, 1), PurposeSet::OHLCV)
                .is_empty()
        );
        // no further trade; event time advances past the [0, SEC) boundary
        let closed = e.advance(SEC + 10);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].hdr.ts_event, 0);
        assert_eq!(closed[0].resolution, SEC);
        assert_eq!(closed[0].volume, 5);
        assert!(e.open_bar(1, SEC).is_none(), "closed bar removed");
    }

    // A close triggered by a later event (e.g. a foreign instrument's traffic)
    // stamps the closing bar's own instrument/venue, not the triggering event's.
    #[test]
    fn foreign_event_closes_with_own_identity() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        // instrument 7 opens a bar in [0, SEC)
        e.apply_trade(&trade(7, 100, 500, 2, 1), PurposeSet::OHLCV);
        // a later event at SEC+5 advances the watermark and closes it
        let closed = e.advance(SEC + 5);
        assert_eq!(closed.len(), 1);
        assert_eq!(
            closed[0].hdr.instrument_id, 7,
            "bar keeps its own instrument"
        );
        assert_eq!(closed[0].hdr.venue, 1, "bar keeps its own venue");
    }

    // A window with no trades between two traded windows produces no bar: only
    // buckets that received a print ever emit.
    #[test]
    fn empty_bucket_silent() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        e.apply_trade(&trade(1, 100, 1000, 1, 1), PurposeSet::OHLCV); // [0, SEC)
        // jump two buckets ahead: [SEC, 2*SEC) is never traded
        let closed = e.apply_trade(&trade(1, 2 * SEC + 100, 1005, 1, 2), PurposeSet::OHLCV);
        assert_eq!(closed.len(), 1, "only the traded [0, SEC) bucket closes");
        assert_eq!(closed[0].hdr.ts_event, 0);
    }

    // flush_all closes every still-open bar (one per resolution) at stream end,
    // in ascending-boundary order.
    #[test]
    fn flush_all_emits_last_bar() {
        let mut e = OhlcvEngine::new(vec![SEC, MIN]);
        e.apply_trade(&trade(1, 100, 1000, 3, 1), PurposeSet::OHLCV);
        let closed = e.flush_all();
        assert_eq!(closed.len(), 2, "the open 1s and 1min bars both flush");
        assert_eq!(closed[0].resolution, SEC, "smaller boundary emits first");
        assert_eq!(closed[1].resolution, MIN);
        assert!(e.open_bar(1, SEC).is_none());
        assert!(e.flush_all().is_empty(), "nothing left after flush");
    }

    // The watermark only rises: an advance to an earlier time closes nothing and
    // leaves an open bar untouched.
    #[test]
    fn monotonic_watermark() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        // watermark -> 5*SEC, bar open in [5*SEC, 6*SEC)
        e.apply_trade(&trade(1, 5 * SEC, 1000, 1, 1), PurposeSet::OHLCV);
        let closed = e.advance(SEC);
        assert!(closed.is_empty(), "a lower advance closes nothing");
        assert!(e.open_bar(1, SEC).is_some(), "open bar untouched");
    }

    // A trade break reverses volume/value/frequency on the still-open bar it
    // fed, but leaves open/high/low/close untouched.
    #[test]
    fn trade_break_reverses_aggregates_only() {
        let mut e = OhlcvEngine::new(vec![SEC]);
        e.apply_trade(&trade(1, 100, 1000, 5, 11), PurposeSet::OHLCV);
        e.apply_trade(&trade(1, 150, 1020, 3, 12), PurposeSet::OHLCV);
        let before = e.open_bar(1, SEC).unwrap();
        assert_eq!(before, (1000, 1020, 1000, 1020, 8, 2));

        e.apply_trade_break(&TradeBreak {
            hdr: hdr(1, 150, 0),
            break_ref: 12,
            ref_kind: md_events::BreakRefKind::MatchNumber,
            _pad: [0u8; 7],
        });
        let after = e.open_bar(1, SEC).unwrap();
        assert_eq!(after.4, 5, "volume reversed by the broken trade's qty");
        assert_eq!(after.5, 1, "trade count reversed");
        assert_eq!(
            (after.0, after.1, after.2, after.3),
            (1000, 1020, 1000, 1020),
            "OHLC untouched by a break"
        );

        // a repeat break for the same ref is a no-op (history entry consumed)
        e.apply_trade_break(&TradeBreak {
            hdr: hdr(1, 150, 0),
            break_ref: 12,
            ref_kind: md_events::BreakRefKind::MatchNumber,
            _pad: [0u8; 7],
        });
        assert_eq!(e.open_bar(1, SEC).unwrap().4, 5, "second break is a no-op");
    }

    // One trade feeds every tracked resolution independently; crossing the
    // 1s boundary closes only the 1s bar, the 1min bar stays open.
    #[test]
    fn resolutions_are_independent() {
        let mut e = OhlcvEngine::new(vec![SEC, MIN]);
        e.apply_trade(&trade(1, 0, 100, 1, 1), PurposeSet::OHLCV);
        let closed = e.apply_trade(&trade(1, SEC, 101, 1, 2), PurposeSet::OHLCV);
        assert_eq!(closed.len(), 1, "only the 1-second bucket crossed");
        assert!(e.open_bar(1, MIN).is_some(), "1-minute bar still open");
    }

    // Board-to-purpose routing: reproduces the legacy "only one board feeds
    // OHLCV" rule as data, not a string comparison.
    #[test]
    fn only_one_board_feeds_ohlcv() {
        let mut m = BoardPurposeMap::new(PurposeSet::NONE);
        let board = |s: &str| {
            let mut b = [0u8; 4];
            b[..s.len()].copy_from_slice(s.as_bytes());
            BoardId(b)
        };
        m.set(board("MAIN"), PurposeSet::OHLCV);
        m.set(board("AUX1"), PurposeSet::TAPE);

        let mut e = OhlcvEngine::new(vec![SEC]);
        let main_out = e.apply_trade(&trade(1, 0, 100, 5, 1), m.purposes(board("MAIN")));
        assert!(
            main_out.is_empty(),
            "first print in a fresh bucket, nothing closed yet"
        );
        assert!(e.open_bar(1, SEC).is_some(), "MAIN feeds the bar");

        let aux1_out = e.apply_trade(&trade(2, 0, 100, 5, 2), m.purposes(board("AUX1")));
        assert!(aux1_out.is_empty());
        assert!(
            e.open_bar(2, SEC).is_none(),
            "AUX1 has no OHLCV purpose, no bar"
        );
    }

    // Full-day-scale fixture: history must retire a trade's memo once every
    // resolution's bucket it fed has closed, not accumulate one entry per
    // trade for the life of the session.
    #[test]
    fn ohlcv_memo_pruned() {
        let mut e = OhlcvEngine::new(vec![SEC, MIN]);
        let total = 5_000u64;
        for i in 0..total {
            // one trade per second: the 1s bucket closes on every print, the
            // 1min bucket closes only once every 60 prints
            e.apply_trade(
                &trade(1, i * SEC, 1_000 + i as i64, 1, i + 1),
                PurposeSet::OHLCV,
            );
        }
        assert!(
            e.history.len() < 100,
            "history must stay bounded after buckets close, got {} entries for {} trades",
            e.history.len(),
            total
        );
    }
}
