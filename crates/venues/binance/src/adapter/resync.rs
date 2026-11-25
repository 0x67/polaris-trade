//! Snapshot-plus-diff synchronization of the local book, following Binance's
//! documented procedure.
//!
//! Diffs buffer until a snapshot joins them. The snapshot is emitted as
//! `SnapshotBegin`, `Clear`, its levels flagged `SNAPSHOT`, `SnapshotEnd`,
//! then the buffered diffs it does not cover. An update id skip emits
//! `GapDetected` and waits for the next snapshot.

use md_events::{ControlKind, LevelAction, Price, RecordFlags, Side};

use super::{BinanceAdapter, LevelRow, Stamp};
use crate::wire::{RestDepth, WsLevel};

/// Diffs held while waiting for a snapshot. Past this, the oldest drop: a
/// fresh snapshot joins the newest diffs, and a stale one is refused anyway.
const MAX_BUFFERED: usize = 8192;

/// Where the local book stands against the update id sequence.
pub(super) enum Sync {
    /// No snapshot has joined the stream yet, or a gap discarded the book.
    Awaiting,
    /// The book holds every update through `last`.
    Live { last: u64 },
}

/// One diff depth event, owned so it can wait in the buffer.
pub(super) struct Delta {
    pub stamp: Stamp,
    pub first: u64,
    pub last: u64,
    pub time: u64,
    pub bids: Vec<WsLevel>,
    pub asks: Vec<WsLevel>,
}

impl BinanceAdapter {
    pub(super) fn on_delta(&mut self, d: Delta) {
        match self.sync {
            Sync::Awaiting => self.buffer_delta(d),
            Sync::Live { last } if d.last <= last => {}
            Sync::Live { last } if d.first > last + 1 => {
                let missed = d.first - last - 1;
                tracing::warn!(
                    last_applied = last,
                    next_first = d.first,
                    missed,
                    "depth update ids skipped, waiting for a snapshot"
                );
                let id = self.instrument(d.time);
                self.control(d.stamp, id, d.time, ControlKind::GapDetected, missed);
                self.sync = Sync::Awaiting;
                self.buffer_delta(d);
            }
            Sync::Live { .. } => {
                self.emit_delta(&d);
                self.sync = Sync::Live { last: d.last };
            }
        }
    }

    /// Join `snap`, current to `last_update_id`, with the buffered diffs. One
    /// older than the oldest diff not yet covered is refused with the buffer
    /// kept, so the caller fetches another. Awaiting with no diff buffered, a
    /// snapshot has nothing to join and is refused. Live, a snapshot older than
    /// the book's update id is ignored so a late reply cannot roll the book
    /// back; any other replaces the book, and the diffs after it continue
    /// from its update id.
    pub(super) fn on_snapshot(&mut self, last_update_id: u64, snap: &RestDepth) {
        match self.sync {
            Sync::Live { last } if last_update_id < last => {
                tracing::debug!(last_update_id, last, "snapshot older than the book");
                return;
            }
            Sync::Awaiting if self.buffer.is_empty() => {
                tracing::debug!(last_update_id, "snapshot before any diff to join");
                return;
            }
            _ => {}
        }
        while self
            .buffer
            .front()
            .is_some_and(|d| d.last <= last_update_id)
        {
            self.buffer.pop_front();
        }
        if self
            .buffer
            .front()
            .is_some_and(|d| d.first > last_update_id + 1)
        {
            tracing::debug!(last_update_id, "snapshot older than the buffered diffs");
            return;
        }
        let stamp = self.stamp;
        let ts = stamp.ts_recv;
        let id = self.instrument(ts);
        self.control(stamp, id, ts, ControlKind::SnapshotBegin, last_update_id);
        let clear = LevelRow {
            price: Price::NULL.raw(),
            qty: 0,
            side: Side::None,
            depth: 0,
            action: LevelAction::Clear,
        };
        self.level(stamp, id, ts, RecordFlags::NONE, clear);
        for (side, rows) in [(Side::Bid, &snap.bids), (Side::Ask, &snap.asks)] {
            // a zero size is an absent level; depth counts the levels kept and
            // saturates at 255, the width of `Level::level`
            let present = rows.iter().filter(|WsLevel(_, qty)| qty.0 != 0);
            for (depth, WsLevel(px, qty)) in present.enumerate() {
                let row = LevelRow {
                    price: px.0,
                    qty: qty.0,
                    side,
                    depth: u8::try_from(depth).unwrap_or(u8::MAX),
                    action: LevelAction::New,
                };
                self.level(stamp, id, ts, RecordFlags::SNAPSHOT, row);
            }
        }
        self.control(stamp, id, ts, ControlKind::SnapshotEnd, last_update_id);
        self.sync = Sync::Live {
            last: last_update_id,
        };
        let held = std::mem::take(&mut self.buffer);
        tracing::info!(last_update_id, replayed = held.len(), "snapshot applied");
        for d in held {
            self.on_delta(d);
        }
    }

    fn buffer_delta(&mut self, d: Delta) {
        // the diff keeps its frame's skip flag; the stream's pending skip
        // stays set until a frame emits a record, since a snapshot can drop it
        if self.buffer.len() == MAX_BUFFERED {
            self.buffer.pop_front();
        }
        self.buffer.push_back(d);
    }

    /// A level set to zero is a removal; any other size replaces the level.
    fn emit_delta(&mut self, d: &Delta) {
        let id = self.instrument(d.time);
        for (side, rows) in [(Side::Bid, &d.bids), (Side::Ask, &d.asks)] {
            for WsLevel(px, qty) in rows {
                let row = LevelRow {
                    price: px.0,
                    qty: qty.0,
                    side,
                    depth: 0,
                    action: if qty.0 == 0 {
                        LevelAction::Delete
                    } else {
                        LevelAction::Change
                    },
                };
                self.level(d.stamp, id, d.time, RecordFlags::NONE, row);
            }
        }
    }
}
