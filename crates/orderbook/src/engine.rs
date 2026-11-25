//! Venue-agnostic order book. Applies normalized order and level events to a
//! per-(instrument, side) book through a venue policy. The mechanics carry no
//! venue conditionals; all venue behavior is a policy hook.

use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
};

use md_events::{Action, Level, LevelAction, Mbo, RecordFlags, Side, TradingState};
use rustc_hash::FxBuildHasher;

use crate::{
    level::{LevelBook, SoaLevelBook},
    policy::{Gate, VenuePolicy},
};

type OrderKey = (u32, Side, u64);

/// Intrusive chain link: pooled slot index plus one, `None` at a chain end.
/// The NonZero +1 encoding keeps a link 4 bytes instead of the 16 an
/// `Option<usize>` costs, so a whole `Order` is 32 bytes and the unlink
/// pointer chase touches half the cache lines. u32 caps the pool at 4.29e9
/// concurrently resting orders; `link` checks the conversion loudly.
type SlotLink = Option<core::num::NonZeroU32>;

#[inline]
fn link(slot: usize) -> SlotLink {
    let raw = u32::try_from(slot + 1).expect("order slot pool exceeds u32 range");
    // SAFETY-free invariant: slot + 1 >= 1, so the NonZero constructor holds.
    core::num::NonZeroU32::new(raw)
}

#[inline]
fn slot_of(link: SlotLink) -> Option<usize> {
    link.map(|l| l.get() as usize - 1)
}

#[inline]
fn link_opt(slot: Option<usize>) -> SlotLink {
    slot.and_then(link)
}

/// One resting order in the pooled slot store. `seq` is arrival order for time
/// priority; a modify takes a fresh `seq` and moves to the level tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Order {
    price: i64,
    qty: u64,
    seq: u64,
    prev: SlotLink,
    next: SlotLink,
}

/// Read-only resting order state exposed to tests and parity tooling.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditOrderState {
    pub price: i64,
    pub qty: u64,
}

/// Read-only aggregate level state exposed to tests and parity tooling.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditLevelState {
    pub qty: u64,
    pub count: u32,
}

/// Outcome of applying one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyResult {
    /// The book was updated.
    Applied,
    /// The event referenced an order the book does not hold.
    UnknownOrder,
    /// The policy refused the event for the instrument's state.
    Dropped,
}

/// What applying an event produced, for the caller to route downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BookOutcome {
    /// Book-side result.
    pub result: ApplyResult,
    /// Eligible for tape/volume/OHLCV (false for non-printable legs).
    pub tape: bool,
    /// Derived from a snapshot, not a live increment.
    pub snapshot: bool,
    /// `Gate::Flag` applied the event but marked it suspect. Caller ORs
    /// `RecordFlags::SUSPECT` onto the header when true.
    pub suspect: bool,
}

/// Read-only depth source for derived-view computation: the top
/// `(price, qty, count)` rows per side, highest bid / lowest ask first.
/// `BookEngine` implements it so `DeriveEngine` reads mbp-10 depth straight off
/// the live book instead of maintaining a second aggregation of the same
/// ladder.
pub trait BookDepth {
    /// Fill `out` with the top depth rows for one side, returning the number
    /// written (0 when the instrument/side has no levels). Never writes past
    /// `out.len()`.
    fn top_levels(&self, instrument: u32, side: Side, out: &mut [(i64, u64, u32)]) -> usize;
}

/// The book. Keyed by the header's `instrument_id` (a `u32`) so it depends only
/// on md-events; the pipeline's minted id wraps the same value.
pub struct BookEngine<P: VenuePolicy, L: LevelBook = SoaLevelBook, S: BuildHasher = FxBuildHasher> {
    policy: P,
    key_to_slot: HashMap<OrderKey, usize, S>,
    // Dense pool: a freed slot keeps stale bytes and is reachable only through
    // `free_slots`; every read goes through `key_to_slot` or a live chain, so
    // occupancy needs no Option tag (32-byte entries, two per cache line).
    slots: Vec<Order>,
    free_slots: Vec<usize>,
    levels: HashMap<(u32, Side), L, S>,
    states: HashMap<u32, TradingState, S>,
    snapshots: HashSet<u32, S>,
    seq: u64,
}

impl<P: VenuePolicy> BookEngine<P, SoaLevelBook, FxBuildHasher> {
    /// New engine using the benchmark-selected default level container and
    /// hasher. Existing callers need no explicit type parameters.
    pub fn new(policy: P) -> Self {
        Self::with_hasher(policy, FxBuildHasher)
    }
}

impl<P, L, S> BookEngine<P, L, S>
where
    P: VenuePolicy,
    L: LevelBook,
    S: BuildHasher + Clone,
{
    /// New engine with an explicit map hasher, used by callers and the
    /// benchmark matrix to compare hash strategies without changing mechanics.
    pub fn with_hasher(policy: P, hasher: S) -> Self {
        BookEngine {
            policy,
            key_to_slot: HashMap::with_hasher(hasher.clone()),
            slots: Vec::new(),
            free_slots: Vec::new(),
            levels: HashMap::with_hasher(hasher.clone()),
            states: HashMap::with_hasher(hasher.clone()),
            snapshots: HashSet::with_hasher(hasher),
            seq: 0,
        }
    }

    /// Borrow the policy.
    pub fn policy(&self) -> &P {
        &self.policy
    }

    /// Set an instrument's trading state (fed by the status machine). Absent
    /// instruments default to `Continuous`.
    pub fn set_state(&mut self, instrument: u32, state: TradingState) {
        self.states.insert(instrument, state);
    }

    /// Enter snapshot mode for an instrument; records apply as snapshot-derived.
    pub fn begin_snapshot(&mut self, instrument: u32) {
        self.snapshots.insert(instrument);
    }

    /// Complete live handoff; subsequent records are live increments.
    pub fn end_snapshot(&mut self, instrument: u32) {
        self.snapshots.remove(&instrument);
    }

    /// Warm the lines an imminent [`apply_mbo`](Self::apply_mbo) for `ev`
    /// will touch: the key's slot-map bucket, the pooled order, and the
    /// level-map entry with one end of its ladder. Read-only and safe at any
    /// distance; sessions issue it a few records ahead so these misses
    /// resolve behind other routing work instead of stalling the apply.
    pub fn prefetch_mbo(&self, ev: &Mbo) {
        let inst = ev.hdr.instrument_id;
        if let Some(&slot) = self.key_to_slot.get(&(inst, ev.side, ev.order_id)) {
            core::hint::black_box(self.slots[slot].seq);
        }
        if let Some(level) = self.levels.get(&(inst, ev.side)) {
            core::hint::black_box(level.best(ev.side));
        }
    }

    /// Apply one order-level event.
    pub fn apply_mbo(&mut self, ev: &Mbo) -> BookOutcome {
        let inst = ev.hdr.instrument_id;
        let snapshot = self.snapshots.contains(&inst);

        if let Action::Reset = ev.action {
            return self.clear(inst);
        }

        let state = self
            .states
            .get(&inst)
            .copied()
            .unwrap_or(TradingState::Continuous);
        // Flag applies through the SAME logic as Accept, just marked suspect;
        // Drop returns early, never touching the book.
        let suspect = match self.policy.book_gate(state, ev.action) {
            Gate::Drop => {
                return BookOutcome {
                    result: ApplyResult::Dropped,
                    tape: false,
                    snapshot,
                    suspect: false,
                };
            }
            Gate::Flag => true,
            Gate::Accept => false,
        };

        let key = (inst, ev.side, ev.order_id);
        let result = match ev.action {
            Action::Add => {
                self.remove_existing(key);
                let seq = self.next_seq();
                let slot = self.allocate(Order {
                    price: ev.price,
                    qty: ev.qty,
                    seq,
                    prev: None,
                    next: None,
                });
                self.key_to_slot.insert(key, slot);
                // Aggregate first so the level (and its endpoint slots) exists
                // before the chain append reads/writes head/tail on it.
                self.level_mut(inst, ev.side).add(ev.price, ev.qty);
                self.append_to_queue(key, slot);
                ApplyResult::Applied
            }
            Action::Modify => match self.key_to_slot.get(&key).copied() {
                Some(slot) => {
                    let old = self.slots[slot];
                    // Unlink off the old level while it still exists, then reduce
                    // may drop it. Re-add the new level before appending there.
                    self.unlink_from_queue(key, slot, old);
                    self.level_mut(inst, ev.side)
                        .reduce(old.price, old.qty, true);
                    let seq = self.next_seq();
                    self.slots[slot] = Order {
                        price: ev.price,
                        qty: ev.qty,
                        seq,
                        prev: None,
                        next: None,
                    };
                    self.level_mut(inst, ev.side).add(ev.price, ev.qty);
                    self.append_to_queue(key, slot);
                    ApplyResult::Applied
                }
                None => ApplyResult::UnknownOrder,
            },
            Action::Cancel | Action::Fill | Action::FillWithPrice => {
                match self.key_to_slot.get(&key).copied() {
                    Some(slot) => {
                        let old = self.slots[slot];
                        let dq = ev.qty.min(old.qty);
                        let remaining = old.qty - dq;
                        // Full removal: unlink the chain before reduce drops the
                        // level, so head/tail are read/patched while present.
                        if remaining == 0 {
                            self.unlink_from_queue(key, slot, old);
                        }
                        self.level_mut(inst, ev.side)
                            .reduce(old.price, dq, remaining == 0);
                        if remaining == 0 {
                            self.key_to_slot.remove(&key);
                            self.release(slot);
                        } else {
                            self.slots[slot].qty = remaining;
                        }
                        ApplyResult::Applied
                    }
                    None => ApplyResult::UnknownOrder,
                }
            }
            // off-book print: no book change
            Action::Trade => ApplyResult::Applied,
            Action::Reset => unreachable!("handled above"),
        };

        let printable = !ev.hdr.flags.contains(RecordFlags::NON_PRINTABLE);
        let tape = matches!(
            ev.action,
            Action::Fill | Action::FillWithPrice | Action::Trade
        ) && self.policy.tape_gate(printable, ev.action)
            && result == ApplyResult::Applied;
        BookOutcome {
            result,
            tape,
            snapshot,
            suspect,
        }
    }

    /// Apply one MBP-native level event. `LevelAction::Clear` empties the named
    /// side, levels and resting orders, or the instrument's whole book as
    /// [`clear`](Self::clear) does when `side` is `Side::None`.
    pub fn apply_level(&mut self, ev: &Level) -> BookOutcome {
        let inst = ev.hdr.instrument_id;
        let snapshot = self.snapshots.contains(&inst);
        match ev.action {
            LevelAction::New => self.level_mut(inst, ev.side).add(ev.price, ev.qty),
            LevelAction::Change => self.level_mut(inst, ev.side).set(ev.price, ev.qty),
            LevelAction::Delete => self.level_mut(inst, ev.side).set(ev.price, 0),
            LevelAction::Clear => return self.retire(inst, ev.side),
        }
        BookOutcome {
            result: ApplyResult::Applied,
            tape: false,
            snapshot,
            suspect: false,
        }
    }

    /// Clear an instrument's book atomically (bulk clear / snapshot recovery).
    /// The caller emits the Reset record.
    pub fn clear(&mut self, instrument: u32) -> BookOutcome {
        self.retire(instrument, Side::None)
    }

    // Drops the resting orders and levels of one side, or of both when `side`
    // is `Side::None`.
    fn retire(&mut self, instrument: u32, side: Side) -> BookOutcome {
        let hit = |i: u32, s: Side| i == instrument && (side == Side::None || s == side);
        let retired: Vec<(OrderKey, usize)> = self
            .key_to_slot
            .iter()
            .filter_map(|(&key, &slot)| hit(key.0, key.1).then_some((key, slot)))
            .collect();
        for (key, slot) in retired {
            self.key_to_slot.remove(&key);
            self.release(slot);
        }
        self.levels.retain(|&(i, s), _| !hit(i, s));
        BookOutcome {
            result: ApplyResult::Applied,
            tape: false,
            snapshot: self.snapshots.contains(&instrument),
            suspect: false,
        }
    }

    /// Best (price, aggregate qty) for a side.
    pub fn best(&self, instrument: u32, side: Side) -> Option<(i64, u64)> {
        self.levels
            .get(&(instrument, side))
            .and_then(|l| l.best(side))
    }

    /// Number of resting orders across all books.
    pub fn order_count(&self) -> usize {
        self.key_to_slot.len()
    }

    /// Read one resting order without exposing mutable book internals.
    #[cfg(any(test, feature = "testing"))]
    pub fn order_state(
        &self,
        instrument: u32,
        side: Side,
        order_id: u64,
    ) -> Option<AuditOrderState> {
        let slot = *self.key_to_slot.get(&(instrument, side, order_id))?;
        let order = self.slots[slot];
        Some(AuditOrderState {
            price: order.price,
            qty: order.qty,
        })
    }

    /// Read one aggregate price level and its resting-order count.
    #[cfg(any(test, feature = "testing"))]
    pub fn level_state(&self, instrument: u32, side: Side, price: i64) -> Option<AuditLevelState> {
        let level = self.levels.get(&(instrument, side))?;
        let qty = level.quantity_at(price)?;
        // No chain head => MBP-native level with no per-order queue; report
        // absent, matching the pre-fold behavior of a missing queue entry.
        let mut current = Some(level.queue_ends(price)?.0?);
        let mut count = 0u32;
        while let Some(slot) = current {
            count += 1;
            current = slot_of(self.slots[slot].next);
        }
        Some(AuditLevelState { qty, count })
    }

    /// Number of resting orders for one instrument.
    #[cfg(any(test, feature = "testing"))]
    pub fn instrument_order_count(&self, instrument: u32) -> usize {
        self.key_to_slot
            .keys()
            .filter(|(candidate, _, _)| *candidate == instrument)
            .count()
    }

    /// Zero-based time-priority position of an order at its price level.
    /// Traversal follows the intrusive chain and validates increasing arrival
    /// sequence, making requeue-on-modify observable to matching-parity users.
    pub fn queue_position(&self, instrument: u32, side: Side, order_id: u64) -> Option<usize> {
        let key = (instrument, side, order_id);
        let target_slot = *self.key_to_slot.get(&key)?;
        let target = self.slots[target_slot];
        let (head, _) = self
            .levels
            .get(&(instrument, side))?
            .queue_ends(target.price)?;
        let mut current = head;
        let mut position = 0;
        let mut prior_seq = 0;
        while let Some(slot) = current {
            let order = self.slots[slot];
            debug_assert!(order.seq > prior_seq);
            if slot == target_slot {
                return Some(position);
            }
            prior_seq = order.seq;
            current = slot_of(order.next);
            position += 1;
        }
        None
    }

    fn level_mut(&mut self, instrument: u32, side: Side) -> &mut L {
        self.levels.entry((instrument, side)).or_default()
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn allocate(&mut self, order: Order) -> usize {
        match self.free_slots.pop() {
            Some(slot) => {
                self.slots[slot] = order;
                slot
            }
            None => {
                let slot = self.slots.len();
                self.slots.push(order);
                slot
            }
        }
    }

    fn release(&mut self, slot: usize) {
        // Freed slots keep stale bytes; poison the arrival seq in debug so the
        // queue_position monotonic-seq assert trips on any use-after-release.
        #[cfg(debug_assertions)]
        {
            self.slots[slot].seq = 0;
        }
        self.free_slots.push(slot);
    }

    fn remove_existing(&mut self, key: OrderKey) {
        let Some(slot) = self.key_to_slot.remove(&key) else {
            return;
        };
        let old = self.slots[slot];
        self.unlink_from_queue(key, slot, old);
        self.level_mut(key.0, key.1)
            .reduce(old.price, old.qty, true);
        self.release(slot);
    }

    // Chain endpoints live in the level container (see level::LevelBook), keyed
    // by price alongside the aggregate, so no separate per-level queue map. The
    // level must already exist (add/reduce runs before these).
    fn append_to_queue(&mut self, key: OrderKey, slot: usize) {
        let order = self.slots[slot];
        let (head, tail) = self
            .levels
            .get(&(key.0, key.1))
            .and_then(|level| level.queue_ends(order.price))
            .expect("level present when appending to its chain");
        if let Some(tail_slot) = tail {
            let tail_order = &mut self.slots[tail_slot];
            debug_assert!(tail_order.seq < order.seq);
            tail_order.next = link(slot);
        }
        {
            let new_order = &mut self.slots[slot];
            new_order.prev = link_opt(tail);
            new_order.next = None;
        }
        self.levels
            .get_mut(&(key.0, key.1))
            .expect("level present when appending to its chain")
            .set_queue_ends(order.price, head.or(Some(slot)), Some(slot));
    }

    fn unlink_from_queue(&mut self, key: OrderKey, slot: usize, order: Order) {
        if let Some(prev_slot) = slot_of(order.prev) {
            self.slots[prev_slot].next = order.next;
        }
        if let Some(next_slot) = slot_of(order.next) {
            self.slots[next_slot].prev = order.prev;
        }

        // Patch endpoints in place. A level emptied here is dropped by the
        // reduce that follows (qty hits 0), taking its endpoints with it.
        if let Some(level) = self.levels.get_mut(&(key.0, key.1))
            && let Some((head, tail)) = level.queue_ends(order.price)
        {
            let new_head = if head == Some(slot) {
                slot_of(order.next)
            } else {
                head
            };
            let new_tail = if tail == Some(slot) {
                slot_of(order.prev)
            } else {
                tail
            };
            level.set_queue_ends(order.price, new_head, new_tail);
        }
    }
}

impl<P, L, S> BookDepth for BookEngine<P, L, S>
where
    P: VenuePolicy,
    L: LevelBook,
    S: BuildHasher + Clone,
{
    fn top_levels(&self, instrument: u32, side: Side, out: &mut [(i64, u64, u32)]) -> usize {
        match self.levels.get(&(instrument, side)) {
            Some(l) => l.top_n(side, out),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use md_events::{BreakRefKind, RecordFlags, RecordHeader, rtype};

    use super::*;
    use crate::policy::{BoardId, BreakScope, Gate, PurposeSet};

    // Layout pin: the SlotLink shrink exists to fit two pooled orders per
    // 64-byte cache line on the unlink pointer chase. A regression to wider
    // links doubles the pool's cache footprint; measured 16% of a full-day
    // NASDAQ replay was this chase.
    #[test]
    fn pooled_order_is_32_bytes() {
        assert_eq!(core::mem::size_of::<Order>(), 32);
    }

    // Always Gate::Flag, never Drop or plain Accept; exercises the suspect path.
    #[derive(Default)]
    struct FlagPolicy;

    impl VenuePolicy for FlagPolicy {
        fn book_gate(&self, _state: TradingState, _action: Action) -> Gate {
            Gate::Flag
        }
        fn purposes(&self, _board: BoardId) -> PurposeSet {
            PurposeSet::BOOK
        }
        fn tape_gate(&self, printable: bool, _action: Action) -> bool {
            printable
        }
        fn on_trade_break(&self, _kind: BreakRefKind) -> BreakScope {
            BreakScope::AggregatesOnly
        }
    }

    // A permissive policy that records every hook call.
    #[derive(Default)]
    struct SpyPolicy {
        book_gate: std::cell::Cell<u32>,
        tape_gate: std::cell::Cell<u32>,
        breaks: std::cell::Cell<u32>,
        purposes: std::cell::Cell<u32>,
    }

    impl VenuePolicy for SpyPolicy {
        fn book_gate(&self, _state: TradingState, _action: Action) -> Gate {
            self.book_gate.set(self.book_gate.get() + 1);
            Gate::Accept
        }
        fn purposes(&self, _board: BoardId) -> PurposeSet {
            self.purposes.set(self.purposes.get() + 1);
            PurposeSet::BOOK
        }
        fn tape_gate(&self, printable: bool, _action: Action) -> bool {
            self.tape_gate.set(self.tape_gate.get() + 1);
            printable
        }
        fn on_trade_break(&self, _kind: BreakRefKind) -> BreakScope {
            self.breaks.set(self.breaks.get() + 1);
            BreakScope::AggregatesOnly
        }
    }

    fn hdr(inst: u32, flags: RecordFlags) -> RecordHeader {
        RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: inst,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags,
            _pad: [0u8; 6],
        }
    }

    fn mbo(inst: u32, oid: u64, side: Side, action: Action, price: i64, qty: u64) -> Mbo {
        mbo_flagged(inst, oid, side, action, price, qty, RecordFlags::NONE)
    }

    fn mbo_flagged(
        inst: u32,
        oid: u64,
        side: Side,
        action: Action,
        price: i64,
        qty: u64,
        flags: RecordFlags,
    ) -> Mbo {
        Mbo {
            hdr: hdr(inst, flags),
            order_id: oid,
            price,
            qty,
            action,
            side,
            participant: [0; 7],
            _pad: [0u8; 7],
        }
    }

    // Add/Cancel/Modify/Fill sequences produce the expected best-of-book, and a
    // modify requeues (priority loss) as the baseline.
    #[test]
    fn apply_fixtures() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.apply_mbo(&mbo(1, 10, Side::Bid, Action::Add, 100, 5));
        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Add, 101, 3));
        assert_eq!(e.best(1, Side::Bid), Some((101, 3)));

        // partial cancel of the top order
        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Cancel, 101, 1));
        assert_eq!(e.best(1, Side::Bid), Some((101, 2)));

        // modify moves order 10 to a new price with a fresh sequence (requeue)
        e.apply_mbo(&mbo(1, 10, Side::Bid, Action::Modify, 102, 5));
        assert_eq!(e.best(1, Side::Bid), Some((102, 5)));

        // fill exhausts order 11
        let out = e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Fill, 101, 2));
        assert_eq!(out.result, ApplyResult::Applied);
        assert!(out.tape, "printable fill reaches the tape");
        assert_eq!(e.best(1, Side::Bid), Some((102, 5)));
    }

    // The (instrument, side, order_id) key collapses venue scoping: the same
    // order_id under a different instrument or side is a different order.
    #[test]
    fn single_order_map() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.apply_mbo(&mbo(1, 7, Side::Bid, Action::Add, 100, 5));
        e.apply_mbo(&mbo(2, 7, Side::Bid, Action::Add, 100, 5)); // same id, other instrument
        e.apply_mbo(&mbo(1, 7, Side::Ask, Action::Add, 200, 5)); // same id, other side
        assert_eq!(e.order_count(), 3);
    }

    // Orders at one price expose arrival order; modifying an order requeues it
    // at the tail while reusing the pooled slot.
    #[test]
    fn pooled_queue_requeues_modify_at_tail() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        for order_id in [10, 11, 12] {
            e.apply_mbo(&mbo(1, order_id, Side::Bid, Action::Add, 100, 5));
        }
        assert_eq!(e.queue_position(1, Side::Bid, 10), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 11), Some(1));
        assert_eq!(e.queue_position(1, Side::Bid, 12), Some(2));

        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Modify, 100, 7));
        assert_eq!(e.queue_position(1, Side::Bid, 10), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 12), Some(1));
        assert_eq!(e.queue_position(1, Side::Bid, 11), Some(2));
        assert_eq!(e.best(1, Side::Bid), Some((100, 17)));

        e.apply_mbo(&mbo(1, 12, Side::Bid, Action::Cancel, 100, 5));
        assert_eq!(e.queue_position(1, Side::Bid, 12), None);
        assert_eq!(e.queue_position(1, Side::Bid, 11), Some(1));
    }

    // A freed slot reused by a new order joining an existing price chain must
    // carry clean links; stale prev/next from the retired occupant would
    // corrupt traversal or FIFO order.
    #[test]
    fn slot_reuse_joins_existing_chain() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        for order_id in [10, 11, 12] {
            e.apply_mbo(&mbo(1, order_id, Side::Bid, Action::Add, 100, 5));
        }
        // full cancel frees mid-chain order 11's slot
        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Cancel, 100, 5));
        assert_eq!(e.queue_position(1, Side::Bid, 10), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 12), Some(1));

        // LIFO free list: order 13 reuses the freed slot, joins the same chain
        e.apply_mbo(&mbo(1, 13, Side::Bid, Action::Add, 100, 7));
        assert_eq!(e.queue_position(1, Side::Bid, 10), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 12), Some(1));
        assert_eq!(
            e.queue_position(1, Side::Bid, 13),
            Some(2),
            "reused slot at tail"
        );
        assert_eq!(
            e.queue_position(1, Side::Bid, 11),
            None,
            "retired order gone"
        );
        assert_eq!(
            e.level_state(1, Side::Bid, 100),
            Some(AuditLevelState { qty: 17, count: 3 })
        );
    }

    // A cross-price modify unlinks from the old price chain (survivors keep
    // relative order) and appends at the new chain's tail: priority loss.
    #[test]
    fn cross_price_modify_appends_at_new_chain_tail() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        for order_id in [10, 11, 12] {
            e.apply_mbo(&mbo(1, order_id, Side::Bid, Action::Add, 100, 5));
        }
        for order_id in [20, 21] {
            e.apply_mbo(&mbo(1, order_id, Side::Bid, Action::Add, 105, 4));
        }

        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Modify, 105, 6));

        // old chain: survivors close ranks, keep relative order
        assert_eq!(e.queue_position(1, Side::Bid, 10), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 12), Some(1));
        assert_eq!(
            e.level_state(1, Side::Bid, 100),
            Some(AuditLevelState { qty: 10, count: 2 })
        );

        // new chain: incumbents unmoved, mover at the tail with new price/qty
        assert_eq!(e.queue_position(1, Side::Bid, 20), Some(0));
        assert_eq!(e.queue_position(1, Side::Bid, 21), Some(1));
        assert_eq!(
            e.queue_position(1, Side::Bid, 11),
            Some(2),
            "mover loses priority"
        );
        assert_eq!(
            e.level_state(1, Side::Bid, 105),
            Some(AuditLevelState { qty: 14, count: 3 })
        );
        assert_eq!(
            e.order_state(1, Side::Bid, 11),
            Some(AuditOrderState { price: 105, qty: 6 })
        );
    }

    // Parity tooling can inspect exact order and aggregate level state without
    // receiving mutable access to the engine's internal containers.
    #[test]
    fn audit_accessors_report_full_depth_state() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.apply_mbo(&mbo(1, 10, Side::Bid, Action::Add, 100, 5));
        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Add, 100, 3));

        assert_eq!(
            e.order_state(1, Side::Bid, 10),
            Some(AuditOrderState { price: 100, qty: 5 })
        );
        assert_eq!(
            e.level_state(1, Side::Bid, 100),
            Some(AuditLevelState { qty: 8, count: 2 })
        );
        assert_eq!(e.instrument_order_count(1), 2);

        e.apply_mbo(&mbo(1, 10, Side::Bid, Action::Cancel, 100, 2));
        assert_eq!(
            e.order_state(1, Side::Bid, 10),
            Some(AuditOrderState { price: 100, qty: 3 })
        );
        assert_eq!(
            e.level_state(1, Side::Bid, 100),
            Some(AuditLevelState { qty: 6, count: 2 })
        );

        e.apply_mbo(&mbo(1, 10, Side::Bid, Action::Cancel, 100, 3));
        e.apply_mbo(&mbo(1, 11, Side::Bid, Action::Cancel, 100, 3));
        assert_eq!(e.level_state(1, Side::Bid, 100), None);
        assert_eq!(e.instrument_order_count(1), 0);
    }

    // The ladder's per-level order count (read via top_levels) matches the
    // count derived independently by walking each price's intrusive queue
    // chain (level_state). This is the invariant that lets mbp-10 depth read
    // straight off the book instead of a parallel aggregation.
    #[test]
    fn top_levels_count_matches_queue_walk() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        // three bid prices with differing depth, plus an ask side
        for oid in [1, 2, 3] {
            e.apply_mbo(&mbo(1, oid, Side::Bid, Action::Add, 100, 5));
        }
        e.apply_mbo(&mbo(1, 4, Side::Bid, Action::Add, 101, 3));
        e.apply_mbo(&mbo(1, 5, Side::Bid, Action::Add, 99, 7));
        e.apply_mbo(&mbo(1, 6, Side::Ask, Action::Add, 105, 2));
        e.apply_mbo(&mbo(1, 7, Side::Ask, Action::Add, 105, 4));
        // churn: partial fill (count holds), a full cancel (count drops), a
        // cross-price modify (leaves one level, joins another)
        e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Cancel, 100, 2)); // partial
        e.apply_mbo(&mbo(1, 2, Side::Bid, Action::Cancel, 100, 5)); // full
        e.apply_mbo(&mbo(1, 4, Side::Bid, Action::Modify, 100, 3)); // 101 -> 100

        for side in [Side::Bid, Side::Ask] {
            let mut rows = [(0i64, 0u64, 0u32); 16];
            let n = e.top_levels(1, side, &mut rows);
            for &(price, qty, count) in &rows[..n] {
                let walked = e
                    .level_state(1, side, price)
                    .expect("ladder price present in queue map");
                assert_eq!(qty, walked.qty, "qty at {price} on {side:?}");
                assert_eq!(count, walked.count, "count at {price} on {side:?}");
            }
        }
    }

    // A non-printable execution mutates the book but stays off the tape.
    #[test]
    fn printable_gate() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, 100, 10));
        let out = e.apply_mbo(&mbo_flagged(
            1,
            1,
            Side::Bid,
            Action::Fill,
            100,
            4,
            RecordFlags::NON_PRINTABLE,
        ));
        assert_eq!(out.result, ApplyResult::Applied);
        assert!(!out.tape, "non-printable leg suppressed from tape");
        assert_eq!(e.best(1, Side::Bid), Some((100, 6)), "book still mutated");
    }

    // Reset clears the instrument atomically; during snapshot mode records are
    // flagged snapshot-derived until handoff.
    #[test]
    fn reset_snapshot_flag() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.begin_snapshot(1);
        let out = e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, 100, 5));
        assert!(out.snapshot, "snapshot-derived until handoff");
        e.end_snapshot(1);
        let live = e.apply_mbo(&mbo(1, 2, Side::Bid, Action::Add, 101, 5));
        assert!(!live.snapshot);

        let cleared = e.apply_mbo(&mbo(1, 0, Side::Bid, Action::Reset, 0, 0));
        assert_eq!(cleared.result, ApplyResult::Applied);
        assert_eq!(e.best(1, Side::Bid), None, "book cleared");
        assert_eq!(e.order_count(), 0);
    }

    // Events at prices beyond any published limit are still accepted; the engine
    // never enforces limits.
    #[test]
    fn limits_not_enforced() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        let out = e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, i64::MAX / 2, 5));
        assert_eq!(out.result, ApplyResult::Applied);
        assert!(e.best(1, Side::Bid).is_some());
    }

    // The policy sees every hook; the core has no venue conditionals (grep gate
    // enforces the latter).
    #[test]
    fn hook_dispatch() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, 100, 5));
        e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Fill, 100, 5));
        assert_eq!(
            e.policy().book_gate.get(),
            2,
            "book_gate per non-reset event"
        );
        assert_eq!(
            e.policy().tape_gate.get(),
            1,
            "tape_gate only on the execution"
        );
    }

    // A fill against an order id the book never held reports UnknownOrder;
    // tape must never fire for a non-Applied result.
    #[test]
    fn unknown_order_no_tape() {
        let mut e: BookEngine<SpyPolicy> = BookEngine::new(SpyPolicy::default());
        let out = e.apply_mbo(&mbo(1, 99, Side::Bid, Action::Fill, 100, 5));
        assert_eq!(out.result, ApplyResult::UnknownOrder);
        assert!(!out.tape, "unknown-order fill never reaches tape");
    }

    // Gate::Flag applies the event through the SAME logic Accept uses but
    // marks the outcome suspect for the caller to OR onto the header flags.
    #[test]
    fn gate_flag_sets_suspect() {
        let mut e: BookEngine<FlagPolicy> = BookEngine::new(FlagPolicy);
        let out = e.apply_mbo(&mbo(1, 1, Side::Bid, Action::Add, 100, 5));
        assert_eq!(out.result, ApplyResult::Applied, "Flag applies like Accept");
        assert!(out.suspect, "Flag marks the outcome suspect");
        assert_eq!(
            e.best(1, Side::Bid),
            Some((100, 5)),
            "book mutated same as Accept"
        );
    }
}
