//! Price-level container for best-of-book access. One instance per
//! (instrument, side). The concrete container is locked only after the bench:
//! two variants live behind this trait until then.

use std::collections::BTreeMap;

use md_events::Side;

/// Aggregate price levels for one side of one instrument. Keeps enough to answer
/// best-of-book and depth without walking every order, including the resting
/// order count per level so mbp-10 depth is read straight off the book.
pub trait LevelBook: Default {
    /// One order joins `price` with `qty`: aggregate qty rises, order count +1
    /// (new level if absent).
    fn add(&mut self, price: i64, qty: u64);
    /// Replace the aggregate at `price` with `qty` (MBP change); `qty` 0 drops
    /// the level. A surviving level keeps its count; a level created here starts
    /// at count 1 (one aggregate posting).
    fn set(&mut self, price: i64, qty: u64);
    /// Remove up to `qty` at `price`. `remove_order` true means one resting
    /// order left the level (count -1); false is a partial fill that leaves the
    /// order resting (count unchanged). Drop the level when qty hits zero.
    fn reduce(&mut self, price: i64, qty: u64, remove_order: bool);
    /// Best (price, aggregate qty) for `side`: highest bid, lowest ask.
    fn best(&self, side: Side) -> Option<(i64, u64)>;
    /// Aggregate resting quantity at one exact price.
    fn quantity_at(&self, price: i64) -> Option<u64>;
    /// Resting order count at one exact price.
    fn count_at(&self, price: i64) -> Option<u32>;
    /// FIFO chain endpoints (head, tail slot indices) co-located with the level.
    /// `None` when the price has no level; the inner ends are `None` on an
    /// MBP-native level that carries no per-order chain. The engine owns the
    /// `slots` links; the container only stores the two endpoints.
    fn queue_ends(&self, price: i64) -> Option<(Option<usize>, Option<usize>)>;
    /// Write a present level's FIFO chain endpoints. No-op when the price has no
    /// level (it vanished with its aggregate).
    fn set_queue_ends(&mut self, price: i64, head: Option<usize>, tail: Option<usize>);
    /// Fill `out` with the top `(price, qty, count)` rows for `side` (highest
    /// bids / lowest asks first), returning the number of rows written. Never
    /// writes past `out.len()`.
    fn top_n(&self, side: Side, out: &mut [(i64, u64, u32)]) -> usize;
    /// Drop every level.
    fn clear(&mut self);
    /// Number of distinct price levels.
    fn len(&self) -> usize;
    /// True when there are no levels.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Struct-of-arrays variant: prices, quantities, resting-order counts, and each
/// level's FIFO chain endpoints in parallel vectors kept sorted ascending by
/// price. Best-of-book is an end lookup; the same binary search that finds the
/// aggregate also yields the chain endpoints.
#[derive(Clone, Debug, Default)]
pub struct SoaLevelBook {
    prices: Vec<i64>,
    qtys: Vec<u64>,
    counts: Vec<u32>,
    heads: Vec<Option<usize>>,
    tails: Vec<Option<usize>>,
}

impl LevelBook for SoaLevelBook {
    #[inline]
    fn add(&mut self, price: i64, qty: u64) {
        match self.prices.binary_search(&price) {
            Ok(i) => {
                self.qtys[i] += qty;
                self.counts[i] += 1;
            }
            Err(i) => {
                self.prices.insert(i, price);
                self.qtys.insert(i, qty);
                self.counts.insert(i, 1);
                self.heads.insert(i, None);
                self.tails.insert(i, None);
            }
        }
    }

    #[inline]
    fn set(&mut self, price: i64, qty: u64) {
        match self.prices.binary_search(&price) {
            Ok(i) if qty == 0 => {
                self.prices.remove(i);
                self.qtys.remove(i);
                self.counts.remove(i);
                self.heads.remove(i);
                self.tails.remove(i);
            }
            Ok(i) => self.qtys[i] = qty,
            Err(_) if qty == 0 => {}
            Err(i) => {
                self.prices.insert(i, price);
                self.qtys.insert(i, qty);
                self.counts.insert(i, 1);
                self.heads.insert(i, None);
                self.tails.insert(i, None);
            }
        }
    }

    #[inline]
    fn reduce(&mut self, price: i64, qty: u64, remove_order: bool) {
        if let Ok(i) = self.prices.binary_search(&price) {
            self.qtys[i] = self.qtys[i].saturating_sub(qty);
            if remove_order {
                self.counts[i] = self.counts[i].saturating_sub(1);
            }
            if self.qtys[i] == 0 {
                self.prices.remove(i);
                self.qtys.remove(i);
                self.counts.remove(i);
                self.heads.remove(i);
                self.tails.remove(i);
            }
        }
    }

    #[inline]
    fn best(&self, side: Side) -> Option<(i64, u64)> {
        if self.prices.is_empty() {
            return None;
        }
        let i = match side {
            Side::Bid => self.prices.len() - 1,
            _ => 0,
        };
        Some((self.prices[i], self.qtys[i]))
    }

    #[inline]
    fn quantity_at(&self, price: i64) -> Option<u64> {
        self.prices.binary_search(&price).ok().map(|i| self.qtys[i])
    }

    #[inline]
    fn count_at(&self, price: i64) -> Option<u32> {
        self.prices
            .binary_search(&price)
            .ok()
            .map(|i| self.counts[i])
    }

    #[inline]
    fn queue_ends(&self, price: i64) -> Option<(Option<usize>, Option<usize>)> {
        self.prices
            .binary_search(&price)
            .ok()
            .map(|i| (self.heads[i], self.tails[i]))
    }

    #[inline]
    fn set_queue_ends(&mut self, price: i64, head: Option<usize>, tail: Option<usize>) {
        if let Ok(i) = self.prices.binary_search(&price) {
            self.heads[i] = head;
            self.tails[i] = tail;
        }
    }

    #[inline]
    fn top_n(&self, side: Side, out: &mut [(i64, u64, u32)]) -> usize {
        let n = out.len().min(self.prices.len());
        for (k, slot) in out.iter_mut().take(n).enumerate() {
            let i = match side {
                Side::Bid => self.prices.len() - 1 - k,
                _ => k,
            };
            *slot = (self.prices[i], self.qtys[i], self.counts[i]);
        }
        n
    }

    fn clear(&mut self) {
        self.prices.clear();
        self.qtys.clear();
        self.counts.clear();
        self.heads.clear();
        self.tails.clear();
    }

    fn len(&self) -> usize {
        self.prices.len()
    }
}

/// One BTree level value: aggregate qty, resting order count, FIFO chain head
/// and tail slots.
type BTreeLevel = (u64, u32, Option<usize>, Option<usize>);

/// BTree variant: a `BTreeMap<price, (qty, count, head, tail)>`, the head/tail
/// being the level's FIFO chain endpoints. Best-of-book is a first/last lookup.
#[derive(Clone, Debug, Default)]
pub struct BTreeLevelBook {
    levels: BTreeMap<i64, BTreeLevel>,
}

impl LevelBook for BTreeLevelBook {
    fn add(&mut self, price: i64, qty: u64) {
        let e = self.levels.entry(price).or_insert((0, 0, None, None));
        e.0 += qty;
        e.1 += 1;
    }

    fn set(&mut self, price: i64, qty: u64) {
        if qty == 0 {
            self.levels.remove(&price);
        } else {
            match self.levels.get_mut(&price) {
                Some(e) => e.0 = qty,
                None => {
                    self.levels.insert(price, (qty, 1, None, None));
                }
            }
        }
    }

    fn reduce(&mut self, price: i64, qty: u64, remove_order: bool) {
        if let Some(e) = self.levels.get_mut(&price) {
            e.0 = e.0.saturating_sub(qty);
            if remove_order {
                e.1 = e.1.saturating_sub(1);
            }
            if e.0 == 0 {
                self.levels.remove(&price);
            }
        }
    }

    fn best(&self, side: Side) -> Option<(i64, u64)> {
        let entry = match side {
            Side::Bid => self.levels.last_key_value(),
            _ => self.levels.first_key_value(),
        };
        entry.map(|(&p, &(q, ..))| (p, q))
    }

    fn quantity_at(&self, price: i64) -> Option<u64> {
        self.levels.get(&price).map(|&(q, ..)| q)
    }

    fn count_at(&self, price: i64) -> Option<u32> {
        self.levels.get(&price).map(|&(_, c, ..)| c)
    }

    fn queue_ends(&self, price: i64) -> Option<(Option<usize>, Option<usize>)> {
        self.levels.get(&price).map(|&(_, _, h, t)| (h, t))
    }

    fn set_queue_ends(&mut self, price: i64, head: Option<usize>, tail: Option<usize>) {
        if let Some(e) = self.levels.get_mut(&price) {
            e.2 = head;
            e.3 = tail;
        }
    }

    fn top_n(&self, side: Side, out: &mut [(i64, u64, u32)]) -> usize {
        let cap = out.len();
        let mut n = 0;
        let mut write = |(&p, &(q, c, ..)): (&i64, &BTreeLevel)| {
            out[n] = (p, q, c);
            n += 1;
        };
        match side {
            Side::Bid => self.levels.iter().rev().take(cap).for_each(&mut write),
            _ => self.levels.iter().take(cap).for_each(&mut write),
        }
        n
    }

    fn clear(&mut self) {
        self.levels.clear();
    }

    fn len(&self) -> usize {
        self.levels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise<L: LevelBook>() {
        let mut b = L::default();
        b.add(100, 5);
        b.add(101, 3);
        b.add(100, 2); // second order aggregates at the same price
        assert_eq!(b.best(Side::Bid), Some((101, 3)));
        assert_eq!(b.best(Side::Ask), Some((100, 7)));
        assert_eq!(b.count_at(100), Some(2), "two resting orders at 100");
        assert_eq!(b.count_at(101), Some(1));
        b.reduce(101, 3, true); // the only order at 101 leaves, level drops out
        assert_eq!(b.best(Side::Bid), Some((100, 7)));
        assert_eq!(b.len(), 1);
        assert_eq!(b.count_at(101), None);

        // partial fill: qty falls, order stays resting, count holds
        b.reduce(100, 1, false);
        assert_eq!(b.quantity_at(100), Some(6));
        assert_eq!(
            b.count_at(100),
            Some(2),
            "partial fill keeps the order count"
        );

        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.best(Side::Bid), None);
    }

    // Both container variants answer best-of-book, aggregation, and per-level
    // order count identically; only their performance differs (bench picks).
    #[test]
    fn variants_agree() {
        exercise::<SoaLevelBook>();
        exercise::<BTreeLevelBook>();
    }

    // top_n reads the depth ladder highest-bid / lowest-ask first with the
    // resting order count per level, the shape mbp-10 depth is built from.
    #[test]
    fn top_n_orders_by_side_with_counts() {
        fn check<L: LevelBook>() {
            let mut b = L::default();
            b.add(100, 5); // one order
            b.add(100, 2); // second order at 100
            b.add(101, 3);
            b.add(99, 7);
            let mut rows = [(0i64, 0u64, 0u32); 4];
            let n = b.top_n(Side::Bid, &mut rows);
            assert_eq!(n, 3);
            assert_eq!(rows[0], (101, 3, 1), "highest bid first");
            assert_eq!(rows[1], (100, 7, 2), "aggregated qty and count");
            assert_eq!(rows[2], (99, 7, 1));

            let mut asks = [(0i64, 0u64, 0u32); 2];
            let m = b.top_n(Side::Ask, &mut asks);
            assert_eq!(m, 2, "out cap honored");
            assert_eq!(asks[0], (99, 7, 1), "lowest ask first");
            assert_eq!(asks[1], (100, 7, 2));
        }
        check::<SoaLevelBook>();
        check::<BTreeLevelBook>();
    }
}
