//! Buffer pool statistics.

/// Snapshot of pool capacity and occupancy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    /// Buffers pool owns.
    pub capacity: usize,
    /// Buffers currently taken from pool.
    pub in_use: usize,
}
