//! Buffer pools and their statistics.

#[doc(hidden)]
pub mod backend;
mod index;
mod region;
mod vec;

pub use index::{IndexFrame, IndexPool};
pub use vec::{VecPool, VecSlab};

/// Snapshot of pool capacity and occupancy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    /// Buffers pool owns.
    pub capacity: usize,
    /// Buffers currently taken from pool.
    pub in_use: usize,
}
