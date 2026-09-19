#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! Transport traits, burst container, pool statistics and errors shared by every backend.
//!
//! [`Transport`] carries identity only. Receive and send are separate capability
//! traits, so backend implements exactly what it supports. Receive is synchronous
//! and batch-first; [`AsyncReady`] is optional for backends with truly async
//! readiness. Each backend builds itself from its own config type.
//!
//! | Feature | Enables |
//! | --- | --- |
//! | `observability` | `telemetry` receive metrics and re-exported `observability_core` gate |

pub mod config;
pub mod error;
pub mod pool;
#[cfg(feature = "observability")]
pub mod telemetry;
mod transport;

pub use config::MulticastInterface;
pub use error::TransportError;
/// Metrics gate. Backends and their tests reach it here so all share one pinned version.
#[cfg(feature = "observability")]
pub use observability_core;
pub use pool::PoolStats;
pub use transport::{
    AsyncReady, DatagramRecv, DatagramSend, FrameBatch, L2Recv, Multicast, StreamRecv, StreamSend,
    StreamTrySend, Transport,
};
