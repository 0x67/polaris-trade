//! Venue-agnostic orderbook engine and derived-view computation.
//!
//! BookEngine over a venue policy, level book, status machine, derive engine.
//! One code path drives vendor-side emission and downstream reconstruction, so
//! views stay bit-identical on both sides.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod derive;
pub mod engine;
pub mod level;
pub mod policy;
pub mod status;

pub use derive::{BookDerive, DeriveEngine, TradeDerive, ohlcv::OhlcvEngine};
pub use engine::{ApplyResult, BookDepth, BookEngine, BookOutcome};
#[cfg(any(test, feature = "testing"))]
pub use engine::{AuditLevelState, AuditOrderState};
pub use level::{BTreeLevelBook, LevelBook, SoaLevelBook};
pub use policy::{BoardId, BoardPurposeMap, BreakScope, Gate, PurposeSet, VenuePolicy};
pub use status::{StatusMachine, Transition};
