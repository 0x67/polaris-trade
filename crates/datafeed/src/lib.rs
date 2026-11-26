//! Market-data feed library. Config-driven wiring: venue session -> client
//! (SoupBinTCP) -> adapter -> engine -> `EgressSequencer` -> sinks, with
//! capture taps at the raw and normalized boundaries. `src/main.rs` only
//! handles argv and exit status; `app`, `live`, `replay`, and `wiring` own the
//! deployment assembly.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod app;
pub mod config;
pub mod error;
pub mod restart;
pub mod session;
pub mod shutdown;
pub mod sink;
pub mod source;
pub mod tap;
pub mod venue;

mod anchor_roll;
mod live;
mod obs;
mod replay;
mod session_window;
mod wiring;

pub use error::{ConfigError, FeedError};
