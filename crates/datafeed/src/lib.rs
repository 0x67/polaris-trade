//! Market-data feed library: venue session -> client (SoupBinTCP) -> adapter
//! -> engine -> `EgressSequencer`, with a capture tap on the raw frames.
//!
//! A host builds a [`FeedSession`] in code with [`FeedSession::builder`] and
//! pulls records with [`FeedSession::poll`] from its own loop. The binary
//! loads a config file and drives the same session through [`app::run`];
//! `src/main.rs` only handles argv and exit status.

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
mod feed;
mod live;
mod obs;
mod record;
mod replay;
mod session_window;
mod wiring;

pub use error::{ConfigError, FeedError};
pub use feed::{FeedSession, FeedSessionBuilder};
pub use record::{MAX_RECORD_LEN, OwnedRecord};
