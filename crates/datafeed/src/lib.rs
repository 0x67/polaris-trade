//! Market-data feed library: venue session -> client (SoupBinTCP) -> adapter
//! -> engine -> `EgressSequencer`, with a capture tap on the raw frames.
//!
//! A host builds a [`FeedSession`] in code with [`FeedSession::builder`] and
//! pulls records with [`FeedSession::poll`] from its own loop, or builds a
//! [`SinkSession`] that publishes into a sink of its own. The binary loads a
//! config file and drives a [`SinkSession`] over its configured sink through
//! [`app::run_config`]; the binary tree handles argv, signals and telemetry.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod app;
pub mod config;
pub mod error;
pub mod restart;
pub mod session;
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
pub use feed::{FeedSession, FeedSessionBuilder, SinkSession};
pub use record::{MAX_RECORD_LEN, OwnedRecord};
