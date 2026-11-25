#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
//! WebSocket [`IngestSource`](md_pipeline::IngestSource) over `tungstenite`.
//!
//! [`WebSocketSource`] holds one connection per [`ChannelConfig`] and polls
//! them in turn over non-blocking sockets, with no runtime and no `.await`.
//! Each text or binary message becomes one [`WebSocketFrame`] that owns the
//! message `Bytes` as read, stamped with `ts_recv` right after the read.
//!
//! A lost connection reconnects inside `recv_batch`: the call polls the live
//! channels first, and the lost channel retries after a backoff with a blocking
//! connect bounded by `connect_timeout` that replays its subscribe messages.
//! The source ends with `IngestError::Closed` once a channel fails its
//! configured number of attempts in a row, counting a reconnect that closes
//! before delivering a message. Parsing venue JSON belongs to the adapter.
//!
//! The source has no frame pool, so it never returns `PoolExhausted`; messages
//! past `max` stay in the socket or in the library's read buffer. The
//! permessage-deflate extension is not supported.

mod config;
mod connect;
mod error;
mod source;

pub use config::{ChannelConfig, WebSocketConfig};
pub use error::WebSocketError;
pub use source::{WebSocketFrame, WebSocketSource};
