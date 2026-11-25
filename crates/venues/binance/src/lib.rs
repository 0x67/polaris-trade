#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
//! Binance Spot venue crate: a [`VenueAdapter`](md_pipeline::VenueAdapter)
//! over the public websocket JSON feed and the REST depth snapshot.
//!
//! One adapter serves one symbol. The market stream carries `depthUpdate`
//! and `trade` events; the REST depth snapshot arrives as a frame on its own
//! stream, because fetching it is the caller's job and the crate has no HTTP
//! client. The adapter follows Binance's procedure for a local order book:
//! it buffers diffs until a snapshot joins them, then emits the snapshot and
//! the diffs after it, and starts over when the update ids skip. Prices are
//! fixed point at `1e-9`, sizes at `1e-8`, and no value passes through `f64`.

mod adapter;
mod wire;

pub use adapter::{
    BinanceAdapter, Config, InvalidSymbol, KIND_DEPTH_UPDATE, KIND_SNAPSHOT, KIND_TRADE,
    MARKET_STREAM, SNAPSHOT_STREAM, Symbol,
};
