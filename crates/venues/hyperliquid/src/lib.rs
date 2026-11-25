#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
//! Hyperliquid perpetuals venue crate: a [`VenueAdapter`](md_pipeline::VenueAdapter)
//! over the public websocket JSON feed.
//!
//! One ingest stream carries every subscription of one connection. The
//! adapter decodes the `trades`, `l2Book` and `activeAssetCtx` channels into
//! `Trade`, `Level` and `Stats` records. Prices are fixed point at `1e-9`,
//! sizes at `1e-8` (`Definition::qty_scale` is `-8`), and no value passes
//! through `f64`.

mod adapter;
mod wire;

pub use adapter::{
    Config, HyperliquidAdapter, KIND_ACTIVE_ASSET_CTX, KIND_L2_BOOK, KIND_TRADES, STREAM,
};
