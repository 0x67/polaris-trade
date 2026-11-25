# venues

Venue crates under `crates/venues/`, one per venue: generated wire parsers for the binary feeds, hand-written adapters for the JSON feeds, plus the plumbing the binary parsers share.

For a binary feed, all parser code comes from [[codegen]]; only the crate shells, `lib.rs`, the adapter and the parity baselines are hand-written. A new binary venue is a new crate shell, then a baseline, then a schema. The JSON venues [[binance]] and [[hyperliquid]] have no schema or codegen output and do not use [[common]]: each is a hand-written serde adapter with a recorded-session fixture.

- [[binance]]: websocket JSON adapter for Binance Spot depth and trades, with REST snapshot resync.
- [[common]]: shared errors, alpha trimming and the TLV appendage walker.
- [[hyperliquid]]: websocket JSON adapter for Hyperliquid perpetuals trades, book pushes and statistics.
- [[nasdaq]]: TotalView-ITCH 5.0 and OUCH 5.0 parsers, the venue adapter and the parity fixture.
- [[sgx]]: Titan ITCH 2.5 parser with GLIMPSE end-of-snapshot.
