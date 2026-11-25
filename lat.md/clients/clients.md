# clients

Protocol clients under `crates/clients/`, built on the transport crates.

Both depend on [[core]] alone and take any backend through its traits; their tests use [[socket]] and the core mock driver.

- [[moldudp]]: MoldUDP64 receiver over caller-built legs, with optional gap recovery.
- [[soupbintcp]]: SoupBinTCP v3.0 session, async and synchronous.

## Fuzzing

The detached `fuzz/` crate (own workspace and lockfile, built by cargo-fuzz on nightly) hammers the two wire codecs with hostile bytes. It also holds the NASDAQ and SGX parser targets, the order-book model and the pbn format targets.

- `moldudp_wire`: header parse and block walk never panic; every block stays inside the datagram and the walk never yields more blocks than the header declares.
- `soupbintcp_framing`: one byte stream parsed whole and split at input-chosen points yields the same frames, consumed total and terminal error.
- `soupbintcp_compressed`: corrupt zlib input is a structured error, and no feed inflates past the reader's cap while each chunk is fed until drained.
- `itch_nasdaq_v5` and `itch_sgx_v2`: one ITCH row of `polaris-venue-nasdaq` or `polaris-venue-sgx`. At the exact length, every view getter equals the field the hand-written owned baseline in the crate's `tests/support/` decodes. At a wrong length, parse returns `WrongLength` with the true expected and received sizes. `parse_match` sends an unknown or variable-length kind to its cold path without a panic.
- `ouch_nasdaq_v5`: one OUCH v5 exchange-to-client row. Views and dispatch agree with the owned `decode_from_exchange` whenever the appendage tail splits cleanly into elements; a malformed inner element must still parse and dispatch without a panic.
- `book_model`: up to 256 arbitrary add, modify, cancel, fill, trade and reset ops go through `BookEngine::apply_mbo` in `polaris-orderbook`. After each op the engine's resting orders, per-level aggregates and best bid and ask must equal a `BTreeMap` model that never calls the engine.
- `stream_walk`: arbitrary bytes through the frame walk of `polaris-format-pbn`. Every accepted frame stays within `MAX_FRAME_LEN`, `decode_any` and `decode_checked` never panic, and plain `decode` equals `decode_checked` for each of the thirteen record types on the frames the checked path accepted. One seed is a 104-byte `Definition` written before the tail, which both paths read.
- `record_roundtrip`: a `Trade` and an `Mbo` built from arbitrary field values go through `write_record`, the frame walk and `decode_checked`. The written bytes are the record's own byte image and the decoded fields equal the source.
- `container`: arbitrary container bytes through `read_stream`. Truncated, overlong or corrupt input, hostile zstd payloads included, returns a `PbnError` without a panic, and the walk over an accepted body ends with every frame within `MAX_FRAME_LEN`.

The venue targets share their oracle macros through `fuzz/fuzz_targets/support/mod.rs`. The pbn targets share record builders and the round-trip and decode-agreement oracles through the crate's library, `fuzz/src/lib.rs`, which `fuzz/tests/gen_seeds.rs` also uses: `cargo test --test gen_seeds` from `fuzz/` rewrites the seven pbn seeds with the format's own writers, byte-identical.

Seed inputs live in `fuzz/corpus/<target>/`. Replay them with `cargo +nightly fuzz run <target> fuzz/corpus/<target> -- -runs=0`.
