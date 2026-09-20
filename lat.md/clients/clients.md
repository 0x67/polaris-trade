# clients

Protocol clients under `crates/clients/`, built on the transport crates.

Both depend on [[core]] alone and take any backend through its traits; their tests use [[socket]] and the core mock driver.

- [[moldudp]]: MoldUDP64 receiver over caller-built legs, with optional gap recovery.
- [[soupbintcp]]: SoupBinTCP v3.0 session, async and synchronous.

## Fuzzing

The detached `fuzz/` crate (own workspace and lockfile, built by cargo-fuzz on nightly) hammers the two wire codecs with hostile bytes.

- `moldudp_wire`: header parse and block walk never panic; every block stays inside the datagram and the walk never yields more blocks than the header declares.
- `soupbintcp_framing`: one byte stream parsed whole and split at input-chosen points yields the same frames, consumed total and terminal error.
- `soupbintcp_compressed`: corrupt zlib input is a structured error, and no feed inflates past the reader's cap while each chunk is fed until drained.

Seed inputs live in `fuzz/corpus/<target>/`. Replay them with `cargo +nightly fuzz run <target> fuzz/corpus/<target> -- -runs=0`.
