# sgx

SGX venue crate `crates/venues/sgx` (package `polaris-venue-sgx`, lib `proto_sgx`): the generated `itch_v2` module for SGX Titan ITCH 2.5 with 14 kinds, including the GLIMPSE `EndOfSnapshot`.

[[codegen]] emits it from `schemas/sgx/itch/v2.yaml`, and the output is drift-gated. The crate has no hand-written adapter; its hand-written part is the parity baseline under `tests/support/`.

## Parity baseline

`tests/support/owned_itch_v2.rs` is an owned decoder transcribed from the vendor documentation without reading the YAML, so a schema error cannot validate itself.

`tests/parity_itch_v2.rs` checks generated view getters against it on deterministic synthetic rows.

The `itch_sgx_v2` fuzz target in [[clients#Fuzzing]] includes the same baseline by path and compares it with the generated views on arbitrary rows.

## Scope

The SoupBinTCP packet types of the GLIMPSE session (login, heartbeat, logout) are transport framing and belong to [[soupbintcp]], so only the `EndOfSnapshot` application message is a schema kind.
