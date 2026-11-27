# datafeed-ffi

Package `polaris-datafeed-ffi` is the C ABI over the `datafeed` library's feed
session. A host in any language with a C bridge opens a session from a JSON
config, polls stamped normalized records into an array the host allocates, and
closes the session, with no Rust toolchain. The library is named
`polaris_feed`, and its header is `include/polaris_abi.h`.

It depends on `polaris-datafeed` without the `bin` feature, so loading it does
not install a signal handler, a `tracing` subscriber or a metrics recorder in
the host process.

Architecture notes are in `lat.md/datafeed/c-abi.md` at the repository root.

## Build

Commands run from the workspace root.

```bash
cargo build --profile ffi -p polaris-datafeed-ffi
```

The `ffi` profile is the release profile with `panic = "unwind"`. It writes
`libpolaris_feed.a` and `libpolaris_feed.so` (`.dylib` on macOS), or
`polaris_feed.lib`, `polaris_feed.dll` and its import library
`polaris_feed.dll.lib` on Windows, to `target/ffi/`. The open, poll and close
functions catch a Rust panic and return `POLARIS_ERR_PANIC`, which needs an
unwinding build.
A library built with `--release` takes the workspace's `panic = "abort"`, and
a panic then aborts the host process.

A C host compiles against `include/polaris_abi.h` and links the static or the
shared library. Linking the static library also takes the system libraries
Rust needs, which `cargo rustc -p polaris-datafeed-ffi --profile ffi
--crate-type staticlib -- --print native-static-libs` lists.

## Use

```c
polaris_feed *feed = NULL;
polaris_record records[64];
size_t written = 0;
if (polaris_abi_version() != POLARIS_ABI_VERSION) { /* built from another ABI revision */ }
int32_t rc = polaris_feed_open(config_json, &feed);
if (rc != POLARIS_OK) { /* negative status */ }
while ((rc = polaris_feed_poll(feed, records, 64, &written)) == POLARIS_OK) {
    /* records[0..written]: stream, len, bytes (RecordHeader, then the body) */
}
/* rc is POLARIS_FEED_END when the source ended, negative on a fault */
rc = polaris_feed_close(feed);
```

`config_json` is a JSON document shaped like the `datafeed` config file
(`crates/datafeed/config.example.yaml`); its `sink` and `observability` keys
are ignored. `polaris_abi_version` returns the ABI version the loaded library
was built with; a host stops when it differs from the header's
`POLARIS_ABI_VERSION`, since records and statuses may then differ. Each
record's `bytes` points into a buffer the handle reuses, and it is valid until
the next call on the same handle. The header documents every function, the
threading rule and every status code. The record layouts are in
`crates/events/include/md_events.h`.

## Tests

```bash
cargo nextest run -p polaris-datafeed-ffi
```

- `tests/header.rs` regenerates the header with cbindgen and fails when the
  committed one differs. After an ABI change, rewrite it with
  `POLARIS_ABI_HEADER_WRITE=1 cargo nextest run -p polaris-datafeed-ffi --test header`.
- `tests/c_host.rs` compiles the header alone under strict C, then builds
  `tests/c/harness.c` against the cdylib and checks that it reads the records
  `FeedSession::poll` hands a Rust host over the NASDAQ prefix fixture.
- The library tests call the entry points from Rust: NULL arguments, the
  status of each refused config, and a caught panic that poisons a handle.
  CI also runs them under Miri.

## License

Dual-licensed under either [MIT](../../LICENSE-MIT) or
[Apache 2.0](../../LICENSE-APACHE), at your option.
