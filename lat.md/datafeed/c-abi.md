# C ABI

`polaris-datafeed-ffi` under `crates/datafeed-ffi/` exposes the [[session#Pull-based feed session]] to C: open from a JSON config, poll records into a host array, close.

The library is named `polaris_feed`, and `include/polaris_abi.h` is its header. It depends on `polaris-datafeed` without the `bin` feature, so loading it into a host process does not install a signal handler, a subscriber or a metrics recorder ([[tests#Host process state]]).

## Entry points

`polaris_feed_open`, `polaris_feed_poll` and `polaris_feed_close` wrap one `FeedSession` behind an opaque handle. Each returns an `int32_t` status: 0 on success, 1 for the end of the source, negative on error.

[[crates/datafeed-ffi/src/lib.rs#polaris_feed_open]] takes a NUL-terminated UTF-8 JSON document shaped like the datafeed config file, decodes it into a `DatafeedConfig` and builds the session through `FeedSessionBuilder::from`. A JSON string mirrors the plugin ABI's `create(config_json)` and does not need a file on disk; the file's `sink` and `observability` keys are ignored, as the builder ignores them. On failure it writes NULL to the out pointer.

[[crates/datafeed-ffi/src/lib.rs#polaris_feed_poll]] clears the handle's `Vec<OwnedRecord>`, calls `FeedSession::poll` with the caller's `cap` as `max`, and writes one [[crates/datafeed-ffi/src/lib.rs#PolarisRecord]] (`polaris_record`: `stream`, `len`, `bytes`) per record into the caller's array, then the count into `*written`. `POLARIS_OK` with a count of 0 means nothing is ready yet. Once the source has ended and every record is out, it returns `POLARIS_FEED_END` (1), the C form of `FeedError::Closed`, on every later call. A fault that ends the source returns its negative status once, after the records routed before it.

[[crates/datafeed-ffi/src/lib.rs#polaris_feed_close]] finishes the capture tap as `FeedSession::close` does, frees the handle either way, and returns `POLARIS_OK` or `POLARIS_ERR_TAP`. NULL is a no-op that returns `POLARIS_OK`.

A NULL config, out pointer, handle, record array or count pointer returns `POLARIS_ERR_NULL`, except that close takes a NULL handle as a no-op. Each `FeedError` variant maps to its own status (config, source, session, tap, runtime), with `POLARIS_ERR_OTHER` for a variant this ABI version has no status for. There is no error message accessor: the status identifies the failing stage, which is what a host branches on, as `FeedError`'s own docs ask of Rust hosts.

The handle is not thread-safe and stays on the thread that opened it, because `FeedSession` is not `Send`.

`POLARIS_ABI_VERSION` is 1. It reaches the host as a `#define` at compile time, and [[crates/datafeed-ffi/src/lib.rs#polaris_abi_version]] returns the value the loaded library was built with. The header asks a host to call it first and stop on a mismatch, so a host that loads a `libpolaris_feed` from another ABI revision fails before it passes a record array sized for its own `polaris_record`.

## Record lifetime

A poll writes pointers into the handle's own record buffer, so no record byte is copied a second time and nothing is allocated per record.

`bytes` points at the record's `OwnedRecord::bytes()`, the `RecordHeader` followed by the body, inside the `Vec<OwnedRecord>` the handle reuses on every poll. The buffer is valid until the next call on the same handle, which the header states on `polaris_record` and `polaris_feed_poll`. The `Vec` grows to the largest poll and keeps that capacity. A poll then costs the copy `FeedSession::poll` already makes plus a 16-byte `polaris_record` per record.

Timed from Rust on `m4pro-macos` on AC power (2026-11-27), over the order-book reference fixture repeated 1,000 times with a cap of 256: `FeedSession::poll` took 62 to 66 ns per record and `polaris_feed_poll` 62.3 to 62.9 ns, a paired difference of -2.38, +0.60 and +0.17 ns over three runs of 33 rounds. The bench was a scratch target and is not in the crate.

## Panics and the ffi profile

`polaris_feed_open`, `polaris_feed_poll` and `polaris_feed_close` run their work under `catch_unwind` and return `POLARIS_ERR_PANIC` for a caught panic. Unwinding out of an `extern "C"` function aborts the process.

A panic on a handle poisons it, because its engine state may be half updated: later polls return `POLARIS_ERR_PANIC`, and close frees it without finishing the capture. Catching needs `panic = "unwind"`, and the workspace `release` profile sets `abort`. The C library is therefore built with the `ffi` profile in the root `Cargo.toml`, which inherits `release` and sets `panic = "unwind"`; its cdylib links the `panic_unwind` runtime, and a `--release` build does not. A library built with `abort` ends the host process on a panic, which the header states on `POLARIS_ERR_PANIC`.

## Packaging and header

The ABI is defined in its own member crate with `crate-type = ["rlib", "staticlib", "cdylib"]`, so only builds of that crate link the C artifacts; `polaris-datafeed` keeps building an rlib.

cbindgen generates `include/polaris_abi.h` from `src/lib.rs` with the crate's `cbindgen.toml`, which renames the Rust types to `polaris_feed` and `polaris_record`, sets an include guard and `extern "C"` guards for C++, and includes only `stddef.h` and `stdint.h`. The header is committed. cbindgen is an exact-pinned dev-dependency, and [[tests#C ABI from a C host#Header drift]] regenerates the header and compares it byte for byte. The status codes are plain `#define`s, as cbindgen emits Rust constants.
