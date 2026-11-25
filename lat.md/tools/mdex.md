# mdex architecture

mdex is an offline diagnostic and replay explorer over pbn market-data files: one crate holding the read/index engine plus a thin `mdex` binary over it, built on [[crates/tools/mdex/src/reader.rs#Reader]] against `pbn` directly for the random access diagnosis needs.

The forward-only SDK decoder cannot seek; diagnosis needs to jump to any record, reconstruct as-of state, and survive a damaged capture. So mdex reads a whole file into memory, indexes it in one pass, and caches that index beside the source. The library never prints; the binary installs one subscriber.

## Reading pbn files

[[crates/tools/mdex/src/reader.rs#Reader#open]] reads a whole pbn file, version-gates on the schema major, and holds the header plus decompressed body for later stages to borrow.

It fails closed before touching the body on bad magic, unknown proto, or a schema-major mismatch, so a stream the build cannot read errors at open rather than mid-walk. The reader carries its own [[crates/tools/mdex/src/reader.rs#READER_MAJOR]] because `pbn` exposes no current-major constant at the current source. Above 1 GiB it warns rather than caps: `open` pulls the whole file plus the decompressed body into memory, but a diagnostic tool must still open the large captures worth inspecting.

## Scan-and-cache index

[[crates/tools/mdex/src/index.rs#Index#build]] makes one pass over the pbn body, building every lookup structure at once. Infallible: a corrupt frame becomes a `Fault` and the walk stops with a partial index intact, so a damaged capture stays inspectable.

One walk yields an `Entry` per record in stream order, a per-instrument list, a `venue_seq` multimap, a symbol map, running `Symbology`, and per-instrument `Definition` timelines. An unknown record carries no header, so nothing keys on it, but its offset still advances so later frames stay aligned. A framing or decode fault is recorded as a [[crates/tools/mdex/src/index.rs#Fault]] with its offset and cause, then the walk stops. A 104-byte `Definition` written before the tail is no fault: `decode_any` reads it with the tail absent, so its symbol and timeline are indexed.

### As-of lookups

[[crates/tools/mdex/src/index.rs#Index#range]], [[crates/tools/mdex/src/index.rs#Index#seek_le]], and [[crates/tools/mdex/src/index.rs#Index#board_as_of]] answer time-scoped questions over one instrument, ordered by `(ts_event, entry index)` so equal-timestamp ties resolve to the stream-latest deterministically.

`range` returns an inclusive `from..=to` window. `seek_le` returns the last entry at or before a timestamp, optionally stepping back to the nearest of one rtype. `board_as_of` reads the greatest `Definition` at or before a timestamp, so an intraday redefinition never retroactively reclassifies earlier records.

### Sidecar cache

[[crates/tools/mdex/src/index.rs#Index#save]] writes a sibling `.mdex.idx` atomically (temp then rename); [[crates/tools/mdex/src/index.rs#Index#load]] rebuilds symbology and timelines from stored `Definition` offsets, never serializing a polaris-events value.

A sidecar is reusable only when its format, reader major, source size, source mtime, and a verbatim 32-byte header image all match the current source. The header image is stored whole, not digested: at 32 bytes it is cheap, needs no hash dependency, and catches a same-size same-mtime content swap that size and mtime alone would miss. Under `--index-dir` a per-source path hash disambiguates same-named files from different directories, so a read-only data directory still caches.

## Logging and diagnostics

The binary installs exactly one `tracing` subscriber at startup; library code emits events through the facade and never prints. [[crates/tools/mdex/src/main.rs#main]] installs a stderr `fmt` subscriber honoring `RUST_LOG`, default `warn`.

stdout stays product output (the query and render surfaces); diagnostics go to stderr. The library's one event today is the reader's oversized-file warn; more land as the query engine grows. mdex is a plain diagnostic binary with no observability pipeline, so the subscriber is a bare `fmt`, not a metrics layer.

## Errors

[[crates/tools/mdex/src/error.rs#MdexError]] is the one typed failure surface for open, index, and query.

A pbn format fault (bad magic, truncation, bad discriminant, schema-major mismatch) arrives already typed and is carried through `Pbn`, so a version mismatch fails closed at open. The rest are the engine's own conditions: `StaleCache`, and the not-found variants for symbol, sequence, instrument, and prior state.

## Command surface (planned)

`query`, `filter`, `render`, and `cli` are declared module stubs whose doc comments fix the intended shape; implementation lands with the CLI.

The query engine will answer events, event, state, order, and instrument lookups over the index; the filter vocabulary covers rtype, instrument, venue, board, and secondary fields; render emits record tables and JSON; cli is the clap subcommand front the binary dispatches through. Until then `main` installs the subscriber and prints its name.
