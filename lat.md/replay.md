# Replay containers and driver

The `polaris-replay` crate turns captured or vendor-distributed byte streams into [[crates/pipeline/src/ingest.rs#IngestSource]] impls, so the pipeline runs identically live or replayed.

## Shared raw container (capture)

[[crates/replay/src/capture.rs#CaptureReplay]] reads the raw container polaris-format-mdc writes: a 16-byte uncompressed header (`MDC1` magic, venue, date, schema, feed) then a zstd body of `(stream, ts_recv, len, payload)` rows.

The header is parsed by [[crates/formats/mdc/src/lib.rs#read_header]], not here. This reader once carried its own copy of the magic, the kind byte, and the sixteen-byte parse, which meant a header change could land in one copy and diverge in the other silently. It also surfaces the container crate's `CaptureError` rather than a near-identical twin of its own.

The feed byte (offset 13) routes decoders downstream; this reader carries it without interpreting it.

Byte 12 is the retired body-kind discriminator, always 0 now. It once selected a normalized body whose rows were records rather than raw frames; that body was dropped, and `read_header` refuses a non-zero value rather than hand this reader records it would decode as garbage.

## pcap

[[crates/replay/src/pcap.rs#PcapReplay]] is a hand-rolled classic-pcap parser, no libpcap dependency. It detects the global header's magic across all four byte-order/resolution variants and derives `ts_recv` from each packet's own header.

Captured bytes are yielded verbatim; stripping L2/L3/L4 framing down to the venue UDP payload is the adapter's job, not the container's. Every frame reports stream 0 since pcap carries no stream demux.

## Binary dump (binary)

[[crates/replay/src/binary.rs#BinaryFileReplay]] streams the published binary-file framing: a 2-byte big-endian length prefix per message, no in-file timestamps. Messages pull from the reader one at a time, never buffering, so a full day replays in constant memory.

Framing verified against a real TotalView-ITCH 5.0 sample: `[len:u16 BE][message]`, lengths matching the ITCH message set. Since the format carries no receive time, `ts_recv` is synthetic (constructor base plus a per-message step); the message's own venue timestamp is read by the adapter at decode time.

Construct from any reader with `new`, from an uncompressed `.bin` path with `from_path`, or from a gzip `.gz` path with `from_gz` (inflated on the fly via `MultiGzDecoder`, so concatenated multi-member archives from rotation-style `cat a.gz b.gz` inflate through every member). Both share one reader-backed path; a truncated tail ends cleanly, a real io error is retained on `io_error`.

Eager variants `from_path_eager`/`from_gz_eager` slurp (and for gz fully inflate) the whole file into a `Vec<u8>` wrapped in a `Cursor`, then feed the same reader-backed `new`, so frames are byte-identical to streaming. Trades memory for isolating pipeline cost from io and inflate cost in benchmarks; a corrupt gzip fails eagerly at construction instead of at drain.

The gzip backend is a compile-time cargo feature: `miniz` (default, pure-Rust, current impl), `zlib-rs` (pure-Rust, no C toolchain), or `zlib-ng` (C, needs cmake). Pick a non-default with `--no-default-features`; if several are enabled (e.g. `--all-features`) flate2 selects by priority (zlib-ng > zlib-rs > miniz), so it still builds. The `gzip_inflate` bench A/Bs them; m4pro-macos measured miniz 822 MB/s, zlib-rs 900, zlib-ng 904 (zlib-rs matches zlib-ng with no toolchain cost).

## Synthetic venue_seq

No replay container carries a real wire sequence, so each overrides [[crates/pipeline/src/ingest.rs#IngestFrame]]'s `venue_seq` default with a 1-based count per stream, independent of `ts_recv`'s synthesis.

`BinaryFileReplay` and `PcapReplay` report every frame on stream 0, so one `emitted: u64` serves; `BinaryFileReplay` shares it with synthetic `ts_recv`. `CaptureReplay` rows carry their own stream id, so it keeps one count per stream (`emitted: Vec<u64>`, indexed by stream id), the way each channel of a multi-channel live feed has its own session sequence. A single count shared by interleaved streams made every switch read as a skip to the other stream's per-channel gap check.

Downstream adapters compare consecutive `venue_seq` values per stream to flag `RecordFlags::GAP_SUSPECT`; since replay containers never skip a row within a stream, this path stays inert in replay and only fires against a live venue's real gap. `CaptureReplay`'s `synthesizes_venue_seq_per_stream` test pins the per-stream count on an interleaved container.

## Pacing driver

[[crates/replay/src/driver.rs#ReplayDriver]] wraps an `IngestSource` and releases frames at delays computed from `ts_recv` deltas per its `Pacing` policy. Pacing lives in the driver, outside the engine, never touching deterministic output.

`PacingMode` is `RealTime`, `Scaled(f64)`, or `Unthrottled`; the wait itself is behind [[crates/replay/src/driver.rs#Pacer]], a trait, so the production impl sleeps real wall clock while tests inject a recording impl and assert pacing math with zero real sleeping. The driver also advances a caller-supplied `VirtualClock` to each released frame's `ts_recv`, so an `EgressSequencer` sharing that clock stamps deterministic replay times.

`Pacing` is either `Fixed(PacingMode)` (whole replay, the original behavior) or `Schedule(PacingSchedule)`. [[crates/replay/src/driver.rs#PacingSchedule]] holds sorted, non-overlapping `PaceWindow`s (`from_ts..to_ts` half-open, each carrying its own mode) over a default mode governing every gap; `new` validates sorted/non-overlapping/non-inverted fail-fast (`PacingScheduleError`). A forward-only cursor picks the governing mode in O(1) per frame and never rewinds on out-of-order `ts_recv`. Pacing is a timeskip, never a dataskip: every frame still releases; skip-to-point-in-time is the one-window case.

## Shared replay clock

[[crates/replay/src/driver.rs#SharedClock]] is an `Rc<Cell<u64>>` implementing `polaris-pipeline`'s `EgressClock`; clones share one cell, single-threaded by design (one session is one thread).

[[crates/replay/src/driver.rs#PacedSource]] wraps a `ReplayDriver` and implements `IngestSource` itself, delegating `recv_batch` to `recv_paced` and advancing the shared clock to each batch's last released `ts_recv` as a side effect. An `EgressSequencer` built on a clone of the same clock stamps `ts_egress` as a deterministic function of replayed `ts_recv` instead of wall time, and session loops stay untouched. `source()` exposes the innermost container for post-run checks like `BinaryFileReplay::io_error`.

## Container detection

[[crates/replay/src/detect.rs#detect_container]] resolves a replay path to a `ContainerKind` by magic bytes first, extension fallback second, refusing unknown input.

Magics: capture `MDC1` (taken from [[crates/formats/mdc/src/lib.rs#MAGIC]], not redeclared), the four classic pcap variants (LE/BE x usec/nanos, same list `PcapReplay` matches), gzip `0x1f 0x8b`. The length-prefixed binary framing carries no magic, so `.bin`/`.itch` resolve by extension (`.gz` too, covering unreadable heads). Anything else is `DetectError::Unknown`, a startup refusal: replaying a misdetected container decodes garbage silently.

Every positive kind is pinned by the `detect_container_magics` replay test against literal magic bytes rather than the constants under test; the refusal path has its own unit test beside the function.
