# datafeed tests

Test specs for `polaris-datafeed`: determinism and parity gates over the session runner, the live path against an in-process SoupBinTCP server, and unit tests that use NASDAQ as their fixture venue.

Tests that need a venue run on NASDAQ: `NasdaqAdapter`, the `NASDAQ` descriptor, `proto_nasdaq::FEED_NASDAQ` (3), New York session timestamps, and capture paths under `nasdaq/v5/`. A test that needs a property the built-in descriptor lacks builds a test-only descriptor from it with struct-update syntax (`..NASDAQ`). Expected values come from the in-test inputs and the descriptor. [[tests#Seam parity gate]] and [[tests#Order book reference parity]] are the gates that compare against committed fixtures.

The integration test binaries share one set of ITCH 5.0 message builders in `tests/support/itch.rs` (system event, stock directory, trading action, add, executed, delete), sized from the generated `proto_nasdaq::itch_v5` views. `tests/support/digest.rs` holds the digest sink and replay assembly of the seam gate, `tests/support/capture_tree.rs` the capture tree reader and manifest check, and `tests/support/soupbin.rs` the packet builders of the mock servers. The binaries that hash or compare records as a sink stores them read each header's bytes through [[crates/datafeed/tests/support/record.rs#header_bytes]].

## Output determinism

Replaying identical input must yield byte-identical normalized L0-L3 output: the pipeline is a pure function of `(venue bytes, ts_recv)` under a logical `VirtualClock`, so capture-hash storage and double-run parity hold.

[[crates/datafeed/tests/determinism.rs]] hashes each stamped egress record's verbatim `repr(C)` bytes, the exact pattern a downstream consumer receives off the sink, so a stable digest is also a reproducible stored file. A `SystemClock` wall-clock read anywhere on the path would diverge two runs and fail the gate. The cases run on `NasdaqAdapter` with venue id 2 and a zero session anchor. The shared `run` helper fails a run whose adapter counted a malformed or unknown-kind frame, because a frame the adapter drops would pass every digest.

### Synthetic NASDAQ replay is byte-identical

A hand-built ITCH 5.0 stream runs on every push and asserts one replay's normalized digest equals the next, covering L3 `Mbo`, L2 `Mbp10`, L1 `Bbo`, L0 `Trade`, `Status`, `Definition`, and `Control`.

[[crates/datafeed/tests/determinism.rs#nasdaq_synthetic_output_is_byte_identical_across_replays]] feeds eight messages through `session::run` with a digest-and-coverage sink, twice: system event `O`, a stock directory, a trading action, a bid add and an ask add, two executions of the bid at 1 s and 100 s, and system event `C`. It compares the per-rtype digests and counts, the record count, and the overall hash.

### Full-day capture replay is byte-identical

An `#[ignore]`d run over a local full-day NASDAQ sample (`NASDAQ_ITCH_SAMPLE`) replays twice and compares per-rtype digests, exercising the record families at scale with L0 `Ohlcv` added.

[[crates/datafeed/tests/determinism.rs#nasdaq_full_day_output_is_byte_identical]] opens the sample through [[crates/datafeed/tests/support/reference_parity.rs#open_binary_sample]], the call the order book parity test makes, and runs it twice at batch 512. It requires equal overall digests and the presence of `Mbo`, `Mbp10`, `Bbo`, `Trade`, `Ohlcv`, `Status`, and `Definition`, and prints one line per rtype with both counts and the digest. The digests are printed for the record and are not stored in a file.

A companion [[crates/datafeed/tests/determinism.rs#diagnose_mbp10_divergence_offsets]] byte-compares the first 20,000 `Mbp10` records of two runs and fails when a differing byte is in a field instead of at a padding offset (header bytes 50 to 56, body bytes 75 to 80). Records have explicit zeroed padding ([[events#No implicit padding]]), so two runs do not diverge today, and the test stays as the tool that localizes a divergence.

### OHLCV output is batch-invariant

The OHLCV watermark advances on each record's `ts_event`, never on frame-batch boundaries, so the emitted record sequence is identical at any batch size.

[[crates/datafeed/tests/determinism.rs#ohlcv_output_is_batch_invariant]] runs the synthetic stream at batch 4, 1, and 7 and asserts the overall digest, per-rtype digests, and record count all match, and that `Ohlcv` records appear once watermark close plus the end-of-session flush are in play.

### Bars carry their resolution and flush at session end

Every emitted `Ohlcv` carries its bucket width in `resolution` and an interval-open `ts_event`; open bars flush at stream end so the last window of each traded instrument is not lost.

[[crates/datafeed/tests/determinism.rs#session_bars_carry_resolution_and_flush_at_eof]] drives a one-trade stream through `session::run` (system event `O`, a bid add, its execution at 1 s, a second add at 3 s, system event `C`), collects the `Ohlcv` records, and asserts each width is one of the seven configured, `ts_event` is bucket-aligned, and every configured resolution produced a bar.

### A non-trade record flushes a due bar mid-stream

A non-trade record whose `ts_event` crosses an open bar's boundary closes that bar as the session routes it, before stream end, so an illiquid instrument's bar still prints on time.

[[crates/datafeed/tests/determinism.rs#non_trade_flushes_due_bar_mid_stream]] feeds the same one-trade stream, records the egress rtype order, and asserts the first `Ohlcv` is followed by a non-`Ohlcv` record (a mid-stream flush) with more bars trailing at EOF.

### Partitioned output is byte-identical across replays

Writing the partitioned tree twice from one input yields byte-identical files and per-dir manifests under both a daily and an hourly partition, so the schema and time split stays a pure function of event time.

[[crates/datafeed/tests/determinism.rs#partitioned_output_is_byte_identical_across_replays]] replays the synthetic stream into a fresh [[crates/datafeed/src/tap.rs#MdCaptureTap]] on the `NASDAQ` descriptor under `1d` and `1h`, with the header feed id `wiring::build_tap` stamps. It snapshots each output tree and asserts the two runs match byte-for-byte (a wall-clock read would diverge them), that a `manifest.json` exists, and that the two partitions label their windows differently.

## Seam parity gate

Seam output is checked per stream against recorded digests. The NASDAQ digests are the per-venue runner's output from before the one session runner replaced it.

[[crates/datafeed/tests/seam_parity.rs]] parses a replay config the way the binary does and runs the one session runner on the erased adapter the descriptor's `make` builds. It hashes every published record per `(stream, rtype)` and compares against `tests/fixtures/seam-parity/`, plus the session counters and the shared adapter counters. The vendored order book reference stream runs on every pass. The ITCH prefix case is `#[ignore]`d behind `NASDAQ_ITCH_PREFIX`, which takes the path of the venue crate's fixture `crates/venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin` ([[nasdaq#NASDAQ venue adapter#Parity fixture]]).

A mismatch is an output change to fix. A baseline line is rebased only for an intended output change whose cause is proven: the bytes and records that change are only those the cause explains (for example, masking the bytes makes the old and new output hash the same), and every other record count and counter holds. The new lines land in the commit that makes the change, so its diff keeps the before and after digests, and this section gains an entry naming the cause and its proof. The test's module doc states the rule and points here.

The NASDAQ lines have been rebased once, for the definition tail below. Before that they were the per-venue runner's output, and moving the crate into this workspace did not change a digest, a record count, or a counter.

### Definition tail rebase

`Definition` grew from 104 to 176 bytes with the derivative and FX tail ([[events#Definition tail growth]]), which changed the `definition` and `all` lines of both NASDAQ fixtures.

The cause is the record itself. Every published `Definition` now ends in `tail` set to `DefinitionTail::ABSENT`, because the NASDAQ adapter does not set any tail term, and its length word is 44 where it was 26. No other record changed.

For the proof, a scratch copy of the test's sink checked that each published `Definition` had a 120-byte body ending in the 72 bytes of `DefinitionTail::ABSENT` and a length word of 44, then hashed it as its pre-tail image (length word 26, the first 48 body bytes) and every other record as it is. Against the old baseline, both cases (the reference stream and the ITCH prefix, `--run-ignored all`) passed: all eleven lines, record counts included, and every session and adapter counter held. The rebased lines, old digest to new:

1. `nasdaq-itch-prefix/all`: `6025d0af` to `8d164acf`.
2. `nasdaq-itch-prefix/stream0.definition`: `2ad33e69` to `ed6a5408`.
3. `nasdaq-orderbook-reference/all`: `8fe596ba` to `591663fb`.
4. `nasdaq-orderbook-reference/stream0.definition`: `228d31f7` to `e1d679cb`.

A NASDAQ adapter that later sets tail terms changes the same four lines again and needs its own entry here.

## Order book reference parity

Replayed NASDAQ order flow is compared against reference L1, L2 and L3 book state: a vendored synthetic stream on every push, and a local full-day sample behind `#[ignore]`.

[[crates/datafeed/tests/orderbook_reference_parity.rs#synthetic_nasdaq_replay_matches_exact_l1_l2_l3_fixture]] replays a checked-in `A/F/E/C/X/D/U` stream and compares exact L1-L3 state, participant carry, FIFO, full-depth touched levels, MBP10/BBO, and final emptiness. Eleven simultaneous ask levels keep one live level outside MBP10, and both same-price and price-changing replaces lose priority atomically (see [[session#Session hot loop and record routing#Atomic MBO source-event publication]]). Its downstream MBO rebuilder groups by `(stream, venue_seq)` and explicitly flushes the last group at end-of-stream. Reference zero prices for an absent side compare against `Price::NULL` only after both sides map through one shared null-price normalization; quantity and count remain exact zeros. Every fixture L1, L2, and L3 row must be consumed exactly once; leftover rows fail. Fixture provenance and lifecycle coverage live in `tests/fixtures/orderbook-reference/README.md`.

Injected L1, L2, and L3 mismatches exercise the shared first-divergence diagnostic. It identifies the event/change index, venue sequence, wire kind, timestamps, instrument, layer, side, subject, field, values, and at most sixteen preceding normalized mutations.

[[crates/datafeed/tests/orderbook_reference_parity.rs#full_day_nasdaq_replay_matches_reference_l1_l2_l3]] streams a local full-day sample, which is not in the repository, against gzip reference CSVs through [[crates/datafeed/tests/support/reference_parity.rs#FullDayComparator]]. The comparator watches both seams and keeps one state across them: raw pre-adapter frames arrive through [[crates/datafeed/tests/support/reference_parity.rs#ComparatorTap]] and published records off [[crates/datafeed/tests/support/reference_parity.rs#ComparatorSink]], shared by `Rc<RefCell<..>>` since one session is one thread. It keeps a FIFO of expected events rather than a single current one, and applies expected L2 ledger rows at consume time: the session legitimately decodes frames ahead of routing under the prefetch ring, so raw-side state must never run ahead of the consumed record stream. Every started event still retires complete (all L3 rows, one MBP10, one BBO) or the run fails. Before any replay, [[crates/datafeed/tests/support/reference_parity.rs#verify_reference_artifacts]] hashes each consumed artifact (decompressed, `MultiGzDecoder`) against `strict-l3-manifest.json` and refuses comparison on mismatch, naming the file; first success writes a dot-prefixed `.verified-<manifest-sha256>` trust marker beside the artifacts so later runs skip re-hashing until the manifest changes.

`PARITY_MAX_MESSAGES` bounds the run via [[crates/datafeed/tests/support/reference_parity.rs#PrefixSource]], a frame-capped `IngestSource` wrapper that prints a progress line every 10M frames: per-event asserts stay hard, but stream-exhaustion and final-empty-book asserts are skipped and a partial report prints. Unset, assert semantics are unchanged. Sibling `#[ignore]` probes [[crates/datafeed/tests/orderbook_reference_parity.rs#full_day_nasdaq_decode_only_drain]] and [[crates/datafeed/tests/orderbook_reference_parity.rs#full_day_nasdaq_session_noop_tap]] stage the runtime decomposition: replay decode alone, then the full session behind a no-op tap. `PARITY_BATCH` overrides the 512-frame reap batch, and `PARITY_OHLCV_RESOLUTIONS` (comma-separated tokens, e.g. `1s,1m`) narrows the session probe's OHLCV resolution set to isolate that engine's cost; the full-day gate itself stays pinned to the production default. The sample opens through [[crates/datafeed/tests/support/reference_parity.rs#open_binary_sample]], which picks the container by extension: `.zst` streams through a zstd decoder, anything else inflates as gzip. The crate README gives the commands for the gate, a bounded smoke run, and the probes.

## Live session against a mock server

`tests/soupbintcp_mock.rs` binds `127.0.0.1:0`, speaks minimal SoupBinTCP framing in-process, and drives the live source and the session runner end to end: connect, login, adapt ITCH 5.0 bytes, book, sequence, publish.

The adapter cases build a `NasdaqAdapter` and feed it in-test add and delete messages:

- `nasdaq_live_session_produces_normalized_records`: the server accepts the login at sequence 1 and sends a system event, an add, and end of session. It expects two frames in the session stats, two decoded frames in the adapter stats, and one `Control` plus one `Mbo` in the sink, with the `Mbo` on stream 0 at `venue_seq` 2.
- `venue_seq_threaded`: the server accepts the login at sequence 50 and sends one add. The `Mbo` must have `venue_seq` 50, which only the SoupBinTCP session sequence can produce.
- `reconnect_resumes`: the first connection sends a system event and an add, then drops. On the second connection the server asserts that the login requests sequence 3 and answers with a delete of the same order. The test then requires an empty unknown-order count (the book kept the order across the reconnect), an `egress_seq` contiguous from 1, and an `Mbo` with `venue_seq` 3.

The other cases do not build an adapter and read frames off the source: `rejected_resume_falls_back_to_sequence_one`, `mixed_dual_resume_reconnects_both_legs_at_sequence_one`, and `dual_legs_tag_frames_with_caller_stream_ids`, which passes stream ids other than 0 and 1 to catch a hardcoded tag. They are the integration coverage of [[session#Restart and resume seam]] for a dual source.

These cases need a client built without `compressed`, because the mock writes plain packets ([[soupbintcp#Tests#Compressed build]]). Its config sets a 30 s heartbeat interval and a 60 s timeout, which keeps heartbeats out of sessions this short. The client crate tests the timeout that closes a session ([[soupbintcp#Tests]]); the datafeed does not have a test that waits for one.

## Replay session

`tests/replay_nasdaq.rs` runs `session::run` over `BinaryFileReplay` on a length-prefixed dump built in the test, the same session function the live cases use with only the source construction changed.

`nasdaq_replay_session_produces_normalized_records` asserts the normalized output of a stock directory, a system event, and an add; `nasdaq_replay_reads_gzip_dump` reads the same kind of dump through the gzip container.

## Feed session from a host

`tests/feed_session.rs` drives a `FeedSession` and a `SinkSession` through the public API only, built with the builder and no config file.

`host_poll_receives_the_runner_records_then_closed` replays the order-book reference fixture and polls five records at a time into one `Vec` it never clears. It compares each record's stream and bytes with what `session::run` publishes over the same frames under the binary's replay assembly. The first batch routes more than five records, so a record dropped past `max` fails it, and a step that wrote over what the host's `Vec` already held fails it too. The last record must be an OHLCV bar, so the step that ends the source routes records and the held end is exercised. Two more polls must return `Closed`. `sink_session_publishes_the_runner_records_then_closed` runs the binary's path over the same fixture into a sink that keeps each record's bytes and expects the runner's records, then `Closed`.

`poll_returns_a_fault_after_the_records_routed_before_it` replays an MDC1 capture that opens the feed, lists a security and trades against a resting order, with its last row on stream 1. The first poll returns the four frames' records, the second the OHLCV bars that the faulting step flushed, the third `SourceError::UndeclaredStream`, and the fourth `Closed`. `sink_session_returns_the_fault_once_after_publishing_the_flush` replays the same capture through a `SinkSession`: the fault returns on the second poll with the flushed bars already in the sink, and `Closed` follows.

## Host process state

`tests/host_process.rs` is unix-only and holds two cases: one builds, polls and closes a replay `FeedSession`, the other runs a replay config through `app::run_config` with a never-set stop flag.

Each reads the SIGINT and SIGTERM dispositions before and after and expects them unchanged, and expects no global `tracing` subscriber. A library that registered a handler or installed a subscriber, in the session or in `run_config`, fails it. CI runs it with `--no-default-features` in the `datafeed-embedded` job, where the `bin` feature and its crates are absent from the build.

## Deployed binary

`tests/binary_deploy.rs` runs the built `datafeed` binary on the vendored order book reference fixture with a raw capture tap, and compares what it leaves on disk with the library path and the committed seam baseline.

The binary's sink discards records and the tap stores raw frames, so the binary's normalized output is not observed. The digest checks prove the capture holds the baseline frames, normalized by an adapter the test builds from its own config values. The target is declared with `required-features = ["bin"]`, so builds without the feature (the `datafeed-embedded` CI job and pre-push command) skip it; the workspace pre-push `nextest` runs it.

### Binary capture matches the library and the baseline

The binary exits 0 and writes the same capture tree as `app::run_config` over the same config, with every manifest verified, and the records rebuilt from its capture match the committed digests.

[[crates/datafeed/tests/binary_deploy.rs#binary_capture_tree_matches_the_library_path_and_the_seam_baseline]] runs one JSON config through the binary and through [[crates/datafeed/src/app.rs#run_config]] with a different `raw_dir`, and requires equal trees, manifests included. [[crates/datafeed/tests/support/capture_tree.rs#assert_manifests_verify]] checks each listed file against its sha256 and its record count, and that no capture leaf sits outside a manifest. The leaf then replays through the adapter and its digests must equal the `nasdaq-orderbook-reference` lines of [[tests#Seam parity gate]], so a binary that dropped or reordered frames fails even when it matches the library.

### Binary capture of the prefix matches the baseline

The NASDAQ ITCH prefix, many 64-frame batches long, goes through the binary and its capture matches the `nasdaq-itch-prefix` digest lines, which the one-batch reference fixture cannot show.

[[crates/datafeed/tests/binary_deploy.rs#binary_capture_of_the_itch_prefix_matches_the_seam_baseline]] is `#[ignore]` like its `seam_parity` twin and needs `NASDAQ_ITCH_PREFIX` set to the venue crate's prefix fixture. It requires exit status 0, verified manifests, and the baseline digests from a replay of the captured leaf.

### SIGINT during a replay changes nothing

A replay ignores the stop flag, so a SIGINT in the middle of one neither kills the process nor shortens the capture.

[[crates/datafeed/tests/binary_deploy.rs#sigint_during_a_replay_leaves_the_tree_of_an_undisturbed_run]] is unix-only. It paces a replay in real time at 100 ms per frame, waits one second, checks the child is still running, sends SIGINT, and requires exit status 0, a tree equal to an unpaced library run, and verified manifests. A change that lets a replay stop on SIGINT fails it on purpose: amend it together with the replay contract.

## Live shutdown

A live run whose stop flag is set mid-run ends, and its capture tap is finished with a manifest. The binary cannot be driven live, because NASDAQ declares no live source.

[[crates/datafeed/tests/live_shutdown.rs#stop_mid_run_finishes_the_tap_and_writes_the_manifest]] parses a live JSON config, swaps in a test-only descriptor with `soupbin_live: true`, and runs [[crates/datafeed/src/app.rs#run_config]], the call `main` makes, on a thread against an in-process SoupBinTCP server that sends a system event and an add and then holds the connection open. It sets the flag once the capture file exists, requires `Ok`, and requires one manifest-listed file whose sha256 and rows verify. A `run_config` that does not pass the flag to a live session never returns and fails on the deadline, and one that drops the session without `close` leaves no manifest.

## C ABI from a C host

`polaris-datafeed-ffi` checks its C ABI ([[c-abi]]) from a real C program and from Rust: the committed header, a C harness against the cdylib, and the entry points' NULL, refusal and panic paths.

### Header drift

`tests/header.rs` runs cbindgen with the crate's `cbindgen.toml` over `src/lib.rs` and `polaris-plugin`'s `src/abi.rs` and requires the output to equal the committed `include/polaris_abi.h` byte for byte.

A changed signature, constant or doc comment without a regenerated header fails it. `POLARIS_ABI_HEADER_WRITE=1` makes the test write the header first, for the commit that changes the ABI.

### C harness

`tests/c_host.rs` builds C with the platform compiler that the `cc` crate finds, warnings as errors, `-std=c99 -pedantic` on GCC and Clang and `/std:c11` on MSVC, which has no C99 mode.

`header_compiles_alone_under_strict_c` compiles `tests/c/header_only.c`, which includes only the header, so a header that depends on an include from its includer fails. Its one function uses the plugin ABI as a host does: it takes a `polaris_adapter_entry_fn`, checks `abi_version`, then calls `create` and `drain` through the vtable and reads a record's `polaris_record_header`. The header's record header size check runs in the same compile, and a size other than 56 fails it ([[plugin#Header]]). `c_host_reads_the_records_rust_polls` links `tests/c/harness.c` against the cdylib cargo builds for the test's library dependency, runs it on the NASDAQ venue crate's committed ITCH prefix fixture (14,348 records), and compares each record's stream and bytes, in order, with what `FeedSession::poll` hands a Rust host over the same config. The harness first compares `polaris_abi_version()` with the header's `POLARIS_ABI_VERSION` and exits 1 when they differ. It polls 7 records at a time, so records carry over between polls, and it must end on `POLARIS_FEED_END` with close returning `POLARIS_OK`. Cargo 1.96 puts that cdylib beside the test binary in `deps/`. Cargo 1.100, the CI stable toolchain, gives every build unit its own `build/<package>/<hash>/out/`, and the test takes the newest cdylib among the package's units. Both tests run on Linux, macOS and Windows; on Windows the test puts the DLL's directory on `PATH`, elsewhere the harness is linked with an rpath to it.

### Entry points from Rust

The library tests in `src/tests.rs` call the `extern "C"` functions directly, which also lets CI run them under Miri.

`null_pointers_return_the_null_status` passes NULL for each pointer argument, on an open handle where one is needed, and expects `POLARIS_ERR_NULL`, a NULL handle after a failed open, and a no-op close of NULL. `open_returns_the_status_of_each_refusal` expects `POLARIS_ERR_CONFIG` for a non-UTF-8 config, invalid JSON and an unknown venue, and `POLARIS_ERR_SOURCE` for a replay file that does not exist. `a_caught_panic_poisons_the_handle` polls an open handle once, panics inside the handle guard that every poll goes through, and expects `POLARIS_ERR_PANIC` from the guard, from the next poll and from close. Without `catch_unwind` the panic fails the test, and without the poison flag the poll returns records.

## Unit tests on the NASDAQ fixture venue

Unit tests under `src/` that need a venue run on the `NASDAQ` descriptor, or on a test-only descriptor built from it when they need a property NASDAQ lacks.

- `tap.rs`: timestamps count from a literal New York midnight of 2026-05-21 (04:00 UTC), so the session code under test does not compute its own expected values, and leaves are `nasdaq/v5/<date>/nasdaq_v5_<window>.mdc1.zst`. `session_day_advance_rolls_dir_and_flushes_manifest` puts two frames in one UTC day and two New York sessions, so a roll on the UTC date would not fire. `earlier_session_day_rejected` expects `DayRegression`. `zero_date_adopts_first_frame_session` uses a frame at 02:00 UTC whose session label is the previous date. The continuous-session and unknown-zone tests build their descriptors with `..NASDAQ`.
- `app.rs`: `NASDAQ_NEXT` (same venue id, version `v6`, the next feed id) gives the reconcile and the path-name guard a second feed to reject, and `FIXED_UTC9` (`RollModel::DailyFixed`, 9 h offset) gives the session-midnight derivation a fixed-offset venue.
- `live.rs`: `NASDAQ_SOUPBIN` sets `soupbin_live: true`, so `live_dial` returns a dial and `live_session_refuses_a_set_session_anchor` reaches the anchor check instead of the unsupported-source refusal. `live_session_polls_and_closes_inside_a_host_runtime` builds, polls and closes a live session inside a `#[tokio::test]` runtime with the stop flag already set, so the dial starts on the session's runtime and ends at once; a dial on the caller's thread or a blocking runtime drop panics it.
- `wiring.rs`: `build_tap_stamps_venue_feed_into_header` configures a descriptor whose feed id is `FEED_NASDAQ + 1` and reads that id back from the header, so a tap that stamped a fixed id fails. Its frame at 00:00 UTC belongs to the previous New York date, which the expected dir name shows.
- `replay.rs`: `peek_capture_meta_reads_header_venue_date_and_feed` expects `FEED_NASDAQ`. `capture_row_on_undeclared_stream_fails_replay` writes three system events with the third on stream 1 and expects `SourceError::UndeclaredStream` naming that stream, the two rows before it captured, and a manifest entry of 2 records.
- `config.rs`: `venue_names_resolve_to_on_disk_identity` pins `("nasdaq", "v5", 3)` and rejects an unknown name.
- `session/tests.rs`: frames come from the file's NASDAQ add and replace builders, capture leaves sit under `nasdaq/v5/`, and the anchor roll cases use New York anchors resolved from the descriptor.

No venue adapter in this workspace emits `Control::RecoveryComplete` or decodes two streams, so NASDAQ cannot show a recovery sequence reaching `RestartState` or one session feeding two streams. [[tests#Two-stream test adapter]] checks both through the session runner.

## Two-stream test adapter

Per-stream feed and publish, and the path from a decoded `Control::RecoveryComplete` to `RestartState`, run through a test-only adapter. NASDAQ declares one stream and never emits that control.

[[crates/datafeed/src/session/tests.rs#TwoStreamAdapter]] implements `md_pipeline::DynAdapter`, the trait [[crates/datafeed/src/session.rs#run]] drives, and declares streams 0 and 1. Its wire frame is one 8-byte big-endian resume sequence. A frame on a declared stream decodes to one `Control` of kind `RecoveryComplete` whose `aux` is that sequence, stamped with the frame's `ts_recv` and `venue_seq`, and counts in `AdapterStats::decoded`. A frame of another length counts as `malformed`. A frame on any other stream is ignored and moves no counter, which `VenueAdapter::feed` requires of every adapter ([[pipeline#Venue adapter seam]]).

### Per-stream feed and publish

Three frames on streams 0, 1 and 2 in one batch publish two `Control` records, on streams 0 and 1 with their own `venue_seq`. A runner that feeds or publishes on one fixed stream fails the test.

[[crates/datafeed/src/session/tests.rs#each_frame_feeds_and_publishes_on_its_own_stream]] also expects `stats.frames` of 3 and two decoded frames in the adapter stats. With every frame fed on stream 0, the adapter decodes the third frame too and a third record publishes on stream 2.

### Recovery sequence reaches the restart state

One frame that decodes to `Control::RecoveryComplete` leaves its resume sequence in `RestartState` once the session has routed it.

[[crates/datafeed/src/session/tests.rs#decoded_recovery_sequence_reaches_restart_state]] compares the whole `RestartState`: `glimpse_resume_seq` holds the frame's sequence and the three socket resume points stay unset, since those come from a closed source ([[session#Restart and resume seam]]). The test fails when the session publishes the `Control` without handing it to `RestartState`.

### Adapter fault ends the session

An adapter that reports a fault ends the run with `SessionError::Adapter`, after the records drained before the fault in the same batch have published.

[[crates/datafeed/src/session/tests.rs#adapter_fault_ends_the_session_after_routing_earlier_frames]] sets the adapter's `fault_after` to 1 and puts three frames in one batch: the first decodes, the second faults the adapter as a plugin panic does ([[plugin#Host wrapper#Faults into the session]]), and the third reaches a faulted adapter that ignores it. It expects `AdapterFault::Panicked` and one published record. A runner that never checks `DynAdapter::fault` returns `Ok`, and one that returns the fault before routing the batch publishes nothing.

## Bench timing

`tests/bench_regression.rs` checks the committed baseline of the NASDAQ pipeline bench against its manifest and, when a fresh run is present, compares that run with the baseline.

[[crates/datafeed/tests/bench_regression.rs#nasdaq_bench_baseline_matches_its_manifest_and_gates_fresh_runs]] reads JSON files under the crate and needs no capture and no feature, so it runs in the normal suite. [[session#Pipeline stage observability#NASDAQ pipeline bench]] describes the bench and its report.

It reads three files:

- `benches/baseline/manifest.json` names what a baseline is valid for (`capture.sha256`, `capture.max_messages`, `baseline_host`) and the `tolerance` values.
- `benches/baseline/nasdaq_metrics.json` is the committed run.
- `benches/results/nasdaq_metrics.json` is a fresh run. Git ignores the directory, so the file exists only where someone has run the bench.

On every run the baseline must have the manifest's sha256, message cap and host label, so a baseline or manifest refreshed without the other fails. Its `capture.path` must be relative: the bench stores `NASDAQ_ITCH_SAMPLE` as given, and an absolute path in a committed file would publish a local directory name. CI has no fresh run and stops there. With a fresh run present:

- Another sha256 or cap fails the test, and so does a message count that differs from the baseline's: the runs replayed different inputs and their timings cannot be compared.
- A host label other than `baseline_host` passes, with a printed line that the timing comparison was skipped. The label is the `BENCH_HOST` of the run, compared as a string.
- On the baseline host, [[crates/datafeed/tests/bench_regression.rs#timing_regressions]] collects every timing outside its tolerance and the test fails with the whole list. The p50 and p99 of feed, book apply, book derive and egress each have a ceiling: the baseline plus `stage_p50_pct` or `stage_p99_pct` percent, or plus `stage_slack_ns`, whichever is higher. The decode mean has a ceiling of `stage_p50_pct` percent without the slack, and throughput has a floor `throughput_pct` percent under the baseline.

The test reads the fields it compares by name. A report's `layout` and window means ([[session#Pipeline stage observability#NASDAQ pipeline bench#Stack position and window means]]) are not compared, and the committed baseline has neither.

The committed baseline was recorded on `m4pro-macos` (Apple M4 Pro) from the first 100,000,000 messages of the 2020-01-30 capture, the day of [[tests#Order book reference parity]]: 2.41M msgs/s, a decode mean of 5.6 ns, and p50/p99 of 84/333 ns for feed, 83/667 ns for book apply, 42/250 ns for book derive and 1/42 ns for egress.

The tolerances are 50 percent for a stage p50, 60 percent for a stage p99, 30 percent for throughput, and a slack of 43 ns. They were chosen for that host from eight runs on 2026-11-18, four of them while a backup and other system work kept the host busy. Its monotonic clock ticks every 41.67 ns, so a stage percentile is a whole number of ticks, and one tick is 50 percent or more of every stage p50. The slack lets a percentile rise by one tick where the percentage would allow less. The report holds whole nanoseconds, so the same tick count reads 1 ns apart between samples (41 or 42 ns for one tick, 83 or 84 for two): a rise of one tick reads as 41 to 43 ns, which is why the slack is 43, and a rise of two ticks reads as 82 ns or more. Across the eight runs throughput ranged from 2.03M to 2.45M msgs/s (17 percent) and the decode mean from 5.56 to 5.75 ns. The feed p50 was 84 ns on the calmer host and 125 ns on the busy one, and the other p50s did not move. The p99s ranged from 292 to 375 ns for feed, 625 to 750 ns for book apply and 250 to 291 ns for book derive. For a stage p50 the tolerance allows the one-tick step measured and no second one. The p99 and throughput percentages leave about twice the spread measured, because one morning of runs does not bound the noise of a laptop that runs other work. All eight runs pass against the committed baseline. The manifest's `note` records the same numbers.

The crate README has the bench command and the steps to refresh the baseline.
