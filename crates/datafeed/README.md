# datafeed

Package `polaris-datafeed` builds the `datafeed` binary and the `datafeed`
library behind it. It runs the market-data normalization layer: ingest venue feeds,
adapt to normalized events, reconstruct books, derive views, and publish over
pluggable transports. One binary across managed and self-hosted deployments,
separated by config and output wiring.

Wires the workspace's pipeline, order-book, replay and capture crates, the
NASDAQ venue adapter (behind its `adapter` feature), the SoupBinTCP client and
the socket transport. NASDAQ live is rejected until a real MoldUDP subscription
is implemented.

Architecture notes are in `lat.md/datafeed/` at the repository root.

## Build and run

Commands run from the workspace root.

```bash
cargo build --release -p polaris-datafeed
cargo run --release -p polaris-datafeed -- crates/datafeed/config.yaml
```

The built binary is `target/release/datafeed`:

```
datafeed <config.yaml|config.json> [--ohlcv-resolutions=1s,5m,1h]
datafeed --version
```

Add `--features observability` to compile in the per-stage timers and the
metrics exporters. A config with an `observability` block fails at startup in a
build without the feature.

## Embedding the library

The `bin` feature is on by default. It holds everything that touches process
state: the SIGINT and SIGTERM handlers, the `tracing` subscriber and the
metrics recorder and exporter. Only the binary uses it. A host that embeds the
library builds with `default-features = false`, installs its own signals,
subscriber and recorder, and passes its own stop flag to
`FeedSessionBuilder::stop` or `app::run_config`.

```bash
cargo nextest run -p polaris-datafeed --no-default-features
```

## Configuration

`config.example.yaml` documents the venue, live/replay, container, loading,
pacing, sink, capture, and SoupBinTCP options in one commented template. Copy it
to `crates/datafeed/config.yaml`, select one alternative per field, fill in
paths, then run the command above. Relative paths in the config resolve against
the directory the binary starts in.

The parser is chosen by file extension: `.yaml`/`.yml` is YAML (the default, so
comments live beside each option), `.json` is strict JSON, and any other
extension falls back to YAML (a JSON superset). `config.yaml`, `config.yml` and
`config.json` are gitignored inside `crates/datafeed/`, so a local config with
live endpoints or SoupBinTCP credentials placed there stays out of git; the
`config.example.*` template stays checked in. A config kept anywhere else in the
repository is not ignored.

### Capturing raw data

Normalized records go to the in-process `sink`: `null` discards them and
`collecting` keeps them in memory. Nothing writes them to disk. The raw capture
is the replay source of truth, so replaying it through the adapter reproduces
the normalized stream. To capture a live session, set `taps.raw_dir` (a bucket
root directory, not a file) beside a live `source` block from
`config.example.yaml`:

```yaml
taps:
  raw_dir: ./out/raw
  partition: 1d              # one file per trading session
```

The tap lays out `<venue>/<version>/<YYYY-MM-DD>/` under the root and derives
`<venue>_<version>_<window>.mdc1.zst` leaf names, so a NASDAQ session writes
`./out/raw/nasdaq/v5/2026-05-21/nasdaq_v5_2026-05-21.mdc1.zst` (an MDC1 zstd
container) and a `manifest.json` beside it. The date is the venue session of
the first frame received, so leave `taps.capture_date` unset on live: a set
value fails at startup, since it would go stale once a restart lands on a later
day. On replay the source file already is
the raw feed, so leave `raw_dir` unset. `taps.normalized_dir` was removed with
normalized capture; a config that still sets it fails at startup. NASDAQ live is
rejected until a real MoldUDP subscription exists, so no built-in venue runs
live today.

`out/` is gitignored at the workspace root and inside `crates/datafeed/`, so the
`./out/raw` root above stays out of git from either start directory. A capture
root anywhere else in the repository is not ignored.

## Tests

```bash
cargo nextest run -p polaris-datafeed
cargo nextest run -p polaris-datafeed --features observability
```

The tests that need local data are `#[ignore]`d and take their inputs from the
environment. A test runs with `crates/datafeed/` as its working directory, so
give absolute paths.

Seam parity on the ITCH prefix fixture of the NASDAQ venue crate:

```bash
NASDAQ_ITCH_PREFIX="$PWD/crates/venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin" \
  cargo nextest run --release -p polaris-datafeed --test seam_parity --run-ignored all
```

The next commands read a full-day NASDAQ TotalView-ITCH 5.0 sample
(`NASDAQ_ITCH_SAMPLE`, a length-prefixed dump as `.bin.zst` or `.gz`) and, for
the parity gate, a reference order-book directory (`ORDERBOOK_REFERENCE_DIR`).
Neither is in the repository.

Determinism over the full day (two replays, per-rtype digests compared):

```bash
NASDAQ_ITCH_SAMPLE=/path/to/nasdaq_v5_2020-01-30.bin.zst \
  cargo nextest run --release -p polaris-datafeed --test determinism \
  --run-ignored ignored-only --no-capture
```

Full-day order-book parity against the reference. It prints a progress line
every 10M frames. The first run hashes the reference files once and writes a
`.verified-<sha256>` marker next to them:

```bash
NASDAQ_ITCH_SAMPLE=/path/to/nasdaq_v5_2020-01-30.bin.zst \
ORDERBOOK_REFERENCE_DIR=/path/to/reference-orderbook/2020-01-30 \
  cargo nextest run --release -p polaris-datafeed --test orderbook_reference_parity \
  --run-ignored ignored-only --no-capture \
  -E 'test(full_day_nasdaq_replay_matches_reference_l1_l2_l3)'
```

For a bounded smoke run of the same test, set `PARITY_MAX_MESSAGES=10000000` on
that command. It stops after that many frames.

The throughput probes run a decode-only drain, then the session behind a no-op
tap. `PARITY_MAX_MESSAGES` bounds them, `PARITY_BATCH` overrides the 512-frame batch,
and `PARITY_OHLCV_RESOLUTIONS=1s,1m` narrows the session probe's OHLCV set:

```bash
NASDAQ_ITCH_SAMPLE=/path/to/nasdaq_v5_2020-01-30.bin.zst PARITY_MAX_MESSAGES=10000000 \
  cargo nextest run --release -p polaris-datafeed --test orderbook_reference_parity \
  --run-ignored ignored-only --no-capture \
  -E 'test(full_day_nasdaq_decode_only_drain) | test(full_day_nasdaq_session_noop_tap)'
```

## Bench

`benches/nasdaq_pipeline.rs` replays the first 100,000,000 messages of a
full-day NASDAQ ITCH 5.0 capture three times: decode alone, the session with
its stage timers off (throughput), and the session with them on (feed, book
apply, book derive and egress latency). It needs the `observability` feature
and a capture that is not in the repository, so CI does not run it.

```bash
NASDAQ_ITCH_SAMPLE=/path/to/nasdaq_v5_2020-01-30.bin.zst BENCH_HOST=<label> \
  cargo bench -p polaris-datafeed --features observability --bench nasdaq_pipeline
```

A run takes two to three minutes on the baseline host and writes
`crates/datafeed/benches/results/nasdaq_metrics.json`, which git ignores.
`BENCH_MAX_MESSAGES` changes the message cap, `BENCH_CPU` adds a CPU name to the
report, and `BENCH_OUT` moves the report. A relative `NASDAQ_ITCH_SAMPLE`
resolves against `crates/datafeed/`, and the report records the path as given.

The speed of a run depends on where the process stack starts. On the baseline
host (Apple M4 Pro, 16 KiB pages) a 16-byte access to a stack slot is slow when
a page boundary falls in its middle. `book_derive` keeps its two depth arrays on
the stack, with 24-byte rows written by 16-byte stores. The book derive stage is
therefore slow at ten stack positions 48 bytes apart in every 16,384 bytes:
about 65 ns per segment against 29 at five of them, and 32 to 37 ns at the other
five. Other frames on the per-message path have accesses of the same kind at
positions of their own, some of them outside every timed stage. The stack
pointer at `main` moves 16 bytes for each 16 bytes of environment and arguments,
and two runs with a byte-identical environment can still start 16 bytes apart.
The report therefore records two addresses of its run and the mean of each
window of samples:

- `layout.stack_low14` is the low 14 bits of the address of a local in `main`.
  `layout.adapter_low14` is the low 14 bits of the address of the boxed adapter
  in the instrumented pass, a heap address. On the baseline host address
  randomization moves the stack by whole 16 KiB pages, so it leaves the low 14
  bits alone.
- Each stage has `window_samples` (1,000,000) and `window_means_ns`, the mean
  of each full window of that many samples in arrival order. A partial last
  window is left out. When a stage changes speed partway through a run, these
  means show a step.

To compare two builds, start both with the same small environment (for example
`env -i` with the bench variables alone), check that their reports show the same
`layout.stack_low14`, and keep that value where every frame from `main` down
lies inside one page: about 5,300 to 14,400 on the baseline host. In a run
capped at 10,000,000 messages a `stages.book_derive.p50_ns` of 41 goes with a
level derive stage and 42 or more with a raised one: p50 and p90 read 83 or 84
at the five slow positions and p50 reads 42 at the other five. The stage figures
cannot rule out a position on their own, because part of the affected code runs
outside the timed stages, so compare throughput over several runs as well.

`tests/bench_regression.rs` compares a run with the committed baseline. It is
part of the normal suite:

```bash
cargo nextest run -p polaris-datafeed --test bench_regression
```

Without a fresh run it checks that `benches/baseline/nasdaq_metrics.json` has
the capture sha256, message cap and host that `benches/baseline/manifest.json`
names, and that its `capture.path` is relative. With `benches/results/nasdaq_metrics.json` present, a run from another
capture or with another cap fails, and a run whose `BENCH_HOST` differs from the
manifest's `baseline_host` passes with a line saying the timing comparison was
skipped. A run from the baseline host fails when a stage p50 or p99, the decode
mean or the throughput is outside the manifest's `tolerance`.

To refresh the baseline:

1. Close other heavy work on the baseline host, and run no other cargo job
   during the bench.
2. Run the bench command with the manifest's capture and
   `BENCH_HOST=<baseline_host>`. Give `NASDAQ_ITCH_SAMPLE` as a relative path
   for this run: the report stores it in `capture.path`, the baseline is
   committed, and an absolute path would publish a local directory name. The
   test fails on a baseline whose `capture.path` is absolute.
3. Copy `benches/results/nasdaq_metrics.json` over
   `benches/baseline/nasdaq_metrics.json`.
4. For a new capture or cap, set `capture.sha256` and `capture.max_messages` in
   the manifest to the values in the report. For a new host, set
   `baseline_host` and choose the tolerances again from at least three runs.
5. Run the test command above.

`benches/feed_session.rs` times the two feed session handoffs. It replays the
vendored order-book reference fixture, repeated 1,000 times, through
`session::run`, which publishes in place, through a `SinkSession`, the binary's
path, and through `FeedSession::poll` with a host loop that publishes each
polled record. It prints the median ns per record of each pass and the median
paired difference of each session against `session::run`. It needs no capture
and no feature:

```bash
cargo bench -p polaris-datafeed --bench feed_session
```

`BENCH_ROUNDS` sets the number of rounds per pass (33 when unset). It has no
baseline or regression test.

## License

MIT OR Apache-2.0, at your option.
