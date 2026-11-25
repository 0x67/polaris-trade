# binance

Binance Spot venue crate `crates/venues/binance` (package `polaris-venue-binance`, lib `venue_binance`): a hand-written [[pipeline#Pipeline boundary#Venue adapter seam|venue adapter]] over the public websocket feed and the REST depth snapshot, with snapshot resync.

Nothing here comes from [[codegen]]. The wire is JSON, so the crate parses with `serde_json`, borrowing the symbol from the frame. One adapter serves one symbol, and a frame that names another symbol counts as `malformed`.

## Streams

Two ingest streams: `MARKET_STREAM` (id 0) for websocket events and `SNAPSHOT_STREAM` (id 1) for REST depth bodies. The crate has no HTTP client, so the caller fetches the snapshot and feeds the body as a frame.

`BinanceAdapter::needs_snapshot` is true while the adapter holds diffs back for lack of one: once the first diff is buffered, after a gap, and after a refused snapshot. It is false at the start, so the caller follows Binance's order: open the websocket, buffer, then fetch `GET /api/v3/depth` when it turns true. Market frames are raw stream payloads, not the combined-stream wrapper.

## Mapping

- `depthUpdate` becomes `Level` records, a bid and an ask row set per event. A size of zero is `LevelAction::Delete`, any other size `LevelAction::Change`, so the book engine replaces the level instead of adding to it. `ts_event` is the event time `E`.
- `trade` becomes `Trade`: `t` is `trade_ref`, `T` is `ts_event`, and `m` true (the buyer was resting) makes the seller the aggressor, `Side::Ask`.
- A REST body becomes the snapshot sequence below. It has no time, so every record takes `ts_recv` as `ts_event`.
- Any other event, a command reply, or a REST error body counts as `unknown_kind`.

The session is `RollModel::Continuous` with `ClosureModel::None`. The `Definition` is `AssetClass::Spot` with `qty_scale` `-8`, minted on the first record. `record_types` lists `Level`, `Trade`, `Control` and `Definition`.

## Resync

The adapter follows Binance's procedure for a local order book: buffer diffs, join a snapshot, check that update ids continue, and start over on a skip.

Diffs arriving before a snapshot wait in a buffer of at most 8192, oldest dropped first. A snapshot with `lastUpdateId` discards buffered diffs whose last id it covers. If the first remaining diff starts past `lastUpdateId + 1`, the snapshot is too old: nothing is emitted, the buffer stays and `needs_snapshot` stays true. A snapshot fed while no diff is buffered has nothing to check against and is ignored, so a reply that lands before the first diff never publishes an unverified book.

Otherwise the adapter emits `Control{SnapshotBegin}`, one `Level{Clear}` with side none, the snapshot levels as `New` flagged `SNAPSHOT` with `level` the zero-based depth (saturating at 255, the width of the field, so a snapshot deeper than 256 levels marks every level past it 255), and `Control{SnapshotEnd}`; `aux` of both controls is `lastUpdateId`. The buffered diffs then replay through the live rule, each with its own `ts_recv` and `venue_seq`.

Live, a diff whose last id is at or below the book's is ignored, one whose first id is past the book's last id plus one emits `Control{GapDetected}` with `aux` the count of missing ids, and any other applies. After a gap the adapter buffers again, starting with the diff that showed the gap, and runs the sequence on the next snapshot. Trades keep flowing throughout. A snapshot fed while live replaces the book the same way, unless its `lastUpdateId` is below the book's: a late or retried reply is ignored so it cannot roll the book back.

Gap and snapshot events go through `tracing`: a `warn` on a skip and an `info` when a snapshot applies. Nothing logs per message.

## Fixed point

Prices go through `Price::try_from_venue` at `1e-9` and sizes become `u64` at `1e-8`; no value touches `f64`.

A value outside its range, finer than its grid, negative, or not a plain decimal fails the whole frame as `malformed`. Digits past the grid are accepted only when zero. A malformed frame changes no state, the update id sequence included.

## Gaps in the ingest sequence

`venue_seq` is whatever the source stamps, per stream. A skip flags `GAP_SUSPECT` on every record of the next frames on that stream until one emits a record.

A buffered diff keeps the flag it was stamped with and replays flagged, but buffering does not spend the skip, so a snapshot that drops the diff cannot lose it.

## Tests

The contract suite, a recorded session, and the resync and mapping cases below pin the adapter. A criterion bench measures decode.

### Adapter contract

A snapshot, a diff and a trade, plus an unknown frame on each stream, run through `assert_adapter_contract`: counters, drops, the gap flag and the declared record types.

### Recorded session parity

Frames recorded from the public feed on 2026-11-25 in `tests/fixtures/session` must produce the committed records.

The session holds two diffs before the REST snapshot (both covered by it), the snapshot, then diffs and trades. No frame replays a buffered diff, so the join of a snapshot inside a diff's range is pinned by its own case below. `ts_recv` and `venue_seq` in `frames.txt` are local receive times and per-stream counters, not wire fields.

### Gap resync matches an uninterrupted book

The recorded session with one diff removed emits `GapDetected` with the missing id count, keeps passing trades, and joins the later diffs to a second snapshot. Through the book engine, the book equals the uninterrupted session's, level for level.

The test writes the second snapshot from the recorded diffs instead of fetching it, because only one REST body was recorded. That snapshot holds every level the diffs set, deeper than a `limit=20` refetch would, so the equality shows the join and replay logic, not that depth-limited bodies resync to the full book. The ruling is recorded in a decision.

### Stale snapshot is refused

A snapshot older than the buffered diffs emits nothing and leaves `needs_snapshot` true. A later snapshot that joins them replays the diff after it.

### Snapshot before any diff

A snapshot fed before the first diff is buffered emits nothing and leaves `needs_snapshot` false. After a diff arrives, a snapshot that joins it emits the sequence with no `GapDetected`.

### Snapshot inside a diff range

A snapshot whose `lastUpdateId` falls inside the first buffered diff's id range joins that diff, which replays whole, and the next diff applies without a gap.

### Old snapshot while live

A snapshot with a `lastUpdateId` below the live book's is ignored, and the next diff applies without a `GapDetected`.

### Dropped diff keeps the ingest gap flag

A `venue_seq` skip carried by a buffered diff that a snapshot then drops flags the next record the stream emits, and only that frame.

### Buffer is bounded

After 8200 buffered diffs a snapshot older than the oldest kept diff is refused, which an unbounded buffer would accept.

### Mapping rules

Zero sizes become `Delete`, other sizes `Change`, decimals scale to the fixed-point grids, and `m` picks the aggressor side.

### Bad frames change nothing

Out-of-range, too-fine, negative, reversed-id and wrong-symbol frames emit nothing, count as malformed, and leave the next diff applicable.

### Symbol spelling

`Symbol::new` accepts only 1 to 20 uppercase ASCII letters and digits, the spelling of event payloads.

## Bench

`benches/decode.rs` feeds one recorded trade, snapshot and diff through `feed` and drains the output. Run it on AC power only.
