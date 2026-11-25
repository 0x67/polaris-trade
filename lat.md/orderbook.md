# Order book engine

The `polaris-orderbook` crate reconstructs books from normalized events and derives views. The same code path runs vendor-side and downstream, so reconstructions stay bit-identical. Venue behavior enters only through a policy trait.

## Book mechanics

A single order map keyed (instrument, side, order_id) collapses every venue's id scoping; a per-side price-level container answers best-of-book. See [[crates/orderbook/src/engine.rs#BookEngine]].

Resting orders live in pooled reusable slots. A separate key-to-slot map finds them, while intrusive `prev`/`next` links form each price level's time-priority chain. The chain's head and tail slots are folded into the level container beside the aggregate, so one price lookup yields both depth and queue endpoints and there is no separate per-level queue map. Modify unlinks and appends at the tail with a fresh sequence, making priority loss observable through `queue_position`. Links are 4-byte NonZero slot indices (plus-one encoding) and freed slots keep stale bytes rather than an occupancy tag, pinning a pooled order at 32 bytes, two per cache line on the unlink chase; a layout test guards the size.
The read-only `prefetch_mbo` probe on [[crates/orderbook/src/engine.rs#BookEngine]] warms an event's slot-map bucket, pooled order, and level entry so sessions can issue it a few records ahead of the apply and pay those misses behind other work.

Non-printable executions mutate the book but stay off the tape. Reset clears an instrument atomically and flags records snapshot-derived until live handoff. A `Level` with `LevelAction::Clear` and `Side::None` goes through the same clear: both sides and every resting order of the instrument go, so the `Action::Reset` record the derive engine builds from it has zero depth rows. With `Side::Bid` or `Side::Ask` it empties that side only, its levels and its resting orders, and the derived record still carries `Action::Reset` with the other side's depth. A resync (clear, then the fresh ladder) leaves the same book and derived depth as a build from that ladder alone. Price limits are published as stats, never enforced by the engine.

The pre-book gate's three [[crates/orderbook/src/policy.rs#Gate]] outcomes are: `Drop` refuses the event, `Accept` applies it normally, `Flag` applies it through the same path as `Accept` but marks `BookOutcome.suspect`, for the caller to OR the `SUSPECT` provenance bit onto the header. Tape eligibility is gated on `ApplyResult::Applied`, so a fill against an order id the book never held (`UnknownOrder`) never reaches the tape even when the policy would otherwise allow it.

## Policy seam

All venue-specific behavior is a hook on [[crates/orderbook/src/policy.rs#VenuePolicy]]: the pre-book gate, board-to-purpose routing, the tape gate, and trade-break scope. The mechanical core carries zero venue conditionals, so adding a venue never edits engine internals.

## Level container

The price-level container is chosen by benchmark: a sorted struct-of-arrays and a BTree variant both live behind [[crates/orderbook/src/level.rs#LevelBook]] until measured. SoA is the current default; the two agree on every result and differ only in cost.

Each level carries its resting order count alongside aggregate quantity, so mbp-10 depth reads `(price, qty, count)` straight off the book with no second aggregation. `add` joins one order (count +1), `reduce(remove_order)` drops one when it fully leaves (a partial fill leaves the count), and `top_n` returns the top rows highest-bid / lowest-ask first. `BookEngine` exposes this depth through [[crates/orderbook/src/engine.rs#BookDepth]].

Each level also carries its FIFO chain endpoints (head/tail slots), read and written by price through `queue_ends` / `set_queue_ends`, so the engine finds a level's time-priority chain with the same lookup that finds its aggregate. The container owns only the two endpoints; the engine still owns the per-order `slots` links. Endpoints vanish with the level when `reduce`/`set` drops it at qty 0, so no stale endpoint outlives its aggregate.

Adapter coverage is tracked separately from book handling by the pipeline `CoverageMatrix`, so "decoded" is never conflated with "handled".

## Hasher and storage benchmark

The book engine accepts a caller-selected `BuildHasher` for its order and level maps; `FxBuildHasher` is the default selected by the local release benchmark, while `RandomState` remains available for comparison.

The derive engine keeps only the running vwap map, so its hasher no longer touches any book path.

On the 2026-07-13 M4 Pro run, `book_ops` measured std/SoA 136.7 ns, std/BTree 155.6 ns, Fx/SoA 43.3 ns, and Fx/BTree 40.4 ns per loop iteration. The existing SoA default remains locked by its broader container benchmark; this matrix does not reopen that decision. `derive_ops` was since restructured to time book apply plus depth read off the book, superseding the earlier derive-only hasher numbers.

The same `book_ops` workload measured the Fx pooled-slot plus priority-chain path at 43.3 ns versus 18.5 ns for the old plain-map shape. The chain is retained for observable matching priority and slot reuse; the measured cost is documented rather than hidden.

The atomic replacement benchmark applies a full cancel-plus-add source batch to the book, then derives once off it. `derive_ops` now times that combined book-apply-plus-depth-read path; its numbers are re-measured on the perf box rather than carried from the earlier derive-only shape.

## Status machine

[[crates/orderbook/src/status.rs#StatusMachine]] tracks each instrument's current trading state and classifies every applied [[crates/orderbook/src/status.rs#Transition]] (halt entry/exit, session open/close) over the superset `TradingState` enum.

The machine covers three surveyed venue encoding families generically, without a venue conditional: single numeric code, multi-state name-string, and state-plus-reason (market-wide or single-stock circuit breaker). Venue reason detail is never stored here; it rides the wire `Status` record's `venue_reason` field untouched.

## Derive engine

[[crates/orderbook/src/derive/mod.rs#DeriveEngine]] computes mbp-10 depth, bbo, and running vwap for the normalized `Mbo`/`Level`/`Trade` record stream. Depth and bbo are read from the caller's already-applied `BookEngine` through [[crates/orderbook/src/engine.rs#BookDepth]], not a second copy of the ladder.

Only the running vwap lives in the derive engine, since it has no home in the book. The caller applies the event to the book first, then derives.

Board-to-purpose routing gates participation: only an instrument whose board carries the `BOOK` purpose feeds depth/bbo, and only `TAPE` feeds the running vwap. [[crates/orderbook/src/derive/ohlcv.rs#OhlcvEngine]] builds multi-resolution OHLCV bars from the same trade stream, gated by the `OHLCV` purpose. Each emitted bar carries its own `resolution` (bucket width in nanoseconds) so it self-describes its interval on the shared `rtype 0x16` stream, where 1m and 1h bars are otherwise indistinguishable. A bar closes on a monotonic event-time watermark (the max `ts_event` seen across the stream): once the watermark passes a bar's boundary it closes, so a bar with no following trade still closes (`advance`), and a close may fire on a foreign instrument's event; `flush_all` closes any still-open bars at stream end. Never wall clock, so replay stays deterministic. The closed record's `ts_event` is the interval open (`bucket_start`), its `ts_recv` the watermark at close. A `TradeBreak` reverses a bar's volume/value/frequency by `break_ref` against retained per-trade history, but never its open/high/low/close.

A depth level with no resting orders on that side pads its price to the normalized null sentinel (`i64::MAX`), never a real price of `0`; size and count stay `0`. `DeriveEngine` reverses its own running vwap accumulator on a trade break too (`apply_trade_break`, caller-supplied price/qty, mirrors `OhlcvEngine`'s pattern) since it keeps no per-trade memo of its own.

`OhlcvEngine` retires a trade's retained history entry once every resolution's bucket it fed has closed naturally (a reverse index from bucket to feeding trades, decremented per close), not only on a trade break. Without this a full session leaks one entry per trade forever; a trade feeding a still-open coarser resolution stays reversible until that bucket closes too.

### Atomic MBO batches

One wire event may normalize into multiple order mutations, so derivation observes the whole source batch before emitting a view.

`crates/orderbook/src/derive/mod.rs::DeriveEngine::apply_mbo_batch` takes the already-applied book and returns one MBP10/BBO pair built from the final row's metadata and the book's final depth. Single-row `apply_mbo` delegates, keeping callers source-compatible aside from the added book argument.

Cross-id replacement is represented as full cancel of the old id followed by add of the new id. Both MBO rows remain visible to L3 consumers, while derived L1/L2 consumers never observe the transient state between them.

### Parity audit surface

Full-depth parity needs read-only inspection beyond MBP10 without exposing mutable engine internals.

The `testing` feature enables `order_state`, `level_state`, and `instrument_order_count`; `BookEngine::queue_position` remains the FIFO check. Production builds compile no audit observer branch, and the accessors do not mutate state.
