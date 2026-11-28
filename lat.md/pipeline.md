# Pipeline boundary

The `polaris-pipeline` crate holds the source-agnostic ingest contract, the venue-adapter seam, the output seam, and the egress sequencer that is the pipeline's single clock point.

## Ingest

Live transport clients and replay containers feed the pipeline through one contract yielding (payload, ts_recv, stream) frames, so the engine is identical for live and replayed data. See [[crates/pipeline/src/ingest.rs#IngestSource]].

`StreamId` is `#[repr(transparent)]` over `u16`, so a plugin hands its stream list to the host as a `uint16_t` array.

`recv_batch` is batch-first: `Ok(0)` means nothing ready, `PoolExhausted` is the backpressure signal, `Closed` ends the source. Semantics mirror `transport_core::DatagramSource::recv_burst`. A conformance suite (behind the `testing` feature) verifies impls. `run_ingest_source` runs all four cases; `run_ingest_cases` runs a chosen subset, for a source with no frame pool that never yields `PoolExhausted` (see [[websocket#Tests]]).

[[crates/pipeline/src/ingest.rs#IngestFrame]] carries a `venue_seq` default method returning `0` ("not yet threaded"), additive so every existing frame type compiles unchanged until a source threads its real venue sequence. Every replay frame type now overrides it: see [[replay#Synthetic venue_seq]].

## Venue adapter seam

One trait turns any venue's wire bytes into normalized events, so a consumer holds one type parameter instead of one concrete adapter per venue. See [[crates/pipeline/src/adapter/contract.rs#VenueAdapter]].

`VenueAdapter` takes an associated `Config` because construction is venue-shaped (tier tables, session anchors), and a `StreamId` on `feed` and `coverage` because a venue can decode more than one channel and every frame already names its stream. `drain` returns `impl Iterator`, which keeps the hot path unboxed and makes the trait not object-safe, so [[crates/pipeline/src/adapter/contract.rs#DynAdapter]] is the erased form for a registry or plugin loader, blanket-implemented for every `VenueAdapter`. Its `drain_into` appends to a caller-owned vector, so the erased path allocates once per session.

`VenueAdapter::set_session_anchor` replaces the anchor a venue adds wire time-of-day offsets to, for every frame fed after the call. It is a provided no-op, right for a venue whose wire carries absolute time, so implementors without an anchor write nothing. `DynAdapter` declares it without a default and the blanket impl forwards it, so a hand-written erased impl must implement it too; [[crates/pipeline/src/adapter/testing/tests.rs#erased_session_anchor_reaches_adapter]] proves the erased call reaches the venue's override. A runner calls it at session open and each trade-date roll, never per frame, so the per-frame path compiles unchanged.

Counters live in one [[crates/pipeline/src/adapter/contract.rs#AdapterStats]] (`decoded`, `unknown_kind`, `malformed`, counted per wire frame), marked `#[non_exhaustive]` so fields add without breaking callers. `VenueAdapter::stats` lends them, and `DynAdapter::stats` returns a copy, because an adapter behind the [[plugin]] ABI copies them out of the plugin on each call.

`DynAdapter::fault` reports the [[crates/pipeline/src/adapter/contract.rs#AdapterFault]] that stopped an erased adapter: a caught panic, a record the host cannot read, or a status its interface does not define. It defaults to `None`, which every `VenueAdapter` keeps, so only a plugin adapter faults. A faulted adapter emits nothing more, and the datafeed's runner checks once per frame batch and ends the session ([[plugin#Host wrapper#Faults into the session]]). NASDAQ implements the seam in this workspace; see [[nasdaq#NASDAQ venue adapter#Seam config and session]].

Each adapter fills its `coverage` matrix from the presets on [[crates/pipeline/src/coverage.rs#Coverage]] (`REFERENCE`, `BOOK_AND_DERIVED`, `TAPE_ONLY`), so the three classes keep one definition across venues.

Each adapter also declares the record types it builds from the wire through [[crates/pipeline/src/adapter/contract.rs#VenueAdapter]]`::record_types`, a static slice of `md_events::rtype` bytes, and on the erased path through `DynAdapter::record_types`. A consumer reads it to predict the output of a venue without naming it. Records the derive engine builds (`Mbp10`, `Bbo`, `Ohlcv`) are not listed, and the bands still follow from `Category::of_rtype`. It is separate from `coverage`, which keys wire kinds and says what each feeds. NASDAQ declares `Control`, `Mbo`, `Trade`, `TradeBreak`, `Imbalance`, `Status` and `Definition`.

Each in-tree adapter's tests check the book and handled axes against emission: every decoded kind, fed alone to a fresh adapter, must emit an `Mbo` exactly when coverage marks it book-applied, and an `Mbo`, `Trade` or `TradeBreak` exactly when it marks it handled. Emission cannot show whether a book-applied kind also feeds a derived view, so `derived` is checked only on kinds that skip the book. See [[crates/venues/nasdaq/tests/adapter_itch_v5.rs#coverage_book_and_handled_axes_match_emission]]. The rule stays out of the conformance suite: it holds for order-level feeds, while an MBP-native venue applies `Level` to its book.

### Market events

[[crates/pipeline/src/adapter/event.rs#MarketEvent]] is exactly the union of the ten records venue adapters emit: adapters emit events, engines derive snapshots.

`Mbp10`, `Bbo`, and `Ohlcv` come from the book and bar engines, so they are not variants. `Mbp10` alone is 480 bytes and would widen every emit, the 96-byte `Mbo` path included, to a 488-byte copy. A compile-time assert holds the enum at 176 bytes or less, the width of its widest variant, `Definition` with its derivative and FX tail ([[events#Definition tail growth]]). The enum is exactly 176 bytes, since the discriminant fits a niche of `option_type`, so every emit copies 176 bytes where it copied 120 before the tail. `as_record_ref` borrows the `repr(C)` record as header plus body bytes, the form sinks and the C ABI take, with no serialization. `as_bytes` borrows the whole record as one slice, the form a plugin's `drain` hands across.

### Session model

A venue declares its trading day as a [[crates/pipeline/src/adapter/session.rs#SessionModel]]: a roll rule plus a separate closure rule, so FX can roll daily and still close for the weekend.

Roll rules are `Continuous` (UTC midnight, crypto), `DailyFixed` (constant offset such as UTC+9, no zone database), and `DailyZoned` (IANA zone plus a roll time relative to local midnight, offset derived per date: NASDAQ ET midnight). A `Weekend` closure marks the sessions from `close_dow` up to `open_dow` as not trading without moving any anchor. Labels follow the roll alone. A negative roll time rolls the evening before, so FX at -7h opens at 17:00 New York and labels that session with the next date, which is the convention that folds the Sunday evening open into Monday.

[[crates/pipeline/src/adapter/session.rs#SessionModel#resolve_anchor]] runs at session open and again at every trade-date roll, never per record, and one date's anchor is never reused for another because a zoned offset changes at a DST transition. The runner hands each result to the adapter through `set_session_anchor`. Per-record code keeps the resolved `u64` and floors windows from it in UTC nanoseconds, which keeps a 25-hour fall-back day unambiguous. `session_span_ns` ends a day partition at the next date's anchor rather than 24h on, and rejects any span other than 23h, 24h, or 25h.

### Time zones

Zone offsets arrive through the [[crates/pipeline/src/adapter/session.rs#TzResolver]] seam, so `polaris-pipeline` links a zone database only with its `tz` feature, which is off by default. Crates that only route or decode never need it; the binary that resolves zoned sessions enables it.

polaris-venue-nasdaq enables `tz` only as a dev-dependency, for the test that resolves its own New York anchors across DST.

[[crates/pipeline/src/adapter/session.rs#NoTzdb]] answers every lookup with `None`, so a zoned session fails at open instead of anchoring on UTC; an embedder running only continuous or fixed-offset venues leaves `tz` off and links no zone data. [[crates/pipeline/src/adapter/tz.rs#BundledTzdb]] reads the IANA database `jiff-tzdb` compiles into the binary and never the host zoneinfo, because anchors feed window labels and a replay must come out byte-identical on every machine. The lockfile pins the `jiff` and `jiff-tzdb` versions, so the bundled tzdb moves only with a lockfile change.

A local roll time inside a DST gap or fold takes the offset in effect before the transition: jiff's compatible rule, written out in the resolver so an upstream default change cannot move archived anchors.

### Adapter conformance suite

Any adapter proves the seam contract by calling [[crates/pipeline/src/adapter/testing.rs#assert_adapter_contract]] from its own tests under the `testing` feature.

The caller passes its config and [[crates/pipeline/src/adapter/testing.rs#ContractFrames]]: decodable samples labelled with their wire kind, in feed order, covering every kind the adapter dispatches on each stream whether or not its coverage marks it, plus one unknown-kind frame per declared stream. The coverage check sees only sampled kinds, so a dispatched kind with no sample can hide a coverage gap. At least one sample should emit two or more events; with only single-event samples, a `decoded` that counts events instead of frames goes unnoticed. So should the first sample on each stream to emit anything but a `Definition`, or case 9 cannot see a gap flag missing from some of that frame's records; the in-tree caller leads with such a kind (NASDAQ `E`). The suite derives the truncated frames (each sample minus its last byte) the undeclared-stream probes (the lowest and highest ids `streams()` leaves free), and an empty frame per declared stream itself, builds fresh adapters per case, and catches a panic inside a case as that case's failure, so a bad arm cannot hide the other results. It does not probe over-long frames. A sample must not also be a frame another declared stream decodes, unless that stream's coverage marks the sample's kind, because the suite feeds every sample there and expects it dropped. [[crates/pipeline/src/adapter/testing.rs#run_adapter_contract]] returns the per-case report; the assert form panics naming each failed case.

The cases in [[crates/pipeline/src/adapter/testing.rs#AdapterCase]]:

1. A drain empties the buffer, and a second drain yields nothing.
2. Exact counters, on one adapter drained after every frame: a decodable frame adds exactly one to `decoded` and moves nothing else, however many events it emits; its truncated copy adds one to `malformed`, an unknown-kind frame one to `unknown_kind`, an empty frame one to either, and a frame on an undeclared stream nothing; no drain moves a counter. Every drained record's header must name its own rtype and size and, unless it is a `Definition` the adapter mints from its registry, carry the `ts_recv` and `venue_seq` of its frame. What drops emit is case 7's job.
3. An unknown-kind frame adds one to `unknown_kind`, moves no other counter, emits nothing, and does not panic.
4. A truncated frame adds one to `malformed`, moves no other counter, emits nothing, and does not panic. So does an empty frame on each declared stream, adding one to `malformed` or `unknown_kind`.
5. Each declared stream's `coverage` equals the kinds decoded on it via `CoverageMatrix::matches_dispatch`. A frame that moves `decoded` and an error counter together counts as not dispatched; the ITCH adapters' cold arms would do that if `classify` ever passed a kind `parse_match` cannot handle, and case 2 rejects such a frame outright. Each sample is also fed, as a primed drop probe, on every other declared stream whose coverage does not mark its kind: it must add one to `unknown_kind` or to `malformed` and move nothing else, which catches kind confusion between channels such as a primary frame read on the secondary stream.
6. `streams()` is non-empty, and frames on undeclared streams move no counter and emit nothing.
7. Decode after drops: with a truncated, an unknown-kind, or an undeclared-stream frame right before each decodable frame, the decodable frames decode to the same records as on a reference adapter that saw no drops. The reference skips the `ts_recv` slots the drops used, so both adapters stamp the same `ts_recv`, and the comparison covers the whole record except `venue_seq`, which the drops shift; the gap flag is compared too, since every stream stays gap-free on both adapters. This catches a drop path that leaves state behind, in the body or the header, such as a field read before the length check, and a drop that emits anything. A dropped frame that carries a sequence gap is case 9's.
8. Record lifetime: copying events out after every frame, as a plugin host must before its next call into the adapter, matches one drain at the end. An adapter that patches a pending event on a later feed, or re-delivers a drained one, fails here.
9. Gap survives drops: per stream with decodable samples, a reference and a probed adapter both take every decodable frame, the probed one then takes that stream's unknown-kind frame after a skipped `venue_seq`, and both take the decodable frames again. On the skipped stream, the first frame emitting anything but a `Definition` must flag each such record `GAP_SUSPECT`; with that bit masked it, and every other frame on any stream, must equal the reference. This catches a gap flag recomputed per frame, which the dropped frame takes with it, a flag set on only the first record of the frame that consumes the gap, and a pending gap one stream's frame consumes for another's. It makes per-stream contiguity gap detection part of the seam contract: an adapter that never sets `GAP_SUSPECT`, or one for a venue whose per-stream `venue_seq` is not contiguous, fails it.
10. Record types match emitted: `record_types()` equals the set of rtypes of every event the decodable frames emit, so a declared record type never goes unemitted and an emitted one is never undeclared.

Cases 3, 4 and 6, and the foreign-stream half of case 5, probe a primed adapter: every decodable frame is fed first and nothing is drained until the end, so counters sit above zero and events are pending. Each probe must move the counters exactly as its rule says, and the final drain must equal that of an adapter fed only the decodable frames. A drop path that resets counters or discards, edits, or adds pending events fails; on a fresh adapter it would look like a no-op.

Empty input fails the suite as a whole, and fails every case for an adapter that declares streams and decoded kinds. The suite's own tests run it against a stub adapter with one injected defect at a time; each defect fails only the cases guarding its rule. The stub has two streams with their own kinds and unknown frames, plus one kind decoded on both so the foreign-stream probes must honor coverage, numbered from 1 so the undeclared probe on stream 0 isolates the non-empty `streams()` rule, and it counts a `ts_recv` that does not rise on a declared stream as malformed. A `venue_seq` gap there stays pending per stream, across dropped frames, until that stream's next emitting frame flags its trades, the behaviour case 9 asks of a venue. The stub's first sample is `B`, which emits two trades, so the mutant that flags only the first trade fails case 9.

### Adapter fixture parity

A hand-written adapter pins its mapping with a recorded fixture: venue frames in, normalized output out, compared line by line. It replaces the codegen drift gate for venues whose wire is not fixed-offset binary.

A fixture is a directory with `frames.txt` and `expected.txt`. Each `frames.txt` line is `<stream> <ts_recv> <venue_seq> <payload hex>`, with `-` for an empty payload and `#` for comments. [[crates/pipeline/src/adapter/testing/parity.rs#assert_fixture_parity]] feeds the frames to a fresh adapter, drains after each, and renders one `frame N <event>` line per emitted event plus a final `stats` line. Any difference from `expected.txt` fails with the first differing line, expected and actual, so the changed record is named; a digest would only say that something moved.

An intended mapping change is accepted by rerunning the test with `POLARIS_REWRITE_EXPECTED=1`, then reviewing the `expected.txt` diff in the same commit. The same value creates `expected.txt` for a new fixture, and only `1` turns rewriting on. Only the assertion rewrites, so the negative tests that run defective stubs against the committed fixture never touch it. The helper lives beside the conformance suite under the `testing` feature, so every adapter crate uses the same format and assertion. [[crates/pipeline/src/adapter/testing/parity_tests.rs#altered_mapping_names_the_differing_record]] proves it against the conformance stub: a changed `venue_seq` mapping fails at the exact trade that differs.

## Egress

Records leave through a sink trait, generic on the hot path with no boxed trait objects, so deployment tiers differ only in wiring. See [[crates/pipeline/src/output.rs#OutputSink]].

The [[crates/pipeline/src/output.rs#EgressSequencer]] stamps `ts_egress` from a pluggable clock and assigns the per-stream, 1-based, gapless egress sequence at exactly the output boundary. Real clock for live, virtual clock for replay, so everything upstream stays deterministic.

`publish` stamps candidate `egress_seq`/`ts_egress` on the header before calling the sink, but commits the counter only on `Ok(())`. A sink error leaves the counter untouched, so retrying the same record reuses the identical sequence and never burns a gap; `ts_egress` legitimately differs per retry since it measures latency, not content identity. `ts_egress` itself is clamped against a joint `last_egress` high-water mark, so a clock that steps backward (NTP correction) never produces a decreasing stamp.

Per-stream counters live in a plain vector indexed directly by the u16 stream id (grown on demand), not a map: `publish` runs once per record, so a hashed lookup here put two hash computations on the hot path for a key that is already a small dense integer.

## Identity

Instrument identity is minted, never venue-assigned: one (venue, symbol, board) always maps to one stable id, resolved point-in-time so native-id reuse and generation renumbering never leak. See [[crates/pipeline/src/identity/registry.rs#InstrumentRegistry]].

Alias intervals record mapping changes only: adapters upsert on every symbol-bearing message (every NASDAQ add order), so a re-sighting of the live mapping is a no-op. One interval per remap keeps upsert O(1) on the hot path; interval-per-sighting made a full-day replay quadratic (the alias vec re-sorted per message).

A venue without numeric ids sends native id 0, which records no alias. `resolve` looks up native ids only and returns `None` for 0, so such a venue calls `resolve_symbol` to find the instrument by (venue, symbol).

The registry rebuilds from its own emitted [[crates/events/src/generated/records.rs#Definition]] stream, so no persisted mapping is needed across restart or replay. Participant identity keys on the alpha broker code, aliases venue numeric ids, and carries per-venue attribution flags so consumers never assume uniform coverage.

## Versioned config

Venue rule config (price-band tiers, board routing, status flag vocabulary, index composition) loads as effective-date-ranged versions and refuses overlap, interior gap, or unknown vocabulary up front. See [[crates/pipeline/src/config.rs#VersionedConfig]].

Dates cross the serde edge as [[crates/pipeline/src/config.rs#CivilDate]] (`{year, month, day}`) and convert to `time::Date` for range math, so ordering and day-successor stay correct across month and leap boundaries; a bad calendar date surfaces as [[crates/pipeline/src/config.rs#ConfigError]]`::BadDate`. `VersionedConfig::at` selects the version effective on the processed event's date; a date outside every configured range is a lookup miss (`None`), not a load-time error.

Each payload type declares the vocabulary tokens it references via [[crates/pipeline/src/config.rs#DeclaresVocabulary]]; `load` cross-checks them against the adapter's declared [[crates/pipeline/src/config.rs#Vocabulary]] before accepting the config. Wire-carried state (prices, sizes, timestamps) is never duplicated into config; only operator-scheduled rules live here.
