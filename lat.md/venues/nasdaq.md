# nasdaq

NASDAQ venue crate `crates/venues/nasdaq` (package `polaris-venue-nasdaq`, lib `proto_nasdaq`): generated `itch_v5` and `ouch_v5` modules plus the hand-written venue adapter behind the `adapter` feature.

`itch_v5` is TotalView-ITCH 5.0 (23 kinds, u48 timestamps). `ouch_v5` is OUCH 5.0 order entry (25 directional kinds, TLV appendages). Both are emitted by [[codegen]] from `schemas/nasdaq/` and drift-gated. The crate's capture feed id is [[crates/venues/nasdaq/src/lib.rs#FEED_NASDAQ]], see [[mdc#Container header]].

## NASDAQ venue adapter

The one venue adapter in this workspace; [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter]] implements the `polaris-pipeline` venue-adapter seam on one stream with a buffer-and-drain shape.

Hand-written, feature-gated behind `adapter` so parser-only and OUCH consumers of `polaris-venue-nasdaq` gain zero extra dependencies. The feature pulls in [[events]], [[orderbook]] and [[pipeline]] as workspace path dependencies.

### Emission contract

`itch_v5::HotVisitor` methods return nothing, so the adapter buffers decoded output for the pipeline to drain, decoupled from any sink.

[[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter]] pushes into an internal `Vec<MarketEvent>` that [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter#drain]] hands to the caller; the whole mapping is unit-testable without a running pipeline. It emits seven of the [[crates/pipeline/src/adapter/event.rs#MarketEvent]] variants: `Definition`, `Mbo`, `Trade`, `Status`, `Imbalance`, `TradeBreak`, `Control`. [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter#feed]] classifies each wire frame against `message_type::BYTE_LEN` before dispatch, so a truncated or unknown frame is dropped and counted, never panics.

`feed(stream, payload, ts_recv, venue_seq)` stamps `venue_seq` verbatim onto every record's `RecordHeader.venue_seq` from that one wire frame, replacing the old self-incrementing internal counter. Every venue adapter also compares the incoming value against its own last-seen one and sets `RecordFlags::GAP_SUSPECT` on a skip, something an adapter-invented counter could never detect since it never gaps. The suspicion stays pending until a frame emits a record built by `next_header`, then clears: a skip carried by a frame the adapter drops (malformed, unknown kind), decodes without emitting, or answers with only a `Definition` still flags every header-built record of the next frame that emits one. The last-seen value advances on every frame, dropped ones included, so a clean drop never reads as a gap. Live sources thread the real venue sequence (for example the SoupBinTCP session sequence); replay containers synthesize a 1-based count per stream, see [[replay#Synthetic venue_seq]].

### Kind-to-record mapping

All 23 `itch_v5` kinds map to a normalized record; [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter#coverage]] declares which are book-applied/derived versus pure reference.

`S` SystemEvent maps to `Control`; `R` StockDirectory mints/aliases an instrument and emits `Definition`; the status family (`H` StockTradingAction, `Y` RegSHO, `L` MarketParticipantPosition, `V`/`W` MWCB, `K` IPO quoting, `J` LULD collar, `h` OperationalHalt, `N` RPII, `O` DLCR price discovery) maps to `Status` through the superset `TradingState`, with venue-specific detail preserved raw in `venue_reason`; `A`/`F` Add, `E`/`C` Executed (also emits `Trade`, since an execution is both a book fill and a tape print; `Trade.aggressor` is the taker, the side opposite the resting order's), `X` Cancel, and `D` Delete map to one `Mbo`; `U` Replace maps to an old-order `Cancel` followed by a new-order `Add`; `P` Trade and `Q` CrossTrade map to `Trade` (aggressor `None`: `P`'s buy/sell indicator is frozen at `B` since 2014, `Q` carries no side); `B` BrokenTrade maps to `TradeBreak`; `I` NOII maps to `Imbalance`. `coverage(StreamId(0))` is tested against the live dispatch arms, directly and through the seam conformance suite, so "decoded" and "handled" never silently diverge.

### Seam config and session

[[crates/venues/nasdaq/src/adapter.rs#Config]] carries `venue` and `session_midnight_ns`; the adapter declares one stream, `StreamId(0)`, and a New York midnight roll.

`session()` returns `RollModel::DailyZoned { tz: "America/New_York", local_ns: 0 }` with `ClosureModel::None`: New York DST moves the UTC anchor per trade date, so the caller resolves it per date and the adapter only declares the model. `Config.session_midnight_ns` is the initial anchor; `set_session_anchor` replaces it for later frames, so a live runner moves it at each roll. [[crates/venues/nasdaq/tests/adapter_itch_v5.rs#session_anchor_follows_new_york_dst]] resolves anchors through the adapter's own model and puts 09:30 ET at 14:30Z on 2026-03-07 and 13:30Z on 2026-03-09, 47h apart across the spring-forward. A frame on any other stream returns before any state moves. `tests/adapter_contract.rs` runs `assert_adapter_contract` with one zero-filled sample per `BYTE_LEN` kind plus an unknown kind.

### Instrument identity and order-state tracking

`stock_locate` is a daily alias on a minted `(venue, symbol, board=zeroed-default)` key via `InstrumentRegistry`; only `R` StockDirectory emits `Definition`.

Every kind that resends `stock` resolves-or-mints opportunistically without emitting a second `Definition`. Kinds carrying only `stock_locate` (`E`, `C`, `X`, `D`, `B`) resolve through the registry's alias table, falling back to the raw locate code as instrument id when no directory entry has been seen yet (isolated unit tests only; a full-day replay always sees `R` first). `timestamp` (u48 ns-since-midnight) adds to the session anchor (the config value or the last `set_session_anchor`) into `ts_event`.

NASDAQ's Executed/Cancel/Delete/Replace messages never resend side or symbol, but `BookEngine::apply_mbo` keys resting orders on `(instrument, side, order_id)`.

`OrderReplaceView`'s own doc says side/symbol/attribution are not resent and must be retained from the original Add. [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter]] therefore keeps its own `order_id -> (instrument, side, price, remaining_qty, participant)` table, populated on Add and consumed on every later reference to that order id: a scope addition beyond a bare per-kind field mapping, required for correctness rather than an architectural choice.

The table tracks `remaining_qty`, set from Add shares. [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter#apply_reduction]] subtracts `E`, `C`, and `X` quantities and evicts at zero. `D` emits one `Cancel` carrying the full remaining quantity. `U` consumes the old state and emits `Cancel(old remaining)` then `Add(new shares)` with identical source timestamps and venue sequence; both rows retain side and participant attribution. [[crates/venues/nasdaq/src/adapter.rs#NasdaqAdapter#order_count]] lets a test prove the table stays bounded across a full-day-scale execution burst with zero deletes.

### Parity fixture

`tests/fixtures/nasdaq_itch_v5_prefix.bin` is a checked-in 524320-byte prefix of a public NASDAQ TotalView-ITCH 5.0 sample file for 2026-05-18.

14348 whole `[len:u16 BE][message]` frames (`S`=1, `R`=10473, `H`=1937, `Y`=1937). [[crates/venues/nasdaq/tests/adapter_itch_v5.rs]] replays it through `polaris-replay`'s `BinaryFileReplay` as a normal (non-`#[ignore]`) test and asserts zero unknown-kind and zero malformed counts. A separate `#[ignore]`-gated full-day test (env `NASDAQ_ITCH_SAMPLE`) is documented for the operator to run against a real `.gz` capture.
