# hyperliquid

Hyperliquid venue crate `crates/venues/hyperliquid` (package `polaris-venue-hyperliquid`, lib `venue_hyperliquid`): a hand-written [[pipeline#Pipeline boundary#Venue adapter seam|venue adapter]] over the public websocket JSON feed of the perpetuals exchange.

Unlike the generated parsers in [[venues]], nothing here comes from [[codegen]]. The wire is JSON, so the crate parses with `serde_json`, borrowing coin names and numeric strings from the frame.

## Mapping

One stream (`STREAM`, id 0) carries every channel of one connection. A frame is `{"channel": ..., "data": ...}`; the channel picks the decoder and doubles as the coverage key.

- `trades` becomes `Trade`: `px` and `sz` convert directly, `tid` is `trade_ref`, `side` `B` is the buyer taking liquidity (`Side::Bid` aggressor) and `A` the seller. `ts_event` is the block time in nanoseconds.
- `l2Book` becomes `Level` records, see below.
- `activeAssetCtx` becomes four `Stats` records: `FundingRate` (rate in `aux`), `OpenInterest` (`qty`), `MarkPrice` and `IndexPrice` (the oracle price) in `price`. The message has no time, so `ts_event` is `ts_recv`.
- `subscriptionResponse`, `pong`, `bbo` and any other channel count as `unknown_kind`.
- No public stream carries liquidations: they exist only in the per-user `userEvents` feed. The adapter emits no `Liquidation` statistic and no `Mbo`, and `record_types` lists `Trade`, `Level`, `Stats` and `Definition`.

The session is `RollModel::Continuous` with `ClosureModel::None`: it rolls on UTC midnight and never closes.

## Instruments

The venue has no numeric instrument id, so a coin name is the whole key. The adapter mints on first sight and emits the `Definition` ahead of the first record on that coin.

Minting uses the registry's symbol index with native id 0. The `Definition` is `AssetClass::Perpetual` with `qty_scale` `-8` and an empty board. Names over 20 bytes fail the frame, and so do spot names (`@<index>` or `BASE/QUOTE`), because the feed serves spot markets on the same channels and the adapter handles perpetuals only.

A frame that fails never mints: every number is converted while serde reads the frame, so the instrument is minted only after the frame decoded, and a dropped frame leaves no coin defined without its `Definition`.

## Fixed point

Prices go through `Price::try_from_venue` at `1e-9`, sizes become `u64` at `1e-8`, and no value touches `f64`.

A value outside its range, finer than its grid, negative where it cannot be, or not a plain decimal fails the whole frame as `malformed`, never a panic. Digits past the grid are accepted only when zero.

Funding rate is not a price and often has ten decimals (`-0.0000110511`), finer than the price tick. It travels in `Stats::aux` as a signed integer at `1e-12`, with `price` null.

## Book pushes

An `l2Book` push carries the whole visible depth, about 20 levels a side, and no sequence number. The adapter emits one `Level{Clear}` with side none, then every level as `New` flagged `SNAPSHOT`, bids first, `level` the zero-based depth.

A diff against the previous push would emit fewer records, but with no sequence number it could never notice a lost frame, and a lost frame would corrupt the book until a reconnect. A full replacement heals on the next push and keeps no per-coin book. The cost is up to 41 records per push. Zero-size levels are skipped and do not count toward `level`, and the order count `n` is not carried.

## Gaps

`venue_seq` is whatever the source stamps; the feed has none of its own. A skip flags `GAP_SUSPECT` on every record of the next frame that emits a non-`Definition` record, kept across dropped frames, as the seam contract requires.

## Tests

The contract suite, a recorded session and three mapping cases pin the adapter, and a criterion bench measures decode.

### Adapter contract

One frame of each channel plus an unknown channel run through `assert_adapter_contract`, which covers counters, drops, the gap flag and the declared record types.

### Recorded session parity

Frames recorded from the public feed on 2026-11-24 (BTC trades, two book pushes, asset contexts) in `tests/fixtures/session` must produce the committed records, statistics band included.

`ts_recv` and `venue_seq` in `frames.txt` are synthetic.

### Out of range values drop the whole frame

A trades frame whose second print has an out-of-range price emits nothing and counts one malformed frame, and the next good frame still emits its `Definition`.

### Book push replaces the book

A push emits `Clear`, then bid and ask levels numbered by depth, and skips a zero-size level without a gap in the numbering.

### Spot coins drop the frame

A trades frame on a spot name (`@107`, `PURR/USDC`) emits nothing, mints nothing and counts one malformed frame.

## Bench

`benches/decode.rs` feeds one recorded frame per channel through `feed` and drains the output. Run it on AC power only.
