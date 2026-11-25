# Orderbook reference fixture

Hand-built NASDAQ ITCH 5.0 lifecycle fixture for
`tests/orderbook_reference_parity.rs::synthetic_nasdaq_replay_matches_exact_l1_l2_l3_fixture`.
Small enough to run in CI on every push; the full-day capture equivalent is the
`#[ignore]`d test against `ORDERBOOK_REFERENCE_DIR`.

## Files

- `stream.hex`: one ITCH 5.0 message per line, hex-encoded, no framing. The test
  harness (`binary_dump`) prepends each message's big-endian `u16` length,
  producing the same BinaryFILE framing `BinaryFileReplay` reads. 34 messages:
  one `R` stock directory for `NVDA` (locate 1), then 33 book events
  (`A`, `F`, `E`, `C`, `X`, `D`, `U`).
- `l1.csv`: expected best bid/ask after every event. Columns:
  `event,venue_seq,bid_price,bid_qty,bid_count,ask_price,ask_qty,ask_count`.
  An empty side is all zeros; the test maps that onto the engine's `Price::NULL`
  convention before comparing.
- `l2.csv`: expected per-level deltas touched by each event. Columns:
  `event,change,venue_seq,side,price,qty,count`; `qty=0` means the level was
  removed.
- `l3.csv`: expected per-order mutation rows, one per normalized MBO. Columns:
  `event,change,venue_seq,action,order_id,side,event_price,event_qty,`
  `resting_price,remaining_qty,participant,present,queue_position`.
  `participant` is 7 bytes hex (14 chars); `present`/`queue_position` describe
  post-event resting state, `queue_position=-1` when the order left the book.

Prices are md-events fixed-point (1e9 per unit): ITCH raw price `1000000`
(100.0000) appears as `100000000000`. `event` counts book events from 1 (the
`R` message starts no event); `change` counts MBO mutations within one event
(only replaces emit two). `venue_seq` is the 1-based wire message index, so
`venue_seq = event + 1`.

## Lifecycle coverage

| Events | Wire | Covers |
| --- | --- | --- |
| 1, 2 | `F`, `A` | attributed add (order 101, MPID `ABCD`) and anonymous add (102) resting at the same price level |
| 3 | `X` | partial cancel (101 sheds 20 of 100) |
| 4 | `E` | partial execution (102 fills 10) |
| 5 | `U` | same-price replace (102 -> 202); FIFO priority lost atomically |
| 6 | `U` | price-changing replace (101 -> 201) carrying the `ABCD` attribution |
| 7 | `C` | execution with explicit price off the resting level (201 fills 20 at 102.0) |
| 8, 9 | `D` | deletes after prior reductions (202, then 201); bid side empties |
| 10-20 | `A` x11 | eleven ask levels on one side; one live level stays outside MBP10 depth |
| 21-31 | `D` x11 | asks removed in reverse; ask side empties |
| 32, 33 | `A`, `E` | final add (400) then terminal execution; book ends empty |

## Provenance and verification

The stream was authored by hand against the NASDAQ ITCH 5.0 message layouts.
The expected CSV rows were computed by applying reference orderbook rules
(price-time FIFO, replace = cancel old + add new) to that stream, matching the
row schema of the full-day reference artifacts.

Verification is the synthetic test itself: it replays `stream.hex` through
`NasdaqAdapter` + `BookEngine`/`DeriveEngine`, checks every L3 mutation,
post-event order state (price, remaining qty, FIFO queue position),
participant carry, every touched L2 level against a second independent
`BTreeMap` book, MBP10/BBO derivation, record ordering, egress sequencing,
downstream MBO-only reconstruction, and final book emptiness. Every L1, L2,
and L3 row must be consumed exactly once; leftover rows fail the test.
