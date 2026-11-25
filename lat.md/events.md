# Normalized event contract

The `polaris-events` crate is the product surface: fixed-layout records every consumer builds against, with zero dependencies and no venue names in the core.

## Record header

Every record opens with [[crates/events/src/generated/header.rs#RecordHeader]]: minted `instrument_id`, venue sequence, per-stream egress sequence, three timestamps (`ts_event`/`ts_recv`/`ts_egress`), and a provenance flags bitfield.

Records are `#[repr(C)]` with a DBN-style length/rtype prefix, so capture files stay skimmable by offset and layout is locked by a unit test. `length` is the full record size in 32-bit words.

## No implicit padding

Every record is packed so `size_of` equals the exact sum of its fields: an explicit trailing (or, for `Mbp10`, interior) `_pad` field absorbs what `#[repr(C)]` alignment would otherwise leave as implicit padding.

Verbatim serialization ships `size_of` bytes, so implicit padding would ship uninitialized, non-reproducible bytes, and reading it is UB, breaking capture-hash reproducibility and double-run determinism. A `no_implicit_padding` unit test locks the exact size of every record, including [[crates/events/src/generated/header.rs#RecordHeader]] and [[crates/events/src/generated/records.rs#Mbp10]]; bodies pack 8-byte fields first so the pad stays trailing. Every constructor sets `_pad` to zero.

## rtype bands

rtypes are allocated from fixed category bands so new record families are additive, never a layout break. See [[crates/events/src/generated/bands.rs#Category]].

Control `0x00-0x0F`, book/tape `0x10-0x2F`, stats `0x30-0x3F`, reference `0x40-0x4F`, then reserved execution/RFQ/AMM bands. A reader decodes known fields and skips any unknown appended tail using the header length.

## Price convention

Prices are `i64` at `1e-9` with `i64::MAX` the only null sentinel. See [[crates/events/src/price.rs#Price]].

Venue sentinel encodings and decimal scaling translate through `SentinelScheme`, named by encoding rather than venue; the venue-to-encoding mapping lives adapter-side so the core never branches on venue price conventions.

The envelope is the `i64` range at that scale: a `1e-9` tick, a largest real price of `i64::MAX - 1` ticks (about 9.22e9 units), and `i64::MAX` as the null. [[crates/events/src/price.rs#Price#try_from_venue]] returns `PriceError::Overflow` for a scaled value outside it and `PriceError::SubTick` for a venue value finer than `1e-9`. `from_venue` covers `decimals` up to 9 for values the caller knows fit, and only the checked call scales a finer venue price.

## Fill attribution record

[[crates/events/src/generated/records.rs#Fill]] carries both sides' resolved participant alpha codes plus fill identity, keeping the derived [[crates/events/src/generated/records.rs#Trade]] record engine-derived and attribution-free.

`rtype 0x18`, book/tape band. `fill_ref`/`ref_kind` reuse [[crates/events/src/generated/records.rs#TradeBreak]]'s own `BreakRefKind`, so a later break on the same match number or secondary trade id joins directly to the `Fill` it reverses, the same way it already joins `Trade.trade_ref`. One side may be zero-sentinel where the venue does not attribute it.

## Definition tail growth

[[crates/events/src/generated/records.rs#Definition]] gained an appended `status_flags`/`call_auction` tail: `status_flags` is a 64-bit bitset (bit `i` set when the venue's instrument status matched the configured status flag vocabulary's rule `i`), `call_auction` is a 0/1 call-auction membership flag.

Additive per the skip-unknown-tail rule: no rtype change, and an old-size reader still recovers the shared prefix and skips the new bytes.

A second append adds the derivative and FX terms and takes the record from 104 to 176 bytes (44 words). The terms are one generated struct, [[crates/events/src/generated/structs.rs#DefinitionTail]] (72 bytes), in the field `tail` at offset 104, so code reads them as `def.tail.<field>`. Their wire offsets: `tick_size` 104, `contract_multiplier` 112, `min_qty` 120, `expiration_ns` 128, `strike` 136, `underlying_id` 144, `base_ccy` 148, `quote_ccy` 156, `settle_ccy` 164, `price_scale` 172, `option_type` 173 (`OptionType`: `None`, `Call`, `Put`), then the generator's `_pad[2]`. Every field before the tail keeps its offset, `_pad[1]` at 103 included. The same fields listed flat would put the pad at 103 in front of the scalar `tick_size`, which the generator rejects as avoidable ([[codegen#Record-schema DSL]]). The nested struct gives the flat offsets with no new generator rule for one record, at the cost of the longer access path. `definition_tail_layout` in `record.rs` pins the size, the pad and every tail offset.

`DefinitionTail::ABSENT` sets every term to its absent value: `Price::NULL` for `tick_size` and `strike`, zero elsewhere, `OptionType::None`. `Definition::new` sets it, and an adapter that knows a term sets it on the returned record. `AssetClass` gained `Fx` 6, `Bond` 7, `Commodity` 8 and `Etf` 9 in the same change.

### Reading a definition written before the tail

A reader built with the 176-byte type still reads a 104-byte definition, through [[crates/events/src/record.rs#decode_prefix]] and every typed decoder built on it.

The entry point is generic `decode_prefix`. A record shorter than `T` decodes only when `T` is `Definition` and the record is exactly `Definition::PRE_TAIL_LEN` (104 bytes, 26 words) long. The type check is a downcast, which needs `Record: 'static`. A hook on the `Record` trait would need a generator rule, because the generator writes every `Record` impl. pbn's `Frame::decode` and `Frame::decode_checked` both call `decode_prefix`, so plain and checked decode accept the same frames, the agreement the `stream_walk` fuzz oracle asserts ([[clients#Fuzzing]]), and `decode_any` and mdex read the record too.

The decoded value has `tail` set to `DefinitionTail::ABSENT`; zero fill would be wrong, because the absent `tick_size` and `strike` are `Price::NULL`. Its `hdr.length` is rewritten from the 26 it was read with to 44. The decoded bytes then differ from the wire bytes in the length word, and in exchange the value writes back as one whole 176-byte frame. A value that kept 26 would frame only 104 of the 176 bytes `write_record` writes, and every record after it would misalign. The walk still advances by the wire length, which `decode_prefix` returns and pbn's `Frame::bytes` keeps verbatim. A reader that decodes a record longer than its own type still keeps the length word it read, as before.

`definition_written_before_tail_reads_with_absent_tail` in `record.rs` decodes the byte image of the old 104-byte layout and round-trips a record with a full tail.

## Schema-generated data types

The record data types (records, enums, `rtype` bands, `Category`, `REGISTERED`, `Record` impls) are generated into `crate::generated` and re-exported at the crate root, so `md_events::Mbo` is a generated type. See [[codegen#Record-schema DSL]].

The hand-written modules keep only the logic layered on the generated data: the [[crates/events/src/record.rs#Record]] trait and [[crates/events/src/record.rs#decode_prefix]], `Price`/`SentinelScheme`, `RecordFlags`, [[crates/events/src/action.rs#StatBand]] with `StatType::band`, and `Definition::new`. The swap that retired the hand-written definitions was gated by a conformance test proving the generated types size, alignment, and per-field-offset identical (and populated-byte identical); going forward the layout is guarded by the retargeted `layout_locked`/`no_implicit_padding`/`rtype_bands`/`fill_layout` tests here plus the codegen drift gate. A `record_round_trips` test encodes and decodes every wire kind through [[crates/events/src/record.rs#decode_prefix|decode_prefix]] with full field equality; the whole package also runs under Miri (the `miri-events` CI job). The container crate [[mdc]] is left out of Miri because its zstd call is C FFI, outside the interpreter; it holds no `unsafe` of its own. [[clients#Fuzzing]] covers the adversarial layer on top.
