# pbn format

Polaris Binary eNcoding: the self-describing wire and file format for normalized market-data records. A fixed header plus a self-delimiting record body, one decode path for live, historical, and replay.

## Stream header

A fixed 32-byte little-endian prefix, magic `PBN1`, carried uncompressed ahead of the body so a reader routes and version-checks a file without decompressing. `schema_major` sits at a fixed offset for a single-byte version gate. See [[crates/formats/pbn/src/header.rs#StreamHeader]].

The byte layout, not the Rust struct, is the contract: the header is serialized by hand to fixed offsets, so the layout survives struct changes and cross-language readers agree.

## Record framing

Each record is self-delimited by its own `RecordHeader.length` (32-bit words at byte 0). The walk validates every boundary, so a truncated or malformed buffer errors instead of reading out of bounds. See [[crates/formats/pbn/src/stream.rs#frames]] and [[crates/formats/pbn/src/stream.rs#Frame]].

The writer [[crates/formats/pbn/src/stream.rs#write_record]] appends a record's verbatim byte image and rejects anything past the 1020-byte frame ceiling (the `u8` word-count length would otherwise wrap).

## Discriminant validation

A record body copied verbatim may carry an out-of-range enum byte, and reading it as its Rust enum is undefined behavior. See [[crates/formats/pbn/src/validate.rs#validate]] and [[crates/formats/pbn/src/stream.rs#Frame]].

Validation checks every enum-bearing field on the raw bytes before any typed decode, so a corrupt discriminant errors instead of being materialized. Per-record layout knowledge is keyed by `offset_of!` so field moves do not silently break it. Unknown rtypes pass, since an older reader cannot validate a record family it does not know.

The accepted ranges follow the schema: `LevelAction` up to `Clear` (3), `AssetClass` up to `Etf` (9) and `OptionType` up to `Put` (2), so `LevelAction` 4 and `AssetClass` 10 are the first bytes rejected. The `option_type` check depends on the frame length: a 104-byte `Definition`, written before the tail, has no `option_type` byte and skips it. `Frame::decode_checked` runs validation and then [[crates/events/src/record.rs#decode_prefix]], so it accepts that short definition exactly as plain `Frame::decode` does, with the tail absent ([[events#Definition tail growth#Reading a definition written before the tail]]). The length check comes first. Any other frame too short for its type gets `RecordTooShort` with that type's size as `need`, whatever bytes an enum check would have read.

## Mixed-record dispatch

A real stream mixes record kinds, so no single typed decode walks it. Dispatch reads each frame by rtype into a typed value, validating discriminants, and reports an unknown rtype as skippable rather than an error. See [[crates/formats/pbn/src/dispatch.rs#AnyRecord]] and [[crates/formats/pbn/src/dispatch.rs#read_records]].

This is the mixed-stream decode path an offline reader uses: walk a downloaded file, feed Definitions to the running symbology, and hand out typed records with no gateway.

## Symbology

Definition records carry the normalized instrument metadata. A stream opens with a Definition preamble and may re-emit Definitions intraday, so symbology is a running map updated on every Definition, not a frozen snapshot. See [[crates/formats/pbn/src/symbology.rs#Symbology]].

A reader resolves instrument id to its Definition from the stream alone, so a downloaded file replays with no external symbol map.

## Versioning

Pre-v1.0 policy: no backward-compatibility guarantee. A schema-major mismatch is rejected fail-closed before any record decodes; additive minor growth passes under skip-unknown-tail. See [[crates/formats/pbn/src/version.rs#check_compat]] and [[crates/formats/pbn/src/version.rs#UpgradePolicy]].

Cross-version upgrade materialization is deferred until real old-version data exists; the policy enum reserves the seam.

### Schema version of the definition tail

The definition tail and the new enum values (`AssetClass` 6 to 9, `LevelAction::Clear`, `OptionType`) keep `schema_major` at 1 and bump no `schema_minor`.

Under the policy in `version.rs` the tail is additive growth, a new trailing field, so it is a minor change: a reader of either build decodes the other's `Definition` (the older one by skip-unknown-tail, the newer one through the short read). No minor is bumped because no producer in the workspace sets one. `StreamHeader.schema_minor` is filled by each caller of `write_stream`, and the callers are tests, the fuzz seed generator and `examples/replay.rs`, all writing 0; the datafeed writes no pbn. No reader branches on the minor: `check_compat` compares the major only and `UpgradePolicy` has only `AsIs`. Each record's own length word says which `Definition` layout it holds, which is what the short read keys on. A reader built before this change rejects the new enum values with `BadDiscriminant`, fail-closed, and a minor bump would not change that, since `check_compat` reads only the major.

## Container

The full stream is the header image plus an optionally zstd-compressed record body. The reader version-gates on the header before decompressing, so an incompatible stream fails closed without touching the body. See [[crates/formats/pbn/src/container.rs#read_stream]] and [[crates/formats/pbn/src/container.rs#write_stream]].

Decompression is bounded by a ratio cap (decompressed size at most a fixed multiple of compressed length, above an absolute floor): untrusted customer input cannot drive a zstd bomb to unbounded allocation. The stream aborts with `PbnError::BodyTooLarge` the moment output passes the cap, never trusting a declared frame content-size. See [[crates/formats/pbn/src/container.rs#decompress]].

## Fuzz and Miri coverage

Three targets in the detached `fuzz/` crate cover the frame walk, the record round trip and the container reader. [[clients#Fuzzing]] lists what each one asserts.

The advisory `miri-pbn` CI job runs the library tests under Miri (`cargo miri nextest run -p polaris-format-pbn --lib`), which covers the raw byte view in [[crates/formats/pbn/src/stream.rs#write_record]] and the checked decode. The three zstd container tests carry `cfg_attr(miri, ignore)`: zstd is a C library, which Miri cannot execute. The uncompressed container and stream tests reach the same `write_record` path.
