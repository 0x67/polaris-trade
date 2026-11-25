# venue common

`crates/venues/common` (package `polaris-venue-common`, lib `proto_common`) holds the hand-written plumbing the generated binary venue crates share: error types, alpha trimming and the TLV appendage walker.

It is version-free and the only allowed coupling between venue schemas; generated code in [[nasdaq]] and [[sgx]] calls into it, and [[codegen]] emits against its names.

## Errors

[[crates/venues/common/src/error.rs#WrongLength]] reports a view parse that got a slice of the wrong size, carrying the expected and received lengths. `TrailingBytes` and `ParseError` sit beside it.

Parse code returns these instead of panicking, which is what the venue fuzz targets in [[clients#Fuzzing]] assert for every wrong-length input.

## Alpha trimming

[[crates/venues/common/src/alpha.rs#trim_alpha]] strips the space padding off a borrowed alpha field. Generated accessors return the raw padded slice, so a caller pays for trimming only when it needs the text.

## Appendage walker

[[crates/venues/common/src/appendage.rs#AppendageIter]] walks length, tag and value elements without allocating, for OUCH-style optional appendages. It stops cleanly on a truncated element rather than reading past the buffer.
