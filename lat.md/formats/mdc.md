# MDC capture container

The `polaris-format-mdc` crate frames raw venue bytes into zstd-compressed timeseries files, so an adapter re-run over a capture regenerates normalized output without re-tapping the wire.

## Container header

Every capture file opens with a fixed 16-byte uncompressed header (magic, venue, date, schema, feed) ahead of the zstd body, so a reader routes a file without decompressing it first. See [[crates/formats/mdc/src/lib.rs#CaptureMeta]] and [[crates/formats/mdc/src/lib.rs#CaptureError]].

Byte 12 is the retired body-kind discriminator and is always 0. It once selected between raw and normalized bodies; the normalized body carried records nothing read back and was dropped. The byte stays reserved rather than reused, so a future body kind can claim it without moving any other field. A non-zero value is refused: those rows are records, not raw frames, and accepting one would hand the caller wrong lengths and wrong timestamps with no error.

The feed byte at offset 13 (formerly the first pad byte) is a decoder discriminator: it names which venue feed wrote the body so a consumer never silently decodes the wrong feed. `0` is a legacy capture written before the byte existed. Reading is total: an unknown id parses as the raw byte, never an error. The remaining two trailing bytes stay pad.

The byte is opaque to the container, which carries it without knowing what an id means. Each venue crate owns its own, here [[crates/venues/nasdaq/src/lib.rs#FEED_NASDAQ]]. Ids 1..=127 are public venues, 128..=255 private, so a private venue can never collide with a public one without a shared registry. The values are an on-disk contract across every capture ever written, so they are never renumbered.

## Raw stream

[[crates/formats/mdc/src/raw.rs#RawWriter]] and [[crates/formats/mdc/src/raw.rs#RawReader]] frame venue bytes tapped before the adapter: each row carries the stream id, receive timestamp, and payload length ahead of the payload bytes.

Raw capture is the replay source of truth, an adapter re-run over these rows regenerates the normalized stream byte-for-byte, so raw files never need re-tapping once written.
