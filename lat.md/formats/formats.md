# formats

Container and wire formats for normalized market data under `crates/formats/`: the raw capture container and the self-describing record format.

Both carry [[events]] records or raw venue bytes behind a small fixed header that a reader checks before touching the body.

- [[mdc]]: MDC1 capture container of raw venue frames, venue-agnostic feed byte.
- [[pbn]]: PBN1 stream and file format for normalized records, with validation, symbology and a bounded zstd container.
