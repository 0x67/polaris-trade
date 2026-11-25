# sources

Ingest sources under `crates/sources/`, each an [[crates/pipeline/src/ingest.rs#IngestSource]] over a transport that the protocol clients do not cover.

- [[websocket]]: WebSocket source over `tungstenite`, one connection per stream.
