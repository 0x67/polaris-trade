# websocket

`polaris-source-websocket` is an ingest source over persistent WebSocket connections: one connection per stream, non-blocking polls on the caller's thread, reconnect inside the batch call.

It lives in its own crate so a consumer with no use for WebSocket or TLS crates leaves it out. Parsing venue JSON stays in the adapter; the source delivers message bytes.

## Source shape

[[crates/sources/websocket/src/source.rs#WebSocketSource]] polls one `tungstenite` socket per channel in turn, with no runtime and no `.await`, like the live SoupBinTCP source.

Each channel in [[crates/sources/websocket/src/config.rs#ChannelConfig]] has a URL, a `StreamId` and the subscribe messages sent after every connect. A text or binary message becomes one [[crates/sources/websocket/src/source.rs#WebSocketFrame]] that holds the message `Bytes` as the library read them, so there is no copy, and `ts_recv` is stamped right after the read returns. The library answers pings and close frames itself, and control frames never become frames.

The source has no frame pool and never returns `PoolExhausted`. When `max` is reached, unread messages stay in the socket or in the library's buffer. The start channel rotates per call so a busy first channel does not starve the others.

## Connect and TLS

A connect is blocking but bounded as a whole: socket connect, TLS and upgrade handshakes and subscribe together take at most `connect_timeout`, then the socket turns non-blocking.

Resolved addresses share the time equally, so one blackholed address cannot use all of it, and the handshakes run on a non-blocking socket polled against the same deadline, so a peer that sends slowly cannot stretch the bound.

[[crates/sources/websocket/src/connect.rs#open]] builds the stream. `wss://` channels use the optional `rustls::ClientConfig` in the configuration, so a test can trust a self-signed certificate; without one the platform root store is loaded once in the constructor and reused by every reconnect, so no reconnect reads the keychain or builds a TLS config. Name resolution is not bounded. The message and frame size caps default to the library's 64 MiB and 16 MiB and are configurable; a message over the cap drops the connection.

## Reconnect

A lost connection reconnects inside `recv_batch` without sleeping: the call polls every live channel first, then retries each lost channel whose backoff has passed. Frames read in that call are returned after the retry ends.

Peer close, a TCP drop with no close frame and a library error all count as lost. The wait starts when the loss is seen, so even the first retry waits `backoff_initial`; each failed attempt doubles it up to `backoff_max`. A reconnect that closes before delivering any message counts as a failed attempt, so a venue that accepts the upgrade and then closes is retried at the backoff rate and ends the source after `reconnect_attempts`; a delivered message clears the count. A retry runs the same blocking connect, so it holds the loop for at most `connect_timeout`, and only while that channel is down. After `reconnect_attempts` failures in a row the source is closed: the call that finds no frames returns `IngestError::Closed`, and every later call does too. With zero attempts the call that detects the loss returns `Closed` unless it also read frames, which it returns first. The first connect in the constructor does not retry.

permessage-deflate is not supported, since `tungstenite` lacks it.

## Tests

Every case runs against a scripted server on `127.0.0.1:0`, plain and over TLS with a self-signed certificate; no external network is touched.

The shared server in `tests/support` accepts a set number of connections and runs one script per connection. The suite runs `EmptyYieldsZero`, `RespectsMax` and `DrainsInOrder` through `md_pipeline::ingest::testing::run_ingest_cases`, building each source from a `ScenarioSpec`; the builder returns only after the server wrote every frame, because `DrainsInOrder` stops at the first `Ok(0)`. Own cases: peer close with the client answering the close frame, TCP drop with no close frame, a half-received frame read as `Ok(0)` and then completed, reconnect with the subscribe messages replayed, a message over the size cap, two channels keeping their own `StreamId`, a backoff that waits out its delay without blocking any call, and a venue that closes every connection right after the upgrade, which must be retried at the backoff rate and then end the source.

### Read path bench

`benches/read_path.rs` is a custom-harness bench for the read path: the cost of an empty poll and messages per second over a loopback socket.

On the M4 Pro host on AC power it measured about 850 ns per empty poll and about 57 ns per 64-byte message (17 million messages per second) in the release profile, matching the shortlist trial of the library. Run `cargo bench -p polaris-source-websocket --bench read_path`; `-- --test` smoke-runs it.
