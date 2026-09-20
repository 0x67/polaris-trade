# client-soupbintcp

SoupBinTCP v3.0 client: wire codec, login and heartbeat session, async and synchronous APIs over one state machine, optional compressed variant.

Depends on [[core]] alone; the stream types of [[socket]] (`MioTcp`, tokio `TcpStream`) are the transports it is tested over.

## Session state machine

`session.rs` holds the one protocol state machine both APIs drive: login state, sequence, outbound buffer and heartbeat deadlines.

Receive lands bytes straight into the decode buffer's spare capacity through `StreamRecv::recv_into`; the buffer advance relies on that `unsafe` trait's length contract, cited at its one `advance_mut`. Outbound bytes queue in one buffer whose front is the resume point of a partial write, shared by both drivers so wire order holds; a payload over 65534 bytes is `FrameTooLarge` and queues nothing, since the `u16` length prefix counts the type byte. `next_deadline` on [[crates/clients/soupbintcp/src/client.rs#SoupBinClient]] sits in an impl with no transport bound: login deadline while authenticating, else the sooner of heartbeat send and server-silence deadlines, leaving the send deadline out while writes are queued (writable readiness wakes the caller then).

Under `compressed`, socket bytes land in a staging buffer and [[crates/clients/soupbintcp/src/compressed.rs#CompressedReader#feed]] inflates at most `decode_buf_capacity` bytes per step, which the caller dispatches before the next step. The socket is read again only once staged input and pending inflate output (`capped`) are used up, and the async driver skips its readiness wait while input is staged, since the socket may hold nothing more. A replay backlog decodes in full; a zlib bomb stays within the per-step bound.

## Synchronous API

`start` (queues login) and `poll` ([[crates/clients/soupbintcp/src/session.rs]]) run a whole session with no runtime over `StreamRecv + StreamTrySend`.

Each `poll(now)` resumes partial writes, queues a client heartbeat when due (before receiving, so a busy feed never starves it), then receives and dispatches until a message or a drained socket; `None` therefore means the socket reported nothing, safe for edge-triggered parking on a `ReadySet` ([[socket#Readiness]]). It yields data and every lifecycle event in [[crates/clients/soupbintcp/src/event.rs#SoupBinEvent]], including login accepted or rejected and heartbeat timeout. Closed sessions flush what is queued, then return `EndOfSession`; a login timeout drops the unsent login, so the next `poll` returns `EndOfSession` too. Peer close without end of session is `Transport(PeerClosed)`.

## Async API

`connect`, `recv`, `recv_managed`, `send_unsequenced`, `logout` and `tick_heartbeat` keep their signatures over `StreamRecv + StreamSend + AsyncReady`.

They call the same state machine: login rejection and timeout are `connect` errors, and `tick_heartbeat` reports a heartbeat timeout as an event, as `poll` does. `recv_managed` (feature `tokio`) also drives heartbeats from a tokio timer. [[crates/clients/soupbintcp/src/frame.rs#Frame]] carries inherent `sequence()` and bytes through `AsRef<[u8]>`.

## Decisions

What changed from the previous client, which trusted a safe trait's length inside `unsafe` code and offered no complete synchronous session.

- The stream receive trait was safe while the client called `unsafe advance_mut(n)` on its result, so a wrong `n` from safe code exposed uninitialised memory. `StreamRecv` is now an `unsafe trait` carrying that contract.
- An empty destination buffer read as end of stream on a live peer; the transport now returns `Ok(0)`, and the client's reserve before each read is no longer load-bearing.
- The old synchronous `poll_recv` covered receive only, after an async connect. `start` and `poll` now run login, heartbeats, partial writes and logout with no runtime.
- Heartbeat timeout became an event that closes the session (later calls return `EndOfSession`) instead of an error variant.
- Compressed receive capped one read's inflated output at `decode_buf_capacity` and failed with `FrameTooLarge` past it, killing any session whose read inflated larger (normal under replay backlog). It now inflates in bounded steps.
- `ClientState::Disconnected` could never be observed, since `start` and `connect` queue the login before returning; a session now starts `Authenticating`.
- Kept: the transport is injected, never built by the client, and the async method signatures.

## Tests

One protocol table in `tests/common` runs through `start`/`poll` over `MioTcp` parked on a `ReadySet` (`table_over_mio_sync_poll`) and through the async API over tokio `TcpStream` (`table_over_tokio_async`).

Cases: login accepted (after an early server heartbeat), rejected and timed out, sequenced data with debug dropped, a burst of about 64 KiB in one server write (under `compressed`, one read inflating far past the decode buffer, then logout so a stalled client fails on the server's read timeout), heartbeats both ways, heartbeat timeout, partial writes resumed byte-exact under a small send buffer, logout, end of session, peer close; each step asserts the message yielded. The mock server is a std thread, zlib-framing its writes under `compressed`. `managed.rs` covers `recv_managed`; `compressed.rs` shows an inflate bomb yields at most the cap per feed and repeat feeds allocate nothing. In-crate tests prove steady-state ingest allocates nothing, login fields justify correctly, an oversize payload queues nothing, `next_deadline` drops the send deadline while writes are queued, and a login timeout under send backpressure is followed by `EndOfSession`. Fuzz targets for framing and inflate are under [[clients#Fuzzing]].
