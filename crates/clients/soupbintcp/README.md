# client_soupbintcp

SoupBinTCP 3.0 client: login handshake, sequenced and unsequenced framing, heartbeats both ways, and an optional compressed variant, over any stream transport, with or without an async runtime.

## What it is

`SoupBinClient<T>` runs one SoupBinTCP 3.0 session state machine (login state, sequence, outbound buffer, heartbeat deadlines) behind two thin drivers:

| API | Bound on `T` | Example transport |
| --- | --- | --- |
| sync: `start`, `poll(now)`, `queue_unsequenced`, `queue_logout` | `StreamRecv + StreamTrySend` | `transport_socket::mio::MioTcp`, `transport_socket::tokio::TcpStream` |
| async: `connect`, `recv`, `recv_managed`, `send_unsequenced`, `logout`, `tick_heartbeat` | `StreamRecv + StreamSend + AsyncReady` | `transport_socket::tokio::TcpStream` |

`next_deadline()` works under either.

## Sync session

`start` queues the login request. Each `poll(now)` resumes pending partial writes, queues a client heartbeat when the send deadline passes (reported as `HeartbeatSent`), then receives and dispatches until it has a message or the socket is drained. It yields sequenced data as `SoupBinMessage::Data` and every lifecycle signal as `SoupBinMessage::Event`: `LoginAccepted`, `LoginRejected`, `HeartbeatReceived`, `HeartbeatSent`, `HeartbeatTimeout`, `EndOfSession`. `Ok(None)` means nothing happened.

A pinned busy-poll loop calls `poll` again at once (see Usage). A parked loop registers the `MioTcp` in a `ReadySet` before `start`, then waits until `next_deadline()` whenever `poll` returns `None`.

`queue_unsequenced` (and async `send_unsequenced`) reject a payload over 65534 bytes with `FrameTooLarge`, queueing nothing: the `u16` length prefix counts the type byte.

After logout, rejected login, heartbeat timeout or end of session the session is closed: `poll` flushes what is still queued (the logout request), then returns `Err(EndOfSession)`. A server closing without end of session is `Err(Transport(PeerClosed))`.

## Async session

`connect` completes the login handshake (`LoginRejected` and `LoginTimeout` are errors there). `recv` yields sequenced data and events; `recv_managed` (feature `tokio`) also sends client heartbeats on its own and reports `HeartbeatTimeout`; other runtimes drive `recv`, `next_deadline` and `tick_heartbeat`.

## Usage

Synchronous session over `transport_socket::mio::MioTcp` (feature `mio`), no runtime:

```rust,ignore
use std::time::Instant;
use client_soupbintcp::{SoupBinClient, SoupBinClientConfig, SoupBinError, SoupBinMessage};
use transport_socket::{TcpConfig, mio::MioTcp};

let tcp = MioTcp::connect(&TcpConfig::new("10.0.0.2:40001".parse()?))?;
let cfg = SoupBinClientConfig {
    username: "USER01".into(),
    password: "SECRET".into(),
    ..SoupBinClientConfig::default()
};
let mut client = SoupBinClient::start(tcp, cfg)?;
loop {
    match client.poll(Instant::now()) {
        Ok(Some(SoupBinMessage::Data(frame))) => handle(frame.sequence(), frame.as_ref()),
        Ok(Some(SoupBinMessage::Event(event))) => tracing::info!(?event, "session event"),
        Ok(None) => {} // nothing happened: spin, or park until next_deadline()
        Err(SoupBinError::EndOfSession) => break,
        Err(e) => return Err(e.into()),
    }
}
```

Async session over `transport_socket::tokio::TcpStream` (feature `tokio` on both crates):

```rust,ignore
let tcp = transport_socket::tokio::TcpStream::connect(&TcpConfig::new(addr)).await?;
let mut client = SoupBinClient::connect(tcp, cfg).await?; // login handshake
loop {
    match client.recv_managed().await? {
        SoupBinMessage::Data(frame) => handle(frame.sequence(), frame.as_ref()),
        SoupBinMessage::Event(event) => tracing::info!(?event, "session event"),
    }
}
```

`SoupBinClientConfig` is serde (JSON or TOML) and every field defaults: login `username`, `password`, `requested_session` (empty joins the current one) and `requested_sequence_number` (1 replays from the start, 0 starts at the newest message; on reconnect pass `next_expected_sequence()`), `login_timeout` (30 s), `heartbeat_interval` (1 s), `heartbeat_timeout` (15 s), `max_frame_size` and `decode_buf_capacity` (64 KiB each). Durations are humantime strings such as `"30s"`.

The client needs no privilege and makes no socket call of its own; it runs wherever its transport does (`transport_socket` covers Linux, macOS and Windows).

## Design

Receive lands transport bytes straight into the decode buffer's spare capacity through `StreamRecv::recv_into`, whose `unsafe` trait contract guarantees the returned length was initialised, so the uncompressed stream has one copy and `BytesMut` framing (`split_to`) adds no copy after. The compressed variant reads into a staging buffer, since inflate needs a contiguous compressed chunk, then inflates at most `decode_buf_capacity` bytes per step and dispatches them before the next step; the socket is read again only once staged input and pending inflate output are used up. A high compression ratio (a replay backlog) therefore decodes in full, and a zlib bomb never grows memory past that bound. Outbound bytes queue in one buffer whose front is where a partial write resumes, shared by both drivers.

## Features

- `compressed` (off by default): Nasdaq compressed variant, via `flate2`. Server to client only; client writes stay plain.
- `tokio` (off by default): `recv_managed`.
- `observability` (off by default): message, session and heartbeat counters through `observability-core`.

## Protocol specification

SoupBinTCP and its compressed variant are Nasdaq protocols. Obtain the
specifications from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Building and testing

```bash
cargo nextest run -p client_soupbintcp
cargo nextest run -p client_soupbintcp --features tokio,compressed,observability
```

One protocol table (login accepted and rejected, login timeout, sequenced data, a burst of about 64 KiB in one server write, heartbeats both ways, heartbeat timeout, partial writes, logout, end of session, peer close) runs through the sync API over `MioTcp` and through the async API over tokio `TcpStream`, against a local mock server.

## Logging

This crate emits [`tracing`](https://docs.rs/tracing) events at session transitions only (login, logout, end of session, timeouts), never per message. Install any subscriber to see them; filter with `RUST_LOG=client_soupbintcp=debug`.

## License

MIT OR Apache-2.0, at your option.
