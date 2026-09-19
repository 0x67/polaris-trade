# transport_socket

Kernel socket transports for market-data feeds, built on socket2 and the traits in `transport_core`. One crate serves sync busy-poll loops, tokio sessions and runtime-free mio sessions, over one receive loop and one socket-option layer.

## What each feature gives

| Feature | Types | Traits |
| --- | --- | --- |
| none | `UdpSocket` | `DatagramRecv`, `DatagramSend`, `Multicast` |
| `tokio` | `tokio::AsyncUdp` | `DatagramRecv`, `DatagramSend`, `Multicast`, `AsyncReady` |
| `tokio` | `tokio::TcpStream` | `StreamRecv`, `StreamSend`, `StreamTrySend`, `AsyncReady` |
| `mio` | `mio::MioUdp` | `DatagramRecv`, `DatagramSend`, `Multicast`, registrable in `mio::ReadySet` (read) |
| `mio` | `mio::MioTcp` | `StreamRecv`, `StreamTrySend`, registrable in `mio::ReadySet` (read and write) |
| `observability` | | receive metrics through `transport_core::telemetry` |

`UdpSocket::bind(&UdpConfig)` builds every UDP type; `AsyncUdp::from_socket` and `MioUdp::from_socket` take the bound socket, keeping its options and pool. TCP types connect from `TcpConfig`. Configs validate before any allocation or syscall: an unspecified TCP remote, a zero connect timeout, `busy_poll_us` off Linux and `reuse_port` on Windows are `InvalidConfig`, never ignored.

Receive is always synchronous. `recv_burst` returns `Ok(0)` only when the socket is idle; data pending with every slab held is `PoolExhausted`. An error met after frames were pushed is returned on the next call, so frames never travel with an error. Backend names (metric label): `udp`, `tokio-udp`, `tokio-tcp`, `mio-udp`, `mio-tcp`.

## Multi-leg receive with `ReadySet`

One thread waits on many sockets without an async runtime. `ReadySet` borrows each socket only to register it, so the consumer can own the legs:

```rust,ignore
let mut set = ReadySet::new(NonZeroUsize::new(8).unwrap())?;
let mut legs = Vec::new();
for (i, cfg) in configs.iter().enumerate() {
    let mut leg = MioUdp::from_socket(UdpSocket::bind(cfg)?);
    leg.join_multicast(group, MulticastInterface::default())?;
    set.register(&mut leg, ReadyToken(i))?;
    legs.push(leg);
}
let mut consumer = Consumer::from_legs(legs);
let mut ready = Vec::new();
loop {
    set.wait(None, &mut ready)?;
    // drain until the consumer reports nothing, then wait again
    while consumer.poll()?.is_some() {}
}
```

`wait` blocks the calling thread and is never async; run it on a dedicated receive thread. An idle leg never hides an active one: every ready token is reported.

## Edge-triggered drain contract

Readiness is edge-triggered on every OS. After `wait` reports a socket, drain it until it yields nothing (`recv_burst` or `recv_into` returns `Ok(0)`, `try_send` returns `Ok(0)`) before waiting again; data left behind may never be reported. Registration reports data already queued, so the first `wait` sees it.

Every syscall on `MioUdp` and `MioTcp` goes through mio's `try_io`, which re-arms Windows interest when a drain reaches would-block.

## tokio readiness

`AsyncUdp` and `TcpStream` receive by calling socket2 directly, so a busy-poll caller never waits on the reactor. `ready()` awaits the reactor and then peeks inside tokio's `try_io`; an idle peek clears readiness that a direct receive left stale, so `ready()` never resolves on a drained socket. Every tokio constructor needs a runtime with its IO driver on the calling thread (outside a runtime it returns `Unavailable`); `TcpStream::connect` also needs the time driver for its timeout.

## Windows

- `ready()` probes with socket2's `peek_sender`, which does not fail on a queued datagram larger than the probe buffer (`WSAEMSGSIZE`).
- A datagram longer than a slab (`WSAEMSGSIZE` on receive) is dropped, counted as `truncated` under `observability`, and the loop continues. On Unix the kernel truncates it silently and it arrives cut to the slab size, so size slabs for the largest datagram.
- `WSAECONNRESET` on a UDP receive (Winsock reporting an ICMP port-unreachable for an earlier send) is skipped, so one unreachable peer cannot end the receive loop.
- `SO_REUSEPORT` and `SO_BUSY_POLL` do not exist; setting them is `InvalidConfig`.

## Send

`send_to` never blocks: a full socket buffer is `Io { stage: "send_to" }` with `error.kind() == WouldBlock`, including ENOBUFS on BSD-derived stacks (the OS error stays inside). `try_send` writes what fits and returns `Ok(0)` when nothing does; `send_all` (tokio) resolves once every byte is written.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
