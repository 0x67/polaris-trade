# transport_socket

Kernel socket transports for market-data feeds, built on socket2 and the traits in `transport_core`. One crate serves sync busy-poll loops, tokio sessions and runtime-free mio sessions, over one receive loop and one socket-option layer, on Linux, macOS and Windows.

## What each feature gives

| Feature | Types | Traits |
| --- | --- | --- |
| none | `UdpSocket` | `DatagramRecv`, `DatagramSend`, `Multicast` |
| `tokio` | `tokio::AsyncUdp` | `DatagramRecv`, `DatagramSend`, `Multicast`, `AsyncReady` |
| `tokio` | `tokio::TcpStream` | `StreamRecv`, `StreamSend`, `StreamTrySend`, `AsyncReady` |
| `mio` | `mio::MioUdp` | `DatagramRecv`, `DatagramSend`, `Multicast`, registrable in `mio::ReadySet` (read) |
| `mio` | `mio::MioTcp` | `StreamRecv`, `StreamTrySend`, registrable in `mio::ReadySet` (read and write) |
| `observability` | | receive metrics through `transport_core::telemetry` |

`UdpSocket::bind(&UdpConfig)` builds every UDP type; `AsyncUdp::from_socket` and `MioUdp::from_socket` take the bound socket, keeping its options and pool. TCP types connect from `TcpConfig`. Backend names (metric label): `udp`, `tokio-udp`, `tokio-tcp`, `mio-udp`, `mio-tcp`.

## Busy-poll receive

```rust,ignore
use std::num::{NonZeroU32, NonZeroUsize};
use transport_core::{DatagramRecv, FrameBatch, Multicast, MulticastInterface};
use transport_socket::{UdpConfig, UdpSocket};

let mut cfg = UdpConfig::new("0.0.0.0:30001".parse()?);
cfg.recv_buf = NonZeroU32::new(8 << 20);
let mut rx = UdpSocket::bind(&cfg)?;
rx.join_multicast("233.54.12.1".parse()?, MulticastInterface::default())?;
let mut out = FrameBatch::with_capacity(NonZeroUsize::new(64).unwrap());
loop {
    rx.recv_burst(&mut out)?;
    for frame in out.drain() {
        handle(frame.peer(), frame.as_ref());
    }
}
```

Receive is always synchronous. `recv_burst` returns `Ok(0)` only when the socket is idle; data pending with every slab held is `PoolExhausted`. An error met after frames were pushed is returned on the next call, so frames never travel with an error.

## Config

Required fields are `new` arguments; the rest are public fields with defaults. Configs validate before any allocation or syscall, and an option the platform cannot apply is `InvalidConfig`, never ignored.

| `UdpConfig` field | Default | Meaning |
| --- | --- | --- |
| `bind` | `new` argument | local address; port 0 picks one, unspecified IP serves multicast |
| `reuse_addr` | off | `SO_REUSEADDR` |
| `reuse_port` | off | `SO_REUSEPORT`, Unix only |
| `recv_buf`, `send_buf` | OS default | `SO_RCVBUF`, `SO_SNDBUF` bytes (Linux reports double) |
| `busy_poll_us` | off | `SO_BUSY_POLL`, Linux only |
| `slab_count` | 1024 | receive slabs, so the most datagrams the caller can hold at once |
| `slab_size` | 2048 | bytes per slab, so the largest datagram received whole |

| `TcpConfig` field (`tokio` or `mio`) | Default | Meaning |
| --- | --- | --- |
| `remote` | `new` argument | peer; unspecified address or port 0 is `InvalidConfig` |
| `local` | OS picks | local bind before connecting |
| `recv_buf`, `send_buf` | OS default | socket buffer bytes |
| `nodelay` | off | `TCP_NODELAY` |
| `connect_timeout` | 5 s | handshake bound; zero is `InvalidConfig` |

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

Every read and write on `MioUdp` and `MioTcp` goes through mio's `try_io`, which re-arms Windows interest when a drain reaches would-block.

## tokio readiness

`AsyncUdp` and `TcpStream` receive by calling socket2 directly, so a busy-poll caller never waits on the reactor. `ready()` awaits the reactor and then peeks inside tokio's `try_io`; an idle peek clears readiness that a direct receive left stale, so `ready()` never resolves on a drained socket. Every tokio constructor needs a runtime with its IO driver on the calling thread (outside a runtime it returns `Unavailable`); `TcpStream::connect` also needs the time driver for its timeout.

## Send

`send_to` never blocks: a full socket buffer is `Io { stage: "send_to" }` with `error.kind() == WouldBlock`, including ENOBUFS on BSD-derived stacks (the OS error stays inside). `try_send` writes what fits and returns `Ok(0)` when nothing does; `send_all` (tokio) resolves once every byte is written.

## Platform and privileges

Linux, macOS and Windows. No privilege is needed, with one exception: raising `busy_poll_us` above `net.core.busy_read` needs `CAP_NET_ADMIN`.

A datagram longer than a slab is dropped whole on every OS, counted as `truncated` under `observability`, and the loop continues: half a message never reaches the caller. Detection takes the cheapest call per platform: Linux `recvfrom` with `MSG_TRUNC`, which returns the whole datagram length, and `recvmsg` elsewhere, whose flags carry the cut (BSD `MSG_TRUNC`, Winsock `WSAEMSGSIZE`). Still size slabs for the largest datagram, since a dropped one is a lost one. On Windows:

- `ready()` probes with socket2's `peek_sender`, which does not fail on a queued datagram larger than the probe buffer (`WSAEMSGSIZE`).
- `WSAECONNRESET` on a UDP receive (Winsock reporting an ICMP port-unreachable for an earlier send) is skipped, so one unreachable peer cannot end the receive loop.
- `SO_REUSEPORT` and `SO_BUSY_POLL` do not exist; setting them is `InvalidConfig`.

## Tests

```bash
cargo nextest run -p transport_socket --features tokio,mio
```

Every type passes the `transport_core` conformance suite on loopback, on all three systems; the readiness regressions, option readback, partial writes and the Winsock cases have their own tests.

## License

MIT OR Apache-2.0, at your option.
