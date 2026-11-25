# transport-core

Traits, burst container, buffer pools, errors and the kernel-bypass shell shared by every transport in this workspace. It makes no syscall and needs no system library, so it builds on any OS.

Consumers write against these traits and take any backend: `transport-socket` (kernel sockets on Linux, macOS and Windows), `transport-io-uring`, `transport-afxdp` and `transport-dpdk` (Linux). Backends build themselves from their own config types; nothing here binds or connects.

## Traits

| Trait | Meaning |
| --- | --- |
| `Transport` | backend name only (metric label, error field) |
| `DatagramRecv` | burst receive of UDP payloads, synchronous |
| `L2Recv` | burst receive of whole Ethernet frames, synchronous |
| `DatagramSend` | `send_to`, never blocks: a full buffer is `Io` with kind `WouldBlock` |
| `Multicast` | join a group on an interface |
| `StreamRecv` | `unsafe trait`: `recv_into` may return `Ok(n)` only after initialising `dst[..n]` |
| `StreamTrySend` | partial non-blocking write, `Ok(0)` when nothing fits |
| `StreamSend` | async whole-buffer write |
| `AsyncReady` | async readiness, only on backends whose readiness is truly asynchronous |

A backend implements only what it can do: the kernel-bypass backends implement no send trait at all.

Frames are owned handles implementing `AsRef<[u8]>`: each holds its buffer until dropped, on any thread. `FrameBatch` has a non-zero capacity fixed at construction and never grows; receive pushes at most `spare()` frames, and callers drain before receiving, so `Ok(0)` always means idle.

```rust,ignore
use std::num::NonZeroUsize;
use transport_core::{DatagramRecv, FrameBatch, TransportError};

// works over any backend: a socket, io_uring, or UdpDecap over AF_XDP or DPDK
fn run<T: DatagramRecv>(rx: &mut T, mut handle: impl FnMut(&[u8])) -> Result<(), TransportError> {
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(64).unwrap());
    loop {
        if rx.recv_burst(&mut out)? == 0 {
            continue; // idle: spin, or wait on the backend's readiness
        }
        for frame in out.drain() {
            handle(frame.as_ref());
        }
    }
}
```

`recv_burst` returns `PoolExhausted` when data is pending but the caller holds every buffer; drop frames to free them. Backends that cannot see pending data (AF_XDP, DPDK) never return it and count drops instead.

## Pools

`VecPool` holds heap slabs for socket backends; `IndexPool` is one page-aligned, zeroed region of fixed-stride slots that io_uring and AF_XDP register with the kernel. Consumers see only `pool_stats()` (`capacity`, `in_use`), never pool memory: slab access lives in a hidden module for backend crates, and `IndexPool::frame` is `unsafe` because only the driver knows which slots the kernel has handed back.

## L2-to-UDP decap

`decap::UdpDecap` wraps any `L2Recv` and yields a `DatagramRecv` of UDP payloads to one destination port, and optionally one destination address:

```rust,ignore
use transport_core::decap::UdpDecap;

let mut feed = UdpDecap::new(l2, 26_400, None, NonZeroUsize::new(32).unwrap());
```

It parses Ethernet II with at most one 802.1Q tag, then IPv4 and UDP. Fragments, other destinations, non-IPv4 and truncated frames are dropped and counted per reason (`stats()`). IPv4 only; checksums are not verified.

## Kernel-bypass shell

`bypass::BypassTransport<D>` wraps a backend's `bypass::Driver` and adds, once for every backend: burst telemetry, drop-counter deltas every `STATS_EVERY` (1024) calls, `PoolExhausted` when the driver is out of buffers with nothing pushed, and deferral of an error met after frames were pushed, so frames never arrive together with `Err`. A driver's `Layer` (`L4` or `L2`) picks `DatagramRecv` or `L2Recv`.

## Errors

`TransportError` is one `#[non_exhaustive]` enum: `Bind`, `Connect`, `InvalidConfig`, `Io`, `PoolExhausted`, `PeerClosed`, `Unsupported`, `Unavailable`. The OS error is a typed `io::Error` in a field named `error`, shown in `Display` (logs format errors with `%err`) and not repeated through `source()`. Match on `error.kind()`. Every constructor validates its whole config and returns `InvalidConfig` before any allocation or syscall.

## Features

| Feature | Enables |
| --- | --- |
| `observability` | `telemetry`: `transport.recv.packets`, `transport.recv.bytes` and `transport.recv.drops` (reasons `no_buffer`, `nic_missed`, `truncated`, `decap_filtered`) behind the `observability-core` runtime gate |
| `testing` | `testing::conformance` (the suite every backend passes) and `bypass::MockDriver`; for dev-dependencies, no stability promise |

Both are off by default. `observability` pulls `observability-core` as a git dependency, which is not yet on crates.io, so crates enabling it cannot be published.

## Tests

```bash
cargo nextest run -p transport-core --features testing
cargo +nightly miri nextest run -p transport-core --test miri_pool
```

The suite runs over the mock driver on both layers, a counting allocator proves steady-state bursts allocate nothing, and Miri covers both pools.

## License

MIT OR Apache-2.0, at your option.
