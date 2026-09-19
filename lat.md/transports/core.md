# transport_core

Shared transport layer: capability traits, fixed-capacity bursts, buffer pools, L2-to-UDP decap, kernel-bypass shell and one typed error. Makes no syscall; backends own drivers and configs.

## Capability traits

Base trait carries identity only. Receive and send are separate traits, so a receive-only backend implements no send method and nothing returns `Unsupported` for a claimed capability.

[[crates/transports/core/src/transport.rs#Transport]] gives the backend name. [[crates/transports/core/src/transport.rs#DatagramRecv]] reaps UDP payloads and [[crates/transports/core/src/transport.rs#L2Recv]] reaps whole Ethernet frames, both synchronous and burst-first. [[crates/transports/core/src/transport.rs#DatagramSend]] sends without blocking: a full socket buffer is an `Io` error of kind `WouldBlock`.

[[crates/transports/core/src/transport.rs#StreamRecv]] is an `unsafe trait`: `Ok(n)` promises `dst[..n]` initialised, which lets SoupBinTCP advance its buffer without zeroing. [[crates/transports/core/src/transport.rs#StreamTrySend]] is a partial non-blocking write; [[crates/transports/core/src/transport.rs#StreamSend]] and [[crates/transports/core/src/transport.rs#AsyncReady]] are async and exist only where readiness is truly asynchronous.

## Burst container

[[crates/transports/core/src/transport.rs#FrameBatch]] has a non-zero capacity fixed at construction and never grows, so steady-state receive allocates nothing.

Callers drain before receiving (`spare() > 0` is a debug-asserted precondition), so `Ok(0)` from a receive always means idle, never "no room".

## Errors

[[crates/transports/core/src/error.rs#TransportError]] is one non-exhaustive enum. The OS error sits in a typed field named `error`, shown in `Display` and not exposed as `source()`.

Workspace logs format errors with `%err` (Display only), so a source-only OS error would vanish from logs, and exposing it twice would print it twice in chain formatters. Reasons are `&'static str`, so building an error never allocates. Config checks live in `config::validate` and return `InvalidConfig` before any allocation.

## Buffer pools

Two pools, both in core: heap slabs for sockets and one contiguous slot region for kernel-bypass drivers. Consumers only see `PoolStats`, never pool memory.

[[crates/transports/core/src/pool/vec.rs#VecPool]] preallocates zeroed slabs; backends take them through the hidden `pool::backend` module (`acquire`, `buf_mut`, `set_len`), and `new` returns `InvalidConfig` rather than panicking on an oversized pool. [[crates/transports/core/src/pool/index.rs#IndexPool]] maps fixed-stride slots of one 64 KiB-aligned zeroed region; only the driver knows which slots the kernel handed back, so `frame` is `unsafe`, and dropped frames return through `drain_freed`. Both count `in_use` as the shared handle's strong count minus one, so no per-packet atomic is added.

## L2-to-UDP decap

[[crates/transports/core/src/decap.rs#UdpDecap]] turns any `L2Recv` into a `DatagramRecv`, so AF_XDP and DPDK serve datagram consumers such as MoldUDP64.

It parses Ethernet II with at most one 802.1Q tag, IPv4 without fragments, and UDP to one destination port (optionally one destination address). The payload is bounded by the IPv4 total length, so Ethernet padding never leaks in. Frames that do not fit the caller's batch wait for the next call; a reap whose frames were all filtered is followed by another reap, so `Ok(0)` still means idle. Drops are counted per reason in [[crates/transports/core/src/decap.rs#DecapStats]]; checksums are not verified.

## Kernel-bypass shell

One generic transport serves io_uring, AF_XDP and DPDK: each backend supplies only a driver, and the shell adds telemetry, exhaustion mapping and error deferral once.

[[crates/transports/core/src/bypass/mod.rs#Driver]] reaps frames without blocking and reports monotonic counters (`no_buffer`, `nic_missed`, `truncated`, `syscalls`); its `Layer` (`L4` or `L2`) decides whether [[crates/transports/core/src/bypass/mod.rs#BypassTransport]] implements `DatagramRecv` or `L2Recv`. `Exhausted` with nothing pushed becomes `PoolExhausted`; a driver error met after frames were pushed is returned on the next call, so frames never travel with `Err` and `Ok(0)` stays idle. While the metrics gate is on, the shell records each non-empty burst and reads driver counters every `STATS_EVERY` (1024) calls, empty or not, reporting only their increase.

[[crates/transports/core/src/bypass/mock.rs#MockDriver]] (feature `testing`) copies injected bytes into a free `IndexPool` slot at once, as NIC DMA would, and counts `no_buffer` when none is free. Its `L2` flavour carries whole Ethernet frames for `UdpDecap`. Integration tests run the conformance suite on both layers and prove steady-state bursts allocate nothing.

## Telemetry

Behind the `observability` feature, [[crates/transports/core/src/telemetry.rs#record_recv_burst]] counts packets and bytes, skipping empty bursts before reading the gate, so an idle spin pays one compare.

[[crates/transports/core/src/telemetry.rs#record_drops]] reports increases of a backend's drop counters under reason labels (`no_buffer`, `nic_missed`, `truncated`, `decap_filtered`), read at a bounded interval, never per packet.

## Conformance suite

One generic suite behind feature `testing`, documented unstable, proves each backend's receive and send contract. It names no backend and sizes pools through its build hook, never `acquire`.

[[crates/transports/core/src/testing/conformance/datagram.rs#run_datagram]] takes a [[crates/transports/core/src/testing/conformance/datagram.rs#DatagramHarness]] (build with pool capacity, inject one datagram, read drop count, declared [[crates/transports/core/src/testing/conformance/datagram.rs#ExhaustionSignal]]) and checks payload bytes and order, the `spare()` bound, exhaustion then cross-thread reclaim on a two-buffer pool, and `pool_stats` accounting.

[[crates/transports/core/src/testing/conformance/stream.rs#run_stream]] and [[crates/transports/core/src/testing/conformance/stream.rs#run_stream_async]] pair the transport with a blocking std `TcpStream` peer and check empty and idle reads, ordered bytes, `PeerClosed`, resumed `try_send` and 8 MiB `send_all`. Sync waits poll under a 5 s bound; the async runner uses no timer, so the caller supplies runtime and overall timeout.
