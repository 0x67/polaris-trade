# transport_core

Shared transport layer: capability traits, fixed-capacity bursts, buffer pools, L2-to-UDP decap, kernel-bypass shell and one typed error. Makes no syscall; backends own drivers and configs.

Every backend builds on it: [[socket]] lands datagrams in `VecPool` slabs, and [[io-uring]], [[afxdp]] and [[dpdk]] plug a driver into the [[core#Kernel-bypass shell]]. The clients [[moldudp]] and [[soupbintcp]] depend on this crate alone and take any backend through its traits.

## Capability traits

Base trait carries identity only. Receive and send are separate traits, so a receive-only backend implements no send method and nothing returns `Unsupported` for a claimed capability.

[[crates/transports/core/src/transport.rs#Transport]] gives the backend name. [[crates/transports/core/src/transport.rs#DatagramRecv]] reaps UDP payloads and [[crates/transports/core/src/transport.rs#L2Recv]] reaps whole Ethernet frames, both synchronous and burst-first. [[crates/transports/core/src/transport.rs#DatagramSend]] sends without blocking: a full socket buffer is an `Io` error of kind `WouldBlock`. [[crates/transports/core/src/transport.rs#Multicast]] joins a group.

[[crates/transports/core/src/transport.rs#StreamRecv]] is an `unsafe trait`: `Ok(n)` promises `dst[..n]` initialised, which lets SoupBinTCP advance its buffer without zeroing. [[crates/transports/core/src/transport.rs#StreamTrySend]] is a partial non-blocking write; [[crates/transports/core/src/transport.rs#StreamSend]] and [[crates/transports/core/src/transport.rs#AsyncReady]] are async and exist only where readiness is truly asynchronous.

The split follows the consumers: MoldUDP64 needs datagram receive plus one sender for re-requests, SoupBinTCP needs stream receive and send, and AF_XDP and DPDK can only offer L2 receive. Constructors are inherent functions on each backend, never trait methods, so each takes its own config.

## Burst container

[[crates/transports/core/src/transport.rs#FrameBatch]] has a non-zero capacity fixed at construction and never grows, so steady-state receive allocates nothing.

Callers drain before receiving (`spare() > 0` is a debug-asserted precondition), so `Ok(0)` from a receive always means idle, never "no room". Frames are owned handles: each holds its buffer until dropped, on any thread.

## Errors

[[crates/transports/core/src/error.rs#TransportError]] is one non-exhaustive enum. The OS error sits in a typed field named `error`, shown in `Display` and not exposed as `source()`.

Workspace logs format errors with `%err` (Display only), so a source-only OS error would vanish from logs, and exposing it twice would print it twice in chain formatters. Reasons are `&'static str`, so building an error never allocates. Config checks live in `config::validate` and return `InvalidConfig` before any allocation.

## Buffer pools

Two pools, both in core: heap slabs for sockets and one contiguous slot region for kernel-bypass drivers. Consumers only see `PoolStats`, never pool memory.

[[crates/transports/core/src/pool/vec.rs#VecPool]] preallocates zeroed slabs; backends take them through the hidden `pool::backend` module (`acquire`, `buf_mut`, `set_len`), and `new` returns `InvalidConfig` rather than panicking on an oversized pool. [[crates/transports/core/src/pool/index.rs#IndexPool]] maps fixed-stride slots of one 64 KiB-aligned zeroed region; only the driver knows which slots the kernel handed back, so `frame` is `unsafe`, and dropped frames return through `drain_freed`. Both count `in_use` as the shared handle's strong count minus one, so no per-packet atomic is added.

## L2-to-UDP decap

[[crates/transports/core/src/decap.rs#UdpDecap]] turns any `L2Recv` into a `DatagramRecv`, so [[afxdp]] and [[dpdk]] serve datagram consumers such as MoldUDP64.

It parses Ethernet II with at most one 802.1Q tag, IPv4 without fragments, and UDP to one destination port (optionally one destination address, so A and B feeds sharing a port on one queue stay apart). The payload is bounded by the IPv4 total length, so Ethernet padding never leaks in. Frames that do not fit the caller's batch wait for the next call; a reap whose frames were all filtered is followed by another reap, so `Ok(0)` still means idle. Drops are counted per reason in [[crates/transports/core/src/decap.rs#DecapStats]] and reported as `decap_filtered`. IPv4 only; checksums are not verified.

## Kernel-bypass shell

One generic transport serves io_uring, AF_XDP and DPDK: each backend supplies only a driver, and the shell adds telemetry, exhaustion mapping and error deferral once.

[[crates/transports/core/src/bypass/mod.rs#Driver]] reaps frames without blocking and reports monotonic counters (`no_buffer`, `nic_missed`, `truncated`, `syscalls`); its `Layer` (`L4` or `L2`) decides whether [[crates/transports/core/src/bypass/mod.rs#BypassTransport]] implements `DatagramRecv` or `L2Recv`. `Exhausted` with nothing pushed becomes `PoolExhausted`; a driver error met after frames were pushed is returned on the next call, so frames never travel with `Err` and `Ok(0)` stays idle. While the metrics gate is on, the shell records each non-empty burst and reads driver counters every [[crates/transports/core/src/bypass/mod.rs#STATS_EVERY]] (1024) calls, empty or not, reporting only their increase.

Pooled drivers ([[io-uring]], [[afxdp]]) recycle freed slots at the start of each reap; [[dpdk]] frees mbufs in place. Each backend wraps the shell in its own type without exposing it, so users never reach a real driver.

[[crates/transports/core/src/bypass/mock.rs#MockDriver]] (feature `testing`) copies injected bytes into a free `IndexPool` slot at once, as NIC DMA would, and counts `no_buffer` when none is free. Its `L2` flavour carries whole Ethernet frames for `UdpDecap`.

## Telemetry

Behind the `observability` feature, [[crates/transports/core/src/telemetry.rs#record_recv_burst]] counts packets and bytes, skipping empty bursts before reading the gate, so an idle spin pays one compare.

[[crates/transports/core/src/telemetry.rs#record_drops]] reports increases of a backend's drop counters under reason labels (`no_buffer`, `nic_missed`, `truncated`, `decap_filtered`), read at a bounded interval, never per packet. Metric names live here once, so backends never drift.

## Conformance suite

One generic suite behind feature `testing`, documented unstable, proves each backend's receive and send contract. It names no backend and sizes pools through its build hook, never `acquire`.

[[crates/transports/core/src/testing/conformance/datagram.rs#run_datagram]] takes a [[crates/transports/core/src/testing/conformance/datagram.rs#DatagramHarness]] (build with pool capacity, inject one datagram, read drop count, declared [[crates/transports/core/src/testing/conformance/datagram.rs#ExhaustionSignal]]) and checks payload bytes and order, the `spare()` bound, exhaustion then cross-thread reclaim on a two-buffer pool, and `pool_stats` accounting.

[[crates/transports/core/src/testing/conformance/stream.rs#run_stream]] and [[crates/transports/core/src/testing/conformance/stream.rs#run_stream_async]] pair the transport with a blocking std `TcpStream` peer and check empty and idle reads, ordered bytes, `PeerClosed`, resumed `try_send` and 8 MiB `send_all`. Sync waits poll under a 5 s bound; the async runner uses no timer, so the caller supplies runtime and overall timeout.

## Features and platform

Pure logic and memory: builds and runs on every OS with no system library and no privilege. Both features are off by default.

`observability` pulls `observability-core` (a git dependency, not yet on crates.io, so crates enabling it cannot be published) and `metrics`; every backend and client forwards it as its own `observability` feature. `testing` exposes the conformance suite and `MockDriver` for dev-dependencies and carries no stability promise.

## Decisions

Design choices carried over from the previous transport crates, and the ones reversed, each with the reason.

### Kept

Owned frames, synchronous burst-first receive and a syscall-free core stay, because every consumer and backend depends on them.

- Owned frames: a consumer can hold a frame or move it to another thread without borrowing the transport. The cost, one `Arc` clone and one mutex push per pooled frame, stays until a bench on real Linux justifies a borrowed-burst path.
- Synchronous, burst-first receive with optional `AsyncReady`: busy-poll and kernel-bypass backends serve every consumer without an executor.
- No syscall in core: pools are memory only, so core builds everywhere and Miri runs it.
- One gated telemetry check per burst, no per-frame atomics or spans.
- No `Box<dyn Transport>`: consumers are generic over the traits.
- The conformance suite names no backend.
- A driver seam generic over the driver, so mock and real drivers share one receive contract; it now exists once here instead of once per backend.
- Deferred buffer return: a dropped frame queues its slot and the driver recycles it on its own thread, since fill and buffer rings are single-producer.

### Reversed

The old trait shape forced every backend to claim capabilities it lacked, and exposed pool memory the kernel could write.

- `TransportCore` (with an async `send` on every backend) and `TransportBind` (UDP and TCP constructors on one type) became capability traits. Their runtime `Unsupported` stubs, a UDP `send` that always failed for lack of a destination, and an unused pool on every TCP stream are gone.
- `PoolAccess`, which exposed `acquire` on the live pool, became `pool_stats`. On io_uring and AF_XDP a public `acquire` handed out memory the kernel could still write.
- `AsPayload` put `sequence` and `stream_id` on every raw frame, as constant zero. Frames are now `AsRef<[u8]>`; sequence numbers live on the client frames.
- The `max` argument and `FrameBatch::default()` (capacity zero) are gone: `Ok(0)` from a zero-capacity batch looked like idle while data waited.
- `Display` strings are no longer locked for log matching (no alert or dashboard used them), and OS errors keep their typed `io::Error` instead of a string.
- The shared serde config built from `Default` became one plain config per backend, required fields as `new` arguments. Backends silently ignored shared fields they could not apply, and a defaulted TCP remote would connect to localhost. Client configs stay serde.
- `PoolExhausted` is returned only when data is pending. Backends that cannot see pending data (AF_XDP, DPDK) report drop counters instead.

## Tests

Unit and integration tests need no privilege and run on every OS; Miri covers the pools and the bypass shell.

- `tests/bypass.rs`: `l4_shell_passes_datagram_conformance` and `l2_shell_under_decap_passes_datagram_conformance` run the suite over the mock; `decap_delivers_reap_larger_than_out_across_calls`, `driver_error_after_frames_waits_one_call_and_returns_once`, `exhausted_is_pool_exhausted_only_when_nothing_pushed`, and (with `observability`) `no_buffer_delta_reported_every_stats_every_calls`.
- `tests/miri_pool.rs` (also under Miri): every slab handed out then reused, frames at non-zero offsets reading their own bytes, cross-thread drop returning a slot, oversized pools rejected.
- `tests/zero_alloc.rs`: steady-state bursts allocate nothing on the L4 shell and on the L2 shell under `UdpDecap`.
- In-crate: `parse_udp` over plain, VLAN, IPv4 options, padding, fragments, wrong port or address, non-UDP, non-IPv4 and truncated frames; zero-count telemetry records nothing; config validation.

Benches: `benches/pool.rs` (acquire and drop, frame, drop and drain), `benches/bypass.rs` (drain burst and idle spin over the mock) and `benches/decap.rs`.
