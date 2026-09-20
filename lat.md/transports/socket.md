# transport-socket

Kernel socket transports on socket2: sync UDP, tokio UDP and TCP, runtime-free mio UDP and TCP. One receive loop and one option layer serve every type, on Linux, macOS and Windows.

Built on the [[core#Capability traits]] and the `VecPool` of [[core#Buffer pools]]. Both clients test against it: [[moldudp]] legs and requester, [[soupbintcp]] streams.

## Socket types and configs

[[crates/transports/socket/src/udp.rs#UdpSocket]] binds from [[crates/transports/socket/src/config.rs#UdpConfig]] and is the sync busy-poll UDP type; tokio and mio UDP types are built from it, keeping options and pool.

Feature `tokio` adds [[crates/transports/socket/src/tokio/udp.rs#AsyncUdp]] and [[crates/transports/socket/src/tokio/tcp.rs#TcpStream]]; feature `mio` adds [[crates/transports/socket/src/mio/udp.rs#MioUdp]] and [[crates/transports/socket/src/mio/tcp.rs#MioTcp]]. TCP types connect from [[crates/transports/socket/src/config.rs#TcpConfig]] with a connect timeout. Configs validate before any allocation or syscall: an unspecified remote, a zero timeout, or an option the OS lacks (`SO_BUSY_POLL` off Linux, `SO_REUSEPORT` on Windows) is `InvalidConfig`. Options go through socket2 safe setters in `sockopt`.

Every platform-limited option defaults to off, and an explicit one the platform cannot apply is an error, so no option is ever silently ignored and no warning path exists.

## Receive loop

[[crates/transports/socket/src/recv.rs#burst]] is the only datagram receive loop. Each socket type passes its recv call and a peek as closures, so the final would-block passes through that type's readiness wrapper.

Datagrams land in `VecPool` slabs as [[crates/transports/socket/src/recv.rs#UdpFrame]], which carries the real sender address. With the pool empty and nothing pushed, the loop peeks: a queued datagram is `PoolExhausted`, an idle socket `Ok(0)`. An error met after frames were pushed waits in the socket's deferred slot and returns first on the next call. A datagram the kernel cut is dropped, counted `Truncated` under `observability`, and the slab goes back to the pool; on Windows `WSAECONNRESET` is skipped. The recv closure sees the slab as `MaybeUninit` bytes but must never write uninitialised ones, since the slab is read as `[u8]` afterwards. TCP reads map through [[crates/transports/socket/src/recv.rs#stream]]: empty destination and would-block are `Ok(0)`, a zero-byte read is `PeerClosed`.

Every UDP type receives through [[crates/transports/socket/src/recv.rs#datagram]], which takes the cheapest call that reports the cut: on Linux `recvfrom` with `MSG_TRUNC`, whose returned length is the whole datagram and so exceeds the slab when it did not fit, and elsewhere `recvmsg` over one slab, whose flags carry the cut (`MSG_TRUNC` on the BSDs, `WSAEMSGSIZE` folded in by socket2 on Windows). Plain `recv_from` delivers the cut payload unsignalled on Unix, and the receive loop has no `WSAEMSGSIZE` arm of its own.

## Readiness

tokio types receive through socket2 directly and confirm readiness with a peek inside tokio `try_io`, so a drained socket never reads as ready. mio types report through one caller-owned [[crates/transports/socket/src/mio/ready.rs#ReadySet]].

[[crates/transports/socket/src/recv.rs#peek_ready]] uses socket2 `peek_sender`, which never fails on a large queued datagram. `ReadySet` borrows each `MioUdp` or `MioTcp` only to register it, so legs can move into a consumer; `wait` blocks and is never async. Readiness is edge-triggered: callers drain a reported source until it yields nothing. Every mio read and write runs inside `try_io`, which Windows needs to re-arm a drained leg.

## Platform

Linux, macOS and Windows, unprivileged. A few options and behaviours depend on the OS.

- `busy_poll_us` is Linux only; raising it above `net.core.busy_read` needs `CAP_NET_ADMIN`.
- `reuse_port` is Unix only.
- A datagram longer than its slab is dropped and counted on every OS, never delivered cut. Size slabs for the largest datagram: a dropped one is a lost one.
- tokio constructors need a runtime with its IO driver on the calling thread (else `Unavailable`); `TcpStream::connect` also needs the time driver.

## Decisions

Choices carried over from the previous tokio and mio crates, and the ones reversed, with the reason.

### Kept

The tokio cleared-readiness fix and its regression test stay, and `AsyncReady` stays on tokio types, where readiness is truly asynchronous.

- tokio receive bypasses the reactor, and readiness peeks inside `try_io` so tokio clears cached readiness a direct receive left stale; `AsyncFd` would be Unix-only.
- `recvmmsg` and kernel drop-counter readback wait for a measured bench delta.
- Default pool shape stays 1024 slabs of 2048 bytes; consumers size their own.

### Reversed

The two runtime crates merged into one, and the mio path stopped pretending to be async.

- mio's `AsyncReady` was an `async fn` that blocked on its own poll, so an idle MoldUDP64 leg starved the other. `ReadySet` replaced it: one caller-owned poll over many sockets.
- mio's probe before parking went: registration reports data already queued (epoll, kqueue and the Windows AFD poll all do), and callers drain until empty before waiting. `first_wait_reports_data_queued_before_registration` keeps the lost-wakeup case.
- mio's send slept 1 ms and retried; `send_to` now never blocks and returns `WouldBlock` for the caller to handle.
- The byte-identical pool, frame, socket options and peek probes of the two crates exist once. The UDP-or-TCP enums and their runtime `Unsupported` arms are gone.
- The hand-written `SO_BUSY_POLL` setsockopt became socket2's safe setter; `SO_RXQ_OVFL` (set, never read) and timestamping options were removed, as was the CPU affinity field no socket could honour.
- mio's hard-coded 5 s connect timeout became the `connect_timeout` field, used by both TCP types.

## Tests

Integration tests prove each type's contract on loopback only, on every OS the crate supports.

- `tests/conformance.rs` runs the core suite on all five types: `udp_socket_meets_datagram_contract`, `mio_udp_meets_datagram_contract`, `async_udp_meets_datagram_contract`, `mio_tcp_meets_stream_contract`, `tokio_tcp_meets_stream_contract`, `tokio_tcp_meets_async_stream_contract`.
- `tests/ready_mio.rs`: data queued before registration, idle leg beside active one, re-report after drain, wake while blocked, TCP readable. `tests/ready_tokio.rs`: no stale ready after drain, UDP and TCP.
- `tests/tcp.rs`: config rejection before connect, refused connect as `Connect`, partial write resumed on writable readiness. On Linux and macOS a connect to a listener with a full accept queue fails `Connect` of kind `TimedOut` within bound (Winsock refuses instead).
- `tests/sockopt.rs` reads options back from the kernel and checks platform-limited ones fail loudly; buffer sizes above `i32::MAX` and `ReadySet` above 65536 events are `InvalidConfig` naming the field.
- `tests/udp.rs`: `Bind` carries `AddrInUse` and the OS text, a `send_to` flood only ever fails with `WouldBlock`, frames carry the sender, a datagram longer than the one slab is dropped whole and the next one still lands, counted as a truncated drop under `observability`, a second join of one group is refused by the kernel (proving membership), and an interface of the other family is `InvalidConfig`.
- In-crate: `recv::burst` with the pool run dry after a push keeps the frame, returns no error and delivers the queued datagram next call; `send_to` maps ENOBUFS to `WouldBlock` keeping the OS error and passes EAGAIN through unwrapped.
- `tests/zero_alloc.rs`: steady-state receive allocates nothing. `tests/windows.rs`: `WSAECONNRESET` never ends the receive loop, and a large queued datagram makes `AsyncUdp` ready.

`benches/recv.rs` times the drain of a pre-filled socket at burst sizes 1 to 64, with the sends outside the timed region, and an idle spin.
