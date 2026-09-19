# transport_socket

Kernel socket transports on socket2: sync UDP, tokio UDP and TCP, runtime-free mio UDP and TCP. One receive loop and one option layer serve every type, on Linux, macOS and Windows.

## Socket types and configs

[[crates/transports/socket/src/udp.rs#UdpSocket]] binds from [[crates/transports/socket/src/config.rs#UdpConfig]] and is the sync busy-poll UDP type; tokio and mio UDP types are built from it, keeping options and pool.

Feature `tokio` adds [[crates/transports/socket/src/tokio/udp.rs#AsyncUdp]] and [[crates/transports/socket/src/tokio/tcp.rs#TcpStream]]; feature `mio` adds [[crates/transports/socket/src/mio/udp.rs#MioUdp]] and [[crates/transports/socket/src/mio/tcp.rs#MioTcp]]. TCP types connect from [[crates/transports/socket/src/config.rs#TcpConfig]] with a connect timeout. Configs validate before any allocation or syscall: an unspecified remote, a zero timeout, or an option the OS lacks (`SO_BUSY_POLL` off Linux, `SO_REUSEPORT` on Windows) is `InvalidConfig`. Options go through socket2 safe setters in `sockopt`.

## Receive loop

[[crates/transports/socket/src/recv.rs#burst]] is the only datagram receive loop. Each socket type passes its recv call and a peek as closures, so the final would-block passes through that type's readiness wrapper.

Datagrams land in `VecPool` slabs as [[crates/transports/socket/src/recv.rs#UdpFrame]]. With the pool empty and nothing pushed, the loop peeks: a queued datagram is `PoolExhausted`, an idle socket `Ok(0)`. An error met after frames were pushed waits in the socket's deferred slot and returns first on the next call. On Windows, `WSAEMSGSIZE` counts a truncated drop and `WSAECONNRESET` is skipped. TCP reads map through [[crates/transports/socket/src/recv.rs#stream]]: empty destination and would-block are `Ok(0)`, a zero-byte read is `PeerClosed`.

## Readiness

tokio types receive through socket2 directly and confirm readiness with a peek inside tokio `try_io`, so a drained socket never reads as ready. mio types report through one caller-owned [[crates/transports/socket/src/mio/ready.rs#ReadySet]].

[[crates/transports/socket/src/recv.rs#peek_ready]] uses socket2 `peek_sender`, which never fails on a large queued datagram. `ReadySet` borrows each `MioUdp` or `MioTcp` only to register it, so legs can move into a consumer; `wait` blocks and is never async. Readiness is edge-triggered: callers drain a reported source until it yields nothing. Every mio syscall runs inside `try_io`, which Windows needs to re-arm a drained leg.

## Tests

Integration tests prove each type's contract on loopback only, on every OS the crate supports.

`tests/conformance.rs` runs the core suite on all five types. `ready_mio.rs` and `ready_tokio.rs` keep the readiness regressions (data queued before registration, idle leg beside active one, re-report after drain, no stale ready after drain). `tcp.rs` covers config rejection, connect failure and a partial write resumed on writable readiness; `sockopt.rs` reads options back from the kernel; `zero_alloc.rs` proves steady-state receive allocates nothing; `windows.rs` covers the Winsock cases.
