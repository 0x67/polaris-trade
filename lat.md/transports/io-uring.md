# transport_io_uring

Linux io_uring UDP receive over the kernel-bypass shell; picks legacy provided buffers, buffer ring or multishot receive at run time. Receive only; on other OS the crate is empty.

## Bind and config

[[crates/transports/io-uring/src/lib.rs#IoUringUdp]] is `BypassTransport<UringDriver>` from the core [[core#Kernel-bypass shell]], implementing `DatagramRecv` with `IndexFrame` frames and `Multicast`, backend name `io-uring`.

[[crates/transports/io-uring/src/config.rs#IoUringConfig]] takes the bind address in `new`; the rest are public fields with defaults, no builder and no serde. Validation runs before any allocation or syscall: at most 32768 slots (one u16 buffer id per slot), slot size and `SO_RCVBUF` within C `int`, a single-shot fleet of at most 4096 and never deeper than the pool. Multicast joins go through socket2 on the bound socket; no io_uring op is involved.

## Receive path detection

[[crates/transports/io-uring/src/probe.rs#open_ring]] maps EPERM and ENOSYS to `Unavailable`, which covers the io_uring sysctl and default Docker seccomp. [[crates/transports/io-uring/src/probe.rs#probe]] then finds which [[crates/transports/io-uring/src/probe.rs#RecvPath]] values work.

Legacy needs the `Recv` and `ProvideBuffers` opcodes plus fast poll. Buffer rings are not probeable, so a one-entry ring on an mmap page is registered and unregistered: arguments valid by construction make EINVAL mean unsupported. Multishot is armed once on the real socket against that empty ring: prep rejects an unknown multishot flag with EINVAL inline, a supporting kernel fails it at once with ENOBUFS before any data moves. [[crates/transports/io-uring/src/probe.rs#select]] honours a forced path or returns `Unsupported`, else takes the best supported path; none at all is `Unavailable`.

## Buffers and recv fleet

Every path lands datagrams in one `IndexPool` region; buffer id equals slot. Slots return to the kernel in one batch per reap.

Legacy provides the whole region with one `ProvideBuffers` at bind and one per contiguous run of freed slots afterwards, with `SKIP_SUCCESS` when the kernel has it. BufRing and Multishot push entries into [[crates/transports/io-uring/src/ring_mem.rs#RingMem]], an mmap'd ring whose tail sits in entry 0; pushes write entry fields one by one, never that tail, and one Release store publishes the batch. Legacy and BufRing keep a fleet of `depth` single-shot recvs armed; Multishot keeps one multishot recv, re-armed whenever a completion lacks `F_MORE`. Every recv passes `MSG_TRUNC`, so [[crates/transports/io-uring/src/completion.rs#classify]] sees a longer datagram's real length: it counts `truncated` and its slot goes straight back.

## Idle spin and exhaustion

[[crates/transports/io-uring/src/driver.rs#UringDriver#reap]] enters the kernel only when the submission queue holds work or the completion queue overflowed, so an idle spin makes no syscall; `DriverStats::syscalls` counts every enter.

Default setup flags only: `SINGLE_ISSUER` with `DEFER_TASKRUN` cannot deliver completions without an enter, and SQPOLL is gone. An ENOBUFS completion counts `no_buffer` and starves the fleet: nothing is re-armed until a freed slot goes back, since a recv on an empty group fails at once and would cost one syscall per spin. With the fleet starved and nothing reaped, the shell returns `PoolExhausted`; the datagram waits in the socket buffer. A recv error met after frames were pushed is deferred by the shell.

## Teardown

Dropping the driver cancels armed recvs, waits a bounded second for each to end, then closes the ring, then frees region and buffer ring.

[[crates/transports/io-uring/src/driver.rs#UringDriver#cancel_recvs]] submits one `AsyncCancel` per enter: a cancelled request leaves the kernel's lookup only after the enter returns, so a batch of cancels would all hit the same request, and the all-match flag needs 5.19. If the wait runs out, closing the ring starts an asynchronous teardown that may still write, so region and buffer ring are leaked and one `warn` event is logged.

## Tests

Pure logic is tested in-crate on any Linux host; everything touching a real ring is in `tests/real_io_uring.rs`, ignored, run privileged.

In-crate: `classify` for every completion shape, `select` forced and auto, config limits, `RingMem` pushes and publish across a u16 tail wrap (entry 0 writes leave the published tail alone), and the starved gate. Ignored, per forced path: the core conformance suite with `PoolExhausted` signalling, `no_buffer` rising at exhaustion, a truncated datagram freeing the only slot, 10000 idle bursts with `syscalls` flat, and drop confirming cancellation idle and under traffic. Also a multicast join receiving a group datagram, and `Unavailable` when run without io_uring access. `benches/classify.rs` times `classify` over a mixed completion stream.
