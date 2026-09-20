# transport_io_uring

Linux io_uring UDP receive over the kernel-bypass shell; picks legacy provided buffers, buffer ring or multishot receive at run time. Receive only; on other OS the crate is empty.

## Bind and config

[[crates/transports/io-uring/src/lib.rs#IoUringUdp]] is `BypassTransport<UringDriver>` from the core [[core#Kernel-bypass shell]], implementing `DatagramRecv` with `IndexFrame` frames and `Multicast`, backend name `io-uring`.

[[crates/transports/io-uring/src/config.rs#IoUringConfig]] takes the bind address in `new`; the rest are public fields with defaults, no builder and no serde. Validation runs before any allocation or syscall: at most 32768 slots (one u16 buffer id per slot), slot size and `SO_RCVBUF` within C `int`, a single-shot fleet of at most 4096 and never deeper than the pool. Multicast joins go through socket2 on the bound socket; no io_uring op is involved. One `info` event at bind names the chosen path.

## Receive path detection

[[crates/transports/io-uring/src/probe.rs#open_ring]] maps EPERM and ENOSYS to `Unavailable`, which covers the io_uring sysctl and default Docker seccomp. [[crates/transports/io-uring/src/probe.rs#probe]] then finds which [[crates/transports/io-uring/src/probe.rs#RecvPath]] values work.

Legacy needs the `Recv` and `ProvideBuffers` opcodes plus fast poll. Buffer rings are not probeable, so a one-entry ring on an mmap page is registered and unregistered: arguments valid by construction make EINVAL mean unsupported. Multishot is armed once on the real socket against that empty ring: prep rejects an unknown multishot flag with EINVAL inline, a supporting kernel fails it at once with ENOBUFS before any data moves. The probe ring is unregistered before any probe error propagates; if unregister fails, the ring is leaked, never unmapped under the kernel. [[crates/transports/io-uring/src/probe.rs#select]] honours a forced path or returns `Unsupported`, else takes the best supported path; none at all is `Unavailable`.

Detection asks the running kernel rather than its version string, so distribution backports are honoured.

## Buffers and recv fleet

Every path lands datagrams in one `IndexPool` region; buffer id equals slot. Slots return to the kernel in one batch per call.

Legacy provides the whole region with one `ProvideBuffers` at bind and one per contiguous run of freed slots afterwards, with `SKIP_SUCCESS` when the kernel has it. BufRing and Multishot push entries into [[crates/transports/io-uring/src/ring_mem.rs#RingMem]], an mmap'd ring whose tail sits in entry 0; pushes write entry fields one by one, never that tail, and one Release store publishes the batch. Legacy and BufRing keep a fleet of `depth` single-shot recvs armed; Multishot keeps one multishot recv, re-armed whenever a completion lacks `F_MORE`. Every recv passes `MSG_TRUNC`, so [[crates/transports/io-uring/src/completion.rs#classify]] sees a longer datagram's real length: it counts `truncated` and its slot goes straight back. The completion queue is sized for every slot plus the fleet, so it does not overflow in normal operation.

An empty datagram, which any host reaching the port can send, completes on 6.0+ with result 0 and no buffer id, the kernel keeping the buffer: `classify` returns `Empty`, nothing is delivered, the recv is re-armed as for data, and the driver reads on. Before 6.0 it arrives as a 0-byte frame. Only a non-empty success without buffer id is `EIO`.

[[crates/transports/io-uring/src/driver.rs#provide_runs]] hands legacy runs over one by one; a run that cannot be queued stays in `back` with every later run for the next call, so no slot leaves the pool, and only handed slots reopen a starved fleet. The driver returns a recv error before a refill error; a failed refill leaves its work queued, so it recurs.

## Idle spin and exhaustion

[[crates/transports/io-uring/src/driver.rs#UringDriver#poll_frames]] enters the kernel only when the submission queue holds work or the completion queue overflowed, so an idle spin makes no syscall; `DriverStats::syscalls` counts every enter.

Default setup flags only: `SINGLE_ISSUER` with `DEFER_TASKRUN` cannot deliver completions without an enter, and SQPOLL is gone. An ENOBUFS completion counts `no_buffer` and starves the fleet: nothing is re-armed until a freed slot goes back, since a recv on an empty group fails at once and would cost one syscall per spin. With the fleet starved and nothing received, the shell returns `PoolExhausted`; the datagram waits in the socket buffer. A recv error met after frames were pushed is deferred by the shell.

## Teardown

Dropping the driver cancels armed recvs, waits a bounded second for each to end, then closes the ring, then frees region and buffer ring.

[[crates/transports/io-uring/src/driver.rs#UringDriver#cancel_recvs]] submits one `AsyncCancel` per enter: a cancelled request leaves the kernel's lookup only after the enter returns, so a batch of cancels would all hit the same request, and the all-match flag needs 5.19. If the wait runs out, closing the ring starts an asynchronous teardown that may still write, so region and buffer ring are leaked and one `warn` event is logged.

## Platform and privileges

Linux only, and io_uring must be allowed: the process needs no capability, but sysctl and seccomp can block it.

- `kernel.io_uring_disabled` must be 0, or 1 with the process in `kernel.io_uring_group`. Docker's default seccomp profile blocks `io_uring_setup`; bind then returns `Unavailable` with the OS error.
- Kernels before 5.12 charge ring memory to `RLIMIT_MEMLOCK`.
- Kernel floors per path: Legacy needs provided buffers and fast poll, BufRing 5.19, Multishot 6.0. All three were verified on 7.0; 5.14 to 5.19 kernels have not been run.
- The bounded teardown wait needs `IORING_FEAT_EXT_ARG` (5.11); without it the region leaks whenever recvs are still armed at drop, with the warning.

## Decisions

Choices carried over from the previous io_uring backend, and the ones reversed, with the reason.

### Kept

The single-shot recv fleet stays for Legacy and BufRing, since it runs on kernels without multishot.

- Owned `IndexFrame` frames and deferred slot return through the shared pool.
- Receive only: io_uring serves the datagram hot path; re-requests and sessions use [[socket]].

### Reversed

The old backend used only legacy provided buffers, entered the kernel on every spin and could not send.

- One `ProvideBuffers` per recycled buffer became one per contiguous run, and buffer ring and multishot paths were added, chosen by probing the running kernel.
- `submit` on every receive became submit only when work is queued, so an idle spin is syscall-free; the bind-time completion overflow is gone because the queue is sized for every slot.
- A recv re-armed on every spin while the pool was empty (one syscall and one ENOBUFS per spin) now waits for a freed slot.
- `send`, which always failed on the unconnected socket, and the stub TCP connect were removed.
- SQPOLL and the CPU pinning options were removed: with buffer rings and multishot the submission queue is empty in steady state, so an SQPOLL thread would only burn a core.
- Frames no longer carry a `peer()` that was always unspecified.

## Tests

Pure logic is tested in-crate on any Linux host; everything touching a real ring is in `tests/real_io_uring.rs`, ignored, run privileged.

In-crate: `classify_maps_every_completion_shape`, `select_honours_forced_path_only_when_present`, `select_auto_picks_best_supported`, config limits, `push_and_publish_wrap_u16_tail_without_touching_published_tail` for `RingMem`, the starved gate (`starved_fleet_arms_nothing_until_slot_goes_back`, `fleet_rearms_only_when_request_ends`), and `failed_provide_keeps_unhanded_slots_for_next_call`. Probe cleanup on failure and the `poll_frames` error order need a failing kernel and are not tested.

Ignored, per forced path (`legacy_path`, `buf_ring_path`, `multishot_path`): the core conformance suite with `PoolExhausted` signalling, `no_buffer` rising at exhaustion, a truncated datagram freeing the only slot, an empty datagram followed by a payload never failing a burst, 10000 idle bursts with `syscalls` flat, and drop confirming cancellation idle and under traffic. Also `multicast_join_receives_group_datagram`, and `bind_without_io_uring_access_is_unavailable`, which must run unprivileged. `benches/classify.rs` times `classify` over a mixed completion stream.
