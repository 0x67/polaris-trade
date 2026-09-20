# transport_io_uring

io_uring UDP receive for Linux, built on the kernel-bypass shell in `transport_core`. Datagrams land in slots of one preallocated region and come back as frames without a copy; an idle receive loop makes no syscall. Receive only: no send, no stream.

On every other OS the crate compiles to nothing, so a workspace that depends on it still builds on macOS and Windows.

## Use

```rust,ignore
use std::num::{NonZeroU32, NonZeroUsize};
use transport_core::{DatagramRecv, FrameBatch, Multicast, MulticastInterface};
use transport_io_uring::{IoUringConfig, IoUringUdp};

let mut cfg = IoUringConfig::new("0.0.0.0:30001".parse()?);
cfg.slots = NonZeroU32::new(4096).unwrap();
let mut rx = IoUringUdp::bind(&cfg)?;
rx.join_multicast("233.54.12.1".parse()?, MulticastInterface::default())?;
let mut out = FrameBatch::with_capacity(NonZeroUsize::new(64).unwrap());
loop {
    rx.recv_burst(&mut out)?;
    for frame in out.drain() {
        handle(frame.as_ref());
    }
}
```

`IoUringUdp` implements `DatagramRecv` (frames are `IndexFrame`) and `Multicast`. Backend name (errors, metric label): `io-uring`.

## Config

| Field | Default | Meaning |
| --- | --- | --- |
| `bind` | `new` argument | local address; unspecified IP serves multicast |
| `slots` | 1024 | receive slots, so datagrams the caller can hold at once; at most 32768 |
| `slot_size` | 2048 | bytes per slot; a longer datagram counts `truncated` and is dropped |
| `depth` | 64 | single-shot recvs kept armed (legacy and buffer-ring paths); at most `slots` and 4096 |
| `path` | `None` | force a `RecvPath`; `None` picks the best the kernel supports |
| `recv_buf` | OS default | `SO_RCVBUF` bytes |

A value past a limit is `InvalidConfig` before any allocation or syscall.

## Receive paths

| `RecvPath` | Kernel | Buffers | Recvs |
| --- | --- | --- | --- |
| `Multishot` | 6.0 or later | registered buffer ring | one multishot recv |
| `BufRing` | 5.19 or later | registered buffer ring | `depth` single-shot recvs |
| `Legacy` | provided buffers and fast poll | `ProvideBuffers`, one per contiguous run | `depth` single-shot recvs |

Detection runs at bind on the running kernel, not from its version string, so distribution backports are honoured. A forced path the kernel lacks is `Unsupported`. All three paths are verified on Linux 7.0; 5.14 to 5.19 kernels are not yet run.

## Platform and privileges

- io_uring must be allowed: `kernel.io_uring_disabled` at 0 (or 1 with the process in `kernel.io_uring_group`), and no seccomp filter blocking `io_uring_setup`. Docker's default profile blocks it; bind then returns `Unavailable` with the OS error.
- Kernels before 5.12 charge ring memory to `RLIMIT_MEMLOCK`; a low limit fails ring setup.
- With every slot held by the caller, `recv_burst` returns `PoolExhausted` and counts `no_buffer`; the datagram waits in the socket buffer until a frame is dropped.
- An empty datagram never fails `recv_burst`: kernels 6.0 and later drop it without a frame, older kernels deliver it as a 0-byte frame.
- Dropping the transport cancels armed recvs and waits up to one second for them. If they do not end in time the receive memory is leaked, never freed under the kernel, and one `warn` event is logged. The bounded wait needs `IORING_FEAT_EXT_ARG` (5.11); on older kernels the memory leaks whenever recvs are still armed at drop, with the warning.
- No capability is needed once io_uring is allowed.

## Features

- `observability`: receive and drop metrics through `transport_core::telemetry`.

## Tests

In-crate tests are pure and run on any Linux host. `tests/real_io_uring.rs` needs a live ring and is ignored by default; run it privileged:

```sh
cargo nextest run -p transport_io_uring --run-ignored ignored-only
```

`bind_without_io_uring_access_is_unavailable` expects io_uring blocked, so run it unprivileged.

## License

MIT OR Apache-2.0, at your option.
