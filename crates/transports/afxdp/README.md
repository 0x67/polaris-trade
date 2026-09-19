# transport_afxdp

Linux AF_XDP receive for market-data feeds, over a raw XSK driver written against the kernel uapi with `libc`: no libbpf, no libxdp, no C toolchain. It loads its own six-instruction XDP redirect program, or plugs into an externally loaded one, and hands whole Ethernet frames to `transport_core`'s decap adapter for datagram consumers.

## Types

| Type | Traits |
| --- | --- |
| `AfxdpL2` | `L2Recv` (frame = `IndexFrame`, whole Ethernet frame), `Multicast` |
| `UdpDecap<AfxdpL2>` (from `transport_core::decap`) | `DatagramRecv`, `Multicast` |

`AfxdpL2::bind(&AfxdpConfig)` validates the config before any allocation or syscall, then opens the socket, registers one `IndexPool` region as UMEM, maps the fill and receive rings, hands every frame to the kernel, binds the queue and installs the redirect. The payload of each frame starts at the descriptor's address; a descriptor running past its frame is counted as `truncated` and its frame recycled.

```rust,ignore
let mut cfg = AfxdpConfig::new("eth0", 0);   // interface, queue
cfg.redirect = XdpRedirect::Builtin { mode: XdpMode::Drv };
let mut feed = UdpDecap::new(AfxdpL2::bind(&cfg)?, 26_400, None, NonZeroUsize::new(64).unwrap());
feed.join_multicast(group, MulticastInterface::default())?;
```

Config fields: `frames` (UMEM frames and ring entries, power of two, default 4096), `frame_size` (power of two, 2048 up to the page size, default 2048), `headroom` (bytes ahead of the kernel's 256-byte XDP headroom, default 0), `zero_copy` (default off), `redirect`.

## Redirect modes

| `XdpRedirect` | Behaviour |
| --- | --- |
| `Builtin { mode: Skb }` (default) | Loads `bpf_redirect_map(xskmap, rx_queue_index, XDP_PASS)` and attaches it in generic mode through `BPF_LINK_CREATE`. Works on any interface. Frames of other queues and lookup misses go to the kernel stack. Drop, or process exit, detaches it. |
| `Builtin { mode: Drv }` | Same program in native driver mode; the driver must support XDP. No automatic fallback to `Skb`. |
| `Pinned { path }` | Opens the XSKMAP an external program pinned at `path` (for example `xdp-loader` with `LIBBPF_PIN_BY_NAME`), checks it is an XSKMAP with 4-byte key and value covering the queue, and inserts the socket. The program stays attached after drop. |

Only one program can be attached per interface and mode, so a second built-in transport on another queue of the same interface fails with `Unavailable` ("use Pinned"). Multi-queue deployments load one external program and give every transport `Pinned`.

## Privileges and kernel

- `CAP_NET_RAW` for the AF_XDP socket; built-in mode also needs `CAP_BPF` and `CAP_NET_ADMIN` (or `CAP_SYS_ADMIN`). Pinned mode needs no BPF capability on 7.0; older kernels with unprivileged BPF disabled likely need `CAP_BPF`.
- The UMEM is charged to `RLIMIT_MEMLOCK` unless the process has `CAP_IPC_LOCK`; 4096 frames of 2048 bytes is 8 MiB.
- Kernel 5.9 or later (XDP through `BPF_LINK_CREATE`).
- Little-endian hosts only for the built-in program; big-endian returns `Unsupported`.

## Errors

| Failure | Error |
| --- | --- |
| empty or over-long interface name, bad frame sizes, headroom leaving no room | `InvalidConfig`, before any syscall |
| socket EPERM | `Unavailable`: needs `CAP_NET_RAW` |
| `XDP_UMEM_REG` ENOBUFS | `Unavailable`: `RLIMIT_MEMLOCK` too low, grant `CAP_IPC_LOCK` |
| bind EBUSY | `Unavailable`: queue busy or previous socket still releasing |
| map create or program load EPERM | `Unavailable`: needs `CAP_BPF` and `CAP_NET_ADMIN` |
| program load rejected | `Io { stage: "bpf(PROG_LOAD)" }`, verifier log emitted once at `error` level |
| link create EBUSY | `Unavailable`: an XDP program is already attached; use `Pinned` |
| pinned path missing or not an XSKMAP | `InvalidConfig { field: "redirect.path" }` |
| queue beyond the map / queue already in the map | `InvalidConfig { field: "queue" }` / `Unavailable` |
| `join_multicast` with any `iface` field set | `InvalidConfig { field: "iface" }` |

After a transport drops, binding the same queue can fail `Unavailable` (EBUSY) for up to about 14 seconds while the kernel finishes releasing the old socket. Retry with a deadline, or run an RCU barrier (`/sys/module/rcutree/parameters/do_rcu_barrier`) where the kernel offers one.

## Receive and counters

In copy mode `recv_burst` makes no syscall, apart from the counter read every 1024 calls while metrics are on; zero-copy drivers that ask for a wakeup get one `recvfrom` kick on an idle burst. `recv_burst` never returns `PoolExhausted`: while the caller holds every frame, the kernel drops arriving frames. `stats()` reads `XDP_STATISTICS` with one `getsockopt`:

| `DriverStats` | Source |
| --- | --- |
| `no_buffer` | kernel `rx_dropped`: no free UMEM frame, or a frame longer than `frame_size - headroom - 256` |
| `nic_missed` | kernel `rx_ring_full`: the receive ring was full |
| `truncated` | descriptors running past their frame, counted by the driver |
| `syscalls` | wakeup kicks |

With `observability`, the bypass shell reports these every 1024 calls as `transport.recv.drops`, backend label `afxdp`.

## Multicast

`join_multicast` opens a kernel UDP socket on first use, holds it for the transport's life and joins the group on the bound interface by index. The kernel sends the IGMP or MLD report and programs the NIC's multicast filter; the redirect program hands the group's frames to the socket. The interface argument must be the default.

## Limitations

- Native mode was verified only on veth, and zero-copy not at all; both stay opt-in until run on a real NIC.
- A MoldUDP re-request reply arriving on a redirected queue is captured by the AF_XDP socket, not the requester's kernel socket. Steer re-request replies to another queue with a flow rule on the requester's port, or bind the requester to the feed's destination port with `UdpDecap`'s `dst_ip` filter unset so the unicast reply passes through the leg.
- `UdpDecap` handles IPv4 only.

## Tests

In-crate tests are syscall-free: descriptor mapping (payload at the descriptor address for several offsets, overrun and out-of-UMEM descriptors counted, not framed), receive over heap-backed rings, the ring index protocol across `u32` wrap (also under Miri for `x86_64-unknown-linux-gnu`), the redirect program bytes, and config validation. `tests/real_afxdp.rs` runs the conformance suite through `UdpDecap<AfxdpL2>` in each redirect mode plus a multicast case; it is `#[ignore]` and needs a privileged veth setup from the kernel-proof script (environment listed in the file header).

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
