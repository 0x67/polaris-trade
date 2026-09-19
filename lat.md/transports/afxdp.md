# transport_afxdp

Linux AF_XDP receive over a raw XSK driver and its own XDP redirect program, feeding datagram consumers through the decap adapter. libc only: no libbpf, libxdp or C toolchain; other targets build an empty crate.

## Transport and config

[[crates/transports/afxdp/src/lib.rs#AfxdpL2]] wraps the bypass shell over the XSK driver: `L2Recv` of whole Ethernet frames plus `Multicast`. `UdpDecap<AfxdpL2>` serves datagram consumers.

[[crates/transports/afxdp/src/lib.rs#AfxdpL2#bind]] validates [[crates/transports/afxdp/src/config.rs#AfxdpConfig]] before any allocation or syscall: interface name 1 to 15 bytes, `frames` a power of two, `frame_size` a power of two of at least 2048, headroom leaving packet room, built-in queue below `u32::MAX`, pinned path non-empty without NUL. Bind order is socket, UMEM, rings, bind, map insert, program load, link attach.

Backend name `afxdp`. The bypass shell never sees exhaustion: with every frame held the kernel drops arriving frames, so the transport is a drop-counter backend and never returns `PoolExhausted`.

## Driver and payload mapping

[[crates/transports/afxdp/src/driver.rs#XskDriver]] registers one `IndexPool` region as UMEM (chunk = `frame_size`) and sizes fill and receive rings to `frames`, so every free slot fits the fill ring at once.

Each slot sits in exactly one place: fill ring, kernel, receive ring, one live frame, the pool's freed list or the driver's free list. `reap` first moves freed slots back to the fill ring, then drains the receive ring until it pushes a frame, fills the batch or the ring runs empty; frames are minted only through `unsafe IndexPool::frame`.

[[crates/transports/afxdp/src/driver.rs#locate]] maps a descriptor to slot and offset: payload starts at the descriptor address (chunk + headroom + 256, or wherever a program moved it), never at a fixed offset. A descriptor running past its frame counts `truncated` and its slot is recycled; one outside the UMEM counts `truncated` alone.

Fields drop as link, rings, ring mappings, socket, pool: the program detaches first and the region is freed after the socket closes. `stats` reads `XDP_STATISTICS`: `no_buffer` is kernel `rx_dropped`, `nic_missed` is `rx_ring_full`, `truncated` the driver's own count, `syscalls` the wakeup kicks. Copy mode never kicks; a zero-copy driver asking for wakeup gets one `recvfrom` on an idle burst.

## Ring index protocol

[[crates/transports/afxdp/src/ring.rs#Producer]] and [[crates/transports/afxdp/src/ring.rs#Consumer]] implement the single-producer, single-consumer protocol over shared words and a descriptor array, with no syscall.

Indices run free and wrap at `u32`; the writer publishes with a Release store of its word and the reader Acquire-loads it before touching entries. `produce` writes at most the free room and leaves untaken items in the caller's iterator; `consume` hands at most `max` entries and releases them at once. Both clamp a corrupt peer word to one lap. The tests run on heap-backed rings, so they pass under Miri for `x86_64-unknown-linux-gnu`.

## XDP redirect

[[crates/transports/afxdp/src/xdp/mod.rs#install]] routes the bound queue to the socket: built-in mode creates an XSKMAP, inserts, loads and attaches; pinned mode opens an external map and inserts.

`xdp/sys` holds one `#[repr(C, align(8))]` attr prefix per `bpf()` command with named padding, const size and offset asserts, the `unsafe trait Attr` layout contract and one syscall wrapper; EPERM maps to `Unavailable` naming `CAP_BPF` and `CAP_NET_ADMIN`.

[[crates/transports/afxdp/src/xdp/program.rs#instructions]] encodes `bpf_redirect_map(xskmap, rx_queue_index, XDP_PASS)` in six instructions; [[crates/transports/afxdp/src/xdp/program.rs#load]] loads it with no log, reloads with a 64 KiB verifier log on failure and emits that log once at `error` level. Big-endian hosts get `Unsupported`.

[[crates/transports/afxdp/src/xdp/map.rs#XskMap]] creates a map of `queue + 1` entries or opens a pinned one, checking type, key and value size and that the queue fits; inserts use `BPF_NOEXIST` (E2BIG is `InvalidConfig { field: "queue" }`, EEXIST `Unavailable`). [[crates/transports/afxdp/src/xdp/link.rs#XdpLink]] attaches through `BPF_LINK_CREATE`; its fd is the only handle, so drop or process exit detaches, and EBUSY suggests `Pinned`.

## Multicast

Joining opens a kernel UDP socket per address family on first use and joins on the bound interface by index; the kernel sends the report and programs the NIC filter, the redirect hands group frames to the socket.

Any `iface` field set is `InvalidConfig { field: "iface" }`, since AF_XDP receives only on its bound interface.

## Tests

In-crate tests need no kernel: descriptor mapping, receive over heap rings, ring protocol across wrap, program bytes and config validation. Kernel paths run only in `tests/real_afxdp.rs`, ignored by default.

`driver` tests deliver payloads at the default, configured and program-moved offsets and check frame bytes equal them, with the burst bounded by `spare`; an overrun and a foreign descriptor are counted, not framed, and only the overrun slot returns to the fill ring while a held frame's slot does not. `program` pins the instruction bytes verified on kernel 7.0.

`tests/real_afxdp.rs` runs the conformance suite through `UdpDecap<AfxdpL2>` in built-in SKB, built-in DRV and pinned mode, checks attach state after drop, and checks a multicast join in `/proc/net/igmp` plus a group datagram. The kernel-proof script supplies `AFXDP_IFACE`, `AFXDP_DST`, `AFXDP_PEER_NETNS` and `AFXDP_PINNED_MAP`.
