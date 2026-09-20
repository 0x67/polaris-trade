# transport_afxdp

Linux AF_XDP receive over a raw XSK driver and its own XDP redirect program, feeding datagram consumers through the decap adapter. libc only: no libbpf, libxdp or C toolchain; other targets build an empty crate.

## Transport and config

[[crates/transports/afxdp/src/lib.rs#AfxdpL2]] wraps the [[core#Kernel-bypass shell]] over the XSK driver: `L2Recv` of whole Ethernet frames plus `Multicast`. `UdpDecap<AfxdpL2>` ([[core#L2-to-UDP decap]]) serves datagram consumers such as [[moldudp]].

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

[[crates/transports/afxdp/src/xdp/program.rs#instructions]] encodes a 25-instruction filter in front of `bpf_redirect_map(xskmap, rx_queue_index, XDP_PASS)`: IPv4 UDP frames, untagged or with one 802.1Q tag (the frames `UdpDecap` accepts), are redirected; every other frame returns `XDP_PASS`. Untagged and tagged frames take separate paths with constant offsets, and each packet load follows a `data + N > data_end` check, as the verifier requires. [[crates/transports/afxdp/src/xdp/program.rs#load]] loads it with no log, reloads with a 64 KiB verifier log on failure and emits that log once at `error` level. Big-endian hosts get `Unsupported` for the built-in program; pinned mode still works there.

[[crates/transports/afxdp/src/xdp/map.rs#XskMap]] creates a map of `queue + 1` entries or opens a pinned one, checking type, key and value size and that the queue fits; inserts use `BPF_NOEXIST` (E2BIG is `InvalidConfig { field: "queue" }`, EEXIST `Unavailable`). [[crates/transports/afxdp/src/xdp/link.rs#XdpLink]] attaches through `BPF_LINK_CREATE`; its fd is the only handle, so drop or process exit detaches, and EBUSY suggests `Pinned`. Netlink attach is not offered, since it survives a crash.

The filter checks protocol, never port. ARP, IGMP, ICMP, IPv6, TCP, stacked tags and frames shorter than the Ethernet, IPv4 and UDP headers reach the kernel, so the host keeps answering ARP and IGMP queries on the bound queue. Other UDP on the queue (NTP, DNS replies) still reaches the socket, where `UdpDecap` drops and counts it; hosts receiving such traffic steer the feed to its own queue. Pinned mode is unchanged: the external program decides.

Only one program attaches per interface and mode, so a second built-in transport on another queue of one interface fails; multi-queue deployments load one external program and give each transport `Pinned`.

## Multicast

Joining opens a kernel UDP socket per address family on first use and joins on the bound interface by index; the kernel sends the report and programs the NIC filter, the redirect hands group frames to the socket.

The built-in program passes IGMP to the kernel, so membership queries are answered and a snooping switch keeps forwarding the group.

Any `iface` field set is `InvalidConfig { field: "iface" }`, since AF_XDP receives only on its bound interface.

## Privileges and kernel

Linux 5.9 or later (XDP through `BPF_LINK_CREATE`). Capabilities depend on the redirect mode; the UMEM counts against the memory-lock limit.

- `CAP_NET_RAW` opens the AF_XDP socket (EPERM is `Unavailable` naming it).
- Built-in mode also needs `CAP_BPF` and `CAP_NET_ADMIN` (XSKMAP creation needs the latter), or `CAP_SYS_ADMIN`.
- Before 6.5 with `kernel.unprivileged_bpf_disabled` set (the Ubuntu and RHEL default), every `bpf()` command needs `CAP_BPF`, the pinned mode's `OBJ_GET` and `MAP_UPDATE_ELEM` included; from 6.5 pinned mode needs no BPF capability.
- The UMEM is charged to `RLIMIT_MEMLOCK` unless the process has `CAP_IPC_LOCK`; `XDP_UMEM_REG` ENOBUFS is `Unavailable` saying so. The default UMEM is 8 MiB.
- After a transport drops, binding the same queue can fail EBUSY for seconds while the kernel releases the old socket (lazy RCU); it maps to `Unavailable`.

## Limitations

Native driver mode was verified only on veth and zero-copy not at all; both stay opt-in. Decap handles IPv4 only.

The built-in program redirects all IPv4 UDP on the bound queue, not only the feed's port, so unrelated UDP there never reaches the kernel stack; steer the feed to its own queue on hosts that receive such traffic.

Zero-copy teardown: the NIC may still write the UMEM after the socket closes, until the kernel's deferred teardown ends, and the region is freed at that point today. Copy mode is unaffected. Keeping the region alive until teardown is confirmed is a follow-up in the core region.

A MoldUDP64 re-request reply that lands on the redirected queue reaches the XSK, not the requester's kernel socket; see [[moldudp#Gap recovery]] for the two ways around it.

## Decisions

Choices carried over from the previous AF_XDP backend, and the ones reversed, with the reason.

### Kept

The raw driver's ring code and its Miri coverage stay; the recv-only shape stays.

- Receive only, never `AsyncReady`: a busy-poll backend.
- `bind` stays an inherent constructor with its own config, never a shared bind trait.
- The single-producer, single-consumer ring protocol, now tested on heap rings under Miri.

### Reversed

The old backend read payloads from the wrong offset and could not receive without a program loaded and a map filled by hand.

- Payload at the chunk base, from the assumption that zero UMEM headroom means a frame-aligned receive address, became payload at the descriptor address. The kernel places packet data 256 bytes plus headroom into the chunk, so the old code returned headroom and a cut packet.
- Three drivers (raw, libxdp, xsk-rs) became the raw one: the other two existed for a bench that was never written and linked two libxdp versions through a forked git dependency.
- An out-of-band program with no map insert became the crate's own loader: built-in program and XSKMAP, or insertion into a pinned external map.
- A safe public `acquire` and an unchecked UMEM constructor (zero or overflowing sizes reached undefined behaviour) became the core `IndexPool`, validated before allocation, with `frame` as the only unsafe entry.
- A constant-zero `peer()` and a stub `send` are gone; multicast joins work through a kernel socket on the bound interface.

## Tests

In-crate tests need no kernel: descriptor mapping, receive over heap rings, ring protocol across wrap, program bytes and filter, and config validation. Kernel paths run only in `tests/real_afxdp.rs`, ignored by default.

`driver` tests: `locate_bounds_payload_by_frame_and_umem`, `reap_yields_bytes_at_descriptor_address_bounded_by_spare` (payloads at the default, configured and program-moved offsets, byte-equal), `reap_counts_overrun_descriptor_and_recycles_its_slot_only`. `ring` tests (also under Miri): `ring_producer_stops_at_full_and_resumes_across_wrap`, `ring_consumer_takes_at_most_max_and_releases_across_wrap`, `ring_producer_reports_wakeup_flag`. `instructions_match_verified_filter_program` pins the program bytes verified on kernel 7.0; `filter_redirects_ipv4_udp_untagged_or_under_one_tag` and `filter_passes_everything_else` run the program through a small in-test interpreter over UDP, ARP, IPv6, IGMP, ICMP, TCP, stacked-tag and short frames, failing on any load past the frame end.

`tests/real_afxdp.rs` runs the conformance suite through `UdpDecap<AfxdpL2>`: `builtin_skb_passes_conformance_and_detaches_on_drop`, `builtin_drv_passes_conformance_and_detaches_on_drop`, `pinned_passes_conformance_and_leaves_program_attached`, and `multicast_join_listed_and_group_datagram_received` (`/proc/net/igmp` plus a group datagram). The kernel-proof script supplies `AFXDP_IFACE`, `AFXDP_DST`, `AFXDP_PEER_NETNS` and `AFXDP_PINNED_MAP`. Built-in cases run with no static neighbour entry in the peer, so ARP answered through the loaded program is part of the proof; the pinned case keeps one, since its fixture program redirects every frame.
