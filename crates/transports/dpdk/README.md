# transport_dpdk

DPDK poll-mode receive for market-data feeds, built on the traits in `transport_core`. `DpdkL2` polls one receive queue that the caller has already configured and yields whole Ethernet frames, each owning its mbuf. Wrap it in `transport_core::decap::UdpDecap` to feed a datagram consumer such as MoldUDP64.

## What each feature gives

| Feature | Types | Needs |
| --- | --- | --- |
| none | `DpdkConfig` | nothing: builds on Linux, macOS and Windows with no system library |
| `driver-dpdk` | `DpdkL2` (`L2Recv`), `MbufFrame` | Linux, the DPDK development package (`libdpdk` through `pkg-config`), a C compiler |
| `observability` | | receive and drop metrics through `transport_core::telemetry` |

Backend name (metric label): `dpdk`. Receive only: no send trait, no `Multicast`.

The crate needs no privilege of its own. What the EAL needs (hugepages or `--no-huge`, a NIC bound to a DPDK-capable driver, or a virtual device) is part of the application's EAL setup.

## Attach contract

The crate never calls `rte_eal_init` and configures nothing. The caller initialises the EAL, creates the mempool, configures the port, sets up the receive queue on that mempool and starts the port, then attaches:

```rust,ignore
use std::num::NonZeroUsize;
use transport_core::decap::UdpDecap;
use transport_dpdk::{DpdkConfig, DpdkL2};

let cfg = DpdkConfig::new(port, queue); // burst defaults to 32
// SAFETY: EAL initialised; `port` started with `queue` set up on `mempool`;
// `mempool` has neither RTE_MEMPOOL_F_SC_GET nor RTE_MEMPOOL_F_SP_PUT;
// only this thread polls `queue`; `mempool` outlives every frame.
let l2 = unsafe { DpdkL2::attach(&cfg, mempool)? };
let mut feed = UdpDecap::new(l2, 26_400, None, NonZeroUsize::new(32).unwrap());
```

`attach` is `unsafe` because every one of those conditions is the caller's to uphold:

- The mempool must not be single-consumer (`RTE_MEMPOOL_F_SC_GET`) or single-producer (`RTE_MEMPOOL_F_SP_PUT`). An `MbufFrame` frees its mbuf on whatever thread drops it, so puts come from many threads.
- One thread polls the queue while the transport lives. `DpdkL2` is `Send`, so it may move to that thread.
- The mempool outlives the transport and every `MbufFrame` it yielded.

A null mempool is `InvalidConfig { field: "mempool" }`, returned before any allocation or DPDK call.

## Receive

One shim call per burst runs `rte_eth_rx_burst` and reads each mbuf's data pointer, length and segment count. Single-segment mbufs become `MbufFrame`s; chained mbufs (the mempool's data room is smaller than the frame) are freed together with `rte_pktmbuf_free_bulk` and counted as `truncated`. They never turn a burst into an error, and the count returned is the number of frames delivered. A burst of only chained mbufs is followed by another, so `Ok(0)` still means the queue is empty. Size the mempool's data room for the largest frame the port can deliver.

`recv_burst` asks for at most `min(out.spare(), burst)` mbufs. Vector PMDs round the request down to a multiple of 4 or 8 and return nothing below it, so drain the batch before each call (`UdpDecap` does).

## Drops and pool statistics

`recv_burst` never returns `PoolExhausted`: DPDK cannot report data waiting for a buffer. When the mempool runs dry the PMD counts it in `rx_nombuf` and the NIC drops what does not fit in `imissed`. `DpdkL2::stats()` reports them as `no_buffer` and `nic_missed`, read with one `rte_eth_stats_get` per call; with the metrics gate on, telemetry calls it once every `STATS_EVERY` (1024) bursts. Both counters cover the whole port (every queue) since it started, so with several queues on one port read them from one transport only. `truncated` counts this queue's chained mbufs.

`pool_stats()` calls `rte_mempool_in_use_count`, which walks every lcore cache. It is for debugging and never runs on the receive path.

## Multicast

`DpdkL2` does not join groups. The poll-mode driver owns the port, so no kernel stack sends IGMP or MLD reports for it. Arrange group delivery outside the transport: a static join on the switch, or the port's multicast address filter (`rte_eth_dev_set_mc_addr_list`) or all-multicast mode (`rte_eth_allmulticast_enable`) set when configuring the port.

## Building

Without `driver-dpdk` the build script does nothing. With it, `build.rs` requires a Linux target: it compiles `csrc/shim.c` with the full `pkg-config --cflags libdpdk` (x86_64 DPDK headers need its `-march`), then emits the libdpdk link flags after the shim so `--as-needed` keeps them. The shim exists because `rte_eth_rx_burst` and the mbuf helpers are `static inline` and export no symbol.

## Tests

`cargo nextest run -p transport_dpdk` runs the burst bookkeeping test on any OS. With `--features driver-dpdk` on Linux, `tests/real_dpdk.rs` checks that the test binary links and that a null mempool is rejected; its two ignored tests start a real EAL (`--no-huge`, no PCI, two `net_pcap` vdevs replaying pcap files written by the test) and check that payloads reach a `UdpDecap` consumer byte for byte, and that an exhausted mempool raises `no_buffer` and recovers after frames are dropped on another thread:

```bash
cargo nextest run -p transport_dpdk --features driver-dpdk --run-ignored ignored-only
```

They need the DPDK pcap PMD (Debian: `librte-net-pcap25`, pulled in by `libdpdk-dev`) but no hugepages, and pass in a Docker container run without `--privileged`.

## License

MIT OR Apache-2.0, at your option.
