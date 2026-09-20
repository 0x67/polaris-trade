# transport_dpdk

DPDK poll-mode receive over a caller-initialised EAL, one mbuf per frame, linked through a small C shim. Receive only; serves datagram consumers through `UdpDecap`.

## Attach and config

[[crates/transports/dpdk/src/lib.rs#DpdkL2#attach]] wraps one port queue the caller already configured; it never calls `rte_eal_init`. Its `# Safety` contract carries everything DPDK cannot check.

[[crates/transports/dpdk/src/config.rs#DpdkConfig]] names port and queue (`new` arguments) and the burst bound (default 32), with no builder and no serde. The only runtime check is a null mempool, `InvalidConfig { field: "mempool" }`, before any allocation or DPDK call. The contract: EAL initialised, port started with the queue set up on the mempool, mempool neither `RTE_MEMPOOL_F_SC_GET` nor `RTE_MEMPOOL_F_SP_PUT` (frames free on any thread), one poller per queue, mempool outliving every frame.

[[crates/transports/dpdk/src/lib.rs#DpdkL2]] is `BypassTransport<PmdDriver>` from the core [[core#Kernel-bypass shell]], implementing `L2Recv` with backend name `dpdk`; `UdpDecap<DpdkL2>` ([[core#L2-to-UDP decap]]) feeds [[moldudp]]. No `Multicast`: the PMD owns the port, so group delivery is arranged on the switch or with the port's multicast filter or all-multicast mode.

## Receive path

One shim call per burst reaps mbufs and reads each one's data pointer, length and segment count; no per-frame FFI follows until the frame drops.

[[crates/transports/dpdk/src/driver.rs#PmdDriver]] asks for `min(spare, burst)` mbufs. The pure [[crates/transports/dpdk/src/driver.rs#settle]] then turns single-segment mbufs into frames in arrival order, frees chained ones in one `rte_pktmbuf_free_bulk` and counts them `truncated`, and returns the number delivered, never the number received. A chained mbuf never becomes an error; a burst of only chained mbufs triggers another reap, so `Ok(0)` still means empty. The PMD refills its ring from the mempool itself, so the driver has no recycle step and never reports `Exhausted`.

[[crates/transports/dpdk/src/frame.rs#MbufFrame]] owns exactly one mbuf, caches its data pointer and length so `as_ref` makes no FFI call, and frees the mbuf on drop through the multi-producer mempool put. Vector PMDs round a request down to a multiple of 4 or 8, so callers keep the batch drained and offer the full burst.

## Drops and pool stats

DPDK cannot see data waiting for a buffer, so exhaustion shows only as counters: `rx_nombuf` as `no_buffer`, `imissed` as `nic_missed`.

`Driver::stats` reads both with one `rte_eth_stats_get`, which the shell makes every `STATS_EVERY` bursts while the metrics gate is on; a failed read keeps the last values so counters never fall. Both are port-wide and count from port start. `pool_stats` calls `rte_mempool_in_use_count`, which walks every lcore cache: debug only, off the receive path.

## Build and link

Without `driver-dpdk` the crate is `DpdkConfig` plus the bookkeeping tests, with no system library on any OS; `build.rs` does nothing.

With the feature, `build.rs` requires Linux, compiles `csrc/shim.c` with the full `pkg-config --cflags libdpdk` (the pkg-config crate keeps only `-I`/`-D`, while x86_64 headers need `-march` for the inlined SSSE3 `rte_memcpy`), then probes again to emit the libdpdk link flags after the static shim so `--as-needed` keeps them. The shim wraps `static inline` helpers that export no symbol.

## Decisions

Choices carried over from the previous DPDK backend, and the ones reversed, with the reason.

### Kept

The C shim, the caller-owned EAL and the single-segment frame model stay.

- `attach` never calls `rte_eal_init`: EAL arguments, hugepages and port setup belong to the application.
- A C shim reaches the `static inline` receive and mbuf helpers; Miri stays out of FFI.
- One mbuf per frame, freed on drop from any thread under the multi-producer put contract, which now also forbids a single-consumer mempool.
- Receive only, never `AsyncReady`.

### Reversed

The old backend could not link its own tests, paid three FFI calls per packet and returned an error after pushing frames.

- Link flags emitted before the static shim were dropped by `--as-needed`; they now follow it.
- Three shim calls per packet became one per burst, and chained mbufs are freed in one bulk call.
- A chained mbuf returned `Err` after frames were pushed and the count included rejected mbufs. It is now freed, counted `truncated`, and never an error.
- `rte_mempool_in_use_count` ran on the data path; it is now only in `pool_stats`.
- A separate mock pool type, with panicking methods and many `cfg` branches, gave way to the core mock driver.

## Tests

The bookkeeping test runs on every OS; the EAL tests run on Linux with libdpdk and its pcap PMD, without privilege or hugepages.

The in-crate `settle` test pins delivered versus chained counts, one bulk free and the returned count. `tests/real_dpdk.rs`: `null_mempool_is_invalid_config` (also proves the test binary links); ignored, starting one EAL per process (`--no-huge`, `--no-pci`, two `net_pcap` vdevs over pcap files the test writes), `pcap_payloads_reach_consumer_through_decap_past_chained_mbuf` checks payload bytes through `UdpDecap` with a wrong-port frame filtered and a chained 3000-byte frame counted, and `exhausted_mempool_counts_no_buffer_and_recovers_after_cross_thread_drop` checks that a burst is bounded by the caller's batch and then by free mbufs, that `no_buffer` rises, `PoolExhausted` never appears, and frames dropped on another thread return every mbuf.
