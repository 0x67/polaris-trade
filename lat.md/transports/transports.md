# transports

Transport crates under `crates/transports/`: one shared core, one portable socket crate and three Linux kernel-bypass backends.

Dependencies point one way: every backend depends on [[core]], the bypass backends share its [[core#Kernel-bypass shell]], and the L2 backends reach datagram consumers through [[core#L2-to-UDP decap]]. The [[clients]] depend on core alone.

- [[core]]: traits, bursts, pools, decap, bypass shell, errors, telemetry, conformance suite.
- [[socket]]: sync, tokio and mio UDP and TCP over one receive loop, `ReadySet` readiness. Linux, macOS, Windows.
- [[io-uring]]: io_uring UDP receive, path picked at run time. Linux.
- [[afxdp]]: AF_XDP receive over a raw XSK driver and its own XDP program. Linux.
- [[dpdk]]: DPDK poll-mode receive, one mbuf per frame. Linux with libdpdk.

The three Linux-only crates build empty (or, for DPDK without `driver-dpdk`, config only) on other systems, so the whole workspace builds everywhere.
