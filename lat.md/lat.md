# polaris-trade

Knowledge graph for polaris-trade: architecture of market-data transports and the clients built on them, design decisions behind each, and test specs.

This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md), a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

## Areas

One page per crate area; each describes the code as built.

- [[transports]]: transport crates, one page per crate.
- [[clients]]: protocol clients built on the transports, one page per crate, plus the wire-codec fuzz targets.

## Proof and CI

Linux kernel paths are proven by running them in a privileged container; CI checks everything that runs without privilege, on every change.

- Linux check image (`scripts/kernel-proof/Dockerfile`): toolchain, nextest, DPDK with its pcap driver, bpftool and clang. It serves clippy, tests and link checks of the Linux-only crates from any development host. It proves correctness only; no performance number comes from it.
- Kernel proof (`scripts/transports-kernel-proof.sh`): runs the ignored kernel-path tests of [[io-uring]], [[afxdp]] and [[dpdk]] in that image, privileged: every io_uring receive path, every AF_XDP redirect mode on a fresh veth pair, DPDK over pcap devices, multicast joins, and io_uring refused without privilege. Each case checks payload bytes equal to the bytes sent. It is a local script for now, not a CI job.
- CI (`.github/workflows/`): calls the shared `0x67/ci` workflows to build, lint and test the whole workspace on Linux, macOS and Windows, with default and portable feature sets. It also links the DPDK driver on Linux, runs Miri over the pools, the bypass shell and the AF_XDP ring, and replays the fuzz corpus.
