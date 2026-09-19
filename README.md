# polaris-trade

Rust workspace for market-data transports (kernel sockets, io_uring, AF_XDP, DPDK) and the MoldUDP64 and SoupBinTCP clients built on them.

Layout: `crates/transports/*`, `crates/clients/*`.

## Development

```bash
lefthook install   # git hooks, once per clone
rustup toolchain install nightly --component rustfmt
```

Formatting needs nightly rustfmt: `rustfmt.toml` sets unstable import options. Build, lint and test use the stable toolchain pinned in `rust-toolchain.toml`.

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
