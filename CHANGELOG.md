# Changelog

Version 0.2.0 is the first public release. Earlier versions were internal releases.

## [0.4.0](https://github.com/0x67/polaris-trade/compare/v0.3.0...v0.4.0) (2026-11-27)


### ⚠ BREAKING CHANGES

* **datafeed:** move signal, subscriber and recorder installs into the binary behind a bin feature ([#11](https://github.com/0x67/polaris-trade/issues/11))
* **datafeed:** replace boxed library errors with a typed feed error ([#6](https://github.com/0x67/polaris-trade/issues/6))

### Features

* **datafeed:** add a C ABI over the feed session with a generated header ([#12](https://github.com/0x67/polaris-trade/issues/12)) ([be9fc03](https://github.com/0x67/polaris-trade/commit/be9fc0319fac3fd1fb4e8f58d0d362dd639c53ba))
* **datafeed:** add a pull-based feed session with a builder ([#8](https://github.com/0x67/polaris-trade/issues/8)) ([21cdf85](https://github.com/0x67/polaris-trade/commit/21cdf859aa63a6febe9f33e13e7ed743aaf7ea2b))
* **datafeed:** move signal, subscriber and recorder installs into the binary behind a bin feature ([#11](https://github.com/0x67/polaris-trade/issues/11)) ([03328ad](https://github.com/0x67/polaris-trade/commit/03328ad0bb8557b70e8bd2bfac0724bfc7992030))
* **datafeed:** replace boxed library errors with a typed feed error ([#6](https://github.com/0x67/polaris-trade/issues/6)) ([f032974](https://github.com/0x67/polaris-trade/commit/f032974f1cbe41d9e6117efd46aeb62a242d32cd))


### Performance

* **datafeed:** cut the per-record cost of the feed session handoff ([#10](https://github.com/0x67/polaris-trade/issues/10)) ([d3c1d77](https://github.com/0x67/polaris-trade/commit/d3c1d77b6e2b72d4cda16c2c9ba6159253da1fc0))


### Refactor

* **datafeed:** split the session loop into a per-batch step ([#7](https://github.com/0x67/polaris-trade/issues/7)) ([4d497e0](https://github.com/0x67/polaris-trade/commit/4d497e075691eff522d86583c37d2bd9c353ad8a))


### Tests

* **datafeed:** check the deployed binary's output, captures and shutdown ([#13](https://github.com/0x67/polaris-trade/issues/13)) ([24a27c0](https://github.com/0x67/polaris-trade/commit/24a27c0d097a68ed3ed990271a0711b1a2304c13))

## [0.3.0](https://github.com/0x67/polaris-trade/compare/v0.2.0...v0.3.0) (2026-11-26)


### Features

* **datafeed:** print the version with --version ([#3](https://github.com/0x67/polaris-trade/issues/3)) ([1634913](https://github.com/0x67/polaris-trade/commit/1634913caf94c9887f90f8b86d6669305858e628))


### Bug fixes

* **codegen:** write the provenance schema path with forward slashes on every host ([8cd7f66](https://github.com/0x67/polaris-trade/commit/8cd7f667b4ec916bf302bf2d91bfb92f345b6736))


### Tests

* **source-websocket:** bound the reconnect attempt in the backoff test ([5b814c2](https://github.com/0x67/polaris-trade/commit/5b814c2490e8fcf9fe3e5b838184327c57e6b553))


### Build

* **release:** attach datafeed binaries and installers to each release ([5a464c5](https://github.com/0x67/polaris-trade/commit/5a464c5499e2609bae7adeb2a0f2b4d262e038c2))
* **workspace:** check out text files with lf on every platform ([f2a8787](https://github.com/0x67/polaris-trade/commit/f2a87877d1e9fae2886c7e62b13220982078a6eb))


### CI

* **docs:** publish the workspace rustdoc to github pages on each tag ([45467f4](https://github.com/0x67/polaris-trade/commit/45467f4a1ba73830a7c9f1e2c910a01635cc0874))
* run the stable tests on linux, macos and windows ([a88cd3c](https://github.com/0x67/polaris-trade/commit/a88cd3c0c308aba2b66b0f65c9d5c51d85e26d3c))

## 0.2.0 (2026-11-25)


### ⚠ BREAKING CHANGES

* **workspace:** rename the definition tail, price band, status flag and dual-channel fields
* **pipeline:** declare the record types each venue adapter emits
* **events:** append the definition tail and raise the event bound to 176 bytes


### Features

* **venue-binance:** add a binance spot adapter with snapshot resync
* **venue-hyperliquid:** add a hyperliquid perpetuals adapter
* **events,pipeline:** resolve instruments by venue symbol and check venue price conversion
* **source-websocket:** add a websocket ingest source over tungstenite
* **pipeline:** add a fixture parity harness for hand-written adapters
* **pipeline:** declare the record types each venue adapter emits
* **events:** append the definition tail and raise the event bound to 176 bytes
* **workspace:** import the datafeed as the polaris-datafeed crate


### Bug fixes

* **deps:** bump url to 2.5.9 for RUSTSEC-2026-0592


### Refactor

* **workspace:** rename the definition tail, price band, status flag and dual-channel fields


### Documentation

* **workspace:** cite specifications by title and match the Apache license text


### Tests

* **datafeed:** label bench reports with stack position and windowed stage means
* **datafeed:** check per-stream publish and recovery through a two-stream test adapter
* **datafeed:** add the NASDAQ pipeline bench with its regression gate
* **fuzz:** carry the pbn fuzz targets and miri job into the monorepo


### CI

* **repo:** grant pull-requests read to the pr-title caller


## 0.1.1 (2026-11-16)


### Bug fixes

* **deps:** pin observability-core at observability-v0.2.2

### Documentation

* **lat:** fix wording slips

### CI

* **release:** rename the release-please caller to release-please.yml
