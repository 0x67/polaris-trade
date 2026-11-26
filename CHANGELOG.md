# Changelog

Version 0.2.0 is the first public release. Earlier versions were internal releases.

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
