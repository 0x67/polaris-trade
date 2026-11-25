# Changelog

Version 0.2.0 is the first public release. Earlier versions were internal releases.

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