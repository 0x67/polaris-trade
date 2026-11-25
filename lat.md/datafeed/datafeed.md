# datafeed

`polaris-datafeed` under `crates/datafeed/` is the feed binary and its library (both named `datafeed`): one venue market-data session, live or replay, from ingest through sequenced egress and raw capture.

It wires the workspace's own crates and does not add protocol code of its own: [[pipeline]] for the ingest, adapter and egress seams, [[orderbook]] for the book and derived views, [[replay]] for file sources, [[mdc]] for the capture container, [[nasdaq]] for the one built-in venue, and [[soupbintcp]] over [[socket]] for live TCP sessions. NASDAQ TotalView publishes over MoldUDP64, which has no live source here yet. The built-in venue therefore runs replay only, and the live path is covered by tests against an in-process server.

- [[session]]: config-driven venue and source selection, the live and replay source swap, the session hot loop and record routing, sinks, the restart and resume seam, and stage timers.
- [[capture]]: the raw capture tap, the partitioned storage layout with its manifests, and the capture header reconcile on replay.
- [[tests]]: determinism and parity gates, the live path against a mock SoupBinTCP server, and the unit tests that use NASDAQ as their fixture venue.
