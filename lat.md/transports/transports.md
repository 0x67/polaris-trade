# transports

Transport crates under `crates/transports/`: the shared core first, backends as they land.

- [[core]]: traits, bursts, pools, decap, bypass shell, errors, telemetry, conformance suite.
- [[socket]]: sync, tokio and mio UDP and TCP over one receive loop, `ReadySet` readiness.
- [[io-uring]]: io_uring UDP receive, path picked at run time.
- [[afxdp]]: AF_XDP receive over a raw XSK driver.
