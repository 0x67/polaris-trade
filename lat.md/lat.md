# polaris-trade

Knowledge graph for polaris-trade: architecture of market-data transports and the clients built on them, design decisions behind each, and test specs.

This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md), a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

## Areas

One page per crate area; each describes the code as built.

- [[transports]]: transport crates, one page per crate.
- [[clients]]: protocol clients built on the transports, one page per crate.
