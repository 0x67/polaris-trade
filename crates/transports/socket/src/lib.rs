#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! Kernel socket transports over one receive loop.
//!
//! Every socket is built by socket2 from a typed config, so options mean the
//! same thing under every feature. Datagram receive, for every UDP type, runs
//! through one loop landing datagrams in [`VecPool`](transport_core::pool::VecPool)
//! slabs.
//!
//! | Feature | Enables |
//! | --- | --- |
//! | none | [`UdpSocket`]: sync, non-blocking UDP for busy-poll loops |
//! | `observability` | receive metrics through `transport_core::telemetry` |
//!
//! Backend name ([`Transport::name`](transport_core::Transport::name) and
//! metric label): `udp`.

mod config;
mod recv;
mod sockopt;
mod udp;

use std::io;

pub use config::UdpConfig;
pub use recv::UdpFrame;
use transport_core::TransportError;
pub use udp::UdpSocket;

// `map_err` adapter tagging an OS error with failed stage
fn io_error(stage: &'static str) -> impl FnOnce(io::Error) -> TransportError {
    move |error| TransportError::Io { stage, error }
}
