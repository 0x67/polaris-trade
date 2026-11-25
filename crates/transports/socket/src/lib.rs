#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! Kernel socket transports over one receive loop.
//!
//! Every socket is built by socket2 from a typed config, so options mean the
//! same thing under every feature. Datagram receive, for every UDP type, runs
//! through one loop landing datagrams in [`VecPool`](transport_core::pool::VecPool)
//! slabs; readiness is optional and picked by feature.
//!
//! | Feature | Enables |
//! | --- | --- |
//! | none | [`UdpSocket`]: sync, non-blocking UDP for busy-poll loops |
//! | `tokio` | [`tokio::AsyncUdp`] and [`tokio::TcpStream`] with [`AsyncReady`](transport_core::AsyncReady) |
//! | `mio` | [`mio::MioUdp`], [`mio::MioTcp`] and [`mio::ReadySet`]: one blocking poll over many sockets, no runtime |
//! | `observability` | receive metrics through `transport_core::telemetry` |
//!
//! Backend names ([`Transport::name`](transport_core::Transport::name) and
//! metric label): `udp`, `tokio-udp`, `tokio-tcp`, `mio-udp`, `mio-tcp`.

mod config;
#[cfg(feature = "mio")]
pub mod mio;
mod recv;
mod sockopt;
#[cfg(feature = "tokio")]
pub mod tokio;
mod udp;

use std::io;

#[cfg(any(feature = "tokio", feature = "mio"))]
pub use config::TcpConfig;
pub use config::UdpConfig;
pub use recv::UdpFrame;
use transport_core::TransportError;
pub use udp::UdpSocket;

// `map_err` adapter tagging an OS error with failed stage
fn io_error(stage: &'static str) -> impl FnOnce(io::Error) -> TransportError {
    move |error| TransportError::Io { stage, error }
}

// stream partial write: would-block is `Ok(0)`, caller retries on writable
#[cfg(any(feature = "tokio", feature = "mio"))]
fn written(result: io::Result<usize>) -> Result<usize, TransportError> {
    match result {
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
        result => result.map_err(io_error("send")),
    }
}
