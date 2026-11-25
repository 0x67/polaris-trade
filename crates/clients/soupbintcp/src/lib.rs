#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! `SoupBinTCP` v3.0 client: wire codec, session state machine, heartbeats,
//! optional compressed variant.
//!
//! One protocol state machine drives two APIs over [`SoupBinClient`]:
//! synchronous [`SoupBinClient::start`] and [`SoupBinClient::poll`] for
//! transports with [`transport_core::StreamRecv`] and
//! [`transport_core::StreamTrySend`] (busy-poll or parked, no runtime), and
//! async [`SoupBinClient::connect`] and [`SoupBinClient::recv`] for transports
//! adding [`transport_core::StreamSend`] and [`transport_core::AsyncReady`].
//!
//! | Feature | Enables |
//! | --- | --- |
//! | `compressed` | zlib-inflated server stream (compressed variant) |
//! | `tokio` | `SoupBinClient::recv_managed`, heartbeats driven by tokio timer |
//! | `observability` | message and session counters through `observability-core` |

pub mod client;
#[cfg(feature = "compressed")]
pub mod compressed;
pub mod config;
pub mod error;
pub mod event;
pub mod frame;
mod session;
pub mod wire;

pub use client::{ClientState, SoupBinClient};
#[cfg(feature = "compressed")]
pub use compressed::CompressedReader;
pub use config::SoupBinClientConfig;
pub use error::SoupBinError;
pub use event::{SoupBinEvent, SoupBinMessage};
pub use frame::Frame;
pub use wire::{PacketFrame, PacketType, parse_packet};
