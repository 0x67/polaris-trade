#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
//! `MoldUDP64` client: wire codec, [`SequenceReassembler`], [`GapRequestHandler`],
//! [`AbArbiter`], [`MoldUdpReceiver`].
//!
//! Receiver runs over caller-built legs of any [`transport_core::DatagramRecv`]:
//! caller binds each leg and joins its multicast group, then hands legs over.
//! Receive is synchronous ([`MoldUdpReceiver::poll`]); async `recv` exists when
//! legs implement [`transport_core::AsyncReady`]. Gap recovery is opt-in type
//! state: [`MoldUdpReceiver::with_requester`] attaches unicast socket that sends
//! re-requests and reads retransmissions.
//!
//! | Feature | Enables |
//! | --- | --- |
//! | `observability` | message and gap counters through `observability-core` |

pub mod ab;
pub mod config;
pub mod error;
pub mod event;
pub mod frame;
pub mod gap;
pub mod reassembly;
pub mod receiver;
pub mod wire;

pub use ab::{AbArbiter, ArbiterStats, ArbiterVerdict, StreamStats};
pub use config::MoldUdpReceiverConfig;
pub use error::MoldUdpError;
pub use event::MoldUdpEvent;
pub use frame::{Frame, MessageView, OwnedFrame};
pub use gap::{GapRequest, GapRequestEmitter, GapRequestHandler};
pub use reassembly::{DrainCursor, SequenceReassembler, Slot};
pub use receiver::{
    AsyncRecovery, MIN_LEG_POOL_CAPACITY, MoldUdpOutcome, MoldUdpReceiver, NoRecovery,
    ReceiverStats, Recovery, Requester,
};
pub use wire::{DownstreamHeader, MessageBlockIter, PacketKind, parse_header};
