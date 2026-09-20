//! Runtime-free readiness: one caller-owned [`ReadySet`] over many sockets.
//!
//! [`ReadySet::wait`] blocks calling thread, never async: run it on dedicated
//! receive thread. Sources register by `&mut` and stay caller-owned; mio tracks
//! OS socket, so registered socket may move into consumer.
//!
//! Every read and write on [`MioUdp`] and [`MioTcp`] goes through mio `try_io`:
//! Windows re-arms interest only when would-block passes through it, so leg
//! drained any other way would never be reported again.
//!
//! Multi-leg receive: bind, wrap, join and register each leg, move legs into
//! consumer, loop `wait` and drain consumer until it reports nothing.
//!
//! ```no_run
//! # use std::{net::SocketAddr, num::NonZeroUsize};
//! # use transport_socket::{UdpConfig, UdpSocket, mio::{MioUdp, ReadySet, ReadyToken}};
//! # fn main() -> Result<(), transport_core::TransportError> {
//! let mut set = ReadySet::new(NonZeroUsize::new(8).unwrap())?;
//! let mut legs = Vec::new();
//! for (i, port) in [30_001, 30_002].into_iter().enumerate() {
//!     let bind = SocketAddr::from(([0, 0, 0, 0], port));
//!     let mut leg = MioUdp::from_socket(UdpSocket::bind(&UdpConfig::new(bind))?);
//!     set.register(&mut leg, ReadyToken(i))?;
//!     legs.push(leg);
//! }
//! let mut ready = Vec::new();
//! set.wait(None, &mut ready)?; // then drain legs[r.token.0] per report until Ok(0)
//! # Ok(())
//! # }
//! ```

mod ready;
mod tcp;
mod udp;

pub use ready::{Ready, ReadySet, ReadySource, ReadyToken};
pub use tcp::MioTcp;
pub use udp::MioUdp;

// only this crate's socket types register, since each routes reads and writes through `try_io`
mod sealed {
    pub trait Sealed {
        type Source: ::mio::event::Source;
        const INTEREST: ::mio::Interest;
        fn source(&mut self) -> &mut Self::Source;
    }
}
