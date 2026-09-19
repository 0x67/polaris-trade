//! Sockets registered with tokio reactor, for async consumers.
//!
//! Receive and send call socket2 directly, so busy-poll callers never wait on
//! reactor. [`AsyncReady`](transport_core::AsyncReady) awaits reactor, then
//! peeks inside tokio `try_io`: idle peek clears readiness that direct receive
//! left stale, so `ready()` never resolves on drained socket.
//!
//! Every constructor needs tokio runtime with IO driver on calling thread.

mod tcp;
mod udp;

pub use tcp::TcpStream;
use transport_core::TransportError;
pub use udp::AsyncUdp;

const NO_RUNTIME: TransportError = TransportError::Unavailable {
    backend: "tokio",
    reason: "no tokio runtime on this thread",
    error: None,
};
