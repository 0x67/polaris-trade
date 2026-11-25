#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! `io_uring` UDP receive for Linux, over `transport_core`'s kernel-bypass shell.
//!
//! `IoUringUdp` binds one UDP socket and receives through one ring into one
//! `IndexPool`: datagrams land in pool slots and come back as frames without
//! copy. Receive path is picked at bind from what running kernel supports,
//! best first, unless `IoUringConfig::path` forces one:
//!
//! | `RecvPath` | Buffers reach kernel by | Recvs armed |
//! | --- | --- | --- |
//! | `Multishot` | registered buffer ring | one multishot recv |
//! | `BufRing` | registered buffer ring | single-shot fleet of `depth` |
//! | `Legacy` | `ProvideBuffers`, one per contiguous run of slots | single-shot fleet of `depth` |
//!
//! Receive only: no send, no stream. Idle `recv_burst` makes no syscall. With
//! every slot held, `recv_burst` returns `PoolExhausted` and counts `no_buffer`;
//! datagram waits in socket buffer. `io_uring` disabled, filtered (default Docker
//! seccomp) or absent is `Unavailable` at bind.
//!
//! Linux only: on other OS crate is empty.
//!
//! | Feature | Enables |
//! | --- | --- |
//! | `observability` | receive and drop metrics through `transport_core::telemetry` |

#[cfg(target_os = "linux")]
#[doc(hidden)]
pub mod completion;
#[cfg(target_os = "linux")]
mod config;
#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod probe;
#[cfg(target_os = "linux")]
mod ring_mem;

#[cfg(target_os = "linux")]
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

#[cfg(target_os = "linux")]
pub use config::IoUringConfig;
#[cfg(target_os = "linux")]
pub use probe::RecvPath;
#[cfg(target_os = "linux")]
use transport_core::{
    DatagramRecv, FrameBatch, Multicast, MulticastInterface, PoolStats, Transport, TransportError,
    bypass::{BypassTransport, DriverStats},
    pool::IndexFrame,
};

#[cfg(target_os = "linux")]
use crate::driver::UringDriver;

/// Backend name: [`Transport::name`], error `backend` field and metric label.
#[cfg(target_os = "linux")]
const BACKEND: &str = "io-uring";

/// UDP receiver over `io_uring`: [`DatagramRecv`] with [`IndexFrame`] frames,
/// plus [`Multicast`].
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct IoUringUdp(BypassTransport<UringDriver>);

#[cfg(target_os = "linux")]
impl IoUringUdp {
    /// Validate `cfg`, allocate pool, bind socket, open ring, pick receive
    /// path, hand every slot to kernel and arm recvs.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] before any allocation or syscall when
    /// `cfg` breaks a limit; [`TransportError::Bind`] when bind fails;
    /// [`TransportError::Unavailable`] when `io_uring` is disabled, filtered,
    /// absent or offers no receive path; [`TransportError::Unsupported`] when
    /// forced path is absent; [`TransportError::Io`] when ring setup fails.
    pub fn bind(cfg: &IoUringConfig) -> Result<Self, TransportError> {
        UringDriver::bind(cfg).map(|driver| Self(BypassTransport::new(driver)))
    }

    /// Receive path picked at bind.
    pub fn path(&self) -> RecvPath {
        self.0.driver().path()
    }

    /// Driver counters: `no_buffer` (ENOBUFS completions), `truncated`
    /// (datagrams longer than slot) and `syscalls` (`io_uring_enter` calls).
    pub fn stats(&self) -> DriverStats {
        self.0.stats()
    }

    /// Bound local address, with port OS picked for port 0.
    ///
    /// # Errors
    ///
    /// [`TransportError::Io`] when OS query fails.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        let addr = self
            .0
            .driver()
            .socket()
            .local_addr()
            .map_err(io_error("getsockname"))?;
        // AF_INET and AF_INET6 sockets always have IP address
        addr.as_socket().ok_or_else(|| TransportError::Io {
            stage: "getsockname",
            error: io::ErrorKind::Unsupported.into(),
        })
    }
}

#[cfg(target_os = "linux")]
impl Transport for IoUringUdp {
    fn name(&self) -> &'static str {
        BACKEND
    }
}

#[cfg(target_os = "linux")]
impl DatagramRecv for IoUringUdp {
    type Frame = IndexFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<usize, TransportError> {
        self.0.recv_burst(out)
    }

    fn pool_stats(&self) -> PoolStats {
        self.0.pool_stats()
    }
}

#[cfg(target_os = "linux")]
impl Multicast for IoUringUdp {
    /// Join `group` on socket, no `io_uring` op; interface field of other
    /// address family is `InvalidConfig`.
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        let sock = self.0.driver().socket();
        match group {
            IpAddr::V4(group) => {
                if iface.v6_scope_id.is_some() {
                    return Err(TransportError::InvalidConfig {
                        field: "iface.v6_scope_id",
                        reason: "set for IPv4 group",
                    });
                }
                sock.join_multicast_v4(&group, &iface.v4.unwrap_or(Ipv4Addr::UNSPECIFIED))
            }
            IpAddr::V6(group) => {
                if iface.v4.is_some() {
                    return Err(TransportError::InvalidConfig {
                        field: "iface.v4",
                        reason: "set for IPv6 group",
                    });
                }
                sock.join_multicast_v6(&group, iface.v6_scope_id.unwrap_or(0))
            }
        }
        .map_err(io_error("join_multicast"))
    }
}

// `map_err` adapter tagging OS error with failed stage
#[cfg(target_os = "linux")]
fn io_error(stage: &'static str) -> impl FnOnce(io::Error) -> TransportError {
    move |error| TransportError::Io { stage, error }
}
