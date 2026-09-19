#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! `AF_XDP` receive for Linux over raw XSK driver and its own XDP redirect program.
//!
//! `AfxdpL2` binds one interface queue, registers one
//! [`IndexPool`](transport_core::pool::IndexPool) region as UMEM and reaps whole
//! Ethernet frames through [`transport_core::L2Recv`]. Wrap it in
//! [`UdpDecap`](transport_core::decap::UdpDecap) to serve datagram consumers. Each
//! frame starts at its descriptor's address, never at fixed offset.
//!
//! `XdpRedirect` picks how frames reach socket:
//!
//! | Redirect | Behaviour |
//! | --- | --- |
//! | `Builtin { mode }` (default `Skb`) | loads six-instruction program redirecting queue to XSKMAP, kernel stack on miss; attached through `BPF_LINK_CREATE`, so drop or crash detaches. One per interface and mode: second gets `Unavailable` |
//! | `Pinned { path }` | inserts socket into external program's pinned XSKMAP; program stays attached. Multi-queue deployments |
//!
//! Privileges: `CAP_NET_RAW` for socket; built-in mode adds `CAP_BPF` and
//! `CAP_NET_ADMIN`. UMEM is charged to `RLIMIT_MEMLOCK` unless `CAP_IPC_LOCK`.
//! Kernel 5.9 or later (`BPF_LINK_CREATE` for XDP). Rebinding queue right after
//! drop can fail `Unavailable` for seconds while kernel releases old socket.
//!
//! Backend name ([`transport_core::Transport::name`] and metric label): `afxdp`. Feature
//! `observability` forwards `transport_core/observability`. Linux only: other
//! targets build empty crate.

#[cfg(target_os = "linux")]
mod config;
#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod ring;
#[cfg(target_os = "linux")]
mod xdp;

#[cfg(target_os = "linux")]
use std::{io, net::IpAddr};

#[cfg(target_os = "linux")]
pub use config::{AfxdpConfig, XdpMode, XdpRedirect};
#[cfg(target_os = "linux")]
use socket2::{Domain, InterfaceIndexOrAddress, Protocol, Socket, Type};
#[cfg(target_os = "linux")]
use transport_core::{
    FrameBatch, L2Recv, Multicast, MulticastInterface, PoolStats, Transport, TransportError,
    bypass::{BypassTransport, DriverStats},
    pool::IndexFrame,
};

#[cfg(target_os = "linux")]
use crate::driver::XskDriver;

/// Backend name: [`Transport::name`] and metric label.
#[cfg(target_os = "linux")]
const BACKEND: &str = "afxdp";

/// [`TransportError::Unavailable`] of this backend.
#[cfg(target_os = "linux")]
fn unavailable(reason: &'static str, error: Option<io::Error>) -> TransportError {
    TransportError::Unavailable {
        backend: BACKEND,
        reason,
        error,
    }
}

/// `AF_XDP` receive on one interface queue: [`L2Recv`] of whole Ethernet frames,
/// plus [`Multicast`] through kernel group membership on that interface.
///
/// Never returns [`TransportError::PoolExhausted`]: with every frame held, kernel
/// drops arriving frames and counts them in [`stats`](Self::stats) `no_buffer`.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct AfxdpL2 {
    rx: BypassTransport<XskDriver>,
    ifindex: u32,
    // kernel UDP sockets holding group memberships, opened on first join per family
    group_v4: Option<Socket>,
    group_v6: Option<Socket>,
}

#[cfg(target_os = "linux")]
impl AfxdpL2 {
    /// Validate `cfg`, then open socket, register UMEM, map rings, bind queue, install redirect.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] for invalid `cfg` (before any syscall), pinned
    /// map not found or not XSKMAP, or queue beyond map; [`TransportError::Unavailable`]
    /// naming missing privilege or conflict (queue busy, program attached, queue served);
    /// [`TransportError::Unsupported`] for built-in program on big-endian host;
    /// [`TransportError::Io`] for other kernel failure, stage naming call.
    pub fn bind(cfg: &AfxdpConfig) -> Result<Self, TransportError> {
        cfg.validate()?;
        let ifindex = driver::ifindex(&cfg.ifname)?;
        Ok(Self {
            rx: BypassTransport::new(XskDriver::open(cfg, ifindex)?),
            ifindex,
            group_v4: None,
            group_v6: None,
        })
    }

    /// Driver counters. `no_buffer`: kernel `rx_dropped` (no free UMEM frame, or
    /// frame longer than `frame_size - headroom - 256`); `nic_missed`: kernel
    /// `rx_ring_full`; `truncated`: descriptors running past their frame;
    /// `syscalls`: wakeup kicks. Reading costs one `getsockopt`.
    pub fn stats(&self) -> DriverStats {
        self.rx.stats()
    }
}

#[cfg(target_os = "linux")]
impl Transport for AfxdpL2 {
    fn name(&self) -> &'static str {
        BACKEND
    }
}

#[cfg(target_os = "linux")]
impl L2Recv for AfxdpL2 {
    type Frame = IndexFrame;

    #[inline]
    fn recv_burst(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<usize, TransportError> {
        self.rx.recv_burst(out)
    }

    fn pool_stats(&self) -> PoolStats {
        self.rx.pool_stats()
    }
}

#[cfg(target_os = "linux")]
impl Multicast for AfxdpL2 {
    /// Join `group` on bound interface by index, through kernel UDP socket held
    /// for transport's life: kernel sends IGMP or MLD report and programs NIC
    /// filter, redirect hands group's frames to socket.
    ///
    /// `iface` must be default: `AF_XDP` receives on its bound interface only.
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        if iface != MulticastInterface::default() {
            return Err(TransportError::InvalidConfig {
                field: "iface",
                reason: "AF_XDP joins on its bound interface only",
            });
        }
        let index = self.ifindex;
        match group {
            IpAddr::V4(group) => group_socket(&mut self.group_v4, Domain::IPV4)?
                .join_multicast_v4_n(&group, &InterfaceIndexOrAddress::Index(index)),
            IpAddr::V6(group) => {
                group_socket(&mut self.group_v6, Domain::IPV6)?.join_multicast_v6(&group, index)
            }
        }
        .map_err(|error| TransportError::Io {
            stage: "join_multicast",
            error,
        })
    }
}

// socket of `slot`, opened on first use
#[cfg(target_os = "linux")]
fn group_socket(slot: &mut Option<Socket>, domain: Domain) -> Result<&Socket, TransportError> {
    let sock = match slot.take() {
        Some(sock) => sock,
        None => Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(|error| {
            TransportError::Io {
                stage: "socket(multicast)",
                error,
            }
        })?,
    };
    Ok(slot.insert(sock))
}
