//! Kernel-path cases over one veth pair, all `#[ignore]`: kernel-proof script runs
//! them privileged (`CAP_NET_RAW`, `CAP_BPF`, `CAP_NET_ADMIN`, `CAP_IPC_LOCK`),
//! one fresh pair per case, through `cargo nextest run --run-ignored ignored-only`.
//!
//! Environment the script provides:
//! - `AFXDP_IFACE`: pair end in this netns, one receive queue, holding `AFXDP_DST`.
//! - `AFXDP_DST`: IPv4 address of `AFXDP_IFACE`. Peer keeps static neighbour
//!   entry for it, since redirect swallows ARP.
//! - `AFXDP_PEER_NETNS`: named netns (`/var/run/netns/<name>`) holding other end,
//!   routed to `AFXDP_DST`.
//! - `AFXDP_PINNED_MAP`: pinned case only; XSKMAP of external program attached
//!   to `AFXDP_IFACE`.
//!
//! No other traffic on pair (IPv6 off on both ends): tight-pool case holds two
//! frames and would lose a datagram to any stray frame.
#![cfg(target_os = "linux")]

use std::{
    env,
    fs::{self, File},
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    num::{NonZeroU32, NonZeroUsize},
    os::fd::AsRawFd,
    path::PathBuf,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use socket2::{Domain, Protocol, Socket, Type};
use transport_afxdp::{AfxdpConfig, AfxdpL2, XdpMode, XdpRedirect};
use transport_core::{
    DatagramRecv, FrameBatch, Multicast, MulticastInterface, TransportError,
    decap::UdpDecap,
    testing::conformance::{DatagramHarness, ExhaustionSignal, run_datagram},
};

const PORT: u16 = 47_000;
const BURST: NonZeroUsize = NonZeroUsize::new(4).unwrap();
// rebind waits out lazy release of previous socket (up to ~14 s observed)
const REBIND_DEADLINE: Duration = Duration::from_secs(20);
const RECV_DEADLINE: Duration = Duration::from_secs(5);

struct Env {
    iface: String,
    dst: Ipv4Addr,
    netns: String,
}

impl Env {
    fn read() -> Self {
        Self {
            iface: var("AFXDP_IFACE"),
            dst: var("AFXDP_DST").parse().expect("AFXDP_DST is IPv4 address"),
            netns: var("AFXDP_PEER_NETNS"),
        }
    }

    // run `f` on thread moved into peer netns; sockets it opens stay there
    fn in_peer<R: Send + 'static>(&self, f: impl FnOnce() -> R + Send + 'static) -> R {
        let ns = File::open(format!("/var/run/netns/{}", self.netns)).expect("open peer netns");
        thread::spawn(move || {
            // SAFETY: `ns` is open netns fd; setns switches calling thread only
            let rc = unsafe { libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET) };
            assert_eq!(rc, 0, "setns: {}", io::Error::last_os_error());
            f()
        })
        .join()
        .expect("peer thread")
    }
}

fn var(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} unset: run through kernel-proof script"))
}

// retries while kernel still releases previous socket or link on this queue
fn bind(cfg: &AfxdpConfig) -> AfxdpL2 {
    let deadline = Instant::now() + REBIND_DEADLINE;
    loop {
        match AfxdpL2::bind(cfg) {
            Ok(transport) => return transport,
            Err(TransportError::Unavailable {
                error: Some(error), ..
            }) if error.raw_os_error() == Some(libc::EBUSY) && Instant::now() < deadline => {
                // best effort: flushes deferred release at once where kernel offers it
                let _ = fs::write("/sys/module/rcutree/parameters/do_rcu_barrier", "1");
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("bind {}: {error}", cfg.ifname),
        }
    }
}

fn xdp_attached(iface: &str) -> bool {
    let out = Command::new("ip")
        .args(["-j", "link", "show", "dev", iface])
        .output()
        .expect("run ip");
    assert!(out.status.success(), "ip link show {iface} failed");
    // iproute2 prints `xdp` object only while program attached
    String::from_utf8_lossy(&out.stdout).contains("\"xdp\":")
}

// conformance suite through `UdpDecap<AfxdpL2>`, datagrams sent from peer netns
fn conformance(env: &Env, redirect: &XdpRedirect) {
    let peer = env.in_peer(|| UdpSocket::bind("0.0.0.0:0").expect("peer socket"));
    let dst = SocketAddr::new(env.dst.into(), PORT);
    run_datagram(DatagramHarness {
        build: |frames: NonZeroU32| {
            let mut cfg = AfxdpConfig::new(env.iface.as_str(), 0);
            cfg.frames = frames;
            cfg.redirect = redirect.clone();
            UdpDecap::new(bind(&cfg), PORT, Some(env.dst), BURST)
        },
        inject: |_: &mut UdpDecap<AfxdpL2>, bytes: &[u8]| {
            peer.send_to(bytes, dst).expect("peer send");
        },
        // fill ring empty: kernel drops frame and counts `rx_dropped`
        drops: |t: &UdpDecap<AfxdpL2>| t.inner().stats().no_buffer,
        exhaustion: ExhaustionSignal::DropCounter,
    });
}

#[test]
#[ignore = "privileged kernel path: kernel-proof script"]
fn builtin_skb_passes_conformance_and_detaches_on_drop() {
    let env = Env::read();
    conformance(&env, &XdpRedirect::Builtin { mode: XdpMode::Skb });
    assert!(
        !xdp_attached(&env.iface),
        "built-in program detached after drop"
    );
}

#[test]
#[ignore = "privileged kernel path: kernel-proof script"]
fn builtin_drv_passes_conformance_and_detaches_on_drop() {
    let env = Env::read();
    conformance(&env, &XdpRedirect::Builtin { mode: XdpMode::Drv });
    assert!(
        !xdp_attached(&env.iface),
        "built-in program detached after drop"
    );
}

#[test]
#[ignore = "privileged kernel path: kernel-proof script"]
fn pinned_passes_conformance_and_leaves_program_attached() {
    let env = Env::read();
    let path = PathBuf::from(var("AFXDP_PINNED_MAP"));
    conformance(&env, &XdpRedirect::Pinned { path });
    assert!(xdp_attached(&env.iface), "external program stays attached");
}

// `/proc/net/igmp`: device line, then one tab-led line per group, address as
// hex of its network-order bytes read in host order
fn igmp_lists(iface: &str, group: Ipv4Addr) -> bool {
    let table = fs::read_to_string("/proc/net/igmp").expect("read /proc/net/igmp");
    let hex = format!("{:08X}", u32::from_ne_bytes(group.octets()));
    let mut device = None;
    table.lines().skip(1).any(|line| {
        if line.starts_with('\t') {
            device == Some(iface) && line.split_whitespace().next() == Some(hex.as_str())
        } else {
            device = line
                .split_whitespace()
                .nth(1)
                .map(|d| d.trim_end_matches(':'));
            false
        }
    })
}

#[test]
#[ignore = "privileged kernel path: kernel-proof script"]
fn multicast_join_listed_and_group_datagram_received() {
    let env = Env::read();
    let group = Ipv4Addr::new(239, 1, 2, 3);
    let mut rx = UdpDecap::new(
        bind(&AfxdpConfig::new(env.iface.as_str(), 0)),
        PORT,
        Some(group),
        BURST,
    );
    rx.join_multicast(group.into(), MulticastInterface::default())
        .expect("join group");
    assert!(
        igmp_lists(&env.iface, group),
        "{group} listed for {} in /proc/net/igmp",
        env.iface
    );

    let dst = env.dst;
    let peer = env.in_peer(move || {
        // local address toward `dst` names peer interface for multicast send
        let probe = UdpSocket::bind("0.0.0.0:0").expect("probe socket");
        probe.connect((dst, PORT)).expect("route to AFXDP_DST");
        let IpAddr::V4(local) = probe.local_addr().expect("probe address").ip() else {
            unreachable!("IPv4 socket has IPv4 address")
        };
        let sock =
            Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).expect("peer socket");
        sock.set_multicast_if_v4(&local)
            .expect("multicast interface");
        sock
    });
    let payload = b"group datagram through AF_XDP";
    peer.send_to(payload, &SocketAddr::new(group.into(), PORT).into())
        .expect("peer send");

    let mut out = FrameBatch::with_capacity(BURST);
    let deadline = Instant::now() + RECV_DEADLINE;
    while rx.recv_burst(&mut out).expect("recv") == 0 {
        assert!(
            Instant::now() < deadline,
            "no group datagram within {RECV_DEADLINE:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let frame = out.drain().next().expect("one datagram");
    assert_eq!(frame.as_ref(), payload);
}
