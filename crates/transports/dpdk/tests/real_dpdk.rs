//! `DpdkL2` over real EAL: `--no-huge`, `net_pcap` vdevs replaying known frames.
//!
//! Crate never initialises EAL, so this file does: EAL once per process, then
//! per test one mempool and one port. Ignored: needs Linux with libdpdk and
//! pcap PMD. Run: `cargo nextest run -p transport_dpdk --features driver-dpdk
//! --run-ignored ignored-only`.
#![cfg(all(target_os = "linux", feature = "driver-dpdk"))]

use std::{
    env,
    ffi::{CString, c_char, c_int, c_uint, c_void},
    fs,
    net::Ipv4Addr,
    num::NonZeroUsize,
    path::PathBuf,
    process, ptr,
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use transport_core::{
    DatagramRecv, FrameBatch, L2Recv, PoolStats, TransportError, decap::UdpDecap,
};
use transport_dpdk::{DpdkConfig, DpdkL2, MbufFrame};

// exported EAL, mempool and ethdev setup the crate deliberately leaves to caller
unsafe extern "C" {
    fn rte_eal_init(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rte_pktmbuf_pool_create(
        name: *const c_char,
        n: c_uint,
        cache_size: c_uint,
        priv_size: u16,
        data_room_size: u16,
        socket_id: c_int,
    ) -> *mut c_void;
    fn rte_eth_dev_get_port_by_name(name: *const c_char, port_id: *mut u16) -> c_int;
    fn rte_eth_dev_configure(port: u16, nb_rx_q: u16, nb_tx_q: u16, conf: *const c_void) -> c_int;
    fn rte_eth_rx_queue_setup(
        port: u16,
        queue: u16,
        nb_rx_desc: u16,
        socket_id: c_uint,
        rx_conf: *const c_void,
        mempool: *mut c_void,
    ) -> c_int;
    fn rte_eth_dev_start(port: u16) -> c_int;
}

// SOCKET_ID_ANY
const ANY_SOCKET: c_int = -1;
// RTE_MBUF_DEFAULT_BUF_SIZE: 2048 data room plus 128 headroom
const DATA_ROOM: u16 = 2176;
const DST_PORT: u16 = 9000;
const DEADLINE: Duration = Duration::from_secs(5);

const PAYLOAD_VDEV: &str = "net_pcap0";
const EXHAUST_VDEV: &str = "net_pcap1";
const EXHAUST_FRAMES: u8 = 8;
const EXHAUST_POOL: c_uint = 4;

fn payloads() -> [Vec<u8>; 3] {
    [
        b"first".to_vec(),
        (0..1400_u16)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect(),
        b"third".to_vec(),
    ]
}

// Ethernet II, IPv4 (DF set, checksum unset), UDP; decap verifies neither checksum
fn udp_frame(dst_port: u16, payload: &[u8]) -> Vec<u8> {
    let udp_len = u16::try_from(8 + payload.len()).unwrap();
    let mut f = Vec::new();
    f.extend_from_slice(&[0x02, 0, 0, 0, 0, 0x01, 0x02, 0, 0, 0, 0, 0x02, 0x08, 0x00]);
    f.extend_from_slice(&[0x45, 0]);
    f.extend_from_slice(&(20 + udp_len).to_be_bytes());
    f.extend_from_slice(&[0, 0, 0x40, 0, 64, 17, 0, 0]);
    f.extend_from_slice(&Ipv4Addr::new(10, 0, 0, 1).octets());
    f.extend_from_slice(&Ipv4Addr::new(10, 0, 0, 2).octets());
    f.extend_from_slice(&40_000_u16.to_be_bytes());
    f.extend_from_slice(&dst_port.to_be_bytes());
    f.extend_from_slice(&udp_len.to_be_bytes());
    f.extend_from_slice(&[0, 0]);
    f.extend_from_slice(payload);
    f
}

// classic pcap, microsecond timestamps, linktype Ethernet
fn write_pcap(name: &str, frames: &[Vec<u8>]) -> PathBuf {
    let mut out = Vec::new();
    for word in [0xa1b2_c3d4_u32, 0x0004_0002, 0, 0, 65_535, 1] {
        out.extend_from_slice(&word.to_le_bytes());
    }
    for frame in frames {
        let len = u32::try_from(frame.len()).unwrap();
        for word in [0, 0, len, len] {
            out.extend_from_slice(&word.to_le_bytes());
        }
        out.extend_from_slice(frame);
    }
    let path = env::temp_dir().join(format!("transport_dpdk_{}_{name}.pcap", process::id()));
    fs::write(&path, out).unwrap();
    path
}

// EAL once per process: nextest gives each test its own, cargo test shares one
fn eal() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let [first, second, third] = payloads();
        let payload_pcap = write_pcap(
            PAYLOAD_VDEV,
            &[
                udp_frame(DST_PORT, &first),
                udp_frame(DST_PORT + 1, b"wrong port"),
                // above one segment's data room: PMD chains it, driver frees and counts it
                udp_frame(DST_PORT, &[0xab; 3000]),
                udp_frame(DST_PORT, &second),
                udp_frame(DST_PORT, &third),
            ],
        );
        let exhaust_frames: Vec<_> = (0..EXHAUST_FRAMES)
            .map(|i| udp_frame(DST_PORT, &[i]))
            .collect();
        let exhaust_pcap = write_pcap(EXHAUST_VDEV, &exhaust_frames);
        let eal_args = [
            "transport_dpdk_test".to_owned(),
            "--no-huge".to_owned(),
            "-m".to_owned(),
            "256".to_owned(),
            "--no-pci".to_owned(),
            "--no-shconf".to_owned(),
            "--no-telemetry".to_owned(),
            "-l".to_owned(),
            "0".to_owned(),
            format!("--vdev={PAYLOAD_VDEV},rx_pcap={}", payload_pcap.display()),
            format!("--vdev={EXHAUST_VDEV},rx_pcap={}", exhaust_pcap.display()),
        ]
        .map(|a| CString::new(a).unwrap());
        let mut pointers: Vec<*mut c_char> =
            eal_args.iter().map(|a| a.as_ptr().cast_mut()).collect();
        let count = c_int::try_from(pointers.len()).unwrap();
        // SAFETY: `pointers` into `eal_args`, both live across call; first EAL init in process.
        let rc = unsafe { rte_eal_init(count, pointers.as_mut_ptr()) };
        assert!(rc >= 0, "rte_eal_init failed: {rc}");
    });
}

// one-queue port on fresh mempool (no lcore cache, so in-use count exact), started
fn start_port(vdev: &str, mbufs: c_uint) -> (u16, *mut c_void) {
    eal();
    let name = CString::new(vdev).unwrap();
    let mut port = 0;
    // SAFETY: EAL initialised; `name` NUL-terminated; `port` live out-param.
    let rc = unsafe { rte_eth_dev_get_port_by_name(name.as_ptr(), &raw mut port) };
    assert_eq!(rc, 0, "no port {vdev}");
    let pool_name = CString::new(format!("pool_{vdev}")).unwrap();
    // SAFETY: EAL initialised; `pool_name` NUL-terminated and unique per vdev.
    let mempool =
        unsafe { rte_pktmbuf_pool_create(pool_name.as_ptr(), mbufs, 0, 0, DATA_ROOM, ANY_SOCKET) };
    assert!(!mempool.is_null(), "rte_pktmbuf_pool_create {vdev}");
    // zeroed rte_eth_conf (2280 bytes in DPDK 24.11) is all defaults; buffer overshoots it
    let conf = [0_u64; 512];
    // SAFETY: port exists; `conf` outlives calls and covers `rte_eth_conf`; null
    // rx_conf picks PMD defaults; `mempool` live.
    unsafe {
        assert_eq!(rte_eth_dev_configure(port, 1, 0, conf.as_ptr().cast()), 0);
        let socket = c_uint::MAX; // SOCKET_ID_ANY as unsigned
        let rc = rte_eth_rx_queue_setup(port, 0, 64, socket, ptr::null(), mempool);
        assert_eq!(rc, 0, "rx queue setup {vdev}");
        assert_eq!(rte_eth_dev_start(port), 0, "start {vdev}");
    }
    (port, mempool)
}

fn attach(port: u16, mempool: *mut c_void) -> DpdkL2 {
    // SAFETY: `start_port` set up EAL, port and queue 0 on `mempool`, created
    // without SC_GET/SP_PUT and never freed; this test alone polls queue 0.
    unsafe { DpdkL2::attach(&DpdkConfig::new(port, 0), mempool) }.unwrap()
}

#[test]
fn null_mempool_is_invalid_config() {
    // SAFETY: null is rejected before any DPDK call, so no EAL is needed.
    let err = unsafe { DpdkL2::attach(&DpdkConfig::new(0, 0), ptr::null_mut()) }.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::InvalidConfig {
                field: "mempool",
                ..
            }
        ),
        "{err:?}"
    );
}

#[test]
#[ignore = "needs libdpdk with pcap PMD; run with --run-ignored ignored-only"]
fn pcap_payloads_reach_consumer_through_decap_past_chained_mbuf() {
    let (port, mempool) = start_port(PAYLOAD_VDEV, 1023);
    let mut decap = UdpDecap::new(
        attach(port, mempool),
        DST_PORT,
        None,
        NonZeroUsize::new(32).unwrap(),
    );
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(8).unwrap());
    let mut got = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while got.len() < 3 && Instant::now() < deadline {
        decap.recv_burst(&mut out).unwrap();
        got.extend(out.drain().map(|f| f.as_ref().to_vec()));
    }
    assert_eq!(got, payloads(), "payloads in order, wrong port filtered");
    assert_eq!(decap.stats().wrong_dst, 1);
    assert_eq!(decap.inner().stats().truncated, 1, "jumbo frame chained");
}

#[test]
#[ignore = "needs libdpdk with pcap PMD; run with --run-ignored ignored-only"]
fn exhausted_mempool_counts_no_buffer_and_recovers_after_cross_thread_drop() {
    let (port, mempool) = start_port(EXHAUST_VDEV, EXHAUST_POOL);
    let mut l2 = attach(port, mempool);
    let mut pair = FrameBatch::<MbufFrame>::with_capacity(NonZeroUsize::new(2).unwrap());
    let mut out = FrameBatch::<MbufFrame>::with_capacity(NonZeroUsize::new(16).unwrap());

    assert_eq!(
        l2.recv_burst(&mut pair).unwrap(),
        2,
        "burst bounded by caller batch"
    );
    let mut held: Vec<MbufFrame> = pair.drain().collect();
    // rest of pool arrives on next call
    assert_eq!(
        l2.recv_burst(&mut out).unwrap(),
        2,
        "burst bounded by free mbufs"
    );
    held.extend(out.drain());
    let full = PoolStats {
        capacity: EXHAUST_POOL as usize,
        in_use: EXHAUST_POOL as usize,
    };
    assert_eq!(l2.pool_stats(), full);

    // DPDK cannot see data pending without buffer: never PoolExhausted, counter rises
    assert_eq!(l2.recv_burst(&mut out).unwrap(), 0);
    assert!(l2.stats().no_buffer > 0, "{:?}", l2.stats());

    thread::spawn(move || drop(held)).join().unwrap();
    assert_eq!(l2.pool_stats().in_use, 0, "each frame freed its one mbuf");
    assert!(
        l2.recv_burst(&mut out).unwrap() > 0,
        "freed mbufs refill receive"
    );
}
