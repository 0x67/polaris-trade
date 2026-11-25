//! Raw XSK driver: UMEM over [`IndexPool`] region, fill and receive rings, queue bind.
//!
//! Each slot sits in exactly one place: fill ring, kernel, receive ring, one live
//! [`IndexFrame`], pool's freed list or driver's free list. Frame drop queues its
//! slot on freed list; next `poll_frames` moves it to free list, then fill ring. Kernel
//! hands slot back through receive ring only, so slot never sits in fill ring
//! and in live frame at once.

use std::{
    cell::Cell,
    ffi::{CString, c_int},
    fmt, io, iter,
    num::{NonZeroU32, NonZeroU64},
    os::fd::{AsFd, AsRawFd, RawFd},
    ptr::{self, NonNull},
    sync::atomic::AtomicU32,
};

use socket2::{Domain, Socket, Type};
use transport_core::{
    FrameBatch, PoolStats, TransportError,
    bypass::{Driver, DriverStats, L2, Polled},
    pool::{IndexFrame, IndexPool},
};

use crate::{
    AfxdpConfig, BACKEND,
    ring::{Consumer, Producer, RingPtrs},
    unavailable,
    xdp::{self, XdpLink},
};

#[expect(clippy::cast_possible_truncation, reason = "AF_XDP is 44")]
const XDP_FAMILY: libc::sa_family_t = libc::AF_XDP as libc::sa_family_t;
// sized before mmap offsets are read; process never maps completion ring
const RING_SIZES: [(c_int, &str); 3] = [
    (libc::XDP_UMEM_FILL_RING, "setsockopt(XDP_UMEM_FILL_RING)"),
    (
        libc::XDP_UMEM_COMPLETION_RING,
        "setsockopt(XDP_UMEM_COMPLETION_RING)",
    ),
    (libc::XDP_RX_RING, "setsockopt(XDP_RX_RING)"),
];
const MMAP_STAGE: &str = "mmap(XDP ring)";

/// Interface index of `name`.
pub(crate) fn ifindex(name: &str) -> Result<u32, TransportError> {
    let name = CString::new(name).map_err(|_| TransportError::InvalidConfig {
        field: "ifname",
        reason: "contains NUL byte",
    })?;
    // SAFETY: `name` NUL-terminated and live for call
    match unsafe { libc::if_nametoindex(name.as_ptr()) } {
        0 => Err(TransportError::Io {
            stage: "if_nametoindex",
            error: io::Error::last_os_error(),
        }),
        index => Ok(index),
    }
}

/// Raw XSK receive driver.
///
/// Fields drop in declared order: link (program detaches first), rings, ring
/// mappings, socket, then pool, so UMEM region is freed after socket closes.
pub(crate) struct XskDriver {
    // built-in redirect only
    _link: Option<XdpLink>,
    rings: Rings,
    // memory `rings` points into
    _fill_map: Mapping,
    _rx_map: Mapping,
    socket: Socket,
    pool: IndexPool,
    syscalls: u64,
    // last kernel counters read; kept when read fails, so counters stay monotonic
    kernel: Cell<KernelDrops>,
}

#[derive(Clone, Copy, Default)]
struct KernelDrops {
    rx_dropped: u64,
    rx_ring_full: u64,
}

impl XskDriver {
    /// Open socket, register UMEM, map rings, hand every frame to kernel, bind
    /// queue of `ifindex`, then install redirect. `cfg` already validated.
    pub(crate) fn open(cfg: &AfxdpConfig, ifindex: u32) -> Result<Self, TransportError> {
        let pool = IndexPool::new(cfg.frames, cfg.frame_size)?;
        let socket =
            Socket::new(Domain::from(libc::AF_XDP), Type::RAW, None).map_err(socket_error)?;
        let fd = socket.as_raw_fd();
        register_umem(fd, &pool, cfg)?;
        let entries = cfg.frames.get();
        for (name, stage) in RING_SIZES {
            set_opt(fd, name, &entries).map_err(|error| TransportError::Io { stage, error })?;
        }
        let off = mmap_offsets(fd)?;
        let fill_pgoff = libc::off_t::try_from(libc::XDP_UMEM_PGOFF_FILL_RING).map_err(|_| {
            TransportError::Io {
                stage: MMAP_STAGE,
                error: io::ErrorKind::Unsupported.into(),
            }
        })?;
        let (fill_map, fill) = map_ring::<u64>(fd, &off.fr, entries, fill_pgoff)?;
        let (rx_map, rx) =
            map_ring::<libc::xdp_desc>(fd, &off.rx, entries, libc::XDP_PGOFF_RX_RING)?;
        // SAFETY: `fill` and `rx` point into mappings sized for `entries` and
        // checked for bounds and alignment by `map_ring`; driver keeps mappings
        // after `rings` in field order; kernel owns only the peer side of each ring
        let (fill, rx) = unsafe { (Producer::new(fill, entries), Consumer::new(rx, entries)) };
        let mut rings = Rings::new(fill, rx, cfg.frames, cfg.frame_size);
        rings.refill(&pool);
        bind(fd, cfg, ifindex)?;
        let link = xdp::install(&cfg.redirect, ifindex, cfg.queue, socket.as_fd())?;
        Ok(Self {
            _link: link,
            rings,
            _fill_map: fill_map,
            _rx_map: rx_map,
            socket,
            pool,
            syscalls: 0,
            kernel: Cell::default(),
        })
    }

    // zero-copy drivers wait for kick to take fill entries; copy mode never asks
    fn wake(&mut self) -> Result<(), TransportError> {
        self.syscalls += 1;
        match self.socket.recv_with_flags(&mut [], libc::MSG_DONTWAIT) {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) || matches!(error.raw_os_error(), Some(libc::EBUSY | libc::ENOBUFS)) =>
            {
                Ok(())
            }
            Err(error) => Err(TransportError::Io {
                stage: "recvfrom(AF_XDP wakeup)",
                error,
            }),
        }
    }

    fn kernel_drops(&self) -> KernelDrops {
        let mut stats = libc::xdp_statistics {
            rx_dropped: 0,
            rx_invalid_descs: 0,
            tx_invalid_descs: 0,
            rx_ring_full: 0,
            rx_fill_ring_empty_descs: 0,
            tx_ring_empty_descs: 0,
        };
        // SAFETY: `xdp_statistics` is plain `u64`s, any bit pattern valid
        let read = unsafe { get_opt(self.socket.as_raw_fd(), libc::XDP_STATISTICS, &mut stats) };
        // bound socket with full-size buffer cannot fail; last reading covers it anyway
        if read.is_ok() {
            self.kernel.set(KernelDrops {
                rx_dropped: stats.rx_dropped,
                rx_ring_full: stats.rx_ring_full,
            });
        }
        self.kernel.get()
    }
}

impl Driver for XskDriver {
    type Frame = IndexFrame;
    type Layer = L2;
    const BACKEND: &'static str = BACKEND;

    #[inline]
    fn poll_frames(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<Polled, TransportError> {
        self.rings.refill(&self.pool);
        let pushed = self.rings.poll_frames(&self.pool, out);
        if pushed > 0 {
            return Ok(Polled::Frames(pushed));
        }
        if self.rings.fill.needs_wakeup() {
            self.wake()?;
        }
        Ok(Polled::Idle)
    }

    fn stats(&self) -> DriverStats {
        let kernel = self.kernel_drops();
        DriverStats {
            no_buffer: kernel.rx_dropped,
            nic_missed: kernel.rx_ring_full,
            truncated: self.rings.truncated,
            syscalls: self.syscalls,
        }
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }
}

impl fmt::Debug for XskDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XskDriver")
            .field("frames", &self.rings.frames)
            .field("frame_size", &self.rings.frame_size)
            .field("pool", &self.pool.stats())
            .finish_non_exhaustive()
    }
}

/// Syscall-free receive state: ring indices, slots owed to kernel, overrun count.
struct Rings {
    fill: Producer<u64>,
    rx: Consumer<libc::xdp_desc>,
    // slots owed to fill ring; never above `frames` entries, so never reallocates
    free: Vec<u32>,
    // `drain_freed` swap target, empty between calls
    freed: Vec<u32>,
    frames: NonZeroU32,
    frame_size: NonZeroU32,
    truncated: u64,
}

impl Rings {
    // every slot starts on free list; first `refill` hands all to kernel
    fn new(
        fill: Producer<u64>,
        rx: Consumer<libc::xdp_desc>,
        frames: NonZeroU32,
        frame_size: NonZeroU32,
    ) -> Self {
        Self {
            fill,
            rx,
            free: (0..frames.get()).collect(),
            freed: Vec::with_capacity(frames.get() as usize),
            frames,
            frame_size,
            truncated: 0,
        }
    }

    // slots of dropped frames back to fill ring; ring holds `frames` entries, so all fit
    fn refill(&mut self, pool: &IndexPool) {
        pool.drain_freed(&mut self.freed);
        self.free.append(&mut self.freed);
        let size = u64::from(self.frame_size.get());
        let free = &mut self.free;
        self.fill
            .produce(iter::from_fn(|| free.pop()).map(|slot| u64::from(slot) * size));
    }

    // drain receive ring until frame pushed, `out` full or ring empty
    fn poll_frames(&mut self, pool: &IndexPool, out: &mut FrameBatch<IndexFrame>) -> usize {
        let mut pushed = 0;
        loop {
            let room = u32::try_from(out.spare()).unwrap_or(u32::MAX);
            let taken = self.rx.consume(room, |desc| {
                match locate(desc.addr, desc.len, self.frame_size, self.frames) {
                    Located::Frame { slot, offset, len } => {
                        // SAFETY: kernel handed `slot` back through receive ring,
                        // so it left fill ring, kernel finished writing it, and no
                        // live frame covers it (a slot re-enters fill ring only
                        // after its frame drops); `locate` checked
                        // `slot < frames` and `offset + len <= frame_size`
                        out.push(unsafe { pool.frame(slot, offset, len) });
                        pushed += 1;
                    }
                    Located::Truncated { slot } => {
                        self.truncated += 1;
                        self.free.push(slot);
                    }
                    Located::Foreign => self.truncated += 1,
                }
            });
            if taken == 0 || pushed > 0 || out.spare() == 0 {
                return pushed;
            }
        }
    }
}

/// Where descriptor's bytes sit in UMEM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Located {
    /// `len` bytes at `offset` in `slot`, inside frame.
    Frame { slot: u32, offset: u32, len: u32 },
    /// Runs past end of its frame: counted, `slot` recycled.
    Truncated { slot: u32 },
    /// Address outside UMEM: counted, nothing to recycle.
    Foreign,
}

/// Map descriptor to slot and in-slot offset. Payload starts at `addr` itself:
/// kernel puts configured headroom and 256-byte XDP headroom (moved by any
/// `adjust_head`) before it, so offset varies per frame.
pub(crate) fn locate(addr: u64, len: u32, frame_size: NonZeroU32, frames: NonZeroU32) -> Located {
    let size = NonZeroU64::from(frame_size);
    let (Ok(slot), Ok(offset)) = (u32::try_from(addr / size), u32::try_from(addr % size)) else {
        return Located::Foreign;
    };
    if slot >= frames.get() {
        return Located::Foreign;
    }
    match offset.checked_add(len) {
        Some(end) if end <= frame_size.get() => Located::Frame { slot, offset, len },
        _ => Located::Truncated { slot },
    }
}

/// One mmap of kernel ring memory, unmapped on drop.
struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

// SAFETY: owns mapping outright and never dereferences it; munmap works from any thread
unsafe impl Send for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `base` and `len` came from successful mmap; unmapped only here, once
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

// map ring at `pgoff`, checking kernel offsets for bounds and alignment first
fn map_ring<T>(
    fd: RawFd,
    off: &libc::xdp_ring_offset,
    entries: u32,
    pgoff: libc::off_t,
) -> Result<(Mapping, RingPtrs<T>), TransportError> {
    let bad = || TransportError::Io {
        stage: MMAP_STAGE,
        error: io::ErrorKind::InvalidData.into(),
    };
    let at = |off: u64| usize::try_from(off).map_err(|_| bad());
    let (producer, consumer, flags, desc) = (
        at(off.producer)?,
        at(off.consumer)?,
        at(off.flags)?,
        at(off.desc)?,
    );
    let word = size_of::<AtomicU32>();
    let word_ok =
        |w: usize| w.is_multiple_of(word) && w.checked_add(word).is_some_and(|end| end <= desc);
    if !(word_ok(producer)
        && word_ok(consumer)
        && word_ok(flags)
        && desc.is_multiple_of(align_of::<T>()))
    {
        return Err(bad());
    }
    let len = (entries as usize)
        .checked_mul(size_of::<T>())
        .and_then(|n| n.checked_add(desc))
        .ok_or_else(bad)?;
    // SAFETY: fresh shared mapping of socket's ring; kernel checks `pgoff` and `len`
    let raw = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd,
            pgoff,
        )
    };
    if raw == libc::MAP_FAILED {
        return Err(TransportError::Io {
            stage: MMAP_STAGE,
            error: io::Error::last_os_error(),
        });
    }
    let base = NonNull::new(raw.cast::<u8>()).ok_or_else(bad)?;
    let map = Mapping { base, len };
    // SAFETY: every offset checked above to lie inside `len`-byte mapping
    let (producer, consumer, flags, desc) = unsafe {
        (
            base.add(producer),
            base.add(consumer),
            base.add(flags),
            base.add(desc),
        )
    };
    let ptrs = RingPtrs {
        producer: producer.cast(),
        consumer: consumer.cast(),
        flags: flags.cast(),
        desc: desc.cast(),
    };
    Ok((map, ptrs))
}

fn socket_error(error: io::Error) -> TransportError {
    let reason = match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES) => "needs CAP_NET_RAW",
        Some(libc::EAFNOSUPPORT) => "kernel built without AF_XDP",
        _ => {
            return TransportError::Io {
                stage: "socket(AF_XDP)",
                error,
            };
        }
    };
    unavailable(reason, Some(error))
}

fn register_umem(fd: RawFd, pool: &IndexPool, cfg: &AfxdpConfig) -> Result<(), TransportError> {
    let reg = libc::xdp_umem_reg {
        addr: pool.base().expose_provenance() as u64,
        len: u64::from(cfg.frames.get()) * u64::from(cfg.frame_size.get()),
        chunk_size: cfg.frame_size.get(),
        headroom: cfg.headroom,
        flags: 0,
        tx_metadata_len: 0,
    };
    set_opt(fd, libc::XDP_UMEM_REG, &reg).map_err(|error| {
        if error.raw_os_error() == Some(libc::ENOBUFS) {
            unavailable("RLIMIT_MEMLOCK too low; grant CAP_IPC_LOCK", Some(error))
        } else {
            TransportError::Io {
                stage: "setsockopt(XDP_UMEM_REG)",
                error,
            }
        }
    })
}

fn mmap_offsets(fd: RawFd) -> Result<libc::xdp_mmap_offsets, TransportError> {
    let ring = libc::xdp_ring_offset {
        producer: 0,
        consumer: 0,
        desc: 0,
        flags: 0,
    };
    let mut off = libc::xdp_mmap_offsets {
        rx: ring,
        tx: ring,
        fr: ring,
        cr: ring,
    };
    // SAFETY: `xdp_mmap_offsets` is plain `u64`s, any bit pattern valid
    let len = unsafe { get_opt(fd, libc::XDP_MMAP_OFFSETS, &mut off) }.map_err(|error| {
        TransportError::Io {
            stage: "getsockopt(XDP_MMAP_OFFSETS)",
            error,
        }
    })?;
    // kernels before 5.4 return offsets without `flags`, so no need-wakeup
    if len < optlen::<libc::xdp_mmap_offsets>() {
        return Err(unavailable("kernel predates AF_XDP ring flags (5.4)", None));
    }
    Ok(off)
}

fn bind(fd: RawFd, cfg: &AfxdpConfig, ifindex: u32) -> Result<(), TransportError> {
    let copy = if cfg.zero_copy {
        libc::XDP_ZEROCOPY
    } else {
        libc::XDP_COPY
    };
    let addr = libc::sockaddr_xdp {
        sxdp_family: XDP_FAMILY,
        sxdp_flags: copy | libc::XDP_USE_NEED_WAKEUP,
        sxdp_ifindex: ifindex,
        sxdp_queue_id: cfg.queue,
        sxdp_shared_umem_fd: 0,
    };
    // SAFETY: `addr` is whole `sockaddr_xdp`, passed with its size, live for call
    let rc = unsafe {
        libc::bind(
            fd,
            ptr::from_ref(&addr).cast(),
            optlen::<libc::sockaddr_xdp>(),
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    Err(if error.raw_os_error() == Some(libc::EBUSY) {
        unavailable("queue busy or previous socket still releasing", Some(error))
    } else {
        TransportError::Io {
            stage: "bind(AF_XDP)",
            error,
        }
    })
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "uapi option structs are under 100 bytes"
)]
const fn optlen<T>() -> libc::socklen_t {
    size_of::<T>() as libc::socklen_t
}

// kernel only reads `size_of::<T>()` bytes at `value`
fn set_opt<T>(fd: RawFd, name: c_int, value: &T) -> io::Result<()> {
    // SAFETY: `value` points at `size_of::<T>()` readable bytes, live for call
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_XDP,
            name,
            ptr::from_ref(value).cast(),
            optlen::<T>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Read `SOL_XDP` option `name` into `value`, returning length kernel wrote.
///
/// # Safety
///
/// Every bit pattern is valid `T`: kernel writes up to `size_of::<T>()` bytes.
unsafe fn get_opt<T>(fd: RawFd, name: c_int, value: &mut T) -> io::Result<libc::socklen_t> {
    let mut len = optlen::<T>();
    // SAFETY: `value` points at `len` writable bytes, live for call; caller
    // guarantees any bytes kernel writes form valid `T`
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_XDP,
            name,
            ptr::from_mut(value).cast(),
            &raw mut len,
        )
    };
    if rc == 0 {
        Ok(len)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::ring::heap::HeapRing;

    const FRAME: NonZeroU32 = NonZeroU32::new(2048).unwrap();
    const FRAMES: NonZeroU32 = NonZeroU32::new(4).unwrap();
    const BLANK: libc::xdp_desc = libc::xdp_desc {
        addr: 0,
        len: 0,
        options: 0,
    };

    // pool plus heap fill and receive rings; methods play kernel
    struct Kernel {
        fill: HeapRing<u64>,
        rx: HeapRing<libc::xdp_desc>,
        pool: IndexPool,
    }

    impl Kernel {
        fn new() -> Self {
            Self {
                fill: HeapRing::new(FRAMES.get(), 0, 0),
                rx: HeapRing::new(FRAMES.get(), 0, BLANK),
                pool: IndexPool::new(FRAMES, FRAME).unwrap(),
            }
        }

        // rings as driver builds them, every chunk already taken off fill ring
        fn rings(&self) -> (Rings, Vec<u64>) {
            // SAFETY: `self` outlives returned rings in every test; heap rings
            // play only peer side
            let (fill, rx) = unsafe {
                (
                    Producer::new(self.fill.ptrs(), FRAMES.get()),
                    Consumer::new(self.rx.ptrs(), FRAMES.get()),
                )
            };
            let mut rings = Rings::new(fill, rx, FRAMES, FRAME);
            rings.refill(&self.pool);
            (rings, self.take_fill())
        }

        fn take_fill(&self) -> Vec<u64> {
            iter::from_fn(|| self.fill.pop()).collect()
        }

        // write `bytes` at `addr`, publish its descriptor
        fn deliver(&self, addr: u64, bytes: &[u8]) {
            let at = usize::try_from(addr).unwrap();
            assert!(at + bytes.len() <= (FRAMES.get() * FRAME.get()) as usize);
            // SAFETY: range inside region; its slot sits outside every live frame
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), self.pool.base().add(at), bytes.len());
            }
            self.publish(addr, u32::try_from(bytes.len()).unwrap());
        }

        fn publish(&self, addr: u64, len: u32) {
            assert!(self.rx.push(libc::xdp_desc {
                addr,
                len,
                options: 0,
            }));
        }
    }

    #[test]
    fn locate_bounds_payload_by_frame_and_umem() {
        let at = |addr, len| locate(addr, len, FRAME, FRAMES);
        assert_eq!(
            at(3 * 2048 + 320, 60),
            Located::Frame {
                slot: 3,
                offset: 320,
                len: 60
            },
            "payload starts at addr, not chunk base"
        );
        assert_eq!(
            at(2048 + 1000, 1048),
            Located::Frame {
                slot: 1,
                offset: 1000,
                len: 1048
            }
        );
        assert_eq!(at(2048 + 1000, 1049), Located::Truncated { slot: 1 });
        assert_eq!(at(4 * 2048 + 256, 10), Located::Foreign);
        assert_eq!(at(u64::MAX, 10), Located::Foreign);
    }

    #[test]
    fn poll_frames_yields_bytes_at_descriptor_address_bounded_by_spare() {
        let kernel = Kernel::new();
        let (mut rings, chunks) = kernel.rings();
        assert_eq!(chunks.len(), 4, "first refill hands every frame to kernel");
        // kernel default (256), configured headroom 64, start moved by program
        let sent: [(u64, &[u8]); 3] = [
            (256, b"at default headroom"),
            (256 + 64, b"after configured headroom"),
            (256 + 64 + 38, b"after adjust_head"),
        ];
        for ((offset, bytes), chunk) in sent.iter().zip(&chunks) {
            kernel.deliver(chunk + offset, bytes);
        }

        let mut out = FrameBatch::with_capacity(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            rings.poll_frames(&kernel.pool, &mut out),
            2,
            "bounded by spare"
        );
        let mut frames: Vec<IndexFrame> = out.drain().collect();
        assert_eq!(
            rings.poll_frames(&kernel.pool, &mut out),
            1,
            "rest stays in ring"
        );
        frames.extend(out.drain());
        let got: Vec<&[u8]> = frames.iter().map(AsRef::as_ref).collect();
        let want: Vec<&[u8]> = sent.iter().map(|(_, bytes)| *bytes).collect();
        assert_eq!(got, want);
        assert_eq!(rings.truncated, 0);
    }

    #[test]
    fn poll_frames_counts_overrun_descriptor_and_recycles_its_slot_only() {
        let kernel = Kernel::new();
        let (mut rings, chunks) = kernel.rings();
        let (held, over) = (chunks[0], chunks[1]);
        kernel.deliver(held + 256, b"held");
        kernel.publish(over + 256, 2048 - 255);
        kernel.publish(8 * 2048, 10);

        let mut out = FrameBatch::with_capacity(NonZeroUsize::new(4).unwrap());
        assert_eq!(
            rings.poll_frames(&kernel.pool, &mut out),
            1,
            "only in-bounds descriptor framed"
        );
        assert_eq!(
            rings.truncated, 2,
            "overrun and foreign descriptors counted"
        );

        rings.refill(&kernel.pool);
        assert_eq!(
            kernel.take_fill(),
            [over],
            "overrun slot refilled, held slot not"
        );
        drop(out);
        rings.refill(&kernel.pool);
        assert_eq!(kernel.take_fill(), [held], "dropped frame's slot refilled");
    }
}
