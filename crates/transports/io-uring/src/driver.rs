//! Receive engine under [`BypassTransport`](transport_core::bypass::BypassTransport):
//! buffers, recv fleet, completions, teardown.
//!
//! Buffer id `s` is pool slot `s`. Each slot sits in exactly one place: kernel
//! (group or ring), completion queue, `back` (on its way to kernel), live frame,
//! or pool freed list. `reap` drains freed list, turns completions into frames,
//! hands `back` to kernel, re-arms, and enters kernel only when submission
//! queue holds work or completion queue overflowed, so idle spin makes no syscall.

use std::{
    fmt, io,
    mem::{self, ManuallyDrop},
    os::fd::AsRawFd,
    ptr,
    time::{Duration, Instant},
};

use io_uring::{IoUring, opcode, squeue, types};
use socket2::{Domain, Protocol, Socket, Type};
use transport_core::{
    FrameBatch, PoolStats, TransportError,
    bypass::{Driver, DriverStats, L4, Reap},
    pool::{IndexFrame, IndexPool},
};

use crate::{
    BACKEND, IoUringConfig, RecvPath,
    completion::{Completion, classify},
    io_error,
    probe::{open_ring, probe, select},
    ring_mem::RingMem,
};

// user-data tags; reap ignores tags it does not own
const UD_RECV: u64 = 1;
const UD_PROVIDE: u64 = 2;
const UD_CANCEL: u64 = 3;
/// Multishot probe request, see [`probe`].
pub(crate) const UD_PROBE: u64 = 4;

const BGID: u16 = 0;
/// Bound on waiting at drop for cancelled recvs to end.
const CANCEL_WAIT: Duration = Duration::from_secs(1);

// memory kernel reads or writes while any recv is armed
struct KernelMem {
    pool: IndexPool,
    // `None` on legacy path
    buf_ring: Option<RingMem>,
}

/// Armed recv count and starved gate.
#[derive(Debug)]
struct Fleet {
    // requests kept armed: single-shot fleet, or one multishot
    depth: u32,
    armed: u32,
    // ENOBUFS seen: arm nothing until slot goes back to kernel, else every
    // idle spin would re-arm, fail at once and cost syscall
    starved: bool,
}

impl Fleet {
    fn new(depth: u32) -> Self {
        Self {
            depth,
            armed: 0,
            starved: false,
        }
    }

    fn wanted(&self) -> u32 {
        if self.starved {
            0
        } else {
            self.depth.saturating_sub(self.armed)
        }
    }

    fn complete(&mut self, completion: Completion) {
        let ended = match completion {
            Completion::Data { rearm, .. }
            | Completion::Truncated { rearm, .. }
            | Completion::Empty { rearm } => rearm,
            Completion::NoBuffers => {
                self.starved = true;
                true
            }
            Completion::Failed(_) => true,
        };
        if ended {
            self.armed = self.armed.saturating_sub(1);
        }
    }

    fn refilled(&mut self, slots: usize) {
        if slots > 0 {
            self.starved = false;
        }
    }
}

/// `io_uring` [`Driver`]: one UDP socket, one ring, one [`IndexPool`].
pub(crate) struct UringDriver {
    // both dropped by hand: ring first, memory only once no recv is armed
    ring: ManuallyDrop<IoUring>,
    mem: ManuallyDrop<KernelMem>,
    sock: Socket,
    path: RecvPath,
    slot_size: u32,
    skip_success: bool,
    fleet: Fleet,
    // slots on their way to kernel; capacity `slots`, so pushes never allocate
    back: Vec<u32>,
    stats: DriverStats,
}

impl UringDriver {
    /// Validate `cfg`, bind socket, open ring, pick path, hand every slot to
    /// kernel and arm recvs.
    pub(crate) fn bind(cfg: &IoUringConfig) -> Result<Self, TransportError> {
        cfg.validate()?;
        let (slots, depth) = (cfg.slots.get(), cfg.depth.get());
        let pool = IndexPool::new(cfg.slots, cfg.slot_size)?;
        let sock = socket(cfg)?;
        // room for full fleet plus provide and cancel; CQ holds every slot's
        // completion plus fleet's, so overflow needs caller to stop reaping
        let mut ring = open_ring(
            (depth + 2).next_power_of_two(),
            (slots + depth + 2).next_power_of_two(),
        )?;
        let path = select(cfg.path, probe(&mut ring, sock.as_raw_fd())?)?;
        let buf_ring = match path {
            RecvPath::Legacy => None,
            RecvPath::BufRing | RecvPath::Multishot => {
                let mem = RingMem::new(id16(slots.next_power_of_two() as usize))?;
                // SAFETY: `mem` moves into driver, whose `Drop` frees it only
                // after ring is gone and no recv is armed, else leaks it.
                unsafe { mem.register(&ring.submitter(), BGID) }
                    .map_err(io_error("io_uring register buf ring"))?;
                Some(mem)
            }
        };
        let mut driver = Self {
            skip_success: ring.params().is_feature_skip_cqe_on_success(),
            ring: ManuallyDrop::new(ring),
            mem: ManuallyDrop::new(KernelMem { pool, buf_ring }),
            sock,
            path,
            slot_size: cfg.slot_size.get(),
            fleet: Fleet::new(if path == RecvPath::Multishot {
                1
            } else {
                depth
            }),
            back: Vec::with_capacity(slots as usize),
            stats: DriverStats::default(),
        };
        driver.back.extend(0..slots);
        driver.give_back()?;
        driver.arm()?;
        driver.enter()?;
        tracing::info!(backend = BACKEND, ?path, bind = %cfg.bind, "io_uring receive ready");
        Ok(driver)
    }

    pub(crate) fn path(&self) -> RecvPath {
        self.path
    }

    pub(crate) fn socket(&self) -> &Socket {
        &self.sock
    }

    // frames for data completions; truncated slots queued on `back`
    fn complete(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<usize, TransportError> {
        let multishot = self.path == RecvPath::Multishot;
        let mut pushed = 0;
        let mut cq = self.ring.completion();
        while out.spare() > 0
            && let Some(cqe) = cq.next()
        {
            match cqe.user_data() {
                UD_RECV => {}
                // slots of failed provide are lost to kernel; caller sees error
                UD_PROVIDE if cqe.result() < 0 => {
                    return Err(TransportError::Io {
                        stage: "io_uring provide buffers",
                        error: io::Error::from_raw_os_error(-cqe.result()),
                    });
                }
                _ => continue,
            }
            let completion = classify(cqe.result(), cqe.flags(), self.slot_size, multishot);
            self.fleet.complete(completion);
            match completion {
                Completion::Data { slot, len, .. } => {
                    // SAFETY: completion hands `slot` back, so kernel finished
                    // writing and it is in no ring and no live frame; kernel
                    // returns only ids given out, all below slot count;
                    // `len <= slot_size`, pool stride.
                    out.push(unsafe { self.mem.pool.frame(u32::from(slot), 0, len) });
                    pushed += 1;
                }
                Completion::Truncated { slot, .. } => {
                    self.stats.truncated += 1;
                    self.back.push(u32::from(slot));
                }
                // nothing to deliver; loop reads on, so it never looks idle
                Completion::Empty { .. } => {}
                Completion::NoBuffers => self.stats.no_buffer += 1,
                Completion::Failed(errno) => {
                    return Err(TransportError::Io {
                        stage: "io_uring recv",
                        error: io::Error::from_raw_os_error(errno),
                    });
                }
            }
        }
        Ok(pushed)
    }

    // hand `back` to kernel; slots not handed stay for next reap; any slot
    // handed reopens starved fleet
    fn give_back(&mut self) -> Result<(), TransportError> {
        if self.back.is_empty() {
            return Ok(());
        }
        let mut back = mem::take(&mut self.back);
        let queued = back.len();
        let handed = self.hand_over(&mut back);
        self.fleet.refilled(queued - back.len());
        self.back = back;
        handed
    }

    // ring: one entry each, one publish; legacy: one provide per contiguous
    // run. Handed slots leave `back`
    fn hand_over(&mut self, back: &mut Vec<u32>) -> Result<(), TransportError> {
        let base = self.mem.pool.base();
        let stride = self.mem.pool.stride() as usize;
        if let Some(ring) = &mut self.mem.buf_ring {
            for &slot in &*back {
                let addr = base.wrapping_add(slot as usize * stride).addr() as u64;
                ring.push(addr, self.slot_size, id16(slot as usize));
            }
            ring.publish();
            back.clear();
            return Ok(());
        }
        provide_runs(back, |first, run| {
            let entry = opcode::ProvideBuffers::new(
                base.wrapping_add(first as usize * stride),
                // config caps slot size at `i32::MAX`
                self.slot_size.cast_signed(),
                id16(run),
                BGID,
                id16(first as usize),
            )
            .build()
            .user_data(UD_PROVIDE);
            let entry = if self.skip_success {
                entry.flags(squeue::Flags::SKIP_SUCCESS)
            } else {
                entry
            };
            self.push(&entry)
        })
    }

    fn arm(&mut self) -> Result<(), TransportError> {
        let wanted = self.fleet.wanted();
        if wanted == 0 {
            return Ok(());
        }
        let fd = types::Fd(self.sock.as_raw_fd());
        // MSG_TRUNC: result is datagram's real length, so cut ones show
        let entry = match self.path {
            RecvPath::Multishot => opcode::RecvMulti::new(fd, BGID)
                .flags(libc::MSG_TRUNC)
                .build(),
            RecvPath::Legacy | RecvPath::BufRing => {
                opcode::Recv::new(fd, ptr::null_mut(), self.slot_size)
                    .buf_group(BGID)
                    .flags(libc::MSG_TRUNC)
                    .build()
                    .flags(squeue::Flags::BUFFER_SELECT)
            }
        }
        .user_data(UD_RECV);
        for _ in 0..wanted {
            self.push(&entry)?;
            self.fleet.armed += 1;
        }
        Ok(())
    }

    fn push(&mut self, entry: &squeue::Entry) -> Result<(), TransportError> {
        // SAFETY: entries name only pool region and socket, both kept alive
        // until `Drop` proves no request armed (else region leaked).
        if unsafe { self.ring.submission().push(entry) }.is_ok() {
            return Ok(());
        }
        // queue full: submit to make room
        self.enter()?;
        // SAFETY: as above
        unsafe { self.ring.submission().push(entry) }.map_err(|_| TransportError::Io {
            stage: "io_uring submission queue",
            error: io::ErrorKind::WouldBlock.into(),
        })
    }

    fn submit_if_needed(&mut self) -> Result<(), TransportError> {
        let sq = self.ring.submission();
        // overflowed completions stay in kernel backlog until next enter
        if sq.is_empty() && !sq.cq_overflow() {
            return Ok(());
        }
        drop(sq);
        self.enter()
    }

    fn enter(&mut self) -> Result<(), TransportError> {
        self.stats.syscalls += 1;
        match self.ring.submit() {
            Ok(_) => Ok(()),
            // busy or interrupted: entries stay queued, next reap submits them
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::EAGAIN | libc::EBUSY | libc::EINTR)
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(TransportError::Io {
                stage: "io_uring_enter",
                error,
            }),
        }
    }

    // cancel armed recvs until none is left, bounded; true once none armed
    fn cancel_recvs(&mut self) -> bool {
        let cancel = opcode::AsyncCancel::new(UD_RECV)
            .build()
            .user_data(UD_CANCEL);
        let multishot = self.path == RecvPath::Multishot;
        let deadline = Instant::now() + CANCEL_WAIT;
        while self.fleet.armed > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            // one cancel per enter: cancelled request leaves kernel lookup
            // only after enter returns, so batched cancels hit same request
            if self.push(&cancel).is_err() {
                return false;
            }
            let ts = types::Timespec::from(left);
            let args = types::SubmitArgs::new().timespec(&ts);
            match self.ring.submitter().submit_with_args(1, &args) {
                Ok(_) => {}
                Err(e) if matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR)) => {}
                Err(_) => return false,
            }
            for cqe in self.ring.completion() {
                if cqe.user_data() == UD_RECV {
                    let completion = classify(cqe.result(), cqe.flags(), self.slot_size, multishot);
                    self.fleet.complete(completion);
                }
            }
        }
        true
    }
}

impl Driver for UringDriver {
    type Frame = IndexFrame;
    type Layer = L4;
    const BACKEND: &'static str = BACKEND;

    #[inline]
    fn reap(&mut self, out: &mut FrameBatch<IndexFrame>) -> Result<Reap, TransportError> {
        self.mem.pool.drain_freed(&mut self.back);
        let completed = self.complete(out);
        let refilled = self
            .give_back()
            .and_then(|()| self.arm())
            .and_then(|()| self.submit_if_needed());
        // recv error wins: failed refill leaves its work queued, so it recurs next reap
        let pushed = completed?;
        refilled?;
        Ok(if pushed > 0 {
            Reap::Frames(pushed)
        } else if self.fleet.starved {
            Reap::Exhausted
        } else {
            Reap::Idle
        })
    }

    fn stats(&self) -> DriverStats {
        self.stats
    }

    fn pool_stats(&self) -> PoolStats {
        self.mem.pool.stats()
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        let quiet = self.cancel_recvs();
        // SAFETY: dropped once, here; nothing touches ring afterwards
        unsafe { ManuallyDrop::drop(&mut self.ring) };
        if quiet {
            // SAFETY: no recv armed and ring closed: kernel has no path left
            // into region or buffer ring. Dropped once, here.
            unsafe { ManuallyDrop::drop(&mut self.mem) };
        } else {
            // ring teardown is asynchronous: freeing now could let kernel
            // write into reused memory, so region stays leaked
            tracing::warn!(
                backend = BACKEND,
                armed = self.fleet.armed,
                "recv cancellation not confirmed; receive memory leaked"
            );
        }
    }
}

impl fmt::Debug for UringDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringDriver")
            .field("path", &self.path)
            .field("fleet", &self.fleet)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

// blocking socket: only io_uring reads it, never plain recv
fn socket(cfg: &IoUringConfig) -> Result<Socket, TransportError> {
    let sock = Socket::new(
        Domain::for_address(cfg.bind),
        Type::DGRAM,
        Some(Protocol::UDP),
    )
    .map_err(io_error("socket"))?;
    if let Some(bytes) = cfg.recv_buf {
        sock.set_recv_buffer_size(bytes.get() as usize)
            .map_err(io_error("setsockopt(SO_RCVBUF)"))?;
    }
    sock.bind(&cfg.bind.into())
        .map_err(|error| TransportError::Bind {
            addr: cfg.bind,
            error,
        })?;
    Ok(sock)
}

// sort `back`, hand each contiguous run to `provide(first, len)`; handed slots
// leave `back`, so on error rest stay for next reap instead of leaking
fn provide_runs(
    back: &mut Vec<u32>,
    mut provide: impl FnMut(u32, usize) -> Result<(), TransportError>,
) -> Result<(), TransportError> {
    back.sort_unstable();
    let mut handed = 0;
    while let Some(&first) = back.get(handed) {
        let run = back[handed..]
            .iter()
            .zip(first..)
            .take_while(|&(&slot, next)| slot == next)
            .count();
        if let Err(error) = provide(first, run) {
            back.drain(..handed);
            return Err(error);
        }
        handed += run;
    }
    back.clear();
    Ok(())
}

// buffer id or count field; config caps slots at 32768, so every value fits
#[expect(
    clippy::cast_possible_truncation,
    reason = "slots capped at 32768 by config"
)]
fn id16(v: usize) -> u16 {
    v as u16
}

#[cfg(test)]
mod tests {
    use std::io;

    use transport_core::TransportError;

    use super::{Completion, Fleet, provide_runs};

    const DATA_ENDED: Completion = Completion::Data {
        slot: 0,
        len: 8,
        rearm: true,
    };

    #[test]
    fn starved_fleet_arms_nothing_until_slot_goes_back() {
        let mut fleet = Fleet::new(4);
        fleet.armed = 4;
        fleet.complete(DATA_ENDED);
        assert_eq!(fleet.wanted(), 1, "ended recv not re-armed");
        fleet.complete(Completion::NoBuffers);
        assert_eq!(fleet.wanted(), 0, "starved fleet re-armed");
        fleet.complete(DATA_ENDED);
        assert_eq!(fleet.wanted(), 0, "starved fleet re-armed after data");
        fleet.refilled(0);
        assert_eq!(fleet.wanted(), 0, "empty recycle reopened gate");
        fleet.refilled(1);
        assert_eq!(fleet.wanted(), 3, "recycle did not reopen gate");
    }

    #[test]
    fn fleet_rearms_only_when_request_ends() {
        let mut fleet = Fleet::new(1);
        fleet.armed = 1;
        fleet.complete(Completion::Data {
            slot: 0,
            len: 8,
            rearm: false,
        });
        assert_eq!(fleet.wanted(), 0, "multishot with F_MORE re-armed");
        fleet.complete(Completion::Truncated {
            slot: 1,
            rearm: true,
        });
        assert_eq!(fleet.wanted(), 1, "ended multishot not re-armed");
        fleet.armed = 1;
        fleet.complete(Completion::Failed(libc::ECONNREFUSED));
        assert_eq!(fleet.wanted(), 1, "failed recv not re-armed");
        fleet.armed = 1;
        fleet.complete(Completion::Empty { rearm: false });
        assert_eq!(
            fleet.wanted(),
            0,
            "empty multishot completion with F_MORE re-armed"
        );
        fleet.complete(Completion::Empty { rearm: true });
        assert_eq!(
            fleet.wanted(),
            1,
            "recv ended by empty datagram not re-armed"
        );
    }

    #[test]
    fn failed_provide_keeps_unhanded_slots_for_next_reap() {
        let mut back = vec![9, 2, 5, 1, 3];
        let mut runs = Vec::new();
        let got = provide_runs(&mut back, |first, run| {
            runs.push((first, run));
            if runs.len() == 2 {
                return Err(TransportError::Io {
                    stage: "io_uring submission queue",
                    error: io::ErrorKind::WouldBlock.into(),
                });
            }
            Ok(())
        });
        assert!(got.is_err(), "provide error swallowed");
        assert_eq!(runs, [(1, 3), (5, 1)], "one provide per contiguous run");
        assert_eq!(back, [5, 9], "unhanded slots dropped or handed ones kept");

        runs.clear();
        provide_runs(&mut back, |first, run| {
            runs.push((first, run));
            Ok(())
        })
        .expect("retry provides rest");
        assert_eq!(runs, [(5, 1), (9, 1)]);
        assert!(back.is_empty(), "handed slots left in back: {back:?}");
    }
}
