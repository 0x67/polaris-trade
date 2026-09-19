//! Datagram cases: payload order, burst bound, exhaustion, reclaim, pool accounting.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    thread,
};

use super::{assert_bytes, pattern, poll_until};
use crate::{DatagramRecv, FrameBatch, PoolStats, TransportError};

// pool for cases that must never exhaust
const ROOMY: NonZeroU32 = NonZeroU32::new(16).unwrap();
const TIGHT: NonZeroU32 = NonZeroU32::new(2).unwrap();
const BURST_ROOM: NonZeroUsize = NonZeroUsize::new(3).unwrap();

/// How backend reports datagram arriving while every buffer is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExhaustionSignal {
    /// `recv_burst` returns [`TransportError::PoolExhausted`]; datagram stays queued.
    PoolExhausted,
    /// `drops` rises; datagram may be lost or delivered once buffers free.
    DropCounter,
}

/// Hooks [`run_datagram`] drives backend through.
pub struct DatagramHarness<T, B, I, D>
where
    B: FnMut(NonZeroU32) -> T,
    I: FnMut(&mut T, &[u8]),
    D: FnMut(&T) -> u64,
{
    /// Fresh transport whose pool holds exactly given number of buffers.
    pub build: B,
    /// Send one datagram at transport from outside; may land later.
    pub inject: I,
    /// Backend's monotonic drop count.
    pub drops: D,
    /// How backend reports exhaustion.
    pub exhaustion: ExhaustionSignal,
}

/// Run every datagram case, each on fresh transport from `h.build`.
///
/// Cases: payload bytes and order, burst bound, exhaustion then reclaim after
/// cross-thread drop, `pool_stats` accounting (module docs list each rule).
///
/// # Panics
///
/// On any contract violation, or when awaited datagram, drop or exhaustion
/// signal does not show within 5 s.
pub fn run_datagram<T, B, I, D>(mut h: DatagramHarness<T, B, I, D>)
where
    T: DatagramRecv,
    B: FnMut(NonZeroU32) -> T,
    I: FnMut(&mut T, &[u8]),
    D: FnMut(&T) -> u64,
{
    h.payload_order();
    h.burst_bound();
    h.exhaust_then_reclaim();
    h.pool_accounting();
}

impl<T, B, I, D> DatagramHarness<T, B, I, D>
where
    T: DatagramRecv,
    B: FnMut(NonZeroU32) -> T,
    I: FnMut(&mut T, &[u8]),
    D: FnMut(&T) -> u64,
{
    fn inject_all(&mut self, t: &mut T, datagrams: &[Vec<u8>]) {
        for d in datagrams {
            (self.inject)(t, d);
        }
    }

    // varied lengths catch stale or stride-sized lengths
    fn payload_order(&mut self) {
        const CASE: &str = "payload order";
        let mut t = (self.build)(ROOMY);
        let mut batch = FrameBatch::with_capacity(BURST_ROOM);
        let idle = recv_checked(&mut t, &mut batch, CASE);
        assert!(
            matches!(idle, Ok(0)),
            "{CASE}: fresh transport returned {idle:?}, want Ok(0)"
        );
        let sent = datagrams(1, &[7, 64, 1, 300, 33, 1200, 2, 90]);
        self.inject_all(&mut t, &sent);
        let got = recv_frames(&mut t, sent.len(), CASE);
        let after = recv_checked(&mut t, &mut batch, CASE);
        assert!(
            matches!(after, Ok(0)),
            "{CASE}: recv_burst after every datagram arrived returned {after:?}, want Ok(0)"
        );
        // frames held through every burst above
        assert_payloads(&got, &sent, CASE);
    }

    fn burst_bound(&mut self) {
        const CASE: &str = "burst bound";
        let mut t = (self.build)(ROOMY);
        let sent = datagrams(101, &[40, 41, 42, 43, 44, 45, 46, 47]);
        let mut batch = FrameBatch::with_capacity(BURST_ROOM);
        self.inject_all(&mut t, &sent[..2]);
        poll_until(CASE, "first two datagrams", || {
            recv_ok(&mut t, &mut batch, CASE);
            (batch.len() >= 2).then_some(())
        });
        assert_eq!(batch.len(), 2, "{CASE}: two datagrams sent, batch holds");
        // six queued, one spare slot: burst must stop at it
        self.inject_all(&mut t, &sent[2..]);
        poll_until(CASE, "datagram into last slot", || {
            (recv_ok(&mut t, &mut batch, CASE) > 0).then_some(())
        });
        let mut got: Vec<T::Frame> = batch.drain().collect();
        while got.len() < sent.len() {
            poll_until(CASE, "remaining datagrams", || {
                (recv_ok(&mut t, &mut batch, CASE) > 0).then_some(())
            });
            got.extend(batch.drain());
        }
        assert_payloads(&got, &sent, CASE);
    }

    fn exhaust_then_reclaim(&mut self) {
        let mut t = (self.build)(TIGHT);
        let extra = pattern(203, 40);
        let held = self.exhaust(&mut t, &extra);
        thread::spawn(move || drop(held))
            .join()
            .unwrap_or_else(|_| panic!("reclaim: dropping frames on another thread panicked"));
        self.reclaim(&mut t, &extra);
    }

    // both buffers held: `extra` meets declared signal, held frames stay intact
    fn exhaust(&mut self, t: &mut T, extra: &[u8]) -> Vec<T::Frame> {
        const CASE: &str = "exhaustion";
        let signal = self.exhaustion;
        let sent = datagrams(201, &[900, 700]);
        self.inject_all(t, &sent);
        let held = recv_frames(t, sent.len(), CASE);
        let drops_before = (self.drops)(t);
        (self.inject)(t, extra);
        let mut batch = FrameBatch::with_capacity(NonZeroUsize::MIN);
        poll_until(CASE, "exhaustion signal", || {
            match recv_checked(t, &mut batch, CASE) {
                Ok(0) => {}
                Ok(n) => panic!("{CASE}: {n} frame(s) delivered while every buffer is held"),
                Err(TransportError::PoolExhausted { in_use, capacity }) => {
                    assert!(
                        in_use == capacity && u32::try_from(capacity) == Ok(TIGHT.get()),
                        "{CASE}: PoolExhausted reports {in_use}/{capacity}, want {TIGHT}/{TIGHT}"
                    );
                    if signal == ExhaustionSignal::PoolExhausted {
                        return Some(());
                    }
                }
                Err(e) => panic!("{CASE}: recv_burst failed: {e}"),
            }
            (signal == ExhaustionSignal::DropCounter && (self.drops)(t) > drops_before)
                .then_some(())
        });
        // buffer reused under live frame shows here
        assert_payloads(&held, &sent, CASE);
        held
    }

    // freed buffers take new datagrams; `extra` comes first when still queued
    fn reclaim(&mut self, t: &mut T, extra: &[u8]) {
        const CASE: &str = "reclaim";
        let extra_pending = match self.exhaustion {
            ExhaustionSignal::PoolExhausted => {
                assert_payloads(&recv_frames(t, 1, CASE), &[extra], CASE);
                false
            }
            // one burst lets drivers recycling on reap take freed buffers back
            ExhaustionSignal::DropCounter => {
                let mut batch = FrameBatch::with_capacity(NonZeroUsize::MIN);
                let late = recv_ok(t, &mut batch, CASE) > 0;
                if late {
                    assert_payloads(&batch.drain().collect::<Vec<_>>(), &[extra], CASE);
                }
                !late
            }
        };
        // shorter than held ones: reused buffers must report new lengths
        let fresh = datagrams(204, &[16, 8]);
        self.inject_all(t, &fresh);
        let mut got = recv_frames(t, fresh.len(), CASE);
        if extra_pending && got[0].as_ref() == extra {
            drop(got.remove(0));
            got.extend(recv_frames(t, 1, CASE));
        }
        assert_payloads(&got, &fresh, CASE);
    }

    fn pool_accounting(&mut self) {
        const CASE: &str = "pool_stats";
        let mut t = (self.build)(ROOMY);
        let before = checked_stats(&t, CASE);
        let sent = datagrams(301, &[100, 200, 300]);
        self.inject_all(&mut t, &sent);
        let held = recv_frames(&mut t, sent.len(), CASE);
        let holding = checked_stats(&t, CASE);
        assert!(
            holding.in_use >= held.len(),
            "{CASE}: in_use {} while caller holds {} frames",
            holding.in_use,
            held.len()
        );
        drop(held);
        if checked_stats(&t, CASE).in_use != before.in_use {
            let mut batch = FrameBatch::with_capacity(NonZeroUsize::MIN);
            let n = recv_ok(&mut t, &mut batch, CASE);
            assert_eq!(n, 0, "{CASE}: frames delivered with nothing sent");
        }
        assert_eq!(
            checked_stats(&t, CASE).in_use,
            before.in_use,
            "{CASE}: in_use after every frame dropped, want value before receive"
        );
    }
}

// one datagram per length, seeds counting up from `first`
fn datagrams(first: u64, lens: &[usize]) -> Vec<Vec<u8>> {
    (first..)
        .zip(lens)
        .map(|(seed, &len)| pattern(seed, len))
        .collect()
}

// one `recv_burst`, checked against what it did to caller's batch
fn recv_checked<T: DatagramRecv>(
    t: &mut T,
    batch: &mut FrameBatch<T::Frame>,
    case: &str,
) -> Result<usize, TransportError> {
    let (len, spare) = (batch.len(), batch.spare());
    let result = t.recv_burst(batch);
    let Some(pushed) = batch.len().checked_sub(len) else {
        panic!("{case}: recv_burst removed frames from caller's batch");
    };
    assert!(
        pushed <= spare,
        "{case}: recv_burst pushed {pushed} frames into {spare} spare slots"
    );
    match &result {
        Ok(n) => assert_eq!(
            *n, pushed,
            "{case}: recv_burst count differs from frames pushed"
        ),
        Err(e) => assert_eq!(
            pushed, 0,
            "{case}: recv_burst pushed frames, then failed: {e}"
        ),
    }
    result
}

fn recv_ok<T: DatagramRecv>(t: &mut T, batch: &mut FrameBatch<T::Frame>, case: &str) -> usize {
    recv_checked(t, batch, case).unwrap_or_else(|e| panic!("{case}: recv_burst failed: {e}"))
}

// exactly `n` frames: each burst gets room for the rest only
fn recv_frames<T: DatagramRecv>(t: &mut T, n: usize, case: &str) -> Vec<T::Frame> {
    let mut got = Vec::with_capacity(n);
    while let Some(room) = NonZeroUsize::new(n - got.len()) {
        let mut batch = FrameBatch::with_capacity(room);
        poll_until(case, "datagram", || {
            (recv_ok(t, &mut batch, case) > 0).then_some(())
        });
        got.extend(batch.drain());
    }
    got
}

fn assert_payloads<F: AsRef<[u8]>, W: AsRef<[u8]>>(got: &[F], want: &[W], case: &str) {
    assert_eq!(got.len(), want.len(), "{case}: frame count");
    for (i, (frame, want)) in got.iter().zip(want).enumerate() {
        assert_bytes(frame.as_ref(), want.as_ref(), case, &format!("frame {i}"));
    }
}

fn checked_stats<T: DatagramRecv>(t: &T, case: &str) -> PoolStats {
    let stats = t.pool_stats();
    assert!(
        u32::try_from(stats.capacity) == Ok(ROOMY.get()) && stats.in_use <= stats.capacity,
        "{case}: {stats:?}, want capacity {ROOMY} and in_use within it"
    );
    stats
}
