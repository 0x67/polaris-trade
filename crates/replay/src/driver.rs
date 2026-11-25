//! Replay pacing driver. Wraps any `IngestSource` and releases frames at
//! delays computed from `ts_recv` deltas, outside the engine, so pacing
//! never contaminates deterministic replay output.

use std::{cell::Cell, rc::Rc};

use md_pipeline::{EgressClock, IngestError, IngestFrame, IngestSource, VirtualClock};

/// How the driver paces frame release relative to `ts_recv` deltas.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PacingMode {
    /// Release delay equals the raw `ts_recv` delta between frames.
    RealTime,
    /// Release delay is the `ts_recv` delta divided by `scale`. `2.0` plays
    /// back twice as fast (half the delay); `0.5` plays back at half speed.
    Scaled(f64),
    /// No pacing. Frames release back to back.
    Unthrottled,
}

impl PacingMode {
    // delta is clamped to 0 on the first frame (no predecessor) and on any
    // out-of-order ts_recv, so pacing never waits on a negative delta.
    fn delay_ns(&self, last_ts_recv: Option<u64>, ts_recv: u64) -> u64 {
        let delta = match last_ts_recv {
            Some(last) => ts_recv.saturating_sub(last),
            None => 0,
        };
        match self {
            PacingMode::Unthrottled => 0,
            PacingMode::RealTime => delta,
            PacingMode::Scaled(scale) if *scale > 0.0 => (delta as f64 / scale) as u64,
            PacingMode::Scaled(_) => 0,
        }
    }
}

/// One `ts_recv` window paced at `mode`. Half-open: `from_ts` inclusive,
/// `to_ts` exclusive.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PaceWindow {
    /// Window start `ts_recv`, inclusive, nanoseconds.
    pub from_ts: u64,
    /// Window end `ts_recv`, exclusive, nanoseconds.
    pub to_ts: u64,
    /// Mode governing frames whose `ts_recv` falls inside the window.
    pub mode: PacingMode,
}

/// Why [`PacingSchedule::new`] refused construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PacingScheduleError {
    /// Window has `from_ts >= to_ts`.
    #[error("pace window {index} inverted: from_ts {from_ts} >= to_ts {to_ts}")]
    Inverted {
        /// Offending window position.
        index: usize,
        /// Window start.
        from_ts: u64,
        /// Window end.
        to_ts: u64,
    },
    /// Window starts before its predecessor; segments must be sorted.
    #[error("pace window {index} out of order: from_ts {from_ts} before predecessor")]
    Unsorted {
        /// Offending window position.
        index: usize,
        /// Window start.
        from_ts: u64,
    },
    /// Window starts inside its predecessor.
    #[error("pace window {index} overlaps predecessor ending at {prev_to_ts}")]
    Overlap {
        /// Offending window position.
        index: usize,
        /// Predecessor window end.
        prev_to_ts: u64,
    },
}

/// Ordered, non-overlapping `ts_recv` windows over `default_mode`. Gaps
/// between windows fall back to `default_mode`. Pacing only changes release
/// delay, never which frames release: zero frames dropped under any
/// schedule. Skip-to-point-in-time is the one-window case.
#[derive(Clone, Debug, PartialEq)]
pub struct PacingSchedule {
    default_mode: PacingMode,
    segments: Vec<PaceWindow>,
    // forward-only cursor into `segments`, O(1) advance per frame
    active: usize,
}

impl PacingSchedule {
    /// Validate and build: segments sorted by `from_ts`, non-overlapping,
    /// each `from_ts < to_ts`. Fail-fast at construction, mirroring
    /// `md_pipeline::config::VersionedConfig::load`.
    pub fn new(
        default_mode: PacingMode,
        segments: Vec<PaceWindow>,
    ) -> Result<Self, PacingScheduleError> {
        for (index, w) in segments.iter().enumerate() {
            if w.from_ts >= w.to_ts {
                return Err(PacingScheduleError::Inverted {
                    index,
                    from_ts: w.from_ts,
                    to_ts: w.to_ts,
                });
            }
            if index == 0 {
                continue;
            }
            let prev = segments[index - 1];
            if w.from_ts < prev.from_ts {
                return Err(PacingScheduleError::Unsorted {
                    index,
                    from_ts: w.from_ts,
                });
            }
            if w.from_ts < prev.to_ts {
                return Err(PacingScheduleError::Overlap {
                    index,
                    prev_to_ts: prev.to_ts,
                });
            }
        }
        Ok(PacingSchedule {
            default_mode,
            segments,
            active: 0,
        })
    }

    // cursor never rewinds: out-of-order ts_recv behind the active window
    // falls to default_mode instead of re-entering a passed window
    fn mode_for(&mut self, ts_recv: u64) -> PacingMode {
        while self.active < self.segments.len() && ts_recv >= self.segments[self.active].to_ts {
            self.active += 1;
        }
        match self.segments.get(self.active) {
            Some(w) if ts_recv >= w.from_ts => w.mode,
            _ => self.default_mode,
        }
    }
}

/// Pacing policy for a whole replay: one fixed mode, or a windowed schedule.
#[derive(Clone, Debug, PartialEq)]
pub enum Pacing {
    /// One mode for every frame.
    Fixed(PacingMode),
    /// Windowed [`PacingSchedule`] over a default mode.
    Schedule(PacingSchedule),
}

impl Pacing {
    fn delay_ns(&mut self, last_ts_recv: Option<u64>, ts_recv: u64) -> u64 {
        let mode = match self {
            Pacing::Fixed(mode) => *mode,
            Pacing::Schedule(schedule) => schedule.mode_for(ts_recv),
        };
        mode.delay_ns(last_ts_recv, ts_recv)
    }
}

/// Waits a computed inter-frame delay. Production impl sleeps real wall
/// clock; tests inject a recording impl so pacing math is asserted with zero
/// real sleeping.
pub trait Pacer {
    /// Wait `delay_ns` nanoseconds before the next frame releases.
    fn wait(&mut self, delay_ns: u64);
}

/// Production [`Pacer`]: sleeps on the OS thread clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadSleepPacer;

impl Pacer for ThreadSleepPacer {
    fn wait(&mut self, delay_ns: u64) {
        if delay_ns > 0 {
            std::thread::sleep(std::time::Duration::from_nanos(delay_ns));
        }
    }
}

/// Wraps an `IngestSource` and a [`Pacer`], releasing frames at delays
/// computed from consecutive `ts_recv` deltas per [`PacingMode`]. Advances a
/// caller-supplied `VirtualClock` to each frame's `ts_recv` as it releases,
/// so an `EgressSequencer` sharing that clock stamps deterministic replay
/// times.
pub struct ReplayDriver<S: IngestSource, P: Pacer> {
    source: S,
    pacer: P,
    pacing: Pacing,
    last_ts_recv: Option<u64>,
}

impl<S: IngestSource, P: Pacer> ReplayDriver<S, P> {
    /// Wrap `source`, releasing frames through `pacer` per `pacing`.
    pub fn new(source: S, pacer: P, pacing: Pacing) -> Self {
        ReplayDriver {
            source,
            pacer,
            pacing,
            last_ts_recv: None,
        }
    }

    /// Reap up to `max` frames, pacing release between consecutive frames and
    /// advancing `clock` to each frame's `ts_recv` before appending it to
    /// `out`. Return value mirrors the wrapped source's `recv_batch`.
    pub fn recv_paced(
        &mut self,
        out: &mut Vec<S::Frame>,
        max: usize,
        clock: &mut VirtualClock,
    ) -> Result<usize, IngestError> {
        let mut batch = Vec::new();
        let n = self.source.recv_batch(&mut batch, max)?;
        for frame in batch {
            let ts = frame.ts_recv();
            let delay = self.pacing.delay_ns(self.last_ts_recv, ts);
            self.pacer.wait(delay);
            self.last_ts_recv = Some(ts);
            clock.now = ts;
            out.push(frame);
        }
        Ok(n)
    }

    /// Borrow the wrapped pacer, e.g. to inspect recorded delays in tests.
    pub fn pacer(&self) -> &P {
        &self.pacer
    }

    /// Borrow the wrapped source, e.g. to inspect retained state after the
    /// run ends. see [`crate::binary::BinaryFileReplay::io_error`]
    pub fn source(&self) -> &S {
        &self.source
    }
}

/// Replay egress clock: one `Rc<Cell<u64>>` shared between the replay driver
/// (writer, advances to each released frame's `ts_recv`) and the
/// `EgressSequencer` reading it, so replay `ts_egress` is a deterministic
/// function of `ts_recv`, not wall time. Single-threaded by design (one
/// session is one thread); clones share the same cell.
#[derive(Clone, Debug, Default)]
pub struct SharedClock(Rc<Cell<u64>>);

impl SharedClock {
    /// New clock at time zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current virtual time, nanoseconds.
    pub fn get(&self) -> u64 {
        self.0.get()
    }

    /// Advance to `ts`; every clone reads the new value.
    pub fn set(&self, ts: u64) {
        self.0.set(ts);
    }
}

impl EgressClock for SharedClock {
    fn now(&mut self) -> u64 {
        self.0.get()
    }
}

/// Paced replay source: wraps a [`ReplayDriver`], implements `IngestSource`
/// itself (session loops stay untouched), and advances a [`SharedClock`] to
/// each batch's last released `ts_recv` as a side effect of `recv_batch`.
pub struct PacedSource<S: IngestSource, P: Pacer> {
    driver: ReplayDriver<S, P>,
    clock: SharedClock,
    scratch: VirtualClock,
}

impl<S: IngestSource, P: Pacer> PacedSource<S, P> {
    /// Wrap `driver`; `clock` is the same cell the egress sequencer reads.
    pub fn new(driver: ReplayDriver<S, P>, clock: SharedClock) -> Self {
        let scratch = VirtualClock { now: clock.get() };
        PacedSource {
            driver,
            clock,
            scratch,
        }
    }

    /// Borrow the innermost source, e.g. to check
    /// [`crate::binary::BinaryFileReplay::io_error`] after the run.
    pub fn source(&self) -> &S {
        self.driver.source()
    }
}

impl<S: IngestSource, P: Pacer> IngestSource for PacedSource<S, P> {
    type Frame = S::Frame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        let n = self.driver.recv_paced(out, max, &mut self.scratch)?;
        if n > 0 {
            self.clock.set(self.scratch.now);
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use md_pipeline::{
        StreamId, VirtualClock,
        ingest::testing::{MockFrame, MockSource, ScenarioSpec},
    };

    use super::{
        PaceWindow, PacedSource, Pacer, Pacing, PacingMode, PacingSchedule, PacingScheduleError,
        ReplayDriver, SharedClock,
    };

    #[derive(Default)]
    struct RecordingPacer {
        delays: Vec<u64>,
    }

    impl Pacer for RecordingPacer {
        fn wait(&mut self, delay_ns: u64) {
            self.delays.push(delay_ns);
        }
    }

    // deltas between the 3 frames are 2000ns then 1000ns.
    fn frames() -> Vec<MockFrame> {
        vec![
            MockFrame {
                payload: vec![0],
                ts_recv: 1_000,
                stream: StreamId(0),
            },
            MockFrame {
                payload: vec![1],
                ts_recv: 3_000,
                stream: StreamId(0),
            },
            MockFrame {
                payload: vec![2],
                ts_recv: 4_000,
                stream: StreamId(0),
            },
        ]
    }

    fn build_driver(mode: PacingMode) -> ReplayDriver<MockSource, RecordingPacer> {
        driver_with(frames(), Pacing::Fixed(mode))
    }

    fn driver_with(
        frames: Vec<MockFrame>,
        pacing: Pacing,
    ) -> ReplayDriver<MockSource, RecordingPacer> {
        let source = MockSource::from_scenario(ScenarioSpec {
            frames,
            inject_exhaustion: false,
        });
        ReplayDriver::new(source, RecordingPacer::default(), pacing)
    }

    fn schedule_frames(ts: &[u64]) -> Vec<MockFrame> {
        ts.iter()
            .enumerate()
            .map(|(i, &t)| MockFrame {
                payload: vec![i as u8],
                ts_recv: t,
                stream: StreamId(0),
            })
            .collect()
    }

    // Scaled(2.0) halves each delta; the first frame has no predecessor so
    // its delay is always 0 regardless of mode.
    #[test]
    fn scaled_pacing_halves_deltas() {
        let mut driver = build_driver(PacingMode::Scaled(2.0));
        let mut clock = VirtualClock::default();
        let mut out = Vec::new();
        driver.recv_paced(&mut out, 8, &mut clock).unwrap();

        assert_eq!(driver.pacer().delays, vec![0, 1000, 500]);
        assert_eq!(clock.now, 4_000, "clock tracks the last frame's ts_recv");
    }

    // Unthrottled ignores ts_recv deltas entirely; every delay is 0.
    #[test]
    fn unthrottled_pacing_is_zero() {
        let mut driver = build_driver(PacingMode::Unthrottled);
        let mut clock = VirtualClock::default();
        let mut out = Vec::new();
        driver.recv_paced(&mut out, 8, &mut clock).unwrap();

        assert_eq!(driver.pacer().delays, vec![0, 0, 0]);
    }

    // RealTime plays back the exact recorded delta, no scaling.
    #[test]
    fn realtime_pacing_uses_raw_deltas() {
        let mut driver = build_driver(PacingMode::RealTime);
        let mut clock = VirtualClock::default();
        let mut out = Vec::new();
        driver.recv_paced(&mut out, 8, &mut clock).unwrap();

        assert_eq!(driver.pacer().delays, vec![0, 2000, 1000]);
    }

    // Windowed schedule: real-time deltas inside windows, default in gaps,
    // zero frames dropped; bad window lists refuse construction.
    #[test]
    fn pacing_schedule_windows() {
        let window = |from_ts, to_ts| PaceWindow {
            from_ts,
            to_ts,
            mode: PacingMode::RealTime,
        };
        let schedule = PacingSchedule::new(
            PacingMode::Unthrottled,
            vec![
                window(1_000, 2_000),
                window(3_000, 4_000),
                window(6_000, 7_000),
            ],
        )
        .unwrap();
        let ts = [500, 1_200, 1_500, 2_500, 3_500, 5_000, 6_500, 8_000];
        let mut driver = driver_with(schedule_frames(&ts), Pacing::Schedule(schedule));
        let mut clock = VirtualClock::default();
        let mut out = Vec::new();
        driver.recv_paced(&mut out, 16, &mut clock).unwrap();

        assert_eq!(out.len(), ts.len(), "every frame released, zero dropped");
        assert_eq!(
            driver.pacer().delays,
            vec![0, 700, 300, 0, 1_000, 0, 1_500, 0],
            "real-time deltas inside windows, zero delay in gaps"
        );

        let inverted = PacingSchedule::new(PacingMode::Unthrottled, vec![window(2_000, 1_000)]);
        assert!(matches!(
            inverted,
            Err(PacingScheduleError::Inverted { index: 0, .. })
        ));

        let overlapping = PacingSchedule::new(
            PacingMode::Unthrottled,
            vec![window(1_000, 3_000), window(2_000, 4_000)],
        );
        assert!(matches!(
            overlapping,
            Err(PacingScheduleError::Overlap { index: 1, .. })
        ));

        let unsorted = PacingSchedule::new(
            PacingMode::Unthrottled,
            vec![window(3_000, 4_000), window(1_000, 2_000)],
        );
        assert!(matches!(
            unsorted,
            Err(PacingScheduleError::Unsorted { index: 1, .. })
        ));
    }

    // Pacing is a timeskip, never a dataskip: same frames as Unthrottled,
    // and the window cursor never rewinds on out-of-order ts_recv.
    #[test]
    fn schedule_never_drops_frames() {
        let window = |from_ts, to_ts| PaceWindow {
            from_ts,
            to_ts,
            mode: PacingMode::RealTime,
        };
        // 3_500 advances the cursor past window one; 1_500 arrives out of
        // order; 1_800 probes: a rewound cursor would pace it real-time (300).
        let ts = [500, 3_500, 1_500, 1_800];
        let schedule = PacingSchedule::new(
            PacingMode::Unthrottled,
            vec![window(1_000, 2_000), window(3_000, 4_000)],
        )
        .unwrap();
        let mut paced = driver_with(schedule_frames(&ts), Pacing::Schedule(schedule));
        let mut unthrottled =
            driver_with(schedule_frames(&ts), Pacing::Fixed(PacingMode::Unthrottled));
        let mut clock = VirtualClock::default();

        let mut paced_out = Vec::new();
        paced.recv_paced(&mut paced_out, 16, &mut clock).unwrap();
        let mut unthrottled_out = Vec::new();
        unthrottled
            .recv_paced(&mut unthrottled_out, 16, &mut clock)
            .unwrap();

        assert_eq!(
            paced_out.len(),
            unthrottled_out.len(),
            "frame count equals the unthrottled run"
        );
        assert_eq!(paced_out, unthrottled_out, "same frames, same order");
        assert_eq!(
            paced.pacer().delays,
            vec![0, 3_000, 0, 0],
            "out-of-order ts_recv falls to default mode; cursor never rewinds"
        );
    }

    // PacedSource delegates recv_batch through the driver and advances the
    // shared egress clock to the batch's last released ts_recv.
    #[test]
    fn paced_source_advances_shared_clock() {
        use md_pipeline::{EgressClock as _, IngestSource as _};

        let source = MockSource::from_scenario(ScenarioSpec {
            frames: frames(),
            inject_exhaustion: false,
        });
        let driver = ReplayDriver::new(
            source,
            RecordingPacer::default(),
            Pacing::Fixed(PacingMode::Unthrottled),
        );
        let clock = SharedClock::new();
        let mut reader = clock.clone();
        let mut paced = PacedSource::new(driver, clock);
        let mut out = Vec::new();
        let n = paced.recv_batch(&mut out, 8).unwrap();

        assert_eq!(n, 3);
        assert_eq!(
            reader.now(),
            4_000,
            "shared clock reads the last frame's ts_recv"
        );
    }
}
