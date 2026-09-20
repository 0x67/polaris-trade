//! Bypass shell over mock driver: conformance on both layers, decap carry-over,
//! error deferral, exhaustion mapping, drop telemetry interval.

mod support;

use std::{
    collections::VecDeque,
    io,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{
    DatagramRecv, FrameBatch, PoolStats, TransportError,
    bypass::{BypassTransport, Driver, DriverStats, L2, L4, MockDriver, Reap},
    decap::{DecapStats, UdpDecap},
    pool::IndexPool,
    testing::conformance::{DatagramHarness, ExhaustionSignal, run_datagram},
};

const STRIDE: NonZeroU32 = NonZeroU32::new(2048).unwrap();
const PORT: u16 = 30_001;
const DECAP_BURST: NonZeroUsize = NonZeroUsize::new(4).unwrap();

fn mock<L>(slots: NonZeroU32) -> MockDriver<L> {
    MockDriver::new(IndexPool::new(slots, STRIDE).unwrap())
}

#[test]
fn l4_shell_passes_datagram_conformance() {
    run_datagram(DatagramHarness {
        build: |slots| BypassTransport::new(mock::<L4>(slots)),
        inject: |t, bytes| t.driver_mut().inject(bytes),
        drops: |t| t.stats().no_buffer,
        exhaustion: ExhaustionSignal::DropCounter,
    });
}

#[test]
fn l2_shell_under_decap_passes_datagram_conformance() {
    run_datagram(DatagramHarness {
        build: |slots| {
            UdpDecap::new(
                BypassTransport::new(mock::<L2>(slots)),
                PORT,
                None,
                DECAP_BURST,
            )
        },
        inject: |t, payload| {
            t.inner_mut()
                .driver_mut()
                .inject(&support::udp_frame(PORT, payload));
        },
        drops: |t| t.inner().stats().no_buffer,
        exhaustion: ExhaustionSignal::DropCounter,
    });
}

#[test]
fn decap_delivers_reap_larger_than_out_across_calls() {
    let shell = BypassTransport::new(mock::<L2>(NonZeroU32::new(16).unwrap()));
    let mut t = UdpDecap::new(shell, PORT, None, DECAP_BURST);
    for i in 0..4 {
        t.inner_mut()
            .driver_mut()
            .inject(&support::udp_frame(PORT, &[i; 3]));
    }
    let mut out = FrameBatch::with_capacity(NonZeroUsize::MIN);
    for i in 0..4 {
        assert_eq!(t.recv_burst(&mut out).unwrap(), 1, "call {i}");
        if i == 0 {
            // one delivered, three waiting inside decap: single inner reap took all four
            assert_eq!(
                t.pool_stats().in_use,
                4,
                "inner reap bounded by burst, not by out"
            );
        }
        let frame = out.drain().next().unwrap();
        assert_eq!(frame.as_ref(), [i; 3], "call {i}");
    }
    assert_eq!(t.recv_burst(&mut out).unwrap(), 0);
    assert_eq!(t.stats(), DecapStats::default(), "nothing filtered");
    assert_eq!(t.inner().stats().no_buffer, 0, "nothing lost at inject");
}

#[test]
fn decap_reaps_again_when_whole_reap_is_filtered() {
    let shell = BypassTransport::new(mock::<L2>(NonZeroU32::new(16).unwrap()));
    let mut t = UdpDecap::new(shell, PORT, None, DECAP_BURST);
    // first inner reap takes only other-port frames; match waits for second
    for i in 0..DECAP_BURST.get() {
        let other = support::udp_frame(PORT + 1, &[u8::try_from(i).unwrap()]);
        t.inner_mut().driver_mut().inject(&other);
    }
    t.inner_mut()
        .driver_mut()
        .inject(&support::udp_frame(PORT, b"match"));

    let mut out = FrameBatch::with_capacity(NonZeroUsize::MIN);
    assert_eq!(t.recv_burst(&mut out).unwrap(), 1, "one call, not idle");
    assert_eq!(out.drain().next().unwrap().as_ref(), b"match");
    assert_eq!(
        t.stats(),
        DecapStats {
            wrong_dst: DECAP_BURST.get() as u64,
            ..DecapStats::default()
        }
    );
}

// frame pushed, if any, then what `reap` returns
type Step = (Option<&'static str>, Result<Reap, TransportError>);

// plays one step per reap, then idles; fixed pool stats
struct Scripted {
    steps: VecDeque<Step>,
    pool: PoolStats,
}

impl Driver for Scripted {
    type Frame = Vec<u8>;
    type Layer = L4;
    const BACKEND: &'static str = "scripted";

    fn reap(&mut self, out: &mut FrameBatch<Vec<u8>>) -> Result<Reap, TransportError> {
        let Some((frame, end)) = self.steps.pop_front() else {
            return Ok(Reap::Idle);
        };
        if let Some(frame) = frame {
            out.push(frame.as_bytes().to_vec());
        }
        end
    }

    fn stats(&self) -> DriverStats {
        DriverStats::default()
    }

    fn pool_stats(&self) -> PoolStats {
        self.pool
    }
}

fn scripted(steps: impl IntoIterator<Item = Step>) -> BypassTransport<Scripted> {
    BypassTransport::new(Scripted {
        steps: steps.into_iter().collect(),
        pool: PoolStats {
            capacity: 4,
            in_use: 4,
        },
    })
}

fn io_error(stage: &'static str) -> TransportError {
    TransportError::Io {
        stage,
        error: io::ErrorKind::BrokenPipe.into(),
    }
}

// drains `out` after each call, returning result and frames it pushed
fn recv(t: &mut BypassTransport<Scripted>) -> (Result<usize, TransportError>, Vec<Vec<u8>>) {
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(4).unwrap());
    let result = t.recv_burst(&mut out);
    (result, out.drain().collect())
}

#[test]
fn driver_error_after_frames_waits_one_call_and_returns_once() {
    let mut t = scripted([
        (Some("a"), Err(io_error("after frames"))),
        (Some("b"), Ok(Reap::Frames(1))),
        (None, Err(io_error("nothing pushed"))),
    ]);

    let (first, frames) = recv(&mut t);
    assert!(matches!(first, Ok(1)), "frames before error: {first:?}");
    assert_eq!(frames, [b"a"]);

    let (second, frames) = recv(&mut t);
    assert!(
        matches!(
            second,
            Err(TransportError::Io {
                stage: "after frames",
                ..
            })
        ),
        "deferred error: {second:?}"
    );
    assert!(frames.is_empty(), "deferred error returned without reaping");

    let (third, frames) = recv(&mut t);
    assert!(matches!(third, Ok(1)), "reap resumes: {third:?}");
    assert_eq!(frames, [b"b"]);

    let (fourth, _) = recv(&mut t);
    assert!(
        matches!(
            fourth,
            Err(TransportError::Io {
                stage: "nothing pushed",
                ..
            })
        ),
        "error with empty burst returns at once: {fourth:?}"
    );
    let (fifth, _) = recv(&mut t);
    assert!(
        matches!(fifth, Ok(0)),
        "each error returned once: {fifth:?}"
    );
}

#[test]
fn exhausted_is_pool_exhausted_only_when_nothing_pushed() {
    let mut t = scripted([
        (None, Ok(Reap::Exhausted)),
        (Some("a"), Ok(Reap::Exhausted)),
    ]);

    let (empty, _) = recv(&mut t);
    assert!(
        matches!(
            empty,
            Err(TransportError::PoolExhausted {
                in_use: 4,
                capacity: 4
            })
        ),
        "driver pool stats carried: {empty:?}"
    );
    let (with_frame, frames) = recv(&mut t);
    assert!(
        matches!(with_frame, Ok(1)),
        "frames before exhaustion: {with_frame:?}"
    );
    assert_eq!(frames, [b"a"]);
    let (after, _) = recv(&mut t);
    assert!(
        matches!(after, Ok(0)),
        "exhaustion with frames leaves no error behind: {after:?}"
    );
}

#[cfg(feature = "observability")]
mod drop_telemetry {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use transport_core::{bypass::STATS_EVERY, observability_core};

    use super::*;

    const DROPS: &str = r#"transport.recv.drops [("backend", "mock"), ("reason", "no_buffer")]"#;

    // non-zero counter increases since last call, `name [labels] Counter(n)`, sorted
    fn recorded(snapshotter: &Snapshotter) -> Vec<String> {
        let mut series: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(.., value)| *value != DebugValue::Counter(0))
            .map(|(key, _, _, value)| {
                let labels: Vec<_> = key.key().labels().map(|l| (l.key(), l.value())).collect();
                format!("{} {labels:?} {value:?}", key.key().name())
            })
            .collect();
        series.sort_unstable();
        series
    }

    // idle calls taking shell's call count from `from` to `to`
    fn spin(t: &mut BypassTransport<MockDriver<L4>>, from: u32, to: u32) {
        let mut out = FrameBatch::with_capacity(NonZeroUsize::MIN);
        for _ in from..to {
            assert_eq!(t.recv_burst(&mut out).unwrap(), 0);
        }
    }

    #[test]
    fn no_buffer_delta_reported_every_stats_every_calls() {
        observability_core::set_metrics_enabled(true);
        observability_core::refresh_thread_gate();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut t = BypassTransport::new(mock::<L4>(NonZeroU32::new(2).unwrap()));

        metrics::with_local_recorder(&recorder, || {
            // second burst lands behind first frame: only its own bytes count
            let mut out = FrameBatch::with_capacity(NonZeroUsize::new(2).unwrap());
            for bytes in [b"abc".as_slice(), b"de"] {
                t.driver_mut().inject(bytes);
                assert_eq!(t.recv_burst(&mut out).unwrap(), 1);
            }
            // both slots held in `out`: third datagram finds none
            t.driver_mut().inject(b"f");
            spin(&mut t, 2, STATS_EVERY - 1);
            assert_eq!(
                recorded(&snapshotter),
                [
                    r#"transport.recv.bytes [("backend", "mock")] Counter(5)"#,
                    r#"transport.recv.packets [("backend", "mock")] Counter(2)"#,
                ],
                "burst counted once, drops not before interval"
            );
            spin(&mut t, STATS_EVERY - 1, STATS_EVERY);
            assert_eq!(recorded(&snapshotter), [format!("{DROPS} Counter(1)")]);

            // slots still held: both new datagrams find none
            t.driver_mut().inject(b"g");
            t.driver_mut().inject(b"h");
            spin(&mut t, STATS_EVERY, 2 * STATS_EVERY - 1);
            assert!(recorded(&snapshotter).is_empty(), "nothing mid interval");
            spin(&mut t, 2 * STATS_EVERY - 1, 2 * STATS_EVERY);
            assert_eq!(
                recorded(&snapshotter),
                [format!("{DROPS} Counter(2)")],
                "second interval reports delta, not total"
            );
        });
    }
}
