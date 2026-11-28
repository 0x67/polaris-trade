//! Source-agnostic ingest boundary. Live transport clients and replay
//! containers both feed the pipeline through one contract, so the engine is
//! identical for live and replayed data.

/// Identifies one logical stream (an ingest channel, or an egress record
/// stream). Egress sequencing keys per-stream counters on this. Transparent
/// over `u16`, so a slice of stream ids crosses the plugin ABI as `uint16_t`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct StreamId(pub u16);

/// One unit of feed data: raw venue bytes plus the capture-boundary timestamp
/// and the stream it arrived on.
pub trait IngestFrame {
    /// Raw venue bytes, ready for the adapter to decode.
    fn payload(&self) -> &[u8];
    /// Capture-boundary receive time, nanoseconds UTC. Becomes `ts_recv`.
    fn ts_recv(&self) -> u64;
    /// Stream this frame arrived on.
    fn stream(&self) -> StreamId;
    /// Venue-carried sequence, verbatim. Default `0` means "not yet threaded";
    /// per-source wiring is additive, existing implementors need no change.
    fn venue_seq(&self) -> u64 {
        0
    }
}

/// Why a batch reap could not proceed. `PoolExhausted` is backpressure, not a
/// failure; `Closed` ends the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestError {
    /// Frame pool is fully in use downstream. Stop reaping, drain the pipeline,
    /// then retry. Mirrors `transport_core::TransportError::PoolExhausted`.
    PoolExhausted { in_use: usize, capacity: usize },
    /// Source ended (peer close, end of container). Reader loop terminates.
    Closed,
}

impl core::fmt::Display for IngestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IngestError::PoolExhausted { in_use, capacity } => {
                write!(f, "frame pool exhausted: {in_use}/{capacity} in use")
            }
            IngestError::Closed => write!(f, "ingest source closed"),
        }
    }
}

impl std::error::Error for IngestError {}

/// A batch-first feed source. One contract for live sockets and replay
/// containers.
///
/// `recv_batch` appends up to `max` frames to `out` and returns how many it
/// appended. Semantics mirror `transport_core::DatagramSource::recv_burst`:
/// `Ok(0)` means nothing was ready (retry later), `Ok(n)` appended `n`, and
/// `Err(IngestError::PoolExhausted)` is the backpressure signal (stop reaping,
/// drain downstream, retry). Peer close or end-of-container surfaces as
/// `Err(IngestError::Closed)`, never `Ok(0)`, so a reader loop terminates.
pub trait IngestSource {
    /// Frame type this source yields.
    type Frame: IngestFrame;

    /// Append up to `max` ready frames to `out`; return the count appended.
    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod testing {
    //! Reference mock source and a recv-contract conformance suite, reusable by
    //! md-replay and adapter crates to verify their `IngestSource` impls.
    //! Mirrors `transport_core::testing::run_datagram_source`.

    use std::collections::VecDeque;

    use super::{IngestError, IngestFrame, IngestSource, StreamId};

    /// A concrete [`IngestFrame`] for tests.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct MockFrame {
        /// Raw bytes.
        pub payload: Vec<u8>,
        /// Receive timestamp.
        pub ts_recv: u64,
        /// Origin stream.
        pub stream: StreamId,
    }

    impl IngestFrame for MockFrame {
        fn payload(&self) -> &[u8] {
            &self.payload
        }
        fn ts_recv(&self) -> u64 {
            self.ts_recv
        }
        fn stream(&self) -> StreamId {
            self.stream
        }
    }

    /// What to preload into a source under test for one conformance case.
    #[derive(Clone, Debug, Default)]
    pub struct ScenarioSpec {
        /// Frames the source should deliver, in order.
        pub frames: Vec<MockFrame>,
        /// When set, the source yields `PoolExhausted` instead of delivering.
        pub inject_exhaustion: bool,
    }

    /// Reference [`IngestSource`]: drains a queue in order, honors `max`, yields
    /// `Ok(0)` when empty, and can inject one `PoolExhausted`.
    pub struct MockSource {
        pending: VecDeque<MockFrame>,
        inject_exhaustion: bool,
    }

    impl MockSource {
        /// Build from a scenario spec.
        pub fn from_scenario(spec: ScenarioSpec) -> Self {
            MockSource {
                pending: spec.frames.into(),
                inject_exhaustion: spec.inject_exhaustion,
            }
        }
    }

    impl IngestSource for MockSource {
        type Frame = MockFrame;

        fn recv_batch(
            &mut self,
            out: &mut Vec<Self::Frame>,
            max: usize,
        ) -> Result<usize, IngestError> {
            if self.inject_exhaustion {
                self.inject_exhaustion = false;
                return Err(IngestError::PoolExhausted {
                    in_use: max,
                    capacity: max,
                });
            }
            let n = max.min(self.pending.len());
            out.extend(self.pending.drain(..n));
            Ok(n)
        }
    }

    /// A recv-contract conformance case.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum IngestCase {
        /// An empty source returns `Ok(0)`, never an error.
        EmptyYieldsZero,
        /// A reap never appends more than `max` frames.
        RespectsMax,
        /// Frames come out in the order they went in, then `Ok(0)`.
        DrainsInOrder,
        /// Backpressure surfaces as `Err`, not `Ok(0)`.
        PoolExhaustedSurfaces,
    }

    /// Outcome of a conformance run.
    pub struct IngestConformanceReport {
        /// Per-case pass (`Ok`) or failure with a reason (`Err`).
        pub results: Vec<(IngestCase, Result<(), String>)>,
    }

    impl IngestConformanceReport {
        /// True when every case passed.
        pub fn all_passed(&self) -> bool {
            self.results.iter().all(|(_, r)| r.is_ok())
        }
    }

    fn frame(byte: u8) -> MockFrame {
        MockFrame {
            payload: vec![byte],
            ts_recv: byte as u64,
            stream: StreamId(0),
        }
    }

    /// Run the recv-contract suite against any `IngestSource`, built per case
    /// from a [`ScenarioSpec`] via `build`. Mirrors `run_datagram_source`.
    pub fn run_ingest_source<S, B>(build: B) -> IngestConformanceReport
    where
        S: IngestSource,
        B: Fn(ScenarioSpec) -> S,
    {
        run_ingest_cases(
            &[
                IngestCase::EmptyYieldsZero,
                IngestCase::RespectsMax,
                IngestCase::DrainsInOrder,
                IngestCase::PoolExhaustedSurfaces,
            ],
            build,
        )
    }

    /// Run only the listed cases, in the given order. For a source with no
    /// frame pool, which never yields `PoolExhausted`.
    pub fn run_ingest_cases<S, B>(cases: &[IngestCase], build: B) -> IngestConformanceReport
    where
        S: IngestSource,
        B: Fn(ScenarioSpec) -> S,
    {
        let results = cases
            .iter()
            .map(|&case| (case, run_case(case, &build)))
            .collect();
        IngestConformanceReport { results }
    }

    fn run_case<S, B>(case: IngestCase, build: &B) -> Result<(), String>
    where
        S: IngestSource,
        B: Fn(ScenarioSpec) -> S,
    {
        match case {
            IngestCase::EmptyYieldsZero => {
                let mut s = build(ScenarioSpec::default());
                let mut out = Vec::new();
                match s.recv_batch(&mut out, 8) {
                    Ok(0) if out.is_empty() => Ok(()),
                    other => Err(format!(
                        "empty source returned {other:?}, out.len()={}",
                        out.len()
                    )),
                }
            }
            IngestCase::RespectsMax => {
                let frames: Vec<_> = (0..10).map(frame).collect();
                let mut s = build(ScenarioSpec {
                    frames,
                    inject_exhaustion: false,
                });
                let mut out = Vec::new();
                match s.recv_batch(&mut out, 4) {
                    Ok(n) if n <= 4 && out.len() == n => Ok(()),
                    other => Err(format!(
                        "max=4 reap returned {other:?}, out.len()={}",
                        out.len()
                    )),
                }
            }
            IngestCase::DrainsInOrder => {
                let frames: Vec<_> = (0..3).map(frame).collect();
                let mut s = build(ScenarioSpec {
                    frames,
                    inject_exhaustion: false,
                });
                let mut out = Vec::new();
                while let Ok(n) = s.recv_batch(&mut out, 8) {
                    if n == 0 {
                        break;
                    }
                }
                let seen: Vec<u8> = out.iter().map(|f| f.payload()[0]).collect();
                if seen == vec![0u8, 1, 2] {
                    Ok(())
                } else {
                    Err(format!("drained out of order: {seen:?}"))
                }
            }
            IngestCase::PoolExhaustedSurfaces => {
                let mut s = build(ScenarioSpec {
                    frames: Vec::new(),
                    inject_exhaustion: true,
                });
                let mut out = Vec::new();
                match s.recv_batch(&mut out, 8) {
                    Err(IngestError::PoolExhausted { .. }) => Ok(()),
                    other => Err(format!("expected PoolExhausted, got {other:?}")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{MockSource, run_ingest_source};

    // The reference mock source satisfies the full recv contract: empty->Ok(0),
    // honors max, drains in order, and surfaces backpressure as an error.
    #[test]
    fn conformance_suite() {
        let report = run_ingest_source(MockSource::from_scenario);
        assert!(
            report.all_passed(),
            "failures: {:?}",
            report
                .results
                .iter()
                .filter(|(_, r)| r.is_err())
                .collect::<Vec<_>>()
        );
    }
}
