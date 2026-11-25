//! Output seam and egress stamping. Records leave the pipeline through
//! `OutputSink`; the `EgressSequencer` is the single place `ts_egress` is read
//! from a clock and the per-stream egress sequence is assigned.

use md_events::RecordHeader;

use crate::ingest::StreamId;

/// A record ready to publish: its stream, header (already egress-stamped), and
/// body bytes. Borrowed so the hot path stays allocation-free and unboxed.
pub struct RecordRef<'a> {
    /// Egress stream this record belongs to.
    pub stream: StreamId,
    /// Record header, with `ts_egress`/`egress_seq` already stamped.
    pub header: &'a RecordHeader,
    /// Record bytes after the header.
    pub body: &'a [u8],
}

/// Why a publish could not proceed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkError {
    /// Sink is backpressured. Per-sink policy: block (embedded) or
    /// drop-with-counter (lossy fan-out); egress_seq continuity is never broken.
    Full,
    /// Sink is closed and will accept no more records.
    Closed,
}

impl core::fmt::Display for SinkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SinkError::Full => write!(f, "output sink full"),
            SinkError::Closed => write!(f, "output sink closed"),
        }
    }
}

impl std::error::Error for SinkError {}

/// Publish boundary for normalized records. Deployment tiers differ only in the
/// sink impl (in-process, same-host, cross-host); the engine is generic over
/// `S: OutputSink` with no boxed trait objects on the hot path.
pub trait OutputSink {
    /// Publish one record. Backpressure and closure surface as [`SinkError`].
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError>;
}

/// Source of `ts_egress`. Real clock for live, virtual clock for replay so
/// everything upstream of egress stays deterministic.
pub trait EgressClock {
    /// Current egress time, nanoseconds UTC.
    fn now(&mut self) -> u64;
}

/// Wall-clock egress time for the live tier.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl EgressClock for SystemClock {
    fn now(&mut self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// Virtual egress clock for replay; the replay driver advances `now`.
#[derive(Clone, Copy, Debug, Default)]
pub struct VirtualClock {
    /// Current virtual time, nanoseconds.
    pub now: u64,
}

impl EgressClock for VirtualClock {
    fn now(&mut self) -> u64 {
        self.now
    }
}

/// Stamps `ts_egress` and the per-stream egress sequence at exactly the output
/// boundary, then forwards to the wrapped sink. Egress sequences are 1-based and
/// gapless within each stream; `ts_egress` is read once per record from the
/// clock, making internal latency measurable and consumer-side gap/dedup work.
pub struct EgressSequencer<S: OutputSink, C: EgressClock> {
    sink: S,
    clock: C,
    /// Per-stream sequence counters indexed directly by the u16 stream id,
    /// grown on demand; a hash lookup here costs two hashes per published
    /// record on the hot path.
    counters: Vec<u64>,
    /// Highest `ts_egress` stamped so far, joint across all streams. Clamps
    /// against a clock that steps backward (NTP correction).
    last_egress: u64,
}

impl<S: OutputSink, C: EgressClock> EgressSequencer<S, C> {
    /// Wrap a sink and clock.
    pub fn new(sink: S, clock: C) -> Self {
        EgressSequencer {
            sink,
            clock,
            counters: Vec::new(),
            last_egress: 0,
        }
    }

    /// Stamp `header` (`ts_egress` clamped monotonic, next per-stream
    /// `egress_seq`) and publish it with `body` to the wrapped sink. The
    /// candidate `egress_seq` commits into the per-stream counter only on
    /// `Ok(())`; a sink error leaves the counter untouched so the caller's
    /// retry of the SAME record reuses the identical sequence, never burning
    /// a gap. `ts_egress` is re-read every attempt (latency measure, not
    /// content identity) and always advances `last_egress` since it was
    /// legitimately stamped on the header regardless of publish outcome.
    pub fn publish(
        &mut self,
        stream: StreamId,
        header: &mut RecordHeader,
        body: &[u8],
    ) -> Result<(), SinkError> {
        header.ts_egress = self.clock.now().max(self.last_egress);
        self.last_egress = header.ts_egress;

        let idx = usize::from(stream.0);
        if idx >= self.counters.len() {
            // growing early is safe: the new slot holds 0 until a publish
            // commits, so a sink error still leaves the counter untouched
            self.counters.resize(idx + 1, 0);
        }
        let next_seq = self.counters[idx] + 1;
        header.egress_seq = next_seq;

        self.sink.publish(RecordRef {
            stream,
            header: &*header,
            body,
        })?;

        self.counters[idx] = next_seq;
        Ok(())
    }

    /// Borrow the wrapped sink.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Mutably borrow the clock (the replay driver advances a virtual clock).
    pub fn clock_mut(&mut self) -> &mut C {
        &mut self.clock
    }

    /// Consume and return the wrapped sink.
    pub fn into_sink(self) -> S {
        self.sink
    }
}

#[cfg(test)]
mod tests {
    use md_events::{RecordFlags, RecordHeader, rtype};

    use super::{EgressClock, EgressSequencer, OutputSink, RecordRef, SinkError, VirtualClock};
    use crate::ingest::StreamId;

    /// Sink that records what it was handed.
    #[derive(Default)]
    struct CollectSink {
        got: Vec<(StreamId, u64, u64)>, // (stream, egress_seq, ts_egress)
    }

    impl OutputSink for CollectSink {
        fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
            self.got
                .push((rec.stream, rec.header.egress_seq, rec.header.ts_egress));
            Ok(())
        }
    }

    fn header() -> RecordHeader {
        RecordHeader {
            length: 0,
            rtype: rtype::MBO,
            venue: 1,
            instrument_id: 5,
            ts_event: 10,
            ts_recv: 11,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        }
    }

    // ts_egress comes from the clock and egress_seq is per-stream, 1-based and
    // gapless; two streams sequence independently.
    #[test]
    fn egress_stamping() {
        let mut seq = EgressSequencer::new(CollectSink::default(), VirtualClock { now: 1000 });
        let (a, b) = (StreamId(1), StreamId(2));

        let mut h1 = header();
        seq.publish(a, &mut h1, &[]).unwrap();
        assert_eq!(h1.egress_seq, 1);
        assert_eq!(h1.ts_egress, 1000);

        seq.clock_mut().now = 1001;
        let mut h2 = header();
        seq.publish(a, &mut h2, &[]).unwrap();
        assert_eq!(h2.egress_seq, 2, "same stream increments");

        let mut h3 = header();
        seq.publish(b, &mut h3, &[]).unwrap();
        assert_eq!(h3.egress_seq, 1, "second stream is independent");
        assert_eq!(h3.ts_egress, 1001);

        let got = &seq.sink().got;
        assert_eq!(got, &[(a, 1, 1000), (a, 2, 1001), (b, 1, 1001)]);
    }

    // The virtual clock is monotonic non-decreasing as the driver advances it.
    #[test]
    fn virtual_clock_advances() {
        let mut c = VirtualClock { now: 5 };
        assert_eq!(c.now(), 5);
        c.now = 9;
        assert_eq!(c.now(), 9);
    }

    /// Sink that fails the Nth call (0-indexed) then succeeds on every later call.
    #[derive(Default)]
    struct FlakySink {
        fail_at: usize,
        calls: usize,
        got: Vec<(StreamId, u64, u64)>,
    }

    impl OutputSink for FlakySink {
        fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
            let call = self.calls;
            self.calls += 1;
            if call == self.fail_at {
                return Err(SinkError::Full);
            }
            self.got
                .push((rec.stream, rec.header.egress_seq, rec.header.ts_egress));
            Ok(())
        }
    }

    // A sink error must not burn the egress_seq position. The candidate seq is
    // stamped on the header even on failure, but only commits into the
    // per-stream counter on Ok; retrying the same record reuses it verbatim.
    #[test]
    fn publish_failure_reuses_seq() {
        let mut seq = EgressSequencer::new(FlakySink::default(), VirtualClock { now: 1000 });
        let stream = StreamId(1);
        let mut h = header();

        let err = seq.publish(stream, &mut h, &[]).unwrap_err();
        assert_eq!(err, SinkError::Full);
        assert_eq!(
            h.egress_seq, 1,
            "candidate seq stamped even on the failed attempt"
        );

        seq.publish(stream, &mut h, &[]).unwrap();
        assert_eq!(
            h.egress_seq, 1,
            "retry reuses the unconsumed sequence, no gap"
        );

        assert_eq!(seq.sink().got, &[(stream, 1, 1000)]);
    }

    // ts_egress must never decrease even when the underlying clock steps
    // backward (NTP correction mid-session).
    #[test]
    fn ts_egress_monotonic_clamp() {
        let mut seq = EgressSequencer::new(CollectSink::default(), VirtualClock { now: 1000 });
        let stream = StreamId(1);

        let mut h1 = header();
        seq.publish(stream, &mut h1, &[]).unwrap();
        assert_eq!(h1.ts_egress, 1000);

        seq.clock_mut().now = 500;
        let mut h2 = header();
        seq.publish(stream, &mut h2, &[]).unwrap();
        assert_eq!(h2.ts_egress, 1000, "clamps to last_egress, never decreases");

        seq.clock_mut().now = 2000;
        let mut h3 = header();
        seq.publish(stream, &mut h3, &[]).unwrap();
        assert_eq!(
            h3.ts_egress, 2000,
            "tracks the clock again once it catches up"
        );
    }
}
