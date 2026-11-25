//! In-process `OutputSink` implementations. `NullSink` is the binary's
//! default until a same-host/cross-host transport sink lands (deployment
//! tiers differ only in output wiring); `CollectingSink` backs test
//! assertions.

use md_pipeline::{OutputSink, RecordRef, SinkError, StreamId};

/// Discards every record.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullSink;

impl OutputSink for NullSink {
    fn publish(&mut self, _rec: RecordRef<'_>) -> Result<(), SinkError> {
        Ok(())
    }
}

/// One published record, copied out of the borrowed `RecordRef` so it
/// outlives the `publish` call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectedRecord {
    pub stream: StreamId,
    pub rtype: u8,
    pub instrument_id: u32,
    pub venue_seq: u64,
    pub egress_seq: u64,
    pub ts_egress: u64,
    pub body: Vec<u8>,
}

/// Collects every published record in order, for test assertions.
#[derive(Clone, Debug, Default)]
pub struct CollectingSink {
    pub records: Vec<CollectedRecord>,
}

impl OutputSink for CollectingSink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        self.records.push(CollectedRecord {
            stream: rec.stream,
            rtype: rec.header.rtype,
            instrument_id: rec.header.instrument_id,
            venue_seq: rec.header.venue_seq,
            egress_seq: rec.header.egress_seq,
            ts_egress: rec.header.ts_egress,
            body: rec.body.to_vec(),
        });
        Ok(())
    }
}

/// One concrete sink type selected at wiring time from config, matching
/// `crate::tap::AnyTap`'s enum-dispatch pattern: `main.rs` picks one variant
/// instead of monomorphizing a separate `EgressSequencer<S, _>` per sink
/// choice on top of its venue/source matrix. `OutputSink::publish` is
/// non-generic so a boxed trait object would also work, but the design's
/// no-boxed-trait-object rule for the hot path applies here too.
pub enum AnySink {
    Null(NullSink),
    Collecting(CollectingSink),
}

impl OutputSink for AnySink {
    fn publish(&mut self, rec: RecordRef<'_>) -> Result<(), SinkError> {
        match self {
            AnySink::Null(s) => s.publish(rec),
            AnySink::Collecting(s) => s.publish(rec),
        }
    }
}
