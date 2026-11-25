//! The suite against a stub adapter. Clean, the stub passes every case; with
//! one injected defect, exactly the cases guarding the broken rule fail. The
//! stub also proves the erased path reaches its session-anchor hook.

use core::mem::size_of;
use std::collections::{BTreeMap, BTreeSet};

use md_events::{Record, RecordFlags, RecordHeader, Side, Trade, rtype};

use super::{
    AdapterCase, ContractFrames, SampleFrame, assert_adapter_contract, run_adapter_contract,
};
use crate::{
    adapter::{
        AdapterStats, ClosureModel, DynAdapter, MarketEvent, RollModel, SessionModel, VenueAdapter,
    },
    coverage::{Coverage, CoverageMatrix},
    ingest::StreamId,
};

/// The stub's two streams. Ids start at 1 so stream 0, the lowest id the
/// suite probes as undeclared, is one the stub ignores.
const STREAMS: [StreamId; 2] = [StreamId(1), StreamId(2)];

/// Stream, kind, and body length of every kind the stub decodes. `D` decodes
/// on both streams, so the coverage case must skip a kind the other stream
/// also marks.
const KINDS: [(StreamId, u8, usize); 5] = [
    (STREAMS[0], b'A', 1),
    (STREAMS[0], b'B', 2),
    (STREAMS[0], b'D', 1),
    (STREAMS[1], b'C', 1),
    (STREAMS[1], b'D', 1),
];

/// Faults a [`Stub`] can inject, one contract rule broken per value.
pub(super) mod defect {
    /// The stub honors the whole contract.
    pub const CLEAN: u8 = 0;
    /// A drain with nothing pending hands back the last drained event again.
    pub const STALE_REDRAIN: u8 = 1;
    /// `decoded` counts emitted events instead of frames.
    pub const DECODED_PER_EVENT: u8 = 2;
    /// A drain zeroes the counters.
    pub const DRAIN_RESETS_STATS: u8 = 3;
    /// An unknown kind panics.
    pub const PANIC_ON_UNKNOWN: u8 = 4;
    /// A known kind with the wrong length counts as unknown, not malformed.
    pub const SHORT_AS_UNKNOWN: u8 = 5;
    /// Coverage omits kind `B`, which `feed` decodes.
    pub const COVERAGE_OMITS_B: u8 = 6;
    /// `streams()` declares nothing, though `feed` decodes [`super::STREAMS`].
    pub const NO_STREAMS: u8 = 7;
    /// `feed` decodes frames on any undeclared stream as if they arrived on
    /// the first stream.
    pub const ANY_STREAM: u8 = 8;
    /// Every feed bumps the `qty` of trades still waiting for a drain.
    pub const PATCHES_PENDING: u8 = 9;
    /// A drain zeroes `unknown_kind` and `malformed`, leaving `decoded`.
    pub const DRAIN_RESETS_ERRORS: u8 = 10;
    /// An unknown kind zeroes `decoded` as it counts itself.
    pub const UNKNOWN_RESETS_DECODED: u8 = 11;
    /// A malformed frame zeroes `decoded` as it counts itself.
    pub const MALFORMED_RESETS_DECODED: u8 = 12;
    /// An unknown kind is dropped without being counted.
    pub const UNKNOWN_UNCOUNTED: u8 = 13;
    /// Kind `B` takes a cold arm: it moves `decoded` and `unknown_kind` and
    /// emits nothing, while coverage still marks it decoded.
    pub const COLD_ARM_B: u8 = 14;
    /// A frame the stub drops (undeclared stream, unknown kind, bad length)
    /// still emits a trade.
    pub const DROPPED_EMITS: u8 = 15;
    /// A frame on an undeclared stream zeroes the counters.
    pub const UNDECLARED_RESETS_STATS: u8 = 16;
    /// A frame on an undeclared stream discards the pending events.
    pub const UNDECLARED_CLEARS_PENDING: u8 = 17;
    /// An unknown kind discards the pending events.
    pub const UNKNOWN_CLEARS_PENDING: u8 = 18;
    /// A malformed frame discards the pending events.
    pub const MALFORMED_CLEARS_PENDING: u8 = 19;
    /// An unknown kind on the second stream is dropped without being counted.
    pub const SECOND_STREAM_UNKNOWN_UNCOUNTED: u8 = 20;
    /// A drain adds one to `malformed`.
    pub const DRAIN_BUMPS_MALFORMED: u8 = 21;
    /// A truncated frame keeps its first body byte and adds it to the price
    /// of every later trade, like a field read before the length check.
    pub const TRUNCATED_LEAKS_STATE: u8 = 22;
    /// The second stream also decodes kind `A`, which its coverage omits.
    pub const CROSS_STREAM_KIND: u8 = 23;
    /// Frames on the highest stream id decode as if they arrived on the
    /// first stream.
    pub const UNDECLARED_MAX_DECODES: u8 = 24;
    /// Kind `B` takes a cold arm that moves `decoded` and `malformed` and
    /// emits nothing.
    pub const COLD_ARM_B_MALFORMED: u8 = 25;
    /// Kind `C` emits its trade without counting it in `decoded`.
    pub const UNCOUNTED_C: u8 = 26;
    /// Kind `B` on an undeclared stream decodes as if it arrived on the first
    /// stream.
    pub const UNDECLARED_DECODES_B: u8 = 27;
    /// A truncated `C` is dropped without being counted.
    pub const TRUNCATED_C_UNCOUNTED: u8 = 28;
    /// A kind the other stream decodes is dropped here without being counted.
    pub const FOREIGN_UNCOUNTED: u8 = 29;
    /// A kind the other stream decodes discards the pending events here.
    pub const FOREIGN_CLEARS_PENDING: u8 = 30;
    /// Frames on stream 0 decode as if they arrived on the first stream.
    pub const UNDECLARED_MIN_DECODES: u8 = 31;
    /// A truncated frame keeps its first body byte and adds it to the
    /// `instrument_id` of every later trade.
    pub const HEADER_LEAK_INSTRUMENT: u8 = 32;
    /// A malformed frame marks the next feed, whatever it is, and a trade that
    /// feed emits gets its `trade_ref` shifted.
    pub const MALFORMED_CARRY_ANY_FEED: u8 = 33;
    /// Trades carry `length` 0, so a reader rejects them.
    pub const BAD_LENGTH: u8 = 34;
    /// Trades carry rtype 0.
    pub const BAD_RTYPE: u8 = 35;
    /// Trades carry `ts_recv` 0.
    pub const TS_RECV_ZEROED: u8 = 36;
    /// Trades carry the stub's own count instead of the wire `venue_seq`.
    pub const OWN_VENUE_SEQ: u8 = 37;
    /// The kind byte is read before the length check, so an empty frame panics.
    pub const EMPTY_PANICS: u8 = 38;
    /// After any dropped frame, every later trade carries `GAP_SUSPECT`.
    pub const STICKY_GAP: u8 = 39;
    /// A `venue_seq` gap carried by a frame the stub drops is forgotten with
    /// that frame, as when the flag is recomputed on every feed.
    pub const GAP_LOST_ON_DROP: u8 = 40;
    /// Both streams share one pending gap, so a gap on one flags the next
    /// trades of whichever stream emits first.
    pub const GAP_CROSSES_STREAMS: u8 = 41;
    /// A pending gap flags only the first trade of the frame that consumes it.
    pub const GAP_FLAGS_FIRST_ONLY: u8 = 42;
    /// `record_types()` also declares `Stats`, which the stub never emits.
    pub const RECORD_TYPES_EXTRA: u8 = 43;
    /// `record_types()` declares nothing, though the stub emits trades.
    pub const RECORD_TYPES_EMPTY: u8 = 44;
}

/// Venue on [`STREAMS`] decoding the [`KINDS`]; every body byte becomes one
/// trade, its `ts_event` the session anchor plus its `ts_recv`, as an ITCH
/// adapter adds the wire offset. A `ts_recv` that does not rise on a declared
/// stream counts as malformed. A `venue_seq` gap there stays pending, even
/// across dropped frames, until the stream's next emitting frame flags its
/// trades `GAP_SUSPECT`. `D` picks a [`defect`].
pub(super) struct Stub<const D: u8> {
    out: Vec<MarketEvent>,
    last_drained: Option<MarketEvent>,
    stats: AdapterStats,
    last_ts_recv: u64,
    last_seq: BTreeMap<StreamId, u64>,
    gaps: BTreeSet<StreamId>,
    leaked: i64,
    carried: u64,
    emitted: u64,
    dropped_any: bool,
    anchor: u64,
}

impl<const D: u8> VenueAdapter for Stub<D> {
    type Config = ();

    fn new(_config: ()) -> Self {
        Stub {
            out: Vec::new(),
            last_drained: None,
            stats: AdapterStats::default(),
            last_ts_recv: 0,
            last_seq: BTreeMap::new(),
            gaps: BTreeSet::new(),
            leaked: 0,
            carried: 0,
            emitted: 0,
            dropped_any: false,
            anchor: 0,
        }
    }

    fn streams() -> &'static [StreamId] {
        if D == defect::NO_STREAMS {
            &[]
        } else {
            &STREAMS
        }
    }

    fn coverage(stream: StreamId) -> CoverageMatrix {
        let decoded = Coverage {
            decoded: true,
            ..Coverage::default()
        };
        let mut matrix = CoverageMatrix::new();
        for &(on, kind, _) in &KINDS {
            if on == stream && !(D == defect::COVERAGE_OMITS_B && kind == b'B') {
                matrix.set(kind, decoded);
            }
        }
        matrix
    }

    fn record_types() -> &'static [u8] {
        match D {
            defect::RECORD_TYPES_EXTRA => &[rtype::TRADE, rtype::MBO],
            defect::RECORD_TYPES_EMPTY => &[],
            _ => &[rtype::TRADE],
        }
    }

    fn session(&self) -> SessionModel {
        SessionModel {
            roll: RollModel::Continuous,
            closure: ClosureModel::None,
        }
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        let pending = self.out.len();
        let carried = core::mem::take(&mut self.carried);
        self.decode(stream, payload, ts_recv, venue_seq);
        if D == defect::DROPPED_EMITS && self.out.len() == pending {
            self.out.push(trade(0, ts_recv, venue_seq));
        }
        let emitted = self.out.len() > pending;
        let gap =
            (emitted || D == defect::GAP_LOST_ON_DROP) && self.gaps.remove(&Self::gap_slot(stream));
        for (n, event) in self.out[pending..].iter_mut().enumerate() {
            if let MarketEvent::Trade(trade) = event {
                self.emitted += 1;
                trade.trade_ref += carried;
                trade.hdr.ts_event += self.anchor;
                if gap && !(D == defect::GAP_FLAGS_FIRST_ONLY && n > 0) {
                    trade.hdr.flags = RecordFlags::GAP_SUSPECT;
                }
                match D {
                    defect::BAD_LENGTH => trade.hdr.length = 0,
                    defect::BAD_RTYPE => trade.hdr.rtype = 0,
                    defect::TS_RECV_ZEROED => trade.hdr.ts_recv = 0,
                    defect::OWN_VENUE_SEQ => trade.hdr.venue_seq = self.emitted,
                    defect::STICKY_GAP if self.dropped_any => {
                        trade.hdr.flags = RecordFlags::GAP_SUSPECT;
                    }
                    _ => {}
                }
            }
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = MarketEvent> + '_ {
        if D == defect::DRAIN_RESETS_STATS {
            self.stats = AdapterStats::default();
        }
        if D == defect::DRAIN_RESETS_ERRORS {
            self.stats.unknown_kind = 0;
            self.stats.malformed = 0;
        }
        if D == defect::DRAIN_BUMPS_MALFORMED {
            self.stats.malformed += 1;
        }
        if D == defect::STALE_REDRAIN && self.out.is_empty() {
            self.out.extend(self.last_drained);
        }
        self.last_drained = self.out.last().copied();
        self.out.drain(..)
    }

    fn stats(&self) -> &AdapterStats {
        &self.stats
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        self.anchor = anchor_ns;
    }
}

impl<const D: u8> Stub<D> {
    /// The declared stream whose kinds a frame decodes as, or `None` when the
    /// stub drops the frame's stream.
    fn route(stream: StreamId, payload: &[u8]) -> Option<StreamId> {
        if STREAMS.contains(&stream) {
            return Some(stream);
        }
        let misrouted = D == defect::ANY_STREAM
            || (D == defect::UNDECLARED_MAX_DECODES && stream == StreamId(u16::MAX))
            || (D == defect::UNDECLARED_MIN_DECODES && stream == StreamId(0))
            || (D == defect::UNDECLARED_DECODES_B && payload.first() == Some(&b'B'));
        misrouted.then_some(STREAMS[0])
    }

    /// Where a gap on `stream` waits for an emitting frame.
    fn gap_slot(stream: StreamId) -> StreamId {
        if D == defect::GAP_CROSSES_STREAMS {
            STREAMS[0]
        } else {
            stream
        }
    }

    fn decode(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if D == defect::EMPTY_PANICS && payload[0] == 0 {
            return;
        }
        let Some(home) = Self::route(stream, payload) else {
            self.dropped_any = true;
            if D == defect::UNDECLARED_RESETS_STATS {
                self.stats = AdapterStats::default();
            }
            if D == defect::UNDECLARED_CLEARS_PENDING {
                self.out.clear();
            }
            return;
        };
        if STREAMS.contains(&stream) {
            let last_seq = self.last_seq.insert(stream, venue_seq).unwrap_or(0);
            if last_seq != 0 && venue_seq != last_seq + 1 {
                self.gaps.insert(Self::gap_slot(stream));
            }
            let last_ts_recv = core::mem::replace(&mut self.last_ts_recv, ts_recv);
            if ts_recv <= last_ts_recv {
                self.count_malformed();
                return;
            }
        }
        if D == defect::PATCHES_PENDING {
            for event in &mut self.out {
                if let MarketEvent::Trade(trade) = event {
                    trade.qty += 1;
                }
            }
        }
        let Some((&kind, body)) = payload.split_first() else {
            self.count_malformed();
            return;
        };
        let Some(&(_, _, body_len)) = KINDS.iter().find(|&&(on, k, _)| {
            k == kind && (on == home || (D == defect::CROSS_STREAM_KIND && k == b'A'))
        }) else {
            self.count_unknown(stream, kind);
            return;
        };
        if body.len() != body_len {
            if D == defect::TRUNCATED_LEAKS_STATE || D == defect::HEADER_LEAK_INSTRUMENT {
                self.leaked = body.first().map_or(0, |&b| i64::from(b));
            }
            if D == defect::TRUNCATED_C_UNCOUNTED && kind == b'C' {
                return;
            }
            if D == defect::SHORT_AS_UNKNOWN {
                self.stats.unknown_kind += 1;
            } else {
                self.count_malformed();
            }
            return;
        }
        self.stats.decoded += if D == defect::DECODED_PER_EVENT {
            body_len as u64
        } else if D == defect::UNCOUNTED_C && kind == b'C' {
            0
        } else {
            1
        };
        if D == defect::COLD_ARM_B && kind == b'B' {
            self.stats.unknown_kind += 1;
            return;
        }
        if D == defect::COLD_ARM_B_MALFORMED && kind == b'B' {
            self.stats.malformed += 1;
            return;
        }
        let leaked = self.leaked;
        self.out.extend(body.iter().map(|&b| {
            let mut event = trade(b, ts_recv, venue_seq);
            if let MarketEvent::Trade(trade) = &mut event {
                if D == defect::HEADER_LEAK_INSTRUMENT {
                    trade.hdr.instrument_id += u32::try_from(leaked).unwrap_or(0);
                } else {
                    trade.price += leaked;
                }
            }
            event
        }));
    }

    fn count_unknown(&mut self, stream: StreamId, kind: u8) {
        if D == defect::PANIC_ON_UNKNOWN {
            panic!("stub has no arm for kind {kind:#04x}");
        }
        let foreign = KINDS.iter().any(|&(_, k, _)| k == kind);
        if D == defect::UNKNOWN_UNCOUNTED
            || (D == defect::SECOND_STREAM_UNKNOWN_UNCOUNTED && stream == STREAMS[1])
            || (D == defect::FOREIGN_UNCOUNTED && foreign)
        {
            return;
        }
        if D == defect::FOREIGN_CLEARS_PENDING && foreign {
            self.out.clear();
        }
        self.dropped_any = true;
        self.stats.unknown_kind += 1;
        if D == defect::UNKNOWN_RESETS_DECODED {
            self.stats.decoded = 0;
        }
        if D == defect::UNKNOWN_CLEARS_PENDING {
            self.out.clear();
        }
    }

    fn count_malformed(&mut self) {
        self.dropped_any = true;
        self.stats.malformed += 1;
        if D == defect::MALFORMED_CARRY_ANY_FEED {
            self.carried = 1;
        }
        if D == defect::MALFORMED_RESETS_DECODED {
            self.stats.decoded = 0;
        }
        if D == defect::MALFORMED_CLEARS_PENDING {
            self.out.clear();
        }
    }
}

fn trade(trade_ref: u8, ts_recv: u64, venue_seq: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        hdr: RecordHeader {
            length: u8::try_from(size_of::<Trade>() / 4).expect("trade fits the length byte"),
            rtype: Trade::RTYPE,
            venue: 1,
            instrument_id: 1,
            ts_event: ts_recv,
            ts_recv,
            ts_egress: 0,
            venue_seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        },
        price: 1_000_000_000,
        qty: 1,
        trade_ref: u64::from(trade_ref),
        aggressor: Side::None,
        _pad: [0u8; 7],
    })
}

fn frame(stream: StreamId, kind: u8, body: &[u8]) -> SampleFrame {
    let mut payload = vec![kind];
    payload.extend_from_slice(body);
    SampleFrame {
        stream,
        kind,
        payload,
    }
}

/// Every frame emits at least one event, so a drain after each frame never
/// finds the stub empty, and `B` emits two, so a per-event `decoded` shows.
/// `B` leads, so the first frame to emit after a gap case's skip flags two
/// trades. Both streams get decodable and unknown frames, so per-stream
/// coverage and the one-unknown-frame-per-stream rule are exercised.
fn frames() -> ContractFrames {
    let [first, second] = STREAMS;
    ContractFrames {
        decodable: vec![
            frame(first, b'B', &[2, 3]),
            frame(first, b'A', &[1]),
            frame(second, b'C', &[5]),
            frame(first, b'D', &[6]),
            frame(second, b'D', &[7]),
            frame(first, b'A', &[4]),
        ],
        unknown: vec![frame(first, b'Z', &[0]), frame(second, b'Z', &[0])],
    }
}

fn failed<const D: u8>() -> Vec<AdapterCase> {
    run_adapter_contract::<Stub<D>>(&(), &frames())
        .failures()
        .map(|(case, _)| case)
        .collect()
}

#[test]
fn clean_stub_passes() {
    assert_adapter_contract::<Stub<{ defect::CLEAN }>>(&(), &frames());
}

/// Adapter crates call the assert form, so a failed case must reach them as a
/// panic naming that case.
#[test]
#[should_panic(expected = "RecordLifetime")]
fn assert_panics_naming_the_failed_case() {
    assert_adapter_contract::<Stub<{ defect::PATCHES_PENDING }>>(&(), &frames());
}

/// A runner holds the adapter erased, so the anchor it installs at a trade-date
/// roll must reach the venue's own hook and move `ts_event` on later frames.
#[test]
fn erased_session_anchor_reaches_adapter() {
    const ANCHOR: u64 = 1_776_963_600_000_000_000;
    let mut adapter: Box<dyn DynAdapter> = Box::new(Stub::<{ defect::CLEAN }>::new(()));
    adapter.feed(STREAMS[0], &[b'A', 1], 5, 1);
    adapter.set_session_anchor(ANCHOR);
    adapter.feed(STREAMS[0], &[b'A', 1], 6, 2);
    let mut out = Vec::new();
    adapter.drain_into(&mut out);
    let ts_event: Vec<u64> = out.iter().map(|event| event.header().ts_event).collect();
    assert_eq!(ts_event, [5, ANCHOR + 6]);
}

/// The stub declares streams and decoded kinds, so with nothing to feed no
/// case may pass vacuously.
#[test]
fn empty_input_fails_every_case() {
    let report = run_adapter_contract::<Stub<{ defect::CLEAN }>>(&(), &ContractFrames::default());
    assert_eq!(
        report.failures().count(),
        report.results.len(),
        "{:?}",
        report.results
    );
}

/// A declared stream with no unknown-kind frame can neither prove its unknown
/// path nor carry a skip on a dropped frame, so the input itself fails both
/// cases.
#[test]
fn missing_unknown_frame_for_a_stream_fails_unknown_cases() {
    let mut input = frames();
    input.unknown.retain(|f| f.stream == STREAMS[0]);
    let failed: Vec<_> = run_adapter_contract::<Stub<{ defect::CLEAN }>>(&(), &input)
        .failures()
        .map(|(case, _)| case)
        .collect();
    assert_eq!(
        failed,
        [
            AdapterCase::UnknownKindCounted,
            AdapterCase::GapSurvivesDrops
        ]
    );
}

/// The counter case matches every drained record to the frame it came from,
/// so the re-delivered event, stamped by an earlier frame, fails there too.
#[test]
fn stale_redrain_fails_drain_cases() {
    assert_eq!(
        failed::<{ defect::STALE_REDRAIN }>(),
        [AdapterCase::DrainEmpties, AdapterCase::CountersExact]
    );
}

#[test]
fn declaring_a_record_type_never_emitted_fails_record_types() {
    assert_eq!(
        failed::<{ defect::RECORD_TYPES_EXTRA }>(),
        [AdapterCase::RecordTypesMatchEmitted]
    );
}

#[test]
fn declaring_no_record_type_fails_record_types() {
    assert_eq!(
        failed::<{ defect::RECORD_TYPES_EMPTY }>(),
        [AdapterCase::RecordTypesMatchEmitted]
    );
}

#[test]
fn decoded_per_event_fails_counters() {
    assert_eq!(
        failed::<{ defect::DECODED_PER_EVENT }>(),
        [AdapterCase::CountersExact]
    );
}

#[test]
fn drain_reset_fails_counters() {
    assert_eq!(
        failed::<{ defect::DRAIN_RESETS_STATS }>(),
        [AdapterCase::CountersExact]
    );
}

#[test]
fn drain_bumping_malformed_fails_counters() {
    assert_eq!(
        failed::<{ defect::DRAIN_BUMPS_MALFORMED }>(),
        [AdapterCase::CountersExact]
    );
}

/// Every case that feeds an unknown kind catches and reports the panic,
/// including the coverage case, whose kinds are unknown on the other stream;
/// the remaining cases still run and pass.
#[test]
fn unknown_kind_panic_fails_unknown_cases() {
    assert_eq!(
        failed::<{ defect::PANIC_ON_UNKNOWN }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UnknownKindCounted,
            AdapterCase::CoverageMatchesDispatch,
            AdapterCase::DecodeAfterDrops,
            AdapterCase::GapSurvivesDrops
        ]
    );
}

#[test]
fn drain_error_reset_fails_counters() {
    assert_eq!(
        failed::<{ defect::DRAIN_RESETS_ERRORS }>(),
        [AdapterCase::CountersExact]
    );
}

/// A kind fed on the stream that does not decode it is unknown there, so the
/// coverage case's foreign probes take the same broken path.
#[test]
fn unknown_resetting_decoded_fails_counter_cases() {
    assert_eq!(
        failed::<{ defect::UNKNOWN_RESETS_DECODED }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UnknownKindCounted,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

#[test]
fn malformed_resetting_decoded_fails_counter_cases() {
    assert_eq!(
        failed::<{ defect::MALFORMED_RESETS_DECODED }>(),
        [AdapterCase::CountersExact, AdapterCase::TruncatedMalformed]
    );
}

/// A kind fed on the stream that does not decode it is unknown there, so the
/// coverage case's foreign probes take the same broken path.
#[test]
fn uncounted_unknown_fails_unknown_cases() {
    assert_eq!(
        failed::<{ defect::UNKNOWN_UNCOUNTED }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UnknownKindCounted,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

/// Only the second stream's unknown frames show the defect, so the unknown
/// cases must check every unknown frame, not the first. The coverage case's
/// foreign kinds fed on that stream take the same broken path.
#[test]
fn second_stream_uncounted_unknown_fails_unknown_cases() {
    assert_eq!(
        failed::<{ defect::SECOND_STREAM_UNKNOWN_UNCOUNTED }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UnknownKindCounted,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

/// `B` moved `decoded`, but `unknown_kind` with it, so it was not dispatched.
#[test]
fn cold_arm_fails_counters_and_coverage() {
    assert_eq!(
        failed::<{ defect::COLD_ARM_B }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

/// The coverage case must treat a `malformed` move like an `unknown_kind`
/// one.
#[test]
fn malformed_cold_arm_fails_counters_and_coverage() {
    assert_eq!(
        failed::<{ defect::COLD_ARM_B_MALFORMED }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

/// `C` never moved `decoded`, so the coverage case must not count it as
/// dispatched.
#[test]
fn uncounted_kind_fails_counters_and_coverage() {
    assert_eq!(
        failed::<{ defect::UNCOUNTED_C }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

#[test]
fn cross_stream_kind_fails_coverage() {
    assert_eq!(
        failed::<{ defect::CROSS_STREAM_KIND }>(),
        [AdapterCase::CoverageMatchesDispatch]
    );
}

/// Each case that feeds a frame the adapter must drop checks what it emits,
/// including the coverage case's kinds fed on the other stream. The counter
/// case checks counters only.
#[test]
fn emitting_dropped_frames_fails_drop_cases() {
    assert_eq!(
        failed::<{ defect::DROPPED_EMITS }>(),
        [
            AdapterCase::UnknownKindCounted,
            AdapterCase::TruncatedMalformed,
            AdapterCase::CoverageMatchesDispatch,
            AdapterCase::UndeclaredStreamIgnored,
            AdapterCase::DecodeAfterDrops,
            AdapterCase::GapSurvivesDrops
        ]
    );
}

#[test]
fn short_frame_as_unknown_fails_truncated_cases() {
    assert_eq!(
        failed::<{ defect::SHORT_AS_UNKNOWN }>(),
        [AdapterCase::CountersExact, AdapterCase::TruncatedMalformed]
    );
}

/// Only the third sample, on the second stream, shows the defect, so both
/// cases must cut every decodable frame, not the first.
#[test]
fn uncounted_truncated_kind_fails_truncated_cases() {
    assert_eq!(
        failed::<{ defect::TRUNCATED_C_UNCOUNTED }>(),
        [AdapterCase::CountersExact, AdapterCase::TruncatedMalformed]
    );
}

#[test]
fn coverage_omission_fails_coverage() {
    assert_eq!(
        failed::<{ defect::COVERAGE_OMITS_B }>(),
        [AdapterCase::CoverageMatchesDispatch]
    );
}

/// The streams the stub decodes are undeclared, which the coverage case also
/// rejects. The undeclared-stream probes land on streams the stub ignores, so
/// that case fails only through the empty `streams()`.
#[test]
fn no_streams_fails_stream_cases() {
    assert_eq!(
        failed::<{ defect::NO_STREAMS }>(),
        [
            AdapterCase::CoverageMatchesDispatch,
            AdapterCase::UndeclaredStreamIgnored
        ]
    );
}

#[test]
fn any_stream_fails_undeclared_cases() {
    assert_eq!(
        failed::<{ defect::ANY_STREAM }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UndeclaredStreamIgnored,
            AdapterCase::DecodeAfterDrops
        ]
    );
}

/// Only the highest free id decodes, so every case that probes undeclared
/// streams must probe that end too.
#[test]
fn highest_undeclared_stream_fails_undeclared_cases() {
    assert_eq!(
        failed::<{ defect::UNDECLARED_MAX_DECODES }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UndeclaredStreamIgnored,
            AdapterCase::DecodeAfterDrops
        ]
    );
}

/// Only the second sample's kind decodes off-stream, so every case that
/// probes undeclared streams must send every decodable frame there.
#[test]
fn undeclared_kind_b_fails_undeclared_cases() {
    assert_eq!(
        failed::<{ defect::UNDECLARED_DECODES_B }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UndeclaredStreamIgnored,
            AdapterCase::DecodeAfterDrops
        ]
    );
}

#[test]
fn undeclared_stats_reset_fails_undeclared_cases() {
    assert_eq!(
        failed::<{ defect::UNDECLARED_RESETS_STATS }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UndeclaredStreamIgnored
        ]
    );
}

#[test]
fn undeclared_pending_discard_fails_undeclared_stream() {
    assert_eq!(
        failed::<{ defect::UNDECLARED_CLEARS_PENDING }>(),
        [AdapterCase::UndeclaredStreamIgnored]
    );
}

/// A kind fed on the stream that does not decode it is unknown there, so the
/// coverage case's foreign probes take the same broken path.
#[test]
fn unknown_pending_discard_fails_unknown_cases() {
    assert_eq!(
        failed::<{ defect::UNKNOWN_CLEARS_PENDING }>(),
        [
            AdapterCase::UnknownKindCounted,
            AdapterCase::CoverageMatchesDispatch
        ]
    );
}

#[test]
fn malformed_pending_discard_fails_truncated() {
    assert_eq!(
        failed::<{ defect::MALFORMED_CLEARS_PENDING }>(),
        [AdapterCase::TruncatedMalformed]
    );
}

/// The leak shows only in what later frames decode to, which counters and
/// pending-event checks cannot see.
#[test]
fn truncated_state_leak_fails_decode_after_drops() {
    assert_eq!(
        failed::<{ defect::TRUNCATED_LEAKS_STATE }>(),
        [AdapterCase::DecodeAfterDrops]
    );
}

/// Patching pending trades on every feed also edits them while the drop cases
/// probe a declared stream, the coverage case's foreign kinds included;
/// undeclared frames never reach the patch.
#[test]
fn patched_pending_fails_lifetime_and_drop_cases() {
    assert_eq!(
        failed::<{ defect::PATCHES_PENDING }>(),
        [
            AdapterCase::UnknownKindCounted,
            AdapterCase::TruncatedMalformed,
            AdapterCase::CoverageMatchesDispatch,
            AdapterCase::RecordLifetime
        ]
    );
}

#[test]
fn uncounted_foreign_kind_fails_coverage() {
    assert_eq!(
        failed::<{ defect::FOREIGN_UNCOUNTED }>(),
        [AdapterCase::CoverageMatchesDispatch]
    );
}

#[test]
fn foreign_kind_pending_discard_fails_coverage() {
    assert_eq!(
        failed::<{ defect::FOREIGN_CLEARS_PENDING }>(),
        [AdapterCase::CoverageMatchesDispatch]
    );
}

/// Only the lowest free id decodes, so every case that probes undeclared
/// streams must probe that end too.
#[test]
fn lowest_undeclared_stream_fails_undeclared_cases() {
    assert_eq!(
        failed::<{ defect::UNDECLARED_MIN_DECODES }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::UndeclaredStreamIgnored,
            AdapterCase::DecodeAfterDrops
        ]
    );
}

/// The leak lands in the record header, so the comparison must cover more
/// than the body.
#[test]
fn header_leak_fails_decode_after_drops() {
    assert_eq!(
        failed::<{ defect::HEADER_LEAK_INSTRUMENT }>(),
        [AdapterCase::DecodeAfterDrops]
    );
}

/// The next feed consumes what a malformed frame leaves, so a drop must sit
/// right before a decodable frame for the change to show.
#[test]
fn malformed_carry_fails_decode_after_drops() {
    assert_eq!(
        failed::<{ defect::MALFORMED_CARRY_ANY_FEED }>(),
        [AdapterCase::DecodeAfterDrops]
    );
}

/// The header checks run on the counter case's drains, the only place each
/// record is matched to the frame it came from.
#[test]
fn bad_header_length_fails_counters() {
    assert_eq!(
        failed::<{ defect::BAD_LENGTH }>(),
        [AdapterCase::CountersExact]
    );
}

/// Rtype 0 is `Control`, which the stub does not declare.
#[test]
fn bad_header_rtype_fails_counters() {
    assert_eq!(
        failed::<{ defect::BAD_RTYPE }>(),
        [
            AdapterCase::CountersExact,
            AdapterCase::RecordTypesMatchEmitted
        ]
    );
}

#[test]
fn zeroed_ts_recv_fails_counters() {
    assert_eq!(
        failed::<{ defect::TS_RECV_ZEROED }>(),
        [AdapterCase::CountersExact]
    );
}

#[test]
fn own_venue_seq_fails_counters() {
    assert_eq!(
        failed::<{ defect::OWN_VENUE_SEQ }>(),
        [AdapterCase::CountersExact]
    );
}

/// Both cases that feed an empty frame catch and report the panic.
#[test]
fn empty_frame_panic_fails_empty_cases() {
    assert_eq!(
        failed::<{ defect::EMPTY_PANICS }>(),
        [AdapterCase::CountersExact, AdapterCase::TruncatedMalformed]
    );
}

/// Only a comparison against an adapter that saw no drops can see a flag the
/// drops set, and after a skip only the first emitting frame may carry it.
#[test]
fn sticky_gap_flag_fails_drop_comparisons() {
    assert_eq!(
        failed::<{ defect::STICKY_GAP }>(),
        [AdapterCase::DecodeAfterDrops, AdapterCase::GapSurvivesDrops]
    );
}

/// Only a skip fed on a frame the stub then drops shows the defect: the other
/// cases keep every stream gap-free.
#[test]
fn lost_gap_fails_gap_survives_drops() {
    assert_eq!(
        failed::<{ defect::GAP_LOST_ON_DROP }>(),
        [AdapterCase::GapSurvivesDrops]
    );
}

/// A skip on the second stream flags the first stream's next trades, which
/// only a comparison that covers frames on every stream after the skip sees.
#[test]
fn shared_gap_fails_gap_survives_drops() {
    assert_eq!(
        failed::<{ defect::GAP_CROSSES_STREAMS }>(),
        [AdapterCase::GapSurvivesDrops]
    );
}

/// The first frame to emit after a skip must have every record checked, which
/// only shows on a frame that emits more than one.
#[test]
fn partial_gap_flag_fails_gap_survives_drops() {
    assert_eq!(
        failed::<{ defect::GAP_FLAGS_FIRST_ONLY }>(),
        [AdapterCase::GapSurvivesDrops]
    );
}
