//! Seam tests. Every expected timestamp is hand-computed from the US DST rules
//! and checked against Python `datetime`, never read back from jiff.

use core::{fmt::Debug, mem::size_of};

use md_events::{
    Action, AssetClass, BreakRefKind, Control, ControlKind, Definition, Fill, Imbalance, Level,
    LevelAction, Mbo, Record, RecordFlags, RecordHeader, Side, StatType, Stats, Status, Trade,
    TradeBreak, TradingState, decode_prefix,
};

#[cfg(feature = "tz")]
use super::BundledTzdb;
use super::{ClosureModel, MarketEvent, NoTzdb, RollModel, SessionModel};
use crate::ingest::StreamId;

const HOUR: u64 = 3_600_000_000_000;
const HOUR_I: i64 = 3_600_000_000_000;

const CONTINUOUS: SessionModel = SessionModel {
    roll: RollModel::Continuous,
    closure: ClosureModel::None,
};
const UTC_PLUS_9: SessionModel = SessionModel {
    roll: RollModel::DailyFixed {
        offset_ns: 9 * HOUR_I,
    },
    closure: ClosureModel::None,
};
const NASDAQ: SessionModel = SessionModel {
    roll: RollModel::DailyZoned {
        tz: "America/New_York",
        local_ns: 0,
    },
    closure: ClosureModel::None,
};

/// `DailyFixed` anchors a UTC+9 venue at local midnight, 15:00 UTC the evening
/// before. [`NoTzdb`] answers every lookup with `None`, so a consulted resolver
/// would fail the anchors.
#[test]
fn session_daily_fixed() {
    // 2026-04-23T15:00:00Z and 2026-05-22T15:00:00Z
    assert_eq!(
        UTC_PLUS_9.resolve_anchor(20260424, &NoTzdb),
        Some(1_776_956_400_000_000_000)
    );
    assert_eq!(
        UTC_PLUS_9.resolve_anchor(20260523, &NoTzdb),
        Some(1_779_462_000_000_000_000)
    );
    assert_eq!(
        UTC_PLUS_9.session_label(1_776_956_400_000_000_000, &NoTzdb),
        Some(20260424),
        "the anchor instant opens its own session"
    );
    assert_eq!(
        UTC_PLUS_9.session_label(1_776_956_400_000_000_000 - 1, &NoTzdb),
        Some(20260423)
    );
    assert_eq!(
        UTC_PLUS_9.resolve_anchor(20261301, &NoTzdb),
        None,
        "month 13 names no session"
    );
}

/// ET midnight is 5h after UTC midnight in January (EST) and 4h in June (EDT),
/// the pair datafeed's `app.rs` `et_session_midnight_applies_dst` pins. One model
/// answers both dates, so the offset is derived per date and never cached.
#[cfg(feature = "tz")]
#[test]
fn session_daily_et_dst() {
    let tz = BundledTzdb::new();
    // 2026-01-15T05:00:00Z
    assert_eq!(
        NASDAQ.resolve_anchor(20260115, &tz),
        Some(1_768_453_200_000_000_000)
    );
    // 2026-06-15T04:00:00Z
    assert_eq!(
        NASDAQ.resolve_anchor(20260615, &tz),
        Some(1_781_496_000_000_000_000)
    );
    assert_eq!(
        NASDAQ.session_label(1_781_496_000_000_000_000, &tz),
        Some(20260615)
    );
    assert_eq!(
        NASDAQ.session_label(1_781_496_000_000_000_000 - 1, &tz),
        Some(20260614)
    );
    assert_eq!(NASDAQ.resolve_anchor(20261301, &tz), None);
}

/// A zoned model with no zone database fails at every entry point rather than
/// falling back to a UTC anchor that would mislabel every session.
#[test]
fn zoned_without_resolver_is_none() {
    assert_eq!(NASDAQ.resolve_anchor(20260115, &NoTzdb), None);
    assert_eq!(
        NASDAQ.session_label(1_768_453_200_000_000_000, &NoTzdb),
        None
    );
    assert_eq!(NASDAQ.session_span_ns(20260115, &NoTzdb), None);
}

/// A day partition ends at the next session's anchor: 23h across the
/// 2026-03-08 spring-forward, 25h across the 2026-11-01 fall-back, 24h on an
/// ordinary date. Fixed and continuous rolls are 24h on the same dates. Lord
/// Howe's half-hour DST step yields 23h30m, which no day partition can tile.
#[test]
fn span_23_24_25() {
    for date in [20260308, 20261101] {
        assert_eq!(UTC_PLUS_9.session_span_ns(date, &NoTzdb), Some(24 * HOUR));
        assert_eq!(CONTINUOUS.session_span_ns(date, &NoTzdb), Some(24 * HOUR));
    }
    #[cfg(feature = "tz")]
    {
        let tz = BundledTzdb::new();
        assert_eq!(NASDAQ.session_span_ns(20260308, &tz), Some(23 * HOUR));
        assert_eq!(NASDAQ.session_span_ns(20261101, &tz), Some(25 * HOUR));
        assert_eq!(NASDAQ.session_span_ns(20260309, &tz), Some(24 * HOUR));

        let lord_howe = SessionModel {
            roll: RollModel::DailyZoned {
                tz: "Australia/Lord_Howe",
                local_ns: 0,
            },
            closure: ClosureModel::None,
        };
        // 2026-10-03T13:30:00Z (+10:30) and 2026-10-04T13:00:00Z (+11:00)
        assert_eq!(
            lord_howe.resolve_anchor(20261004, &tz),
            Some(1_791_034_200_000_000_000)
        );
        assert_eq!(
            lord_howe.resolve_anchor(20261005, &tz),
            Some(1_791_118_800_000_000_000)
        );
        assert_eq!(lord_howe.session_span_ns(20261004, &tz), None);
    }
}

/// New York zoned roll at `local_ns` after local midnight.
#[cfg(feature = "tz")]
fn new_york_at(local_ns: i64) -> SessionModel {
    SessionModel {
        roll: RollModel::DailyZoned {
            tz: "America/New_York",
            local_ns,
        },
        closure: ClosureModel::None,
    }
}

/// 02:30 on 2026-03-08 does not exist in New York: clocks jump from 02:00 EST
/// to 03:00 EDT. The pinned rule applies the pre-gap EST offset, so the anchor
/// is 03:30 EDT, 2026-03-08T07:30:00Z.
#[cfg(feature = "tz")]
#[test]
fn gap_local_time_pinned() {
    let model = new_york_at(5 * HOUR_I / 2);
    assert_eq!(
        model.resolve_anchor(20260308, &BundledTzdb::new()),
        Some(1_772_955_000_000_000_000)
    );
}

/// 01:30 on 2026-11-01 happens twice in New York. The pinned rule takes the
/// earlier instant, 01:30 EDT, 2026-11-01T05:30:00Z.
#[cfg(feature = "tz")]
#[test]
fn fold_local_time_pinned() {
    let model = new_york_at(3 * HOUR_I / 2);
    assert_eq!(
        model.resolve_anchor(20261101, &BundledTzdb::new()),
        Some(1_793_511_000_000_000_000)
    );
}

/// A 20:00 New York roll lands on the next UTC day (01:00Z in EST), so a tick
/// at 00:30Z, 19:30 EST, still belongs to the session that opened the evening
/// before.
#[cfg(feature = "tz")]
#[test]
fn late_zoned_roll_labels_prior_session() {
    let tz = BundledTzdb::new();
    let model = new_york_at(20 * HOUR_I);
    let anchor = 1_768_438_800_000_000_000; // 2026-01-15T01:00:00Z
    assert_eq!(model.resolve_anchor(20260114, &tz), Some(anchor));
    assert_eq!(model.session_label(anchor, &tz), Some(20260114));
    // 2026-01-15T00:30:00Z, 19:30 EST on 2026-01-14
    assert_eq!(
        model.session_label(1_768_437_000_000_000_000, &tz),
        Some(20260113)
    );
}

/// A continuous roll anchors on UTC midnight and keeps one label through 15:00
/// UTC, where a UTC+9 session rolls; the label moves only at UTC midnight.
#[test]
fn session_continuous_no_roll() {
    let midnight = 1_776_988_800_000_000_000; // 2026-04-24T00:00:00Z
    let utc9_roll = 1_777_042_800_000_000_000; // 2026-04-24T15:00:00Z
    let next_midnight = 1_777_075_200_000_000_000; // 2026-04-25T00:00:00Z
    assert_eq!(CONTINUOUS.resolve_anchor(20260424, &NoTzdb), Some(midnight));
    for ts in [midnight, utc9_roll - 1, utc9_roll, next_midnight - 1] {
        assert_eq!(
            CONTINUOUS.session_label(ts, &NoTzdb),
            Some(20260424),
            "ts {ts}"
        );
    }
    assert_eq!(
        CONTINUOUS.session_label(next_midnight, &NoTzdb),
        Some(20260425)
    );
}

/// FX closes from the Friday 17:00 New York roll to the Sunday 17:00 roll,
/// Friday 22:00Z to Sunday 22:00Z in January. Saturday and Sunday morning fall
/// in closed sessions, and the Sunday anchor stays where the roll alone puts it.
#[cfg(feature = "tz")]
#[test]
fn session_weekend_closure() {
    let tz = BundledTzdb::new();
    let daily = new_york_at(17 * HOUR_I);
    let fx = SessionModel {
        closure: ClosureModel::Weekend {
            close_dow: 5,
            open_dow: 0,
        },
        ..daily
    };
    let cases = [
        (1_767_992_400_000_000_000, 20260108, true), // Fri 2026-01-09 16:00 EST
        (1_767_999_600_000_000_000, 20260109, false), // Fri 18:00 EST
        (1_768_064_400_000_000_000, 20260109, false), // Sat 2026-01-10 12:00 EST
        (1_768_143_600_000_000_000, 20260110, false), // Sun 2026-01-11 10:00 EST
        (1_768_172_400_000_000_000, 20260111, true), // Sun 18:00 EST
    ];
    for (ts, label, trades) in cases {
        assert_eq!(fx.session_label(ts, &tz), Some(label), "ts {ts}");
        assert_eq!(fx.is_trading_date(label), trades, "session {label}");
    }
    // 2026-01-11T22:00:00Z
    let sunday = Some(1_768_168_800_000_000_000);
    assert_eq!(fx.resolve_anchor(20260111, &tz), sunday);
    assert_eq!(daily.resolve_anchor(20260111, &tz), sunday);
}

const STREAM: StreamId = StreamId(3);

/// Header for a `T` record, every field distinct so a misplaced byte shows.
fn hdr<T: Record>() -> RecordHeader {
    RecordHeader {
        length: u8::try_from(size_of::<T>() / 4).expect("record fits the length byte"),
        rtype: T::RTYPE,
        venue: 1,
        instrument_id: 9,
        ts_event: 100,
        ts_recv: 101,
        ts_egress: 102,
        venue_seq: 103,
        egress_seq: 104,
        flags: RecordFlags::SNAPSHOT,
        _pad: [0u8; 6],
    }
}

fn header_bytes(header: &RecordHeader) -> &[u8] {
    // SAFETY: RecordHeader is repr(C) with an explicit zeroed `_pad` tail and no implicit
    // padding (md_events `layout_locked`), so all its bytes are initialized for the borrow.
    unsafe {
        core::slice::from_raw_parts(
            (header as *const RecordHeader).cast::<u8>(),
            size_of::<RecordHeader>(),
        )
    }
}

/// Wrap `rec`, split it through `as_record_ref`, lay header and body end to
/// end, and require md-events' reader to give back the identical record.
fn assert_round_trip<T: Record + PartialEq + Debug>(rec: T, wrap: fn(T) -> MarketEvent) {
    let event = wrap(rec);
    assert_eq!(event.rtype(), T::RTYPE);
    let view = event.as_record_ref(STREAM);
    assert_eq!(view.stream, STREAM);
    let mut wire = header_bytes(view.header).to_vec();
    wire.extend_from_slice(view.body);
    let (back, consumed) = decode_prefix::<T>(&wire).expect("whole record on the wire");
    assert_eq!(consumed, size_of::<T>());
    assert_eq!(back, rec);
}

/// Every variant's header and body views reassemble into its record.
#[test]
fn event_record_ref_roundtrip() {
    assert_round_trip(
        Mbo {
            hdr: hdr::<Mbo>(),
            order_id: 77,
            price: 1_000_250_000,
            qty: 300,
            action: Action::Add,
            side: Side::Bid,
            participant: *b"ABCDEFG",
            _pad: [0u8; 7],
        },
        MarketEvent::Mbo,
    );
    assert_round_trip(
        Level {
            hdr: hdr::<Level>(),
            price: 1_000_500_000,
            qty: 400,
            num_deletes: 2,
            side: Side::Ask,
            level: 3,
            action: LevelAction::Change,
            _pad: [0u8; 3],
        },
        MarketEvent::Level,
    );
    assert_round_trip(
        Trade {
            hdr: hdr::<Trade>(),
            price: 5_000_000_000,
            qty: 10,
            trade_ref: 991,
            aggressor: Side::Bid,
            _pad: [0u8; 7],
        },
        MarketEvent::Trade,
    );
    assert_round_trip(
        TradeBreak {
            hdr: hdr::<TradeBreak>(),
            break_ref: 991,
            ref_kind: BreakRefKind::SecondaryTradeId,
            _pad: [0u8; 7],
        },
        MarketEvent::TradeBreak,
    );
    assert_round_trip(
        Imbalance {
            hdr: hdr::<Imbalance>(),
            ref_px: 1,
            far_px: i64::MAX,
            near_px: 3,
            paired_qty: 4,
            imbalance_qty: 5,
            eq_px: 6,
            eq_qty: 7,
            imbalance_side: Side::Ask,
            cross_type: b'O',
            _pad: [0u8; 6],
        },
        MarketEvent::Imbalance,
    );
    assert_round_trip(
        Fill {
            hdr: hdr::<Fill>(),
            price: 8,
            qty: 9,
            fill_ref: 10,
            ref_kind: BreakRefKind::MatchNumber,
            buyer: *b"BUYER01",
            seller: *b"SELLR02",
            aggressor: Side::Ask,
        },
        MarketEvent::Fill,
    );
    assert_round_trip(
        Stats {
            hdr: hdr::<Stats>(),
            price: 11,
            qty: 12,
            aux: -13,
            stat_type: StatType::MarkPrice,
            category: 2,
            _pad: [0u8; 5],
        },
        MarketEvent::Stats,
    );
    assert_round_trip(
        Status {
            hdr: hdr::<Status>(),
            state: TradingState::Halt,
            venue_reason: *b"HALT REASON TEXT 20B",
            _pad: [0u8; 3],
        },
        MarketEvent::Status,
    );
    assert_round_trip(
        Control {
            hdr: hdr::<Control>(),
            aux: 42,
            kind: ControlKind::GapDetected,
            _pad: [0u8; 7],
        },
        MarketEvent::Control,
    );
    assert_round_trip(
        Definition::new(
            9,
            1,
            100,
            777,
            [b'X'; 20],
            *b"MAIN",
            AssetClass::Equity,
            0,
            100,
        ),
        MarketEvent::Definition,
    );
}
