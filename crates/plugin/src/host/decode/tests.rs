use core::mem::{offset_of, size_of};

use md_events::{
    Action, AssetClass, BreakRefKind, Control, ControlKind, Definition, Fill, Imbalance, Level,
    LevelAction, Mbo, OptionType, Side, StatType, Stats, Status, Trade, TradeBreak, TradingState,
    rtype,
};

use super::event;

/// An enum field: its offset, then a variant's value and a value of no
/// variant, in native byte order.
type EnumField = (usize, Vec<u8>, Vec<u8>);

/// 0xEE is no variant of any one-byte md-events enum.
fn u8_field(at: usize, valid: u8) -> EnumField {
    (at, vec![valid], vec![0xEE])
}

/// Every record type a `MarketEvent` holds: rtype, size and enum fields.
fn record_types() -> [(u8, usize, Vec<EnumField>); 10] {
    [
        (
            rtype::MBO,
            size_of::<Mbo>(),
            vec![
                u8_field(offset_of!(Mbo, action), Action::Reset as u8),
                u8_field(offset_of!(Mbo, side), Side::Ask as u8),
            ],
        ),
        (
            rtype::LEVEL,
            size_of::<Level>(),
            vec![
                u8_field(offset_of!(Level, side), Side::Bid as u8),
                u8_field(offset_of!(Level, action), LevelAction::Clear as u8),
            ],
        ),
        (
            rtype::TRADE,
            size_of::<Trade>(),
            vec![u8_field(offset_of!(Trade, aggressor), Side::None as u8)],
        ),
        (
            rtype::TRADE_BREAK,
            size_of::<TradeBreak>(),
            vec![u8_field(
                offset_of!(TradeBreak, ref_kind),
                BreakRefKind::SecondaryTradeId as u8,
            )],
        ),
        (
            rtype::IMBALANCE,
            size_of::<Imbalance>(),
            vec![u8_field(
                offset_of!(Imbalance, imbalance_side),
                Side::Ask as u8,
            )],
        ),
        (
            rtype::FILL,
            size_of::<Fill>(),
            vec![
                u8_field(offset_of!(Fill, ref_kind), BreakRefKind::MatchNumber as u8),
                u8_field(offset_of!(Fill, aggressor), Side::Bid as u8),
            ],
        ),
        (
            rtype::STATS,
            size_of::<Stats>(),
            vec![(
                offset_of!(Stats, stat_type),
                (StatType::VenueSpecific as u16).to_ne_bytes().to_vec(),
                0xEEEE_u16.to_ne_bytes().to_vec(),
            )],
        ),
        (
            rtype::STATUS,
            size_of::<Status>(),
            vec![u8_field(
                offset_of!(Status, state),
                TradingState::NoTrade as u8,
            )],
        ),
        (
            rtype::CONTROL,
            size_of::<Control>(),
            vec![u8_field(
                offset_of!(Control, kind),
                ControlKind::RecoveryComplete as u8,
            )],
        ),
        (
            rtype::DEFINITION,
            size_of::<Definition>(),
            vec![
                u8_field(offset_of!(Definition, asset_class), AssetClass::Etf as u8),
                u8_field(
                    offset_of!(Definition, tail.option_type),
                    OptionType::Put as u8,
                ),
            ],
        ),
    ]
}

/// A record of `size` bytes: a byte pattern under the header's length and
/// rtype, with each enum field set to its variant.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the pattern wraps on purpose, and a record is at most 176 bytes"
)]
fn image(rtype: u8, size: usize, enums: &[EnumField]) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
    bytes[0] = (size / 4) as u8;
    bytes[1] = rtype;
    for (at, valid, _) in enums {
        bytes[*at..*at + valid.len()].copy_from_slice(valid);
    }
    bytes
}

// @lat: [[plugin#Tests#Record rebuild]]
#[test]
fn each_record_type_rebuilds_byte_for_byte() {
    for (rtype, size, enums) in record_types() {
        let bytes = image(rtype, size, &enums);
        let rebuilt = event(&bytes).unwrap_or_else(|| panic!("rtype {rtype:#04x} refused"));
        assert_eq!(rebuilt.as_bytes(), bytes, "rtype {rtype:#04x}");
    }
}

// @lat: [[plugin#Tests#Enum fields are checked]]
#[test]
fn an_enum_field_outside_its_variants_is_refused() {
    for (rtype, size, enums) in record_types() {
        for (at, _, invalid) in &enums {
            let mut bytes = image(rtype, size, &enums);
            bytes[*at..*at + invalid.len()].copy_from_slice(invalid);
            assert_eq!(event(&bytes), None, "rtype {rtype:#04x}, enum at {at}");
        }
    }
}

#[test]
fn a_record_of_another_length_or_rtype_is_refused() {
    let [(rtype, size, enums), ..] = record_types();
    let bytes = image(rtype, size, &enums);
    assert!(event(&bytes).is_some(), "the unchanged image rebuilds");

    assert_eq!(event(&bytes[..size - 4]), None, "one word short");
    let mut longer = bytes.clone();
    longer.extend([0; 4]);
    longer[0] += 1;
    assert_eq!(event(&longer), None, "one word long, header agreeing");
    let mut header_short = bytes.clone();
    header_short[0] -= 1;
    assert_eq!(event(&header_short), None, "header length one word short");
    let mut derived = bytes.clone();
    derived[1] = rtype::BBO;
    assert_eq!(event(&derived), None, "a record type no adapter emits");
    for short in [&[][..], &bytes[..1]] {
        assert_eq!(event(short), None, "{} bytes", short.len());
    }
}
