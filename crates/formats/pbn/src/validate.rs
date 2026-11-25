//! Discriminant validation for hostile bytes.
//!
//! The framing walk (`stream`) is bounds-safe, but a record body copied verbatim
//! may carry an out-of-range enum byte, and reading that field as its Rust enum
//! is undefined behavior. `validate` checks every enum-bearing field of a record
//! against its valid discriminants, on the raw bytes, before any typed decode
//! materializes it.
//!
//! Per-record layout knowledge is hand-maintained here against md-events, keyed
//! by `offset_of!` so field moves do not silently break it. A generated
//! validator (from schemas/records) would remove the duplication. Unknown rtypes
//! pass: an older reader cannot validate a record family it does not know, and
//! the framing walk has already bounds-checked the frame.

use core::mem::offset_of;

use md_events::{
    Control, Definition, Fill, Imbalance, Level, Mbo, Mbp10, Stats, Status, Trade, TradeBreak,
    rtype,
};

use crate::error::PbnError;

fn action_ok(b: u8) -> bool {
    b <= 6
}
fn side_ok(b: u8) -> bool {
    // DBN convention bytes: Bid 66, Ask 65, None 78.
    matches!(b, 65 | 66 | 78)
}
fn level_action_ok(b: u8) -> bool {
    b <= 3
}
fn break_ref_ok(b: u8) -> bool {
    b <= 1
}
fn trading_state_ok(b: u8) -> bool {
    b <= 12
}
fn control_kind_ok(b: u8) -> bool {
    b <= 8
}
fn asset_class_ok(b: u8) -> bool {
    b <= 9
}
fn option_type_ok(b: u8) -> bool {
    b <= 2
}
fn stat_type_ok(v: u16) -> bool {
    matches!(v, 0x0001..=0x000B | 0x0100..=0x0106 | 0xFF00)
}

fn check_u8(bytes: &[u8], rt: u8, offset: usize, ok: fn(u8) -> bool) -> Result<(), PbnError> {
    let b = *bytes.get(offset).ok_or(PbnError::RecordTooShort {
        rtype: rt,
        need: offset + 1,
        got: bytes.len(),
    })?;
    if ok(b) {
        Ok(())
    } else {
        Err(PbnError::BadDiscriminant { rtype: rt, offset })
    }
}

fn check_u16(bytes: &[u8], rt: u8, offset: usize, ok: fn(u16) -> bool) -> Result<(), PbnError> {
    let end = offset + 2;
    if bytes.len() < end {
        return Err(PbnError::RecordTooShort {
            rtype: rt,
            need: end,
            got: bytes.len(),
        });
    }
    let v = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
    if ok(v) {
        Ok(())
    } else {
        Err(PbnError::BadDiscriminant { rtype: rt, offset })
    }
}

/// Validate every enum-typed field of the record `bytes` claims to be (by
/// `rt`). Returns `Ok` for a record with only valid discriminants, or a record
/// family this reader does not know (forward-compat). Errors before any typed
/// decode, so an invalid enum is never read as a Rust enum.
#[inline]
pub fn validate(rt: u8, bytes: &[u8]) -> Result<(), PbnError> {
    match rt {
        rtype::MBO => {
            check_u8(bytes, rt, offset_of!(Mbo, action), action_ok)?;
            check_u8(bytes, rt, offset_of!(Mbo, side), side_ok)?;
        }
        rtype::LEVEL => {
            check_u8(bytes, rt, offset_of!(Level, side), side_ok)?;
            check_u8(bytes, rt, offset_of!(Level, action), level_action_ok)?;
        }
        rtype::MBP10 => {
            check_u8(bytes, rt, offset_of!(Mbp10, action), action_ok)?;
            check_u8(bytes, rt, offset_of!(Mbp10, side), side_ok)?;
        }
        rtype::BBO | rtype::OHLCV => {
            // no enum-typed fields
        }
        rtype::TRADE => {
            check_u8(bytes, rt, offset_of!(Trade, aggressor), side_ok)?;
        }
        rtype::TRADE_BREAK => {
            check_u8(bytes, rt, offset_of!(TradeBreak, ref_kind), break_ref_ok)?;
        }
        rtype::IMBALANCE => {
            check_u8(bytes, rt, offset_of!(Imbalance, imbalance_side), side_ok)?;
        }
        rtype::FILL => {
            check_u8(bytes, rt, offset_of!(Fill, ref_kind), break_ref_ok)?;
            check_u8(bytes, rt, offset_of!(Fill, aggressor), side_ok)?;
        }
        rtype::STATS => {
            check_u16(bytes, rt, offset_of!(Stats, stat_type), stat_type_ok)?;
        }
        rtype::STATUS => {
            check_u8(bytes, rt, offset_of!(Status, state), trading_state_ok)?;
        }
        rtype::CONTROL => {
            check_u8(bytes, rt, offset_of!(Control, kind), control_kind_ok)?;
        }
        rtype::DEFINITION => {
            check_u8(
                bytes,
                rt,
                offset_of!(Definition, asset_class),
                asset_class_ok,
            )?;
            // a definition written before the tail ends where it starts
            if bytes.len() != Definition::PRE_TAIL_LEN {
                check_u8(
                    bytes,
                    rt,
                    offset_of!(Definition, tail.option_type),
                    option_type_ok,
                )?;
            }
        }
        _ => {
            // unknown rtype: framing already bounds-checked; forward-compat pass
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use md_events::{
        Action, AssetClass, Control, ControlKind, Definition, Level, LevelAction, RecordFlags,
        RecordHeader, Side, Trade,
    };

    use super::*;
    use crate::stream::write_record;

    fn trade_bytes() -> Vec<u8> {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Trade>() / 4) as u8,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 5,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        let t = Trade {
            hdr,
            price: 1,
            qty: 1,
            trade_ref: 1,
            aggressor: Side::Bid,
            _pad: [0u8; 7],
        };
        let mut b = Vec::new();
        write_record(&mut b, &t).unwrap();
        b
    }

    #[test]
    fn valid_record_passes() {
        assert!(validate(rtype::TRADE, &trade_bytes()).is_ok());
    }

    #[test]
    fn bad_side_rejected() {
        let mut b = trade_bytes();
        let off = offset_of!(Trade, aggressor);
        b[off] = 0x7F; // not 65/66/78
        assert_eq!(
            validate(rtype::TRADE, &b),
            Err(PbnError::BadDiscriminant {
                rtype: rtype::TRADE,
                offset: off
            })
        );
    }

    #[test]
    fn bad_control_kind_rejected() {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Control>() / 4) as u8,
            rtype: rtype::CONTROL,
            venue: 1,
            instrument_id: 0,
            ts_event: 0,
            ts_recv: 0,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        let c = Control {
            hdr,
            aux: 0,
            kind: ControlKind::GapDetected,
            _pad: [0u8; 7],
        };
        let mut b = Vec::new();
        write_record(&mut b, &c).unwrap();
        assert!(validate(rtype::CONTROL, &b).is_ok());
        b[offset_of!(Control, kind)] = 99;
        assert!(matches!(
            validate(rtype::CONTROL, &b),
            Err(PbnError::BadDiscriminant { .. })
        ));
    }

    // Etf is the last asset class, so the byte after it is the first rejected.
    #[test]
    fn asset_class_range_ends_at_etf() {
        let d = Definition::new(1, 5, 0, 0, [0u8; 20], *b"MAIN", AssetClass::Etf, 0, 100);
        let mut b = Vec::new();
        write_record(&mut b, &d).unwrap();
        assert!(validate(rtype::DEFINITION, &b).is_ok());
        let off = offset_of!(Definition, asset_class);
        b[off] = AssetClass::Etf as u8 + 1;
        assert_eq!(
            validate(rtype::DEFINITION, &b),
            Err(PbnError::BadDiscriminant {
                rtype: rtype::DEFINITION,
                offset: off
            })
        );
    }

    // Clear is the last level action, so the byte after it is the first rejected.
    #[test]
    fn level_action_range_ends_at_clear() {
        let l = Level {
            hdr: RecordHeader {
                length: (core::mem::size_of::<Level>() / 4) as u8,
                rtype: rtype::LEVEL,
                venue: 1,
                instrument_id: 5,
                ts_event: 0,
                ts_recv: 0,
                ts_egress: 0,
                venue_seq: 0,
                egress_seq: 0,
                flags: RecordFlags::NONE,
                _pad: [0u8; 6],
            },
            price: 1,
            qty: 0,
            num_deletes: 0,
            side: Side::None,
            level: 0,
            action: LevelAction::Clear,
            _pad: [0u8; 3],
        };
        let mut b = Vec::new();
        write_record(&mut b, &l).unwrap();
        assert!(validate(rtype::LEVEL, &b).is_ok());
        let off = offset_of!(Level, action);
        b[off] = LevelAction::Clear as u8 + 1;
        assert_eq!(
            validate(rtype::LEVEL, &b),
            Err(PbnError::BadDiscriminant {
                rtype: rtype::LEVEL,
                offset: off
            })
        );
    }

    // A byte past Put at the option_type offset must fail before a typed
    // decode reads it as the enum.
    #[test]
    fn bad_option_type_rejected() {
        let d = Definition::new(1, 5, 0, 0, [0u8; 20], [0u8; 4], AssetClass::Equity, 0, 100);
        let mut b = Vec::new();
        write_record(&mut b, &d).unwrap();
        let off = offset_of!(Definition, tail.option_type);
        b[off] = 3;
        assert_eq!(
            validate(rtype::DEFINITION, &b),
            Err(PbnError::BadDiscriminant {
                rtype: rtype::DEFINITION,
                offset: off
            })
        );
    }

    #[test]
    fn too_short_for_rtype() {
        let b = vec![0u8; 4]; // far shorter than a Trade
        assert!(matches!(
            validate(rtype::TRADE, &b),
            Err(PbnError::RecordTooShort { .. })
        ));
    }

    #[test]
    fn unknown_rtype_passes() {
        // 0x7A is in a reserved/unassigned band this reader does not decode.
        assert!(validate(0x7A, &[0u8; 8]).is_ok());
    }

    #[test]
    fn valid_side_bytes() {
        assert!(side_ok(Side::Bid as u8));
        assert!(side_ok(Side::Ask as u8));
        assert!(side_ok(Side::None as u8));
        assert!(!side_ok(0));
        assert!(action_ok(Action::Trade as u8));
    }
}
