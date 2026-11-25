//! Mixed-record stream dispatch.
//!
//! `decode_prefix` needs a concrete record type, so a real stream of many kinds
//! (Definitions, book/tape records, Control) cannot be walked with one type
//! parameter. This module dispatches each frame on its rtype into a typed
//! `AnyRecord`, validating discriminants along the way, and reports a record
//! family this build does not know as `Unknown` (skipped by length,
//! forward-compat). It is the format's mixed-stream decode path.

use md_events::{
    Bbo, Control, Definition, Fill, Imbalance, Level, Mbo, Mbp10, Ohlcv, RecordHeader, Stats,
    Status, Trade, TradeBreak, rtype,
};

use crate::{
    error::PbnError,
    stream::{Frame, frames},
};

/// A decoded record of any known type, or an unknown rtype skipped by length.
// Mbp10 (480 bytes) dominates the size, but boxing it would add a heap
// allocation per decoded record, which the decode path avoids. AnyRecord stays
// stack-only, yielded one value at a time.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnyRecord {
    Control(Control),
    Mbo(Mbo),
    Level(Level),
    Mbp10(Mbp10),
    Bbo(Bbo),
    Trade(Trade),
    TradeBreak(TradeBreak),
    Ohlcv(Ohlcv),
    Imbalance(Imbalance),
    Fill(Fill),
    Stats(Stats),
    Status(Status),
    Definition(Definition),
    /// A record family this build does not know; skipped by header length.
    Unknown {
        rtype: u8,
        len: usize,
    },
}

impl AnyRecord {
    /// The common record header, or `None` for an unknown rtype.
    #[inline]
    pub fn header(&self) -> Option<&RecordHeader> {
        match self {
            AnyRecord::Control(r) => Some(&r.hdr),
            AnyRecord::Mbo(r) => Some(&r.hdr),
            AnyRecord::Level(r) => Some(&r.hdr),
            AnyRecord::Mbp10(r) => Some(&r.hdr),
            AnyRecord::Bbo(r) => Some(&r.hdr),
            AnyRecord::Trade(r) => Some(&r.hdr),
            AnyRecord::TradeBreak(r) => Some(&r.hdr),
            AnyRecord::Ohlcv(r) => Some(&r.hdr),
            AnyRecord::Imbalance(r) => Some(&r.hdr),
            AnyRecord::Fill(r) => Some(&r.hdr),
            AnyRecord::Stats(r) => Some(&r.hdr),
            AnyRecord::Status(r) => Some(&r.hdr),
            AnyRecord::Definition(r) => Some(&r.hdr),
            AnyRecord::Unknown { .. } => None,
        }
    }

    /// This record's rtype byte.
    #[inline]
    pub fn rtype(&self) -> u8 {
        match self {
            AnyRecord::Unknown { rtype, .. } => *rtype,
            other => other.header().expect("known record has a header").rtype,
        }
    }
}

/// Decode one frame into a typed `AnyRecord`, validating enum discriminants. A
/// known rtype with an out-of-range enum or a too-short body errors; an unknown
/// rtype is reported (not an error) so an older reader skips it.
#[inline]
pub fn decode_any(frame: &Frame) -> Result<AnyRecord, PbnError> {
    macro_rules! d {
        ($ty:ty, $var:ident) => {
            Ok(AnyRecord::$var(
                frame
                    .decode_checked::<$ty>()?
                    .expect("frame rtype matches this variant"),
            ))
        };
    }
    match frame.rtype {
        rtype::CONTROL => d!(Control, Control),
        rtype::MBO => d!(Mbo, Mbo),
        rtype::LEVEL => d!(Level, Level),
        rtype::MBP10 => d!(Mbp10, Mbp10),
        rtype::BBO => d!(Bbo, Bbo),
        rtype::TRADE => d!(Trade, Trade),
        rtype::TRADE_BREAK => d!(TradeBreak, TradeBreak),
        rtype::OHLCV => d!(Ohlcv, Ohlcv),
        rtype::IMBALANCE => d!(Imbalance, Imbalance),
        rtype::FILL => d!(Fill, Fill),
        rtype::STATS => d!(Stats, Stats),
        rtype::STATUS => d!(Status, Status),
        rtype::DEFINITION => d!(Definition, Definition),
        other => Ok(AnyRecord::Unknown {
            rtype: other,
            len: frame.bytes.len(),
        }),
    }
}

/// Walk a record body, yielding each frame decoded into a typed `AnyRecord`.
/// Combines the bounds-checked frame walk with per-record dispatch + validation.
pub fn read_records(body: &[u8]) -> impl Iterator<Item = Result<AnyRecord, PbnError>> + '_ {
    frames(body).map(|f| f.and_then(|frame| decode_any(&frame)))
}

#[cfg(test)]
mod tests {
    use md_events::{AssetClass, ControlKind, RecordFlags, RecordHeader, Side, Trade};

    use super::*;
    use crate::stream::write_record;

    fn trade(seq: u64) -> Trade {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Trade>() / 4) as u8,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 5,
            ts_event: seq,
            ts_recv: seq,
            ts_egress: 0,
            venue_seq: seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        Trade {
            hdr,
            price: 1_000_000_000,
            qty: 50,
            trade_ref: seq,
            aggressor: Side::Bid,
            _pad: [0u8; 7],
        }
    }

    fn control() -> Control {
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
        Control {
            hdr,
            aux: 3,
            kind: ControlKind::GapDetected,
            _pad: [0u8; 7],
        }
    }

    #[test]
    fn dispatches_mixed_stream() {
        let def = Definition::new(5, 1, 0, 0, [b'X'; 20], *b"MAIN", AssetClass::Equity, 0, 100);
        let mut body = Vec::new();
        write_record(&mut body, &def).unwrap();
        write_record(&mut body, &trade(1)).unwrap();
        write_record(&mut body, &control()).unwrap();

        let got: Vec<AnyRecord> = read_records(&body).map(|r| r.unwrap()).collect();
        assert_eq!(got.len(), 3);
        assert!(matches!(got[0], AnyRecord::Definition(_)));
        assert!(matches!(got[1], AnyRecord::Trade(t) if t.trade_ref == 1));
        assert!(matches!(
            got[2],
            AnyRecord::Control(c) if c.kind == ControlKind::GapDetected
        ));
        assert_eq!(got[1].rtype(), rtype::TRADE);
        assert_eq!(got[2].header().unwrap().venue, 1);
    }

    // An unknown rtype is reported, not an error: an older reader skips it.
    #[test]
    fn unknown_rtype_reported() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(0)).unwrap();
        body[1] = 0x7A; // reserved/unassigned band
        let got: Vec<AnyRecord> = read_records(&body).map(|r| r.unwrap()).collect();
        assert!(matches!(
            got[0],
            AnyRecord::Unknown {
                rtype: 0x7A,
                len: 88
            }
        ));
    }

    // A corrupt discriminant on a known rtype propagates as an error.
    #[test]
    fn bad_discriminant_propagates() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(0)).unwrap();
        let off = core::mem::offset_of!(Trade, aggressor);
        body[off] = 0x7F;
        let first = read_records(&body).next().unwrap();
        assert!(matches!(first, Err(PbnError::BadDiscriminant { .. })));
    }
}
