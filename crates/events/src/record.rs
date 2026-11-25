//! The `Record` trait and the skip-unknown-tail decode rule.
//!
//! The record data types (records, `rtype` bands, `Category`, `REGISTERED`, and the
//! `Record` impls) are generated from `schemas/records/l0_l3.yaml` into
//! `crate::generated`. This module carries only the logic layered on them: the trait
//! `decode_prefix` is bound to, and the trait itself.

use core::any::Any;

use crate::generated::{Definition, RecordHeader};

/// A fixed-layout record with a known rtype and header. `Copy` guarantees the
/// byte-pattern is trivially relocatable, which the tail-skip decode relies on;
/// `'static` lets [`decode_prefix`] tell [`Definition`] apart for its short read.
pub trait Record: Copy + 'static {
    /// This record's rtype byte.
    const RTYPE: u8;
    /// Borrow the common header.
    fn header(&self) -> &RecordHeader;
}

/// Decode the compiled prefix of record `T` from `buf`, returning the value and
/// the full on-wire length taken from the header (`length` words). Bytes beyond
/// `size_of::<T>()` are the skip-unknown-tail region: an older reader decodes
/// its known fields and advances by the returned length. Returns `None` if the
/// buffer is too short or the header length is smaller than `T`, except for a
/// [`Definition`] written before its tail ([`Definition::PRE_TAIL_LEN`] bytes),
/// which decodes with the tail absent and `hdr.length` set to the full record.
///
/// Precondition: `buf` holds a well-formed record produced by our own writer
/// (valid enum discriminants); this is not a validator for untrusted wire bytes.
pub fn decode_prefix<T: Record>(buf: &[u8]) -> Option<(T, usize)> {
    if buf.len() < core::mem::size_of::<RecordHeader>() {
        return None;
    }
    let total = (buf[0] as usize) * 4; // `length` is 32-bit words, at offset 0
    if buf.len() < total {
        return None;
    }
    if total < core::mem::size_of::<T>() {
        return decode_short::<T>(&buf[..total]).map(|rec| (rec, total));
    }
    let mut val = core::mem::MaybeUninit::<T>::uninit();
    // SAFETY: copy exactly size_of::<T>() bounds-checked bytes into an uninit T.
    // T is a Copy repr(C) record; a byte pattern our writer produced is a valid
    // value. Downcast to *mut u8 keeps the copy byte-wise, no alignment need.
    unsafe {
        core::ptr::copy_nonoverlapping(
            buf.as_ptr(),
            val.as_mut_ptr() as *mut u8,
            core::mem::size_of::<T>(),
        );
        Some((val.assume_init(), total))
    }
}

/// A record shorter than `T` decodes only when `T` is [`Definition`]. The
/// downcast is the type check, so no other record's bytes are read as one.
fn decode_short<T: Record>(image: &[u8]) -> Option<T> {
    let mut out: Option<T> = None;
    let slot = (&mut out as &mut dyn Any).downcast_mut::<Option<Definition>>()?;
    *slot = Definition::decode_pre_tail(image);
    out
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, offset_of, size_of};

    use crate::{
        Action, Bbo, BreakRefKind, Category, Control, ControlKind, Definition, DefinitionTail,
        Fill, Imbalance, Level, LevelAction, Mbo, Mbp10, Ohlcv, Price, REGISTERED, Record,
        RecordFlags, RecordHeader, Side, StatBand, StatType, Stats, Status, Trade, TradeBreak,
        TradingState, decode_prefix,
        generated::{AssetClass, BidAskPair, rtype},
    };

    // The header layout is the on-disk contract; these offsets must never drift
    // silently or every capture file and adapter breaks.
    #[test]
    fn layout_locked() {
        assert_eq!(size_of::<RecordHeader>(), 56);
        assert_eq!(align_of::<RecordHeader>(), 8);
        assert_eq!(offset_of!(RecordHeader, length), 0);
        assert_eq!(offset_of!(RecordHeader, rtype), 1);
        assert_eq!(offset_of!(RecordHeader, venue), 2);
        assert_eq!(offset_of!(RecordHeader, instrument_id), 4);
        assert_eq!(offset_of!(RecordHeader, ts_event), 8);
        assert_eq!(offset_of!(RecordHeader, ts_recv), 16);
        assert_eq!(offset_of!(RecordHeader, ts_egress), 24);
        assert_eq!(offset_of!(RecordHeader, venue_seq), 32);
        assert_eq!(offset_of!(RecordHeader, egress_seq), 40);
        assert_eq!(offset_of!(RecordHeader, flags), 48);
        // every record is a whole number of 32-bit words so `length` is exact
        for sz in [
            size_of::<Mbo>(),
            size_of::<Level>(),
            size_of::<Mbp10>(),
            size_of::<Bbo>(),
            size_of::<Trade>(),
            size_of::<Status>(),
            size_of::<Stats>(),
            size_of::<TradeBreak>(),
            size_of::<Ohlcv>(),
            size_of::<Control>(),
            size_of::<Imbalance>(),
            size_of::<Fill>(),
        ] {
            assert_eq!(sz % 4, 0, "record size {sz} not word-aligned");
        }
    }

    // No record may carry implicit padding: verbatim serialization (capture
    // writer, egress tap) ships `size_of` bytes, and implicit pad would be
    // uninitialized, non-reproducible, and UB to read. Each expected size is the
    // exact sum of field sizes (`_pad` included); since `size_of >= that sum`
    // always, equality proves zero implicit padding. The trailing `_pad` checks
    // confirm the explicit pad ends the struct.
    #[test]
    fn no_implicit_padding() {
        assert_eq!(size_of::<Mbo>(), 96);
        assert_eq!(size_of::<Level>(), 80);
        assert_eq!(size_of::<Status>(), 80);
        assert_eq!(size_of::<Stats>(), 88);
        assert_eq!(size_of::<TradeBreak>(), 72);
        assert_eq!(size_of::<Control>(), 72);
        assert_eq!(size_of::<Trade>(), 88);
        assert_eq!(size_of::<Ohlcv>(), 120);
        assert_eq!(size_of::<Imbalance>(), 120);
        assert_eq!(size_of::<Mbp10>(), 480);
        assert_eq!(size_of::<Bbo>(), 96);
        assert_eq!(size_of::<Fill>(), 96);

        assert_eq!(offset_of!(Mbo, _pad) + 7, size_of::<Mbo>());
        assert_eq!(offset_of!(Level, _pad) + 3, size_of::<Level>());
        assert_eq!(offset_of!(Status, _pad) + 3, size_of::<Status>());
        assert_eq!(offset_of!(Stats, _pad) + 5, size_of::<Stats>());
        assert_eq!(offset_of!(TradeBreak, _pad) + 7, size_of::<TradeBreak>());
        assert_eq!(offset_of!(Control, _pad) + 7, size_of::<Control>());
        assert_eq!(offset_of!(Trade, _pad) + 7, size_of::<Trade>());
        assert_eq!(offset_of!(Ohlcv, _pad) + 4, size_of::<Ohlcv>());
        assert_eq!(offset_of!(Imbalance, _pad) + 6, size_of::<Imbalance>());
    }

    // The tail starts on the word after the one-byte pad at 103, so every
    // field before it keeps its offset and the tail fields sit at fixed ones.
    #[test]
    fn definition_tail_layout() {
        assert_eq!(size_of::<Definition>(), 176);
        assert_eq!(offset_of!(Definition, _pad), 103);
        assert_eq!(offset_of!(Definition, tail), 104);
        assert_eq!(size_of::<DefinitionTail>(), 72);

        assert_eq!(offset_of!(Definition, tail.tick_size), 104);
        assert_eq!(offset_of!(Definition, tail.contract_multiplier), 112);
        assert_eq!(offset_of!(Definition, tail.min_qty), 120);
        assert_eq!(offset_of!(Definition, tail.expiration_ns), 128);
        assert_eq!(offset_of!(Definition, tail.strike), 136);
        assert_eq!(offset_of!(Definition, tail.underlying_id), 144);
        assert_eq!(offset_of!(Definition, tail.base_ccy), 148);
        assert_eq!(offset_of!(Definition, tail.quote_ccy), 156);
        assert_eq!(offset_of!(Definition, tail.settle_ccy), 164);
        assert_eq!(offset_of!(Definition, tail.price_scale), 172);
        assert_eq!(offset_of!(Definition, tail.option_type), 173);
        assert_eq!(
            offset_of!(Definition, tail._pad) + 2,
            size_of::<Definition>()
        );
    }

    // Registered rtypes sit in their declared band, none land in a reserved
    // band, and the assignments are unique.
    #[test]
    fn rtype_bands() {
        let mut seen = std::collections::HashSet::new();
        for &(rt, cat) in REGISTERED {
            assert_eq!(Category::of_rtype(rt), cat, "rtype {rt:#x} out of band");
            assert!(
                !matches!(
                    cat,
                    Category::ExecReserved | Category::QuoteRfqReserved | Category::AmmReserved
                ),
                "rtype {rt:#x} occupies a reserved band"
            );
            assert!(seen.insert(rt), "duplicate rtype {rt:#x}");
        }
        // reserved bands map to their reserved categories and stay unoccupied
        assert_eq!(Category::of_rtype(0x50), Category::ExecReserved);
        assert_eq!(Category::of_rtype(0x60), Category::QuoteRfqReserved);
        assert_eq!(Category::of_rtype(0x70), Category::AmmReserved);
        assert!(
            !REGISTERED
                .iter()
                .any(|&(rt, _)| (0x50..=0x7F).contains(&rt))
        );
    }

    // Fill's layout is locked the same way RecordHeader's is: offsets pinned
    // so the wire contract never drifts silently, and it must land in the
    // book/tape band alongside Trade/TradeBreak.
    #[test]
    fn fill_layout() {
        assert_eq!(size_of::<Fill>(), 96);
        assert_eq!(align_of::<Fill>(), 8);
        assert_eq!(offset_of!(Fill, hdr), 0);
        assert_eq!(offset_of!(Fill, price), 56);
        assert_eq!(offset_of!(Fill, qty), 64);
        assert_eq!(offset_of!(Fill, fill_ref), 72);
        assert_eq!(offset_of!(Fill, ref_kind), 80);
        assert_eq!(offset_of!(Fill, buyer), 81);
        assert_eq!(offset_of!(Fill, seller), 88);
        assert_eq!(offset_of!(Fill, aggressor), 95);
        assert_eq!(Fill::RTYPE, rtype::FILL);
        assert_eq!(rtype::FILL, 0x18);
        assert_eq!(Category::of_rtype(rtype::FILL), Category::BookTape);
    }

    // A record grown with appended tail fields decodes through an older
    // reader: known fields recover, reader skips the unknown tail by length.
    #[test]
    fn skip_unknown_tail() {
        let tail = 8usize;
        let grown = size_of::<Mbo>() + tail;
        // Build a valid Mbo wire image field-by-field (zeroed elsewhere) plus an
        // unknown appended tail; avoids reading struct padding.
        let mut buf = vec![0u8; grown];
        buf[offset_of!(RecordHeader, length)] = (grown / 4) as u8;
        buf[offset_of!(RecordHeader, rtype)] = rtype::MBO;
        buf[offset_of!(Mbo, order_id)..][..8].copy_from_slice(&9u64.to_ne_bytes());
        buf[offset_of!(Mbo, price)..][..8].copy_from_slice(&1_234_500_000i64.to_ne_bytes());
        buf[offset_of!(Mbo, action)] = Action::Add as u8;
        buf[offset_of!(Mbo, side)] = Side::Bid as u8;

        let (decoded, consumed) = decode_prefix::<Mbo>(&buf).expect("decode");
        assert_eq!(consumed, grown, "reader must advance past the tail");
        assert_eq!(decoded.order_id, 9);
        assert_eq!(decoded.price, 1_234_500_000);
        assert_eq!(decoded.action, Action::Add);
        assert_eq!(decoded.side, Side::Bid);
    }

    // Definition's appended status_flags/call_auction tail is additive per the
    // skip-unknown-tail rule: a reader compiled before the tail existed
    // still recovers the shared prefix and skips the new bytes.
    #[test]
    fn definition_tail_status_flags() {
        // Mirrors Definition's pre-tail shape (no status_flags/call_auction): stands in
        // for a reader compiled before this amendment.
        #[repr(C)]
        #[derive(Clone, Copy)]
        struct OldDefinition {
            hdr: RecordHeader,
            venue_native_id: u64,
            lot_size: u32,
            raw_symbol: [u8; 20],
            board: [u8; 4],
            asset_class: AssetClass,
            qty_scale: i8,
        }
        impl Record for OldDefinition {
            const RTYPE: u8 = rtype::DEFINITION;
            fn header(&self) -> &RecordHeader {
                &self.hdr
            }
        }

        let mut def = Definition::new(
            5,
            1,
            100,
            777,
            [b'X'; 20],
            *b"MAIN",
            AssetClass::Equity,
            0,
            100,
        );
        def.status_flags[0] = 0b0000_0101; // bits 0 and 2 set
        def.call_auction = 1;
        let grown = size_of::<Definition>();

        let mut buf = vec![0u8; grown];
        // SAFETY: Definition is repr(C) Copy; byte-copy into a same-size
        // buffer mirrors what a writer places on the wire.
        unsafe {
            core::ptr::copy_nonoverlapping(
                &def as *const Definition as *const u8,
                buf.as_mut_ptr(),
                grown,
            );
        }

        let (old, consumed) = decode_prefix::<OldDefinition>(&buf).expect("decode");
        assert_eq!(consumed, grown, "old reader must advance past the new tail");
        assert_eq!(old.venue_native_id, 777);
        assert_eq!(old.lot_size, 100);

        let (new, consumed2) = decode_prefix::<Definition>(&buf).expect("decode");
        assert_eq!(consumed2, grown);
        assert_eq!(new.status_flags[0], 0b0000_0101);
        assert_eq!(new.call_auction, 1);
    }

    // A 104-byte definition from a pre-tail writer reads with the tail absent
    // (null prices) and a 176-byte length word; a full tail round-trips.
    #[test]
    fn definition_written_before_tail_reads_with_absent_tail() {
        // The layout a writer emitted before the tail: 104 bytes, 26 words.
        #[repr(C)]
        #[derive(Clone, Copy)]
        struct PreTailDefinition {
            hdr: RecordHeader,
            venue_native_id: u64,
            lot_size: u32,
            raw_symbol: [u8; 20],
            board: [u8; 4],
            asset_class: AssetClass,
            qty_scale: i8,
            status_flags: [u8; 8],
            call_auction: u8,
            _pad: [u8; 1],
        }
        let mut hdr = Definition::new(5, 1, 100, 0, [0; 20], [0; 4], AssetClass::Equity, 0, 1).hdr;
        hdr.length = 26;
        let old = PreTailDefinition {
            hdr,
            venue_native_id: 777,
            lot_size: 100,
            raw_symbol: [b'X'; 20],
            board: *b"MAIN",
            asset_class: AssetClass::Etf,
            qty_scale: -2,
            status_flags: [0b0000_0101, 0, 0, 0, 0, 0, 0, 0],
            call_auction: 1,
            _pad: [0],
        };
        assert_eq!(size_of::<PreTailDefinition>(), Definition::PRE_TAIL_LEN);
        let mut buf = vec![0u8; Definition::PRE_TAIL_LEN];
        // SAFETY: PreTailDefinition is repr(C) Copy with explicit padding, so
        // all 104 bytes are initialized; the copy mirrors the old writer.
        unsafe {
            core::ptr::copy_nonoverlapping(
                &old as *const PreTailDefinition as *const u8,
                buf.as_mut_ptr(),
                buf.len(),
            );
        }

        let (def, consumed) = decode_prefix::<Definition>(&buf).expect("pre-tail definition");
        assert_eq!(consumed, 104, "walk advances by the wire length");
        let mut want = Definition::new(
            5,
            1,
            100,
            777,
            [b'X'; 20],
            *b"MAIN",
            AssetClass::Etf,
            -2,
            100,
        );
        want.status_flags = old.status_flags;
        want.call_auction = 1;
        assert_eq!(def, want);
        assert_eq!(def.hdr.length, 44);
        assert_eq!(def.tail, DefinitionTail::ABSENT);

        want.tail.price_scale = 4;
        want.tail.quote_ccy = *b"USD\0\0\0\0\0";
        want.tail.strike = 12_500_000_000;
        assert_round_trip(want);
    }

    // Lifecycle transitions carry their kind and kind-specific aux detail.
    #[test]
    fn control_lifecycle() {
        let hdr = RecordHeader {
            length: (size_of::<Control>() / 4) as u8,
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
        let gap = Control {
            hdr,
            kind: ControlKind::GapDetected,
            aux: 12,
            _pad: [0u8; 7],
        };
        let reset = Control {
            hdr,
            kind: ControlKind::SeqReset,
            aux: 1,
            _pad: [0u8; 7],
        };
        let snap = Control {
            hdr,
            kind: ControlKind::SnapshotBegin,
            aux: 0,
            _pad: [0u8; 7],
        };
        assert_eq!(gap.kind, ControlKind::GapDetected);
        assert_eq!(gap.aux, 12);
        assert_eq!(reset.kind, ControlKind::SeqReset);
        assert_eq!(snap.kind, ControlKind::SnapshotBegin);
        assert_eq!(Control::RTYPE, rtype::CONTROL);
    }

    // Same stat type, different `category`, is a distinct statistic (static vs
    // dynamic price limits), and PriceLimits sits in the equities band.
    #[test]
    fn stats_kinds() {
        let hdr = RecordHeader {
            length: (size_of::<Stats>() / 4) as u8,
            rtype: rtype::STATS,
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
        let stat_static = Stats {
            hdr,
            stat_type: StatType::PriceLimits,
            price: 1000,
            qty: 0,
            aux: 2000,
            category: 0,
            _pad: [0u8; 5],
        };
        let stat_dynamic = Stats {
            hdr,
            category: 1,
            ..stat_static
        };
        assert_eq!(stat_static.stat_type, StatType::PriceLimits);
        assert_ne!(stat_static.category, stat_dynamic.category);
        assert_eq!(StatType::PriceLimits.band(), StatBand::Equities);
    }

    // Auction imbalance carries the full NOII field set; prices the venue did
    // not provide are the null sentinel.
    #[test]
    fn imbalance_mapping() {
        let hdr = RecordHeader {
            length: (size_of::<Imbalance>() / 4) as u8,
            rtype: rtype::IMBALANCE,
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
        let imb = Imbalance {
            hdr,
            ref_px: 1_000_000_000,
            far_px: Price::NULL.raw(),
            near_px: Price::NULL.raw(),
            paired_qty: 300,
            imbalance_qty: 50,
            imbalance_side: Side::Bid,
            cross_type: b'O',
            eq_px: 1_000_500_000,
            eq_qty: 300,
            _pad: [0u8; 6],
        };
        assert_eq!(Imbalance::RTYPE, rtype::IMBALANCE);
        assert_eq!(imb.far_px, i64::MAX);
        assert!(Price(imb.near_px).is_null());
        assert_eq!(imb.imbalance_side, Side::Bid);
    }

    // The MBP synthesis provenance flag rides on the header and reads back; the
    // adapter sets it on every record derived from an MBP-native level feed.
    #[test]
    fn level_mbp_flag() {
        let level = Level {
            hdr: RecordHeader {
                length: (size_of::<Level>() / 4) as u8,
                rtype: rtype::LEVEL,
                venue: 1,
                instrument_id: 5,
                ts_event: 0,
                ts_recv: 0,
                ts_egress: 0,
                venue_seq: 0,
                egress_seq: 0,
                flags: RecordFlags::MBP_SYNTHESIZED,
                _pad: [0u8; 6],
            },
            side: Side::Ask,
            level: 0,
            action: LevelAction::New,
            price: 1_000_000_000,
            qty: 100,
            num_deletes: 0,
            _pad: [0u8; 3],
        };
        assert!(level.header().flags.contains(RecordFlags::MBP_SYNTHESIZED));
        assert_eq!(Level::RTYPE, rtype::LEVEL);
    }

    // Derived-view records carry their own rtypes in the book/tape band, and a
    // depth snapshot's empty levels default to the zeroed pair, not garbage.
    #[test]
    fn derived_view_records() {
        assert_eq!(Mbp10::RTYPE, rtype::MBP10);
        assert_eq!(Bbo::RTYPE, rtype::BBO);
        assert_eq!(Trade::RTYPE, rtype::TRADE);
        assert_eq!(Ohlcv::RTYPE, rtype::OHLCV);
        assert_eq!(
            BidAskPair::default(),
            BidAskPair {
                bid_px: 0,
                ask_px: 0,
                bid_sz: 0,
                ask_sz: 0,
                bid_ct: 0,
                ask_ct: 0,
            }
        );

        let hdr = RecordHeader {
            length: (size_of::<Trade>() / 4) as u8,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 5,
            ts_event: 100,
            ts_recv: 110,
            ts_egress: 0,
            venue_seq: 7,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        let trade = Trade {
            hdr,
            price: 1_000_000_000,
            qty: 50,
            aggressor: Side::Bid,
            trade_ref: 42,
            _pad: [0u8; 7],
        };
        assert_eq!(trade.header().venue_seq, 7);
        assert_eq!(trade.trade_ref, 42);
    }

    // Nonzero header fields: round-trip equality must not pass on zeroed-buffer echo.
    fn rt_hdr(rt: u8, size: usize) -> RecordHeader {
        RecordHeader {
            length: (size / 4) as u8,
            rtype: rt,
            venue: 3,
            instrument_id: 42,
            ts_event: 1_001,
            ts_recv: 1_002,
            ts_egress: 1_003,
            venue_seq: 7,
            egress_seq: 8,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        }
    }

    // Encode = verbatim struct byte-copy (writer contract), decode via
    // `decode_prefix`, compare whole value.
    fn assert_round_trip<T: Record + PartialEq + core::fmt::Debug>(val: T) {
        let n = size_of::<T>();
        let mut buf = vec![0u8; n];
        // SAFETY: T is repr(C) Copy, zero implicit padding (pinned by
        // no_implicit_padding); full-size byte-copy mirrors writer.
        unsafe {
            core::ptr::copy_nonoverlapping(&val as *const T as *const u8, buf.as_mut_ptr(), n);
        }
        let (decoded, consumed) = decode_prefix::<T>(&buf).expect("decode");
        assert_eq!(consumed, n);
        assert_eq!(decoded, val);
    }

    // Kinds without wire-decode coverage elsewhere; Mbo covered by
    // skip_unknown_tail, Definition by definition_tail_status_flags.
    #[test]
    fn record_round_trips() {
        assert_round_trip(Level {
            hdr: rt_hdr(rtype::LEVEL, size_of::<Level>()),
            price: 1_000_250_000,
            qty: 400,
            num_deletes: 2,
            side: Side::Ask,
            level: 3,
            action: LevelAction::Change,
            _pad: [0u8; 3],
        });
        let mut levels = [BidAskPair::default(); 10];
        levels[0] = BidAskPair {
            bid_px: 999_000_000,
            ask_px: 1_001_000_000,
            bid_sz: 10,
            ask_sz: 20,
            bid_ct: 1,
            ask_ct: 2,
        };
        assert_round_trip(Mbp10 {
            hdr: rt_hdr(rtype::MBP10, size_of::<Mbp10>()),
            price: 1_001_000_000,
            qty: 20,
            action: Action::Modify,
            side: Side::Bid,
            depth: 1,
            _pad: [0u8; 5],
            levels,
        });
        assert_round_trip(Bbo {
            hdr: rt_hdr(rtype::BBO, size_of::<Bbo>()),
            pair: levels[0],
        });
        assert_round_trip(Trade {
            hdr: rt_hdr(rtype::TRADE, size_of::<Trade>()),
            price: 1_000_000_000,
            qty: 50,
            trade_ref: 99,
            aggressor: Side::Ask,
            _pad: [0u8; 7],
        });
        assert_round_trip(TradeBreak {
            hdr: rt_hdr(rtype::TRADE_BREAK, size_of::<TradeBreak>()),
            break_ref: 77,
            ref_kind: BreakRefKind::SecondaryTradeId,
            _pad: [0u8; 7],
        });
        assert_round_trip(Ohlcv {
            hdr: rt_hdr(rtype::OHLCV, size_of::<Ohlcv>()),
            open: 100,
            high: 110,
            low: 90,
            close: 105,
            volume: 1_000,
            value: 104_000,
            resolution: 60_000_000_000,
            trades: 5,
            _pad: [0u8; 4],
        });
        assert_round_trip(Imbalance {
            hdr: rt_hdr(rtype::IMBALANCE, size_of::<Imbalance>()),
            ref_px: 1_000_000_000,
            far_px: Price::NULL.raw(),
            near_px: 1_000_250_000,
            paired_qty: 300,
            imbalance_qty: 50,
            eq_px: 1_000_500_000,
            eq_qty: 300,
            imbalance_side: Side::Bid,
            cross_type: b'O',
            _pad: [0u8; 6],
        });
        assert_round_trip(Fill {
            hdr: rt_hdr(rtype::FILL, size_of::<Fill>()),
            price: 1_000_000_000,
            qty: 25,
            fill_ref: 424_242,
            ref_kind: BreakRefKind::MatchNumber,
            buyer: *b"BUYER01",
            seller: *b"SELLER1",
            aggressor: Side::Bid,
        });
        assert_round_trip(Stats {
            hdr: rt_hdr(rtype::STATS, size_of::<Stats>()),
            price: 1_000_750_000,
            qty: 12,
            aux: 3,
            stat_type: StatType::Vwap,
            category: 1,
            _pad: [0u8; 5],
        });
        assert_round_trip(Status {
            hdr: rt_hdr(rtype::STATUS, size_of::<Status>()),
            state: TradingState::Halt,
            venue_reason: *b"volatility guard\0\0\0\0",
            _pad: [0u8; 3],
        });
        assert_round_trip(Control {
            hdr: rt_hdr(rtype::CONTROL, size_of::<Control>()),
            aux: 123,
            kind: ControlKind::RecoveryComplete,
            _pad: [0u8; 7],
        });
    }
}
