//! `NasdaqAdapter` emission contract: one test per wire kind proves the
//! kind-to-record mapping, plus a coverage-matrix check and a checked-in
//! sample-dump parity replay. Feature-gated with `adapter` since the adapter
//! only exists behind that feature.
#![cfg(feature = "adapter")]

use md_events::{
    Action, BreakRefKind, ControlKind, Price, RecordFlags, SentinelScheme, Side, TradingState,
};
use md_pipeline::{BundledTzdb, MarketEvent, StreamId, VenueAdapter, VenueId};
use proto_nasdaq::{
    adapter::{Config, NasdaqAdapter},
    itch_v5::*,
};

const ITCH: StreamId = StreamId(0);

fn synthetic_row(byte: u8, len: usize) -> Vec<u8> {
    let mut row = vec![0u8; len];
    row[0] = byte;
    for (j, slot) in row.iter_mut().enumerate().skip(1) {
        *slot = byte ^ (j as u8).wrapping_mul(37);
    }
    row
}

fn adapter() -> NasdaqAdapter {
    NasdaqAdapter::new(Config {
        venue: VenueId(1),
        session_midnight_ns: 0,
    })
}

fn feed_one(a: &mut NasdaqAdapter, row: &[u8]) -> Vec<MarketEvent> {
    // decoded+1: consecutive per adapter instance, so repeated feed_one
    // calls on one adapter never look like a sequence gap.
    let venue_seq = a.stats().decoded + 1;
    a.feed(ITCH, row, 100, venue_seq);
    a.drain().collect()
}

/// A live runner replaces the anchor at each New York trade-date roll. The
/// 2026-03-08 spring-forward puts 09:30 ET at 14:30Z on 03-07 and 13:30Z on
/// 03-09, 47h apart; nothing else in the record moves.
#[test]
fn session_anchor_follows_new_york_dst() {
    let mut a = adapter();
    let session = a.session();
    let tz = BundledTzdb::new();
    let mut add = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
    add[5..11].copy_from_slice(&34_200_000_000_000u64.to_be_bytes()[2..]);
    let mut stamp = |date| {
        a.set_session_anchor(session.resolve_anchor(date, &tz).unwrap());
        match feed_one(&mut a, &add)[..] {
            [MarketEvent::Mbo(m)] => m,
            ref other => panic!("expected one Mbo, got {other:?}"),
        }
    };
    let before_dst = stamp(20260307);
    let after_dst = stamp(20260309);
    // 2026-03-07T14:30:00Z and 2026-03-09T13:30:00Z
    assert_eq!(before_dst.hdr.ts_event, 1_772_893_800_000_000_000);
    assert_eq!(after_dst.hdr.ts_event, 1_773_063_000_000_000_000);
    let mut unmoved = after_dst;
    unmoved.hdr.ts_event = before_dst.hdr.ts_event;
    unmoved.hdr.venue_seq = before_dst.hdr.venue_seq;
    assert_eq!(unmoved, before_dst);
}

#[test]
fn maps_system_event_to_control() {
    let row = synthetic_row(b'S', SystemEventView::SIZE);
    let v = SystemEventView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Control(c) => {
            assert_eq!(c.hdr.ts_event, v.timestamp());
            assert_eq!(c.aux, v.event_code() as u64);
            assert_eq!(
                c.kind,
                ControlKind::Heartbeat,
                "synthetic byte is not O or C"
            );
        }
        other => panic!("expected Control, got {other:?}"),
    }
}

#[test]
fn maps_stock_directory_to_definition() {
    let row = synthetic_row(b'R', StockDirectoryView::SIZE);
    let v = StockDirectoryView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Definition(d) => {
            assert_eq!(d.venue_native_id, v.stock_locate() as u64);
            assert_eq!(d.lot_size, v.round_lot_size());
            let mut expected_symbol = [0u8; 20];
            expected_symbol[..8].copy_from_slice(v.stock());
            assert_eq!(d.raw_symbol, expected_symbol);
        }
        other => panic!("expected Definition, got {other:?}"),
    }
}

#[test]
fn maps_stock_trading_action_to_status() {
    let row = synthetic_row(b'H', StockTradingActionView::SIZE);
    let v = StockTradingActionView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            let expected = match v.trading_state() {
                b'H' => TradingState::Halt,
                b'P' => TradingState::Pause,
                b'Q' => TradingState::IntradayAuction,
                b'T' => TradingState::Continuous,
                _ => TradingState::Unknown,
            };
            assert_eq!(s.state, expected);
            assert_eq!(s.venue_reason[0], v.trading_state());
            assert_eq!(&s.venue_reason[1..5], v.reason());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_reg_sho_restriction_to_status_keeping_current_state() {
    let row = synthetic_row(b'Y', RegShoRestrictionView::SIZE);
    let v = RegShoRestrictionView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(s.venue_reason[0], v.reg_sho_action());
            assert_eq!(
                s.state,
                TradingState::Unknown,
                "no prior status: state stays default"
            );
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_market_participant_position_to_status() {
    let row = synthetic_row(b'L', MarketParticipantPositionView::SIZE);
    let v = MarketParticipantPositionView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(&s.venue_reason[0..4], v.mpid());
            assert_eq!(s.venue_reason[4], v.primary_market_maker());
            assert_eq!(s.venue_reason[5], v.market_maker_mode());
            assert_eq!(s.venue_reason[6], v.market_participant_state());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_mwcb_decline_level_to_market_wide_status() {
    let row = synthetic_row(b'V', MwcbDeclineLevelView::SIZE);
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(s.hdr.instrument_id, 0, "MWCB levels are market-wide");
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_mwcb_status_to_market_wide_halt() {
    let row = synthetic_row(b'W', MwcbStatusView::SIZE);
    let v = MwcbStatusView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(s.hdr.instrument_id, 0, "MWCB breach is market-wide");
            assert_eq!(s.state, TradingState::Halt);
            assert_eq!(s.venue_reason[0], v.breached_level());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_ipo_quoting_period_update_to_status() {
    let row = synthetic_row(b'K', IpoQuotingPeriodUpdateView::SIZE);
    let v = IpoQuotingPeriodUpdateView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(
                &s.venue_reason[0..4],
                &v.ipo_quotation_release_time().to_be_bytes()
            );
            assert_eq!(s.venue_reason[4], v.ipo_quotation_release_qualifier());
            assert_eq!(&s.venue_reason[5..9], &v.ipo_price().to_be_bytes());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_luld_auction_collar_to_status() {
    let row = synthetic_row(b'J', LuldAuctionCollarView::SIZE);
    let v = LuldAuctionCollarView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(
                &s.venue_reason[0..4],
                &v.auction_collar_reference_price().to_be_bytes()
            );
            assert_eq!(
                &s.venue_reason[4..8],
                &v.upper_auction_collar_price().to_be_bytes()
            );
            assert_eq!(
                &s.venue_reason[8..12],
                &v.lower_auction_collar_price().to_be_bytes()
            );
            assert_eq!(
                &s.venue_reason[12..16],
                &v.auction_collar_extension().to_be_bytes()
            );
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_operational_halt_to_status() {
    let row = synthetic_row(b'h', OperationalHaltView::SIZE);
    let v = OperationalHaltView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            let expected = match v.operational_halt_action() {
                b'H' => TradingState::Halt,
                b'T' => TradingState::Continuous,
                _ => TradingState::Unknown,
            };
            assert_eq!(s.state, expected);
            assert_eq!(s.venue_reason[0], v.market_code());
            assert_eq!(s.venue_reason[1], v.operational_halt_action());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_add_order_no_mpid_to_mbo_add() {
    let row = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
    let v = AddOrderNoMpidView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Add);
            assert_eq!(m.order_id, v.order_reference_number());
            assert_eq!(m.qty, v.shares() as u64);
            let expected_price =
                Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
            assert_eq!(m.price, expected_price);
            let expected_side = match v.buy_sell_indicator() {
                b'B' => Side::Bid,
                b'S' => Side::Ask,
                _ => Side::None,
            };
            assert_eq!(m.side, expected_side);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

// a skipped venue_seq is a real venue-side gap: only real wire threading
// can detect this, since an adapter-invented counter never skips.
#[test]
fn skipped_venue_seq_flags_gap_suspect() {
    let row = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
    let mut a = adapter();

    a.feed(ITCH, &row, 100, 1);
    match a.drain().next() {
        Some(MarketEvent::Mbo(m)) => {
            assert!(
                !m.hdr.flags.contains(RecordFlags::GAP_SUSPECT),
                "first message on a fresh adapter must not flag a gap"
            );
        }
        other => panic!("expected Mbo, got {other:?}"),
    }

    a.feed(ITCH, &row, 100, 4); // skips 2, 3
    match a.drain().next() {
        Some(MarketEvent::Mbo(m)) => {
            assert!(
                m.hdr.flags.contains(RecordFlags::GAP_SUSPECT),
                "skipped venue_seq must flag a gap an adapter-invented counter could never detect"
            );
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

#[test]
fn maps_add_order_mpid_attribution_to_mbo_add_with_participant() {
    let row = synthetic_row(b'F', AddOrderMpidAttributionView::SIZE);
    let v = AddOrderMpidAttributionView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Add);
            let mut expected_participant = [0u8; 7];
            expected_participant[..4].copy_from_slice(v.attribution());
            assert_eq!(m.participant, expected_participant);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

#[test]
fn maps_order_executed_to_mbo_fill_and_trade() {
    let row = synthetic_row(b'E', OrderExecutedView::SIZE);
    let v = OrderExecutedView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(
        recs.len(),
        2,
        "OrderExecuted is both a book fill and a tape print"
    );
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Fill);
            assert_eq!(m.order_id, v.order_reference_number());
            assert_eq!(m.qty, v.executed_shares() as u64);
            assert_eq!(
                m.side,
                Side::None,
                "no prior Add tracked: standalone fallback"
            );
            assert_eq!(
                m.hdr.instrument_id,
                v.stock_locate() as u32,
                "stock_locate fallback id"
            );
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
    match recs[1] {
        MarketEvent::Trade(t) => {
            assert_eq!(t.trade_ref, v.match_number());
            assert_eq!(t.qty, v.executed_shares() as u64);
            assert_eq!(
                t.aggressor,
                Side::None,
                "unknown resting side stays None, never inverted into garbage"
            );
        }
        other => panic!("expected Trade, got {other:?}"),
    }
}

// aggressor contract: OrderExecuted's side is the RESTING order's, threaded
// from its Add; the emitted taker side is the opposite.
#[test]
fn order_executed_inverts_resting_side_into_aggressor() {
    let mut a = adapter();
    let mut add = vec![0u8; AddOrderNoMpidView::SIZE];
    add[0] = b'A';
    add[11..19].copy_from_slice(&77u64.to_be_bytes()); // order_reference_number
    add[19] = b'B'; // resting side: Bid
    add[20..24].copy_from_slice(&500u32.to_be_bytes()); // shares
    add[24..32].copy_from_slice(b"TEST    ");
    add[32..36].copy_from_slice(&1_000u32.to_be_bytes()); // price
    feed_one(&mut a, &add);

    let mut exec = vec![0u8; OrderExecutedView::SIZE];
    exec[0] = b'E';
    exec[11..19].copy_from_slice(&77u64.to_be_bytes());
    exec[19..23].copy_from_slice(&200u32.to_be_bytes()); // executed_shares
    exec[23..31].copy_from_slice(&999u64.to_be_bytes()); // match_number
    let recs = feed_one(&mut a, &exec);
    assert_eq!(recs.len(), 2);
    match recs[0] {
        MarketEvent::Mbo(m) => assert_eq!(m.side, Side::Bid, "book event keeps resting side"),
        other => panic!("expected Mbo, got {other:?}"),
    }
    match recs[1] {
        MarketEvent::Trade(t) => {
            assert_eq!(t.trade_ref, 999);
            assert_eq!(t.aggressor, Side::Ask, "taker sits opposite resting Bid");
        }
        other => panic!("expected Trade, got {other:?}"),
    }
}

#[test]
fn maps_order_executed_with_price_to_mbo_and_trade() {
    let row = synthetic_row(b'C', OrderExecutedWithPriceView::SIZE);
    let v = OrderExecutedWithPriceView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 2);
    let expected_price =
        Price::from_venue(v.execution_price() as i64, 4, SentinelScheme::NoSentinel).raw();
    let expected_flags = if v.printable() == b'N' {
        RecordFlags::NON_PRINTABLE
    } else {
        RecordFlags::NONE
    };
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::FillWithPrice);
            assert_eq!(m.price, expected_price);
            assert_eq!(m.hdr.flags, expected_flags);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
    match recs[1] {
        MarketEvent::Trade(t) => {
            assert_eq!(t.price, expected_price);
            assert_eq!(t.hdr.flags, expected_flags);
            assert_eq!(t.trade_ref, v.match_number());
        }
        other => panic!("expected Trade, got {other:?}"),
    }
}

#[test]
fn maps_order_cancel_to_mbo_cancel_partial() {
    let row = synthetic_row(b'X', OrderCancelView::SIZE);
    let v = OrderCancelView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Cancel);
            assert_eq!(m.order_id, v.order_reference_number());
            assert_eq!(m.qty, v.cancelled_shares() as u64);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

#[test]
fn maps_order_delete_to_mbo_cancel_full() {
    let mut a = adapter();
    let mut add = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
    add[11..19].copy_from_slice(&101u64.to_be_bytes());
    add[20..24].copy_from_slice(&73u32.to_be_bytes());
    feed_one(&mut a, &add);

    let mut row = synthetic_row(b'D', OrderDeleteView::SIZE);
    row[11..19].copy_from_slice(&101u64.to_be_bytes());
    let v = OrderDeleteView::parse(&row).unwrap();
    let recs = feed_one(&mut a, &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Cancel);
            assert_eq!(m.order_id, v.order_reference_number());
            assert_eq!(m.qty, 73, "full delete carries the remaining quantity");
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

#[test]
fn maps_order_replace_to_cancel_then_add() {
    let mut a = adapter();
    let mut add = synthetic_row(b'F', AddOrderMpidAttributionView::SIZE);
    add[11..19].copy_from_slice(&101u64.to_be_bytes());
    add[20..24].copy_from_slice(&80u32.to_be_bytes());
    add[36..40].copy_from_slice(b"ABCD");
    feed_one(&mut a, &add);

    let mut row = synthetic_row(b'U', OrderReplaceView::SIZE);
    row[11..19].copy_from_slice(&101u64.to_be_bytes());
    row[19..27].copy_from_slice(&202u64.to_be_bytes());
    let v = OrderReplaceView::parse(&row).unwrap();
    let recs = feed_one(&mut a, &row);
    assert_eq!(recs.len(), 2);
    let (cancel, add) = match (recs[0], recs[1]) {
        (MarketEvent::Mbo(cancel), MarketEvent::Mbo(add)) => (cancel, add),
        other => panic!("expected two Mbo records, got {other:?}"),
    };
    assert_eq!(cancel.action, Action::Cancel);
    assert_eq!(cancel.order_id, 101);
    assert_eq!(cancel.qty, 80);
    assert_eq!(&cancel.participant[..4], b"ABCD");
    assert_eq!(add.action, Action::Add);
    assert_eq!(add.order_id, v.new_order_reference_number());
    assert_eq!(add.qty, v.shares() as u64);
    assert_eq!(&add.participant[..4], b"ABCD");
    assert_eq!(cancel.hdr.ts_event, add.hdr.ts_event);
    assert_eq!(cancel.hdr.ts_recv, add.hdr.ts_recv);
    assert_eq!(cancel.hdr.venue_seq, add.hdr.venue_seq);
    {
        let expected_price =
            Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
        assert_eq!(add.price, expected_price);
    }
}

#[test]
fn maps_trade_to_trade_record_with_unknown_aggressor() {
    let row = synthetic_row(b'P', TradeView::SIZE);
    let v = TradeView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Trade(t) => {
            let expected_price =
                Price::from_venue(v.price() as i64, 4, SentinelScheme::NoSentinel).raw();
            assert_eq!(t.price, expected_price);
            assert_eq!(t.qty, v.shares() as u64);
            assert_eq!(t.trade_ref, v.match_number());
            assert_eq!(
                t.aggressor,
                Side::None,
                "buy_sell_indicator frozen at B since 2014"
            );
        }
        other => panic!("expected Trade, got {other:?}"),
    }
}

#[test]
fn maps_cross_trade_to_trade_record() {
    let row = synthetic_row(b'Q', CrossTradeView::SIZE);
    let v = CrossTradeView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Trade(t) => {
            let expected_price =
                Price::from_venue(v.cross_price() as i64, 4, SentinelScheme::NoSentinel).raw();
            assert_eq!(t.price, expected_price);
            assert_eq!(t.qty, v.shares());
            assert_eq!(t.trade_ref, v.match_number());
        }
        other => panic!("expected Trade, got {other:?}"),
    }
}

#[test]
fn maps_broken_trade_to_trade_break() {
    let row = synthetic_row(b'B', BrokenTradeView::SIZE);
    let v = BrokenTradeView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::TradeBreak(tb) => {
            assert_eq!(tb.break_ref, v.match_number());
            assert_eq!(tb.ref_kind, BreakRefKind::MatchNumber);
        }
        other => panic!("expected TradeBreak, got {other:?}"),
    }
}

#[test]
fn maps_noii_to_imbalance() {
    let row = synthetic_row(b'I', NetOrderImbalanceIndicatorView::SIZE);
    let v = NetOrderImbalanceIndicatorView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Imbalance(imb) => {
            let expected_ref = Price::from_venue(
                v.current_reference_price() as i64,
                4,
                SentinelScheme::NoSentinel,
            )
            .raw();
            assert_eq!(imb.ref_px, expected_ref);
            assert_eq!(imb.paired_qty, v.paired_shares());
            assert_eq!(imb.imbalance_qty, v.imbalance_shares());
            let expected_side = match v.imbalance_direction() {
                b'B' => Side::Bid,
                b'S' => Side::Ask,
                _ => Side::None,
            };
            assert_eq!(imb.imbalance_side, expected_side);
            assert_eq!(imb.cross_type, v.cross_type());
            assert!(
                Price(imb.eq_px).is_null(),
                "NASDAQ has no equilibrium price"
            );
            assert_eq!(imb.eq_qty, 0);
        }
        other => panic!("expected Imbalance, got {other:?}"),
    }
}

#[test]
fn maps_retail_price_improvement_indicator_to_status() {
    let row = synthetic_row(b'N', RetailPriceImprovementIndicatorView::SIZE);
    let v = RetailPriceImprovementIndicatorView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(s.venue_reason[0], v.interest_flag());
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[test]
fn maps_direct_listing_price_discovery_to_status() {
    let row = synthetic_row(b'O', DirectListingCapitalRaisePriceDiscoveryView::SIZE);
    let v = DirectListingCapitalRaisePriceDiscoveryView::parse(&row).unwrap();
    let recs = feed_one(&mut adapter(), &row);
    assert_eq!(recs.len(), 1);
    match recs[0] {
        MarketEvent::Status(s) => {
            assert_eq!(s.venue_reason[0], v.open_eligibility_status());
            assert_eq!(
                &s.venue_reason[1..5],
                &v.minimum_allowable_price().to_be_bytes()
            );
            assert_eq!(
                &s.venue_reason[5..9],
                &v.maximum_allowable_price().to_be_bytes()
            );
            assert_eq!(
                &s.venue_reason[9..13],
                &v.near_execution_price().to_be_bytes()
            );
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

// Order-book kinds carry only order_reference_number, never side or symbol,
// on Executed/Cancel/Delete/Replace. This proves the adapter's own order
// table (not the fallback) threads side/instrument through a lifecycle.
#[test]
fn order_state_threads_side_and_instrument_across_lifecycle() {
    let mut a = adapter();

    let mut add_row = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
    add_row[19] = b'S'; // pin buy_sell_indicator so side is deterministic
    let add_qty = AddOrderNoMpidView::parse(&add_row).unwrap().shares() as u64;
    let add_recs = feed_one(&mut a, &add_row);
    let (order_id, instrument_id) = match add_recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.side, Side::Ask);
            (m.order_id, m.hdr.instrument_id)
        }
        other => panic!("expected Mbo, got {other:?}"),
    };

    let mut exec_row = synthetic_row(b'E', OrderExecutedView::SIZE);
    exec_row[11..19].copy_from_slice(&order_id.to_be_bytes());
    exec_row[19..23].copy_from_slice(&1u32.to_be_bytes()); // partial fill: order stays resting for Replace/Delete below
    let exec_recs = feed_one(&mut a, &exec_row);
    match exec_recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(
                m.side,
                Side::Ask,
                "side inherited from Add, not resent on Executed"
            );
            assert_eq!(m.hdr.instrument_id, instrument_id);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }

    let mut replace_row = synthetic_row(b'U', OrderReplaceView::SIZE);
    replace_row[11..19].copy_from_slice(&order_id.to_be_bytes());
    let new_id = u64::from_be_bytes(replace_row[19..27].try_into().unwrap());
    let replace_recs = feed_one(&mut a, &replace_row);
    match replace_recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Cancel);
            assert_eq!(m.order_id, order_id);
            assert_eq!(m.qty, add_qty - 1);
            assert_eq!(
                m.side,
                Side::Ask,
                "side survives replace, not resent on the wire"
            );
            assert_eq!(m.hdr.instrument_id, instrument_id);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
    match replace_recs[1] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Add);
            assert_eq!(m.order_id, new_id);
            assert_eq!(m.side, Side::Ask);
            assert_eq!(m.hdr.instrument_id, instrument_id);
        }
        other => panic!("expected Mbo, got {other:?}"),
    }

    let mut delete_row = synthetic_row(b'D', OrderDeleteView::SIZE);
    delete_row[11..19].copy_from_slice(&new_id.to_be_bytes());
    let delete_recs = feed_one(&mut a, &delete_row);
    match delete_recs[0] {
        MarketEvent::Mbo(m) => {
            assert_eq!(m.action, Action::Cancel);
            assert_eq!(
                m.qty,
                OrderReplaceView::parse(&replace_row).unwrap().shares() as u64
            );
            assert_eq!(m.side, Side::Ask, "side survives to the final delete");
        }
        other => panic!("expected Mbo, got {other:?}"),
    }
}

// A full ITCH day sends no Delete after a full execution: eviction happens
// on the fill path itself, or the order table grows unbounded all session.
#[test]
fn full_execution_evicts_state() {
    let mut a = adapter();
    let order_count = 200u64;

    for i in 0..order_count {
        let mut add_row = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
        add_row[11..19].copy_from_slice(&(i + 1).to_be_bytes()); // order_reference_number
        let add_recs = feed_one(&mut a, &add_row);
        let order_id = match add_recs[0] {
            MarketEvent::Mbo(m) => m.order_id,
            other => panic!("expected Mbo, got {other:?}"),
        };
        assert_eq!(order_id, i + 1);
    }
    assert_eq!(a.order_count() as u64, order_count, "every Add rests");

    for i in 0..order_count {
        let add_row = synthetic_row(b'A', AddOrderNoMpidView::SIZE);
        let v = AddOrderNoMpidView::parse(&add_row).unwrap();
        let mut exec_row = synthetic_row(b'E', OrderExecutedView::SIZE);
        exec_row[11..19].copy_from_slice(&(i + 1).to_be_bytes()); // order_reference_number
        exec_row[19..23].copy_from_slice(&v.shares().to_be_bytes()); // full fill, same qty as the Add
        feed_one(&mut a, &exec_row);
    }

    assert_eq!(
        a.order_count(),
        0,
        "full execution with zero deletes must still evict every resting order"
    );
}

#[test]
fn coverage_matrix_matches_dispatch_arms() {
    let all_23 = *b"SRHYLVWKJhAFECXDUPQBINO";
    let matrix = NasdaqAdapter::coverage(ITCH);
    assert_eq!(matrix.matches_dispatch(&all_23), Ok(()));
}

// Fed alone to a fresh adapter, each decoded kind emits an `Mbo` exactly when
// book-applied, and an `Mbo`, `Trade` or `TradeBreak` exactly when handled.
#[test]
fn coverage_book_and_handled_axes_match_emission() {
    let m = NasdaqAdapter::coverage(ITCH);
    for kind in m.decoded_kinds() {
        let len = BYTE_LEN[usize::from(kind)].expect("every decoded kind is fixed width");
        let recs = feed_one(&mut adapter(), &synthetic_row(kind, usize::from(len.get())));
        let book = recs.iter().any(|e| matches!(e, MarketEvent::Mbo(_)));
        let tape = recs
            .iter()
            .any(|e| matches!(e, MarketEvent::Trade(_) | MarketEvent::TradeBreak(_)));
        let cov = m.get(kind).expect("decoded kind has a row");
        assert_eq!(
            (cov.book_applied, cov.handled()),
            (book, book || tape),
            "kind {}",
            kind as char
        );
    }
}

// Checked-in fixture: 524320 bytes, 14348 whole ITCH 5.0 messages
// (S=1, R=10473, H=1937, Y=1937). Not gated by #[ignore]: it is small and
// local, no network or external service per the workspace unit-test rule.
#[test]
fn fixture_replay_decodes_full_prefix_with_zero_errors() {
    use md_pipeline::{IngestError, IngestFrame, IngestSource};
    use md_replay::BinaryFileReplay;

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/nasdaq_itch_v5_prefix.bin"
    );
    let mut replay = BinaryFileReplay::from_path(path, 0, 1).expect("fixture opens");
    let mut a = adapter();
    let mut frames = Vec::new();
    let mut definitions = 0u64;
    let mut controls = 0u64;
    let mut statuses = 0u64;
    let mut venue_seq = 0u64;
    loop {
        frames.clear();
        match replay.recv_batch(&mut frames, 256) {
            Ok(0) => break,
            Ok(_) => {}
            Err(IngestError::Closed) => break,
            Err(e) => panic!("unexpected ingest error: {e}"),
        }
        for f in &frames {
            venue_seq += 1;
            a.feed(ITCH, f.payload(), f.ts_recv(), venue_seq);
            for rec in a.drain() {
                match rec {
                    MarketEvent::Definition(_) => definitions += 1,
                    MarketEvent::Control(_) => controls += 1,
                    MarketEvent::Status(_) => statuses += 1,
                    _ => {}
                }
            }
        }
    }

    assert_eq!(a.stats().decoded, 14_348);
    assert_eq!(a.stats().unknown_kind, 0);
    assert_eq!(a.stats().malformed, 0);
    assert_eq!(definitions, 10_473, "one Definition per R row");
    assert_eq!(controls, 1, "one Control per S row");
    assert_eq!(statuses, 1_937 + 1_937, "H and Y both map to Status");
}

// Full-day proof against a real gzip sample. Ignored: the file is multi-gigabyte
// and lives outside the repo, so the operator opts in with the path.
//   NASDAQ_ITCH_SAMPLE=/path/to/day.gz cargo test -p proto-nasdaq --features adapter -- --ignored
#[test]
#[ignore = "needs a real full-day .gz via NASDAQ_ITCH_SAMPLE"]
fn full_day_gz_replay_has_no_unknown_kinds() {
    use md_pipeline::{IngestError, IngestFrame, IngestSource};
    use md_replay::BinaryFileReplay;

    let Ok(path) = std::env::var("NASDAQ_ITCH_SAMPLE") else {
        panic!("set NASDAQ_ITCH_SAMPLE to a full-day NASDAQ ITCH 5.0 .gz");
    };
    let mut replay = BinaryFileReplay::from_gz(&path, 0, 1).expect("gz sample opens");
    let mut a = adapter();
    let mut frames = Vec::new();
    let mut venue_seq = 0u64;
    loop {
        frames.clear();
        match replay.recv_batch(&mut frames, 4096) {
            Ok(0) => break,
            Ok(_) => {}
            Err(IngestError::Closed) => break,
            Err(e) => panic!("unexpected ingest error: {e}"),
        }
        for f in &frames {
            venue_seq += 1;
            a.feed(ITCH, f.payload(), f.ts_recv(), venue_seq);
            a.drain().for_each(drop);
        }
    }
    assert_eq!(
        a.stats().unknown_kind,
        0,
        "every wire kind is a known ITCH 5.0 kind"
    );
    assert_eq!(a.stats().malformed, 0, "framing holds for the whole day");
    assert!(
        a.stats().decoded > 14_348,
        "a full day dwarfs the checked-in prefix"
    );
}
