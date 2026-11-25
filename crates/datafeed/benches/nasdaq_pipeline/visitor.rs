//! Decode visitor: every method reads all of its view's fields through
//! `black_box`. Views decode on access (`from_be_bytes` per accessor), so a
//! visitor that ignored them would time dispatch alone.

use std::hint::black_box;

use proto_nasdaq::itch_v5;

pub struct DecodeVisitor;

impl itch_v5::HotVisitor for DecodeVisitor {
    fn system_event(&mut self, v: itch_v5::SystemEventView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.event_code());
    }
    fn stock_directory(&mut self, v: itch_v5::StockDirectoryView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.market_category());
        black_box(v.financial_status_indicator());
        black_box(v.round_lot_size());
        black_box(v.round_lots_only());
        black_box(v.issue_classification());
        black_box(v.issue_sub_type());
        black_box(v.authenticity());
        black_box(v.short_sale_threshold_indicator());
        black_box(v.ipo_flag());
        black_box(v.luld_reference_price_tier());
        black_box(v.etp_flag());
        black_box(v.etp_leverage_factor());
        black_box(v.inverse_indicator());
    }
    fn stock_trading_action(&mut self, v: itch_v5::StockTradingActionView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.trading_state());
        black_box(v.reserved());
        black_box(v.reason());
    }
    fn reg_sho_restriction(&mut self, v: itch_v5::RegShoRestrictionView<'_>) {
        black_box(v.locate_code());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.reg_sho_action());
    }
    fn market_participant_position(&mut self, v: itch_v5::MarketParticipantPositionView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.mpid());
        black_box(v.stock());
        black_box(v.primary_market_maker());
        black_box(v.market_maker_mode());
        black_box(v.market_participant_state());
    }
    fn mwcb_decline_level(&mut self, v: itch_v5::MwcbDeclineLevelView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.level_1());
        black_box(v.level_2());
        black_box(v.level_3());
    }
    fn mwcb_status(&mut self, v: itch_v5::MwcbStatusView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.breached_level());
    }
    fn ipo_quoting_period_update(&mut self, v: itch_v5::IpoQuotingPeriodUpdateView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.ipo_quotation_release_time());
        black_box(v.ipo_quotation_release_qualifier());
        black_box(v.ipo_price());
    }
    fn luld_auction_collar(&mut self, v: itch_v5::LuldAuctionCollarView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.auction_collar_reference_price());
        black_box(v.upper_auction_collar_price());
        black_box(v.lower_auction_collar_price());
        black_box(v.auction_collar_extension());
    }
    fn operational_halt(&mut self, v: itch_v5::OperationalHaltView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.market_code());
        black_box(v.operational_halt_action());
    }
    fn add_order_no_mpid(&mut self, v: itch_v5::AddOrderNoMpidView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.buy_sell_indicator());
        black_box(v.shares());
        black_box(v.stock());
        black_box(v.price());
    }
    fn add_order_mpid_attribution(&mut self, v: itch_v5::AddOrderMpidAttributionView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.buy_sell_indicator());
        black_box(v.shares());
        black_box(v.stock());
        black_box(v.price());
        black_box(v.attribution());
    }
    fn order_executed(&mut self, v: itch_v5::OrderExecutedView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.executed_shares());
        black_box(v.match_number());
    }
    fn order_executed_with_price(&mut self, v: itch_v5::OrderExecutedWithPriceView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.executed_shares());
        black_box(v.match_number());
        black_box(v.printable());
        black_box(v.execution_price());
    }
    fn order_cancel(&mut self, v: itch_v5::OrderCancelView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.cancelled_shares());
    }
    fn order_delete(&mut self, v: itch_v5::OrderDeleteView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
    }
    fn order_replace(&mut self, v: itch_v5::OrderReplaceView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.original_order_reference_number());
        black_box(v.new_order_reference_number());
        black_box(v.shares());
        black_box(v.price());
    }
    fn trade(&mut self, v: itch_v5::TradeView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.order_reference_number());
        black_box(v.buy_sell_indicator());
        black_box(v.shares());
        black_box(v.stock());
        black_box(v.price());
        black_box(v.match_number());
    }
    fn cross_trade(&mut self, v: itch_v5::CrossTradeView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.shares());
        black_box(v.stock());
        black_box(v.cross_price());
        black_box(v.match_number());
        black_box(v.cross_type());
    }
    fn broken_trade(&mut self, v: itch_v5::BrokenTradeView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.match_number());
    }
    fn net_order_imbalance_indicator(&mut self, v: itch_v5::NetOrderImbalanceIndicatorView<'_>) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.paired_shares());
        black_box(v.imbalance_shares());
        black_box(v.imbalance_direction());
        black_box(v.stock());
        black_box(v.far_price());
        black_box(v.near_price());
        black_box(v.current_reference_price());
        black_box(v.cross_type());
        black_box(v.price_variation_indicator());
    }
    fn retail_price_improvement_indicator(
        &mut self,
        v: itch_v5::RetailPriceImprovementIndicatorView<'_>,
    ) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.interest_flag());
    }
    fn direct_listing_capital_raise_price_discovery(
        &mut self,
        v: itch_v5::DirectListingCapitalRaisePriceDiscoveryView<'_>,
    ) {
        black_box(v.stock_locate());
        black_box(v.tracking_number());
        black_box(v.timestamp());
        black_box(v.stock());
        black_box(v.open_eligibility_status());
        black_box(v.minimum_allowable_price());
        black_box(v.maximum_allowable_price());
        black_box(v.near_execution_price());
        black_box(v.near_execution_time());
        black_box(v.lower_price_range_collar());
        black_box(v.upper_price_range_collar());
    }
    fn cold(&mut self, msg_type: u8, data: &[u8]) {
        black_box(msg_type);
        black_box(data);
    }
}
