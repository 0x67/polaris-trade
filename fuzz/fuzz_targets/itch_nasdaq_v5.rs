//! Fuzz `proto_nasdaq::itch_v5`: view parse + dispatch vs hand-written owned baseline.
//! Input = one wire row, `data[0]` = message type byte.
//! Field asserts mirror `crates/venues/nasdaq/tests/parity_itch_v5.rs`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use proto_nasdaq::itch_v5::*;

// same include shape as crates/venues/nasdaq/tests/support/mod.rs
#[path = "../../crates/venues/nasdaq/tests/support/owned_itch_v5.rs"]
mod owned;

mod support;

fn decode_owned(row: &[u8]) -> Option<owned::OwnedMessage> {
    owned::decode_owned(row[0], row)
}

// visitor asserts every view getter against owned baseline field
struct Compare<'a> {
    row: &'a [u8],
}

impl HotVisitor for Compare<'_> {
    fn system_event(&mut self, v: SystemEventView<'_>) {
        let owned = support::owned_kind!(SystemEvent, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.event_code(), owned.event_code, "event_code");
    }

    fn stock_directory(&mut self, v: StockDirectoryView<'_>) {
        let owned = support::owned_kind!(StockDirectory, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(
            v.market_category(),
            owned.market_category,
            "market_category"
        );
        assert_eq!(
            v.financial_status_indicator(),
            owned.financial_status_indicator,
            "financial_status_indicator"
        );
        assert_eq!(v.round_lot_size(), owned.round_lot_size, "round_lot_size");
        assert_eq!(
            v.round_lots_only(),
            owned.round_lots_only,
            "round_lots_only"
        );
        assert_eq!(
            v.issue_classification(),
            owned.issue_classification,
            "issue_classification"
        );
        assert_eq!(
            v.issue_sub_type(),
            &owned.issue_sub_type[..],
            "issue_sub_type"
        );
        assert_eq!(v.authenticity(), owned.authenticity, "authenticity");
        assert_eq!(
            v.short_sale_threshold_indicator(),
            owned.short_sale_threshold_indicator,
            "short_sale_threshold_indicator"
        );
        assert_eq!(v.ipo_flag(), owned.ipo_flag, "ipo_flag");
        assert_eq!(
            v.luld_reference_price_tier(),
            owned.luld_reference_price_tier,
            "luld_reference_price_tier"
        );
        assert_eq!(v.etp_flag(), owned.etp_flag, "etp_flag");
        assert_eq!(
            v.etp_leverage_factor(),
            owned.etp_leverage_factor,
            "etp_leverage_factor"
        );
        assert_eq!(
            v.inverse_indicator(),
            owned.inverse_indicator,
            "inverse_indicator"
        );
    }

    fn stock_trading_action(&mut self, v: StockTradingActionView<'_>) {
        let owned = support::owned_kind!(StockTradingAction, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.trading_state(), owned.trading_state, "trading_state");
        assert_eq!(v.reserved(), owned.reserved, "reserved");
        assert_eq!(v.reason(), &owned.reason[..], "reason");
    }

    fn reg_sho_restriction(&mut self, v: RegShoRestrictionView<'_>) {
        let owned = support::owned_kind!(RegShoRestriction, self.row);
        assert_eq!(v.locate_code(), owned.locate_code, "locate_code");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.reg_sho_action(), owned.reg_sho_action, "reg_sho_action");
    }

    fn market_participant_position(&mut self, v: MarketParticipantPositionView<'_>) {
        let owned = support::owned_kind!(MarketParticipantPosition, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.mpid(), &owned.mpid[..], "mpid");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(
            v.primary_market_maker(),
            owned.primary_market_maker,
            "primary_market_maker"
        );
        assert_eq!(
            v.market_maker_mode(),
            owned.market_maker_mode,
            "market_maker_mode"
        );
        assert_eq!(
            v.market_participant_state(),
            owned.market_participant_state,
            "market_participant_state"
        );
    }

    fn mwcb_decline_level(&mut self, v: MwcbDeclineLevelView<'_>) {
        let owned = support::owned_kind!(MwcbDeclineLevel, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.level_1(), owned.level_1, "level_1");
        assert_eq!(v.level_2(), owned.level_2, "level_2");
        assert_eq!(v.level_3(), owned.level_3, "level_3");
    }

    fn mwcb_status(&mut self, v: MwcbStatusView<'_>) {
        let owned = support::owned_kind!(MwcbStatus, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.breached_level(), owned.breached_level, "breached_level");
    }

    fn ipo_quoting_period_update(&mut self, v: IpoQuotingPeriodUpdateView<'_>) {
        let owned = support::owned_kind!(IpoQuotingPeriodUpdate, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(
            v.ipo_quotation_release_time(),
            owned.ipo_quotation_release_time,
            "ipo_quotation_release_time"
        );
        assert_eq!(
            v.ipo_quotation_release_qualifier(),
            owned.ipo_quotation_release_qualifier,
            "ipo_quotation_release_qualifier"
        );
        assert_eq!(v.ipo_price(), owned.ipo_price, "ipo_price");
    }

    fn luld_auction_collar(&mut self, v: LuldAuctionCollarView<'_>) {
        let owned = support::owned_kind!(LuldAuctionCollar, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(
            v.auction_collar_reference_price(),
            owned.auction_collar_reference_price,
            "auction_collar_reference_price"
        );
        assert_eq!(
            v.upper_auction_collar_price(),
            owned.upper_auction_collar_price,
            "upper_auction_collar_price"
        );
        assert_eq!(
            v.lower_auction_collar_price(),
            owned.lower_auction_collar_price,
            "lower_auction_collar_price"
        );
        assert_eq!(
            v.auction_collar_extension(),
            owned.auction_collar_extension,
            "auction_collar_extension"
        );
    }

    fn operational_halt(&mut self, v: OperationalHaltView<'_>) {
        let owned = support::owned_kind!(OperationalHalt, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.market_code(), owned.market_code, "market_code");
        assert_eq!(
            v.operational_halt_action(),
            owned.operational_halt_action,
            "operational_halt_action"
        );
    }

    fn add_order_no_mpid(&mut self, v: AddOrderNoMpidView<'_>) {
        let owned = support::owned_kind!(AddOrderNoMpid, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.buy_sell_indicator(),
            owned.buy_sell_indicator,
            "buy_sell_indicator"
        );
        assert_eq!(v.shares(), owned.shares, "shares");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.price(), owned.price, "price");
    }

    fn add_order_mpid_attribution(&mut self, v: AddOrderMpidAttributionView<'_>) {
        let owned = support::owned_kind!(AddOrderMpidAttribution, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.buy_sell_indicator(),
            owned.buy_sell_indicator,
            "buy_sell_indicator"
        );
        assert_eq!(v.shares(), owned.shares, "shares");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.attribution(), &owned.attribution[..], "attribution");
    }

    fn order_executed(&mut self, v: OrderExecutedView<'_>) {
        let owned = support::owned_kind!(OrderExecuted, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.executed_shares(),
            owned.executed_shares,
            "executed_shares"
        );
        assert_eq!(v.match_number(), owned.match_number, "match_number");
    }

    fn order_executed_with_price(&mut self, v: OrderExecutedWithPriceView<'_>) {
        let owned = support::owned_kind!(OrderExecutedWithPrice, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.executed_shares(),
            owned.executed_shares,
            "executed_shares"
        );
        assert_eq!(v.match_number(), owned.match_number, "match_number");
        assert_eq!(v.printable(), owned.printable, "printable");
        assert_eq!(
            v.execution_price(),
            owned.execution_price,
            "execution_price"
        );
    }

    fn order_cancel(&mut self, v: OrderCancelView<'_>) {
        let owned = support::owned_kind!(OrderCancel, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.cancelled_shares(),
            owned.cancelled_shares,
            "cancelled_shares"
        );
    }

    fn order_delete(&mut self, v: OrderDeleteView<'_>) {
        let owned = support::owned_kind!(OrderDelete, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
    }

    fn order_replace(&mut self, v: OrderReplaceView<'_>) {
        let owned = support::owned_kind!(OrderReplace, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.original_order_reference_number(),
            owned.original_order_reference_number,
            "original_order_reference_number"
        );
        assert_eq!(
            v.new_order_reference_number(),
            owned.new_order_reference_number,
            "new_order_reference_number"
        );
        assert_eq!(v.shares(), owned.shares, "shares");
        assert_eq!(v.price(), owned.price, "price");
    }

    fn trade(&mut self, v: TradeView<'_>) {
        let owned = support::owned_kind!(Trade, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(
            v.buy_sell_indicator(),
            owned.buy_sell_indicator,
            "buy_sell_indicator"
        );
        assert_eq!(v.shares(), owned.shares, "shares");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.match_number(), owned.match_number, "match_number");
    }

    fn cross_trade(&mut self, v: CrossTradeView<'_>) {
        let owned = support::owned_kind!(CrossTrade, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.shares(), owned.shares, "shares");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.cross_price(), owned.cross_price, "cross_price");
        assert_eq!(v.match_number(), owned.match_number, "match_number");
        assert_eq!(v.cross_type(), owned.cross_type, "cross_type");
    }

    fn broken_trade(&mut self, v: BrokenTradeView<'_>) {
        let owned = support::owned_kind!(BrokenTrade, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.match_number(), owned.match_number, "match_number");
    }

    fn net_order_imbalance_indicator(&mut self, v: NetOrderImbalanceIndicatorView<'_>) {
        let owned = support::owned_kind!(NetOrderImbalanceIndicator, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.paired_shares(), owned.paired_shares, "paired_shares");
        assert_eq!(
            v.imbalance_shares(),
            owned.imbalance_shares,
            "imbalance_shares"
        );
        assert_eq!(
            v.imbalance_direction(),
            owned.imbalance_direction,
            "imbalance_direction"
        );
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.far_price(), owned.far_price, "far_price");
        assert_eq!(v.near_price(), owned.near_price, "near_price");
        assert_eq!(
            v.current_reference_price(),
            owned.current_reference_price,
            "current_reference_price"
        );
        assert_eq!(v.cross_type(), owned.cross_type, "cross_type");
        assert_eq!(
            v.price_variation_indicator(),
            owned.price_variation_indicator,
            "price_variation_indicator"
        );
    }

    fn retail_price_improvement_indicator(&mut self, v: RetailPriceImprovementIndicatorView<'_>) {
        let owned = support::owned_kind!(RetailPriceImprovementIndicator, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(v.interest_flag(), owned.interest_flag, "interest_flag");
    }

    fn direct_listing_capital_raise_price_discovery(
        &mut self,
        v: DirectListingCapitalRaisePriceDiscoveryView<'_>,
    ) {
        let owned = support::owned_kind!(DirectListingCapitalRaisePriceDiscovery, self.row);
        assert_eq!(v.stock_locate(), owned.stock_locate, "stock_locate");
        assert_eq!(
            v.tracking_number(),
            owned.tracking_number,
            "tracking_number"
        );
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.stock(), &owned.stock[..], "stock");
        assert_eq!(
            v.open_eligibility_status(),
            owned.open_eligibility_status,
            "open_eligibility_status"
        );
        assert_eq!(
            v.minimum_allowable_price(),
            owned.minimum_allowable_price,
            "minimum_allowable_price"
        );
        assert_eq!(
            v.maximum_allowable_price(),
            owned.maximum_allowable_price,
            "maximum_allowable_price"
        );
        assert_eq!(
            v.near_execution_price(),
            owned.near_execution_price,
            "near_execution_price"
        );
        assert_eq!(
            v.near_execution_time(),
            owned.near_execution_time,
            "near_execution_time"
        );
        assert_eq!(
            v.lower_price_range_collar(),
            owned.lower_price_range_collar,
            "lower_price_range_collar"
        );
        assert_eq!(
            v.upper_price_range_collar(),
            owned.upper_price_range_collar,
            "upper_price_range_collar"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    let Some(&msg_type) = data.first() else {
        return;
    };
    match BYTE_LEN[msg_type as usize] {
        // exact fixed-size row: dispatch, visitor proves view agrees with owned
        Some(len) if data.len() == usize::from(len.get()) => {
            parse_match(msg_type, data, &mut Compare { row: data });
        }
        // wrong length: parse must return structured WrongLength, never panic
        Some(_) => match msg_type {
            b'S' => support::assert_rejects!(SystemEventView, data),
            b'R' => support::assert_rejects!(StockDirectoryView, data),
            b'H' => support::assert_rejects!(StockTradingActionView, data),
            b'Y' => support::assert_rejects!(RegShoRestrictionView, data),
            b'L' => support::assert_rejects!(MarketParticipantPositionView, data),
            b'V' => support::assert_rejects!(MwcbDeclineLevelView, data),
            b'W' => support::assert_rejects!(MwcbStatusView, data),
            b'K' => support::assert_rejects!(IpoQuotingPeriodUpdateView, data),
            b'J' => support::assert_rejects!(LuldAuctionCollarView, data),
            b'h' => support::assert_rejects!(OperationalHaltView, data),
            b'A' => support::assert_rejects!(AddOrderNoMpidView, data),
            b'F' => support::assert_rejects!(AddOrderMpidAttributionView, data),
            b'E' => support::assert_rejects!(OrderExecutedView, data),
            b'C' => support::assert_rejects!(OrderExecutedWithPriceView, data),
            b'X' => support::assert_rejects!(OrderCancelView, data),
            b'D' => support::assert_rejects!(OrderDeleteView, data),
            b'U' => support::assert_rejects!(OrderReplaceView, data),
            b'P' => support::assert_rejects!(TradeView, data),
            b'Q' => support::assert_rejects!(CrossTradeView, data),
            b'B' => support::assert_rejects!(BrokenTradeView, data),
            b'I' => support::assert_rejects!(NetOrderImbalanceIndicatorView, data),
            b'N' => support::assert_rejects!(RetailPriceImprovementIndicatorView, data),
            b'O' => support::assert_rejects!(DirectListingCapitalRaisePriceDiscoveryView, data),
            _ => unreachable!("BYTE_LEN entry without view arm"),
        },
        // unknown or variable kind: dispatch routes to cold/no-op, never a fixed thunk
        None => parse_match(msg_type, data, &mut Compare { row: data }),
    }
});
