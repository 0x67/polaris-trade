//! Fuzz `proto_sgx::itch_v2`: view parse + dispatch vs hand-written owned baseline.
//! Input = one wire row, `data[0]` = message type byte.
//! Field asserts mirror `crates/venues/sgx/tests/parity_itch_v2.rs`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use proto_sgx::itch_v2::*;

// same include shape as crates/venues/sgx/tests/support/mod.rs
#[path = "../../crates/venues/sgx/tests/support/owned_itch_v2.rs"]
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
    fn seconds(&mut self, v: SecondsView<'_>) {
        let owned = support::owned_kind!(Seconds, self.row);
        assert_eq!(v.second(), owned.second, "second");
    }

    fn order_book_directory(&mut self, v: OrderBookDirectoryView<'_>) {
        let owned = support::owned_kind!(OrderBookDirectory, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.symbol(), &owned.symbol[..], "symbol");
        assert_eq!(v.long_name(), &owned.long_name[..], "long_name");
        assert_eq!(v.isin(), &owned.isin[..], "isin");
        assert_eq!(
            v.financial_product(),
            owned.financial_product,
            "financial_product"
        );
        assert_eq!(
            v.trading_currency(),
            &owned.trading_currency[..],
            "trading_currency"
        );
        assert_eq!(v.price_decimals(), owned.price_decimals, "price_decimals");
        assert_eq!(
            v.nominal_value_decimals(),
            owned.nominal_value_decimals,
            "nominal_value_decimals"
        );
        assert_eq!(v.odd_lot_size(), owned.odd_lot_size, "odd_lot_size");
        assert_eq!(v.round_lot_size(), owned.round_lot_size, "round_lot_size");
        assert_eq!(v.block_lot_size(), owned.block_lot_size, "block_lot_size");
        assert_eq!(v.nominal_value(), owned.nominal_value, "nominal_value");
        assert_eq!(v.number_of_legs(), owned.number_of_legs, "number_of_legs");
        assert_eq!(v.commodity_code(), owned.commodity_code, "commodity_code");
        assert_eq!(v.strike_price(), owned.strike_price, "strike_price");
        assert_eq!(
            v.expiration_date(),
            owned.expiration_date,
            "expiration_date"
        );
        assert_eq!(
            v.strike_price_decimals(),
            owned.strike_price_decimals,
            "strike_price_decimals"
        );
        assert_eq!(v.put_or_call(), owned.put_or_call, "put_or_call");
    }

    fn combination_order_book_leg(&mut self, v: CombinationOrderBookLegView<'_>) {
        let owned = support::owned_kind!(CombinationOrderBookLeg, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(
            v.combination_order_book_id(),
            owned.combination_order_book_id,
            "combination_order_book_id"
        );
        assert_eq!(
            v.leg_order_book_id(),
            owned.leg_order_book_id,
            "leg_order_book_id"
        );
        assert_eq!(v.leg_side(), owned.leg_side, "leg_side");
        assert_eq!(v.leg_ratio(), owned.leg_ratio, "leg_ratio");
    }

    fn tick_size_table_entry(&mut self, v: TickSizeTableEntryView<'_>) {
        let owned = support::owned_kind!(TickSizeTableEntry, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.tick_size(), owned.tick_size, "tick_size");
        assert_eq!(v.price_from(), owned.price_from, "price_from");
        assert_eq!(v.price_to(), owned.price_to, "price_to");
    }

    fn system_event(&mut self, v: SystemEventView<'_>) {
        let owned = support::owned_kind!(SystemEvent, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.event_code(), owned.event_code, "event_code");
    }

    fn order_book_state(&mut self, v: OrderBookStateView<'_>) {
        let owned = support::owned_kind!(OrderBookState, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.state_name(), &owned.state_name[..], "state_name");
    }

    fn add_order(&mut self, v: AddOrderView<'_>) {
        let owned = support::owned_kind!(AddOrder, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_id(), owned.order_id, "order_id");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(
            v.order_book_position(),
            owned.order_book_position,
            "order_book_position"
        );
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(
            v.order_attributes(),
            owned.order_attributes,
            "order_attributes"
        );
        assert_eq!(v.lot_type(), owned.lot_type, "lot_type");
    }

    fn order_executed(&mut self, v: OrderExecutedView<'_>) {
        let owned = support::owned_kind!(OrderExecuted, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_id(), owned.order_id, "order_id");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(
            v.executed_quantity(),
            owned.executed_quantity,
            "executed_quantity"
        );
        assert_eq!(v.match_id(), owned.match_id, "match_id");
        assert_eq!(v.combo_group_id(), owned.combo_group_id, "combo_group_id");
        assert_eq!(v.reserved0(), &owned.reserved0[..], "reserved0");
        assert_eq!(v.reserved1(), &owned.reserved1[..], "reserved1");
    }

    fn order_executed_with_price(&mut self, v: OrderExecutedWithPriceView<'_>) {
        let owned = support::owned_kind!(OrderExecutedWithPrice, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_id(), owned.order_id, "order_id");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(
            v.executed_quantity(),
            owned.executed_quantity,
            "executed_quantity"
        );
        assert_eq!(v.match_id(), owned.match_id, "match_id");
        assert_eq!(v.combo_group_id(), owned.combo_group_id, "combo_group_id");
        assert_eq!(v.reserved0(), &owned.reserved0[..], "reserved0");
        assert_eq!(v.reserved1(), &owned.reserved1[..], "reserved1");
        assert_eq!(v.trade_price(), owned.trade_price, "trade_price");
        assert_eq!(
            v.occurred_at_cross(),
            owned.occurred_at_cross,
            "occurred_at_cross"
        );
        assert_eq!(v.printable(), owned.printable, "printable");
    }

    fn order_replace(&mut self, v: OrderReplaceView<'_>) {
        let owned = support::owned_kind!(OrderReplace, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_id(), owned.order_id, "order_id");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(
            v.new_order_book_position(),
            owned.new_order_book_position,
            "new_order_book_position"
        );
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(
            v.order_attributes(),
            owned.order_attributes,
            "order_attributes"
        );
    }

    fn order_delete(&mut self, v: OrderDeleteView<'_>) {
        let owned = support::owned_kind!(OrderDelete, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_id(), owned.order_id, "order_id");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.side(), owned.side, "side");
    }

    fn trade(&mut self, v: TradeView<'_>) {
        let owned = support::owned_kind!(Trade, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.match_id(), owned.match_id, "match_id");
        assert_eq!(v.combo_group_id(), owned.combo_group_id, "combo_group_id");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(v.trade_price(), owned.trade_price, "trade_price");
        assert_eq!(v.reserved0(), &owned.reserved0[..], "reserved0");
        assert_eq!(v.reserved1(), &owned.reserved1[..], "reserved1");
        assert_eq!(v.printable(), owned.printable, "printable");
        assert_eq!(
            v.occurred_at_cross(),
            owned.occurred_at_cross,
            "occurred_at_cross"
        );
    }

    fn equilibrium_price_update(&mut self, v: EquilibriumPriceUpdateView<'_>) {
        let owned = support::owned_kind!(EquilibriumPriceUpdate, self.row);
        assert_eq!(
            v.timestamp_nanoseconds(),
            owned.timestamp_nanoseconds,
            "timestamp_nanoseconds"
        );
        assert_eq!(v.order_book_id(), owned.order_book_id, "order_book_id");
        assert_eq!(
            v.available_bid_quantity_at_equilibrium_price(),
            owned.available_bid_quantity_at_equilibrium_price,
            "available_bid_quantity_at_equilibrium_price"
        );
        assert_eq!(
            v.available_ask_quantity_at_equilibrium_price(),
            owned.available_ask_quantity_at_equilibrium_price,
            "available_ask_quantity_at_equilibrium_price"
        );
        assert_eq!(
            v.equilibrium_price(),
            owned.equilibrium_price,
            "equilibrium_price"
        );
        assert_eq!(v.best_bid_price(), owned.best_bid_price, "best_bid_price");
        assert_eq!(v.best_ask_price(), owned.best_ask_price, "best_ask_price");
        assert_eq!(
            v.best_bid_quantity(),
            owned.best_bid_quantity,
            "best_bid_quantity"
        );
        assert_eq!(
            v.best_ask_quantity(),
            owned.best_ask_quantity,
            "best_ask_quantity"
        );
    }

    fn end_of_snapshot(&mut self, v: EndOfSnapshotView<'_>) {
        let owned = support::owned_kind!(EndOfSnapshot, self.row);
        assert_eq!(
            v.sequence_number(),
            &owned.sequence_number[..],
            "sequence_number"
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
            b'T' => support::assert_rejects!(SecondsView, data),
            b'R' => support::assert_rejects!(OrderBookDirectoryView, data),
            b'M' => support::assert_rejects!(CombinationOrderBookLegView, data),
            b'L' => support::assert_rejects!(TickSizeTableEntryView, data),
            b'S' => support::assert_rejects!(SystemEventView, data),
            b'O' => support::assert_rejects!(OrderBookStateView, data),
            b'A' => support::assert_rejects!(AddOrderView, data),
            b'E' => support::assert_rejects!(OrderExecutedView, data),
            b'C' => support::assert_rejects!(OrderExecutedWithPriceView, data),
            b'U' => support::assert_rejects!(OrderReplaceView, data),
            b'D' => support::assert_rejects!(OrderDeleteView, data),
            b'P' => support::assert_rejects!(TradeView, data),
            b'Z' => support::assert_rejects!(EquilibriumPriceUpdateView, data),
            b'G' => support::assert_rejects!(EndOfSnapshotView, data),
            _ => unreachable!("BYTE_LEN entry without view arm"),
        },
        // unknown or variable kind: dispatch routes to cold/no-op, never a fixed thunk
        None => parse_match(msg_type, data, &mut Compare { row: data }),
    }
});
