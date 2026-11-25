//! Fuzz `proto_nasdaq::ouch_v5`: outbound view parse + dispatch vs hand-written
//! owned baseline. Input = one wire row, `data[0]` = message type byte.
//! Scope is exchange-to-client (views + `parse_match` dispatch); owned
//! agreement checks `decode_from_exchange`. Field asserts mirror
//! `crates/venues/nasdaq/tests/parity_ouch_v5.rs`.
//!
//! Appendage caveat: the production `AppendageIter` walks the trailing TLV
//! tail tolerantly and stops cleanly on a malformed inner element, so the
//! view accepts rows whose outer appendage-length field is consistent even
//! when an inner element is truncated. The hand-written owned baseline's
//! `decode_appendage` instead asserts on such elements (it is a reference for
//! well-formed tails only). So the visitor compares against the owned baseline
//! only when the tail tiles cleanly; otherwise it still proves parse and
//! dispatch never panic. `BYTE_LEN[t]` is the tail-start offset for every kind.
#![no_main]

use libfuzzer_sys::fuzz_target;
use proto_nasdaq::ouch_v5::*;

// same include shape as crates/venues/nasdaq/tests/support/mod.rs
#[expect(
    dead_code,
    reason = "this target checks only the exchange-to-client side"
)]
#[path = "../../crates/venues/nasdaq/tests/support/owned_ouch_v5.rs"]
mod owned;

mod support;

fn decode_owned(row: &[u8]) -> Option<owned::OwnedMessage> {
    owned::decode_from_exchange(row)
}

// tail suffix after the fixed prefix + 2-byte length field; empty for bare or
// zero-appendage rows. `n` is `BYTE_LEN[t]`, the tail-start offset.
fn tail_after(row: &[u8], n: usize) -> &[u8] {
    if row.len() > n { &row[n..] } else { &[] }
}

// true when the tail tiles exactly into TLV elements each len >= 1 and in
// bounds: exactly the condition under which the owned `decode_appendage`
// does not assert and yields the same elements as the view's `AppendageIter`.
fn appendage_well_formed(tail: &[u8]) -> bool {
    let mut pos = 0;
    while pos < tail.len() {
        let elem_len = tail[pos] as usize;
        if elem_len == 0 || pos + 1 + elem_len > tail.len() {
            return false;
        }
        pos += 1 + elem_len;
    }
    true
}

// raw appendage walk yields `(tag, value)`; owned baseline stores owned copies.
// generic over the iterator so the view's `AppendageIter` type stays unnamed.
fn assert_appendage<'a>(it: impl Iterator<Item = (u8, &'a [u8])>, owned: &[(u8, Vec<u8>)]) {
    let got: Vec<(u8, Vec<u8>)> = it.map(|(t, val)| (t, val.to_vec())).collect();
    assert_eq!(got.as_slice(), owned, "appendage tail");
}

// visitor asserts every view getter against owned baseline field. `compare`
// is false when the accepted row carries a malformed inner appendage the
// strict owned baseline cannot decode; the view still ran without panic.
struct Compare<'a> {
    row: &'a [u8],
    compare: bool,
}

impl HotVisitor for Compare<'_> {
    fn system_event(&mut self, v: SystemEventView<'_>) {
        let owned = support::owned_kind!(SystemEvent, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.event_code(), owned.event_code, "event_code");
    }

    fn order_accepted(&mut self, v: OrderAcceptedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderAccepted, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.symbol(), &owned.symbol[..], "symbol");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.time_in_force(), owned.time_in_force, "time_in_force");
        assert_eq!(v.display(), owned.display, "display");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(v.capacity(), owned.capacity, "capacity");
        assert_eq!(
            v.inter_market_sweep_eligibility(),
            owned.inter_market_sweep_eligibility,
            "inter_market_sweep_eligibility"
        );
        assert_eq!(v.cross_type(), owned.cross_type, "cross_type");
        assert_eq!(v.order_state(), owned.order_state, "order_state");
        assert_eq!(v.cl_ord_id(), &owned.cl_ord_id[..], "cl_ord_id");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_replaced(&mut self, v: OrderReplacedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderReplaced, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.orig_user_ref_num(),
            owned.orig_user_ref_num,
            "orig_user_ref_num"
        );
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.symbol(), &owned.symbol[..], "symbol");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.time_in_force(), owned.time_in_force, "time_in_force");
        assert_eq!(v.display(), owned.display, "display");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_eq!(v.capacity(), owned.capacity, "capacity");
        assert_eq!(
            v.inter_market_sweep_eligibility(),
            owned.inter_market_sweep_eligibility,
            "inter_market_sweep_eligibility"
        );
        assert_eq!(v.cross_type(), owned.cross_type, "cross_type");
        assert_eq!(v.order_state(), owned.order_state, "order_state");
        assert_eq!(v.cl_ord_id(), &owned.cl_ord_id[..], "cl_ord_id");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_canceled(&mut self, v: OrderCanceledView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderCanceled, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.reason(), owned.reason, "reason");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn aiq_canceled(&mut self, v: AiqCanceledView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(AiqCanceled, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(
            v.decrement_shares(),
            owned.decrement_shares,
            "decrement_shares"
        );
        assert_eq!(v.reason(), owned.reason, "reason");
        assert_eq!(
            v.quantity_prevented_from_trading(),
            owned.quantity_prevented_from_trading,
            "quantity_prevented_from_trading"
        );
        assert_eq!(
            v.execution_price(),
            owned.execution_price,
            "execution_price"
        );
        assert_eq!(v.liquidity_flag(), owned.liquidity_flag, "liquidity_flag");
        assert_eq!(v.aiq_strategy(), owned.aiq_strategy, "aiq_strategy");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_executed(&mut self, v: OrderExecutedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderExecuted, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.liquidity_flag(), owned.liquidity_flag, "liquidity_flag");
        assert_eq!(v.match_number(), owned.match_number, "match_number");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn broken_trade(&mut self, v: BrokenTradeView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(BrokenTrade, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.match_number(), owned.match_number, "match_number");
        assert_eq!(v.reason(), owned.reason, "reason");
        assert_eq!(v.cl_ord_id(), &owned.cl_ord_id[..], "cl_ord_id");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn rejected(&mut self, v: RejectedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(Rejected, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.reason(), owned.reason, "reason");
        assert_eq!(v.cl_ord_id(), &owned.cl_ord_id[..], "cl_ord_id");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn cancel_pending(&mut self, v: CancelPendingView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(CancelPending, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn cancel_reject(&mut self, v: CancelRejectView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(CancelReject, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_priority_update(&mut self, v: OrderPriorityUpdateView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderPriorityUpdate, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.price(), owned.price, "price");
        assert_eq!(v.display(), owned.display, "display");
        assert_eq!(
            v.order_reference_number(),
            owned.order_reference_number,
            "order_reference_number"
        );
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_modified(&mut self, v: OrderModifiedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderModified, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.side(), owned.side, "side");
        assert_eq!(v.quantity(), owned.quantity, "quantity");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn order_restated(&mut self, v: OrderRestatedView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(OrderRestated, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.reason(), owned.reason, "reason");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn mass_cancel_response(&mut self, v: MassCancelResponseView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(MassCancelResponse, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.firm(), &owned.firm[..], "firm");
        assert_eq!(v.symbol(), &owned.symbol[..], "symbol");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn disable_order_entry_response(&mut self, v: DisableOrderEntryResponseView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(DisableOrderEntryResponse, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.firm(), &owned.firm[..], "firm");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn enable_order_entry_response(&mut self, v: EnableOrderEntryResponseView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(EnableOrderEntryResponse, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(v.user_ref_num(), owned.user_ref_num, "user_ref_num");
        assert_eq!(v.firm(), &owned.firm[..], "firm");
        assert_appendage(v.appendage(), &owned.appendage);
    }

    fn account_query_response(&mut self, v: AccountQueryResponseView<'_>) {
        if !self.compare {
            return;
        }
        let owned = support::owned_kind!(AccountQueryResponse, self.row);
        assert_eq!(v.timestamp(), owned.timestamp, "timestamp");
        assert_eq!(
            v.next_user_ref_num(),
            owned.next_user_ref_num,
            "next_user_ref_num"
        );
        assert_appendage(v.appendage(), &owned.appendage);
    }
}

fuzz_target!(|data: &[u8]| {
    let Some(&msg_type) = data.first() else {
        return;
    };
    // `S` is the only exact-fixed kind; its dispatch thunk expects an exact
    // BYTE_LEN slice and panics otherwise, so gate it like a fixed ITCH row.
    if msg_type == b'S' {
        match BYTE_LEN[b'S' as usize] {
            Some(len) if data.len() == usize::from(len.get()) => {
                parse_match(
                    msg_type,
                    data,
                    &mut Compare {
                        row: data,
                        compare: true,
                    },
                );
            }
            _ => support::assert_rejects!(SystemEventView, data),
        }
        return;
    }
    // Every other known kind carries a variable appendage: `parse_match` routes
    // a length or length-field mismatch to `cold` and never panics, so any
    // length is safe. Compare against the owned baseline only when the tail
    // tiles cleanly; otherwise the view still proved it does not panic.
    let compare = match BYTE_LEN[msg_type as usize] {
        Some(n) => appendage_well_formed(tail_after(data, usize::from(n.get()))),
        None => true,
    };
    parse_match(msg_type, data, &mut Compare { row: data, compare });
});
