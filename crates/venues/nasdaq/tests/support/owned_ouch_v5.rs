//! Parity baseline for NASDAQ OUCH 5.0 Order Entry, all 25 kinds (8
//! inbound, 17 outbound). Hand transcribed from the field tables in
//! sections 2 and 3 of the OUCH 5.0 Order Entry specification. Never
//! generated, independent of schema yaml and any other venue baseline.
//!
//! Section 1.2 defines only Long(8B)/Integer(4B)/Short(2B)/Byte(1B), but
//! nearly every Appendage Length field is 2 bytes wide yet labeled
//! "Integer", which contradicts the 4-byte Integer definition. A few
//! siblings label the same 2-byte field "Numeric" instead. The doc's ad
//! hoc "Numeric" type also covers 1-byte (UserRefIdx), 4-byte (Quantity),
//! and 8-byte (Order Reference Number) fields elsewhere. All decoded here
//! by wire width, never by the label.
//!
//! Prices are 8-byte unsigned (u64). PegOffset/DiscretionPegOffset are
//! "Signed Price" 4-byte (i32), the signed subtype section 1.2 only
//! gestures at ("unless designated otherwise"). Appendage values stay raw
//! bytes here regardless of subtype.
//!
//! 12 kinds tolerate the Appendage Length field itself being absent from
//! the wire (dual form): inbound X/M/Q and outbound C/D/B/J/P/I/T/M/Q. S
//! carries no appendage at all; every other kind's Appendage Length field
//! is mandatory (value may be zero, field itself always present).
//!
//! Type byte collides across directions on U/X/C/D/E/M/Q; the two entry
//! points below, `decode_to_exchange` and `decode_from_exchange`,
//! disambiguate by direction.

fn be_u16(data: &[u8], at: usize) -> u16 {
    u16::from_be_bytes(data[at..at + 2].try_into().unwrap())
}

fn be_u32(data: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(data[at..at + 4].try_into().unwrap())
}

fn be_u64(data: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(data[at..at + 8].try_into().unwrap())
}

fn be_alpha<const N: usize>(data: &[u8], at: usize) -> [u8; N] {
    data[at..at + N].try_into().unwrap()
}

// walk TagValue elements over the raw appendage tail: each element is a
// length byte L, an option tag byte, then L-1 bytes of value.
fn decode_appendage(tail: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < tail.len() {
        let elem_len = tail[pos] as usize;
        // valid element covers at least the mandatory tag byte
        assert!(elem_len >= 1, "TagValue element length byte is zero");
        assert!(
            pos + 1 + elem_len <= tail.len(),
            "TagValue element runs past appendage tail"
        );
        let tag = tail[pos + 1];
        let value = tail[pos + 2..pos + 1 + elem_len].to_vec();
        out.push((tag, value));
        pos += 1 + elem_len;
    }
    out
}

/// dispatch a client-to-exchange wire row to its owned decoder. `data` is
/// the full row including the leading msg_type byte at `data[0]`. Unknown
/// bytes return `None`.
pub fn decode_to_exchange(data: &[u8]) -> Option<OwnedMessage> {
    let msg_type = *data.first()?;
    match msg_type {
        b'O' => Some(OwnedMessage::EnterOrder(EnterOrderOwned::decode(data))),
        b'U' => Some(OwnedMessage::ReplaceOrderRequest(
            ReplaceOrderRequestOwned::decode(data),
        )),
        b'X' => Some(OwnedMessage::CancelOrderRequest(
            CancelOrderRequestOwned::decode(data),
        )),
        b'M' => Some(OwnedMessage::ModifyOrderRequest(
            ModifyOrderRequestOwned::decode(data),
        )),
        b'C' => Some(OwnedMessage::MassCancelRequest(
            MassCancelRequestOwned::decode(data),
        )),
        b'D' => Some(OwnedMessage::DisableOrderEntryRequest(
            DisableOrderEntryRequestOwned::decode(data),
        )),
        b'E' => Some(OwnedMessage::EnableOrderEntryRequest(
            EnableOrderEntryRequestOwned::decode(data),
        )),
        b'Q' => Some(OwnedMessage::AccountQueryRequest(
            AccountQueryRequestOwned::decode(data),
        )),
        _ => None,
    }
}

/// dispatch an exchange-to-client wire row to its owned decoder. Same
/// shape as [`decode_to_exchange`], separate match arm set because the
/// type byte space collides with inbound on U/X/C/D/E/M/Q.
pub fn decode_from_exchange(data: &[u8]) -> Option<OwnedMessage> {
    let msg_type = *data.first()?;
    match msg_type {
        b'S' => Some(OwnedMessage::SystemEvent(SystemEventOwned::decode(data))),
        b'A' => Some(OwnedMessage::OrderAccepted(OrderAcceptedOwned::decode(
            data,
        ))),
        b'U' => Some(OwnedMessage::OrderReplaced(OrderReplacedOwned::decode(
            data,
        ))),
        b'C' => Some(OwnedMessage::OrderCanceled(OrderCanceledOwned::decode(
            data,
        ))),
        b'D' => Some(OwnedMessage::AiqCanceled(AiqCanceledOwned::decode(data))),
        b'E' => Some(OwnedMessage::OrderExecuted(OrderExecutedOwned::decode(
            data,
        ))),
        b'B' => Some(OwnedMessage::BrokenTrade(BrokenTradeOwned::decode(data))),
        b'J' => Some(OwnedMessage::Rejected(RejectedOwned::decode(data))),
        b'P' => Some(OwnedMessage::CancelPending(CancelPendingOwned::decode(
            data,
        ))),
        b'I' => Some(OwnedMessage::CancelReject(CancelRejectOwned::decode(data))),
        b'T' => Some(OwnedMessage::OrderPriorityUpdate(
            OrderPriorityUpdateOwned::decode(data),
        )),
        b'M' => Some(OwnedMessage::OrderModified(OrderModifiedOwned::decode(
            data,
        ))),
        b'R' => Some(OwnedMessage::OrderRestated(OrderRestatedOwned::decode(
            data,
        ))),
        b'X' => Some(OwnedMessage::MassCancelResponse(
            MassCancelResponseOwned::decode(data),
        )),
        b'G' => Some(OwnedMessage::DisableOrderEntryResponse(
            DisableOrderEntryResponseOwned::decode(data),
        )),
        b'K' => Some(OwnedMessage::EnableOrderEntryResponse(
            EnableOrderEntryResponseOwned::decode(data),
        )),
        b'Q' => Some(OwnedMessage::AccountQueryResponse(
            AccountQueryResponseOwned::decode(data),
        )),
        _ => None,
    }
}

/// owned message, one variant per kind covered by this baseline. Inbound
/// and outbound kinds share this enum; the two decode entry points keep
/// direction unambiguous despite the colliding type bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedMessage {
    EnterOrder(EnterOrderOwned),
    ReplaceOrderRequest(ReplaceOrderRequestOwned),
    CancelOrderRequest(CancelOrderRequestOwned),
    ModifyOrderRequest(ModifyOrderRequestOwned),
    MassCancelRequest(MassCancelRequestOwned),
    DisableOrderEntryRequest(DisableOrderEntryRequestOwned),
    EnableOrderEntryRequest(EnableOrderEntryRequestOwned),
    AccountQueryRequest(AccountQueryRequestOwned),
    SystemEvent(SystemEventOwned),
    OrderAccepted(OrderAcceptedOwned),
    OrderReplaced(OrderReplacedOwned),
    OrderCanceled(OrderCanceledOwned),
    AiqCanceled(AiqCanceledOwned),
    OrderExecuted(OrderExecutedOwned),
    BrokenTrade(BrokenTradeOwned),
    Rejected(RejectedOwned),
    CancelPending(CancelPendingOwned),
    CancelReject(CancelRejectOwned),
    OrderPriorityUpdate(OrderPriorityUpdateOwned),
    OrderModified(OrderModifiedOwned),
    OrderRestated(OrderRestatedOwned),
    MassCancelResponse(MassCancelResponseOwned),
    DisableOrderEntryResponse(DisableOrderEntryResponseOwned),
    EnableOrderEntryResponse(EnableOrderEntryResponseOwned),
    AccountQueryResponse(AccountQueryResponseOwned),
}

// decode() below asserts min/exact length up front, or branches on the bare
// short form first for dual-form kinds; slice ranges are in-bounds by construction.

/// `O` Enter Order (inbound), fixed part 47B + appendage. Appendage
/// Length field is mandatory (never absent), value may be zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnterOrderOwned {
    pub user_ref_num: u32,
    pub side: u8,
    pub quantity: u32,
    pub symbol: [u8; 8],
    pub price: u64,
    pub time_in_force: u8,
    pub display: u8,
    pub capacity: u8,
    pub inter_market_sweep_eligibility: u8,
    pub cross_type: u8,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl EnterOrderOwned {
    pub const FIXED_LEN: usize = 47;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "EnterOrder wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "EnterOrder appendage length mismatch"
        );
        Self {
            user_ref_num: be_u32(data, 1),
            side: data[5],
            quantity: be_u32(data, 6),
            symbol: be_alpha(data, 10),
            price: be_u64(data, 18),
            time_in_force: data[26],
            display: data[27],
            capacity: data[28],
            inter_market_sweep_eligibility: data[29],
            cross_type: data[30],
            cl_ord_id: be_alpha(data, 31),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `U` Replace Order Request (inbound), fixed part 40B + appendage.
/// Appendage Length field is mandatory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceOrderRequestOwned {
    pub orig_user_ref_num: u32,
    pub user_ref_num: u32,
    pub quantity: u32,
    pub price: u64,
    pub time_in_force: u8,
    pub display: u8,
    pub inter_market_sweep_eligibility: u8,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl ReplaceOrderRequestOwned {
    pub const FIXED_LEN: usize = 40;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "ReplaceOrderRequest wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "ReplaceOrderRequest appendage length mismatch"
        );
        Self {
            orig_user_ref_num: be_u32(data, 1),
            user_ref_num: be_u32(data, 5),
            quantity: be_u32(data, 9),
            price: be_u64(data, 13),
            time_in_force: data[21],
            display: data[22],
            inter_market_sweep_eligibility: data[23],
            cl_ord_id: be_alpha(data, 24),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `X` Cancel Order Request (inbound), dual form: bare 9B (no appendage
/// at all) or 11B+ (Appendage Length present, may be zero).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrderRequestOwned {
    pub user_ref_num: u32,
    pub quantity: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl CancelOrderRequestOwned {
    pub const FIXED_CORE: usize = 9;
    pub const FIXED_LONG: usize = 11;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "CancelOrderRequest wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "CancelOrderRequest wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "CancelOrderRequest appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            user_ref_num: be_u32(data, 1),
            quantity: be_u32(data, 5),
            appendage,
        }
    }
}

/// `M` Modify Order Request (inbound), dual form: bare 10B or 12B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrderRequestOwned {
    pub user_ref_num: u32,
    pub side: u8,
    pub quantity: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl ModifyOrderRequestOwned {
    pub const FIXED_CORE: usize = 10;
    pub const FIXED_LONG: usize = 12;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "ModifyOrderRequest wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "ModifyOrderRequest wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "ModifyOrderRequest appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            user_ref_num: be_u32(data, 1),
            side: data[5],
            quantity: be_u32(data, 6),
            appendage,
        }
    }
}

/// `C` Mass Cancel Request (inbound), fixed part 19B + appendage.
/// Appendage Length field is mandatory, zero-filled when no criteria.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MassCancelRequestOwned {
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub symbol: [u8; 8],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl MassCancelRequestOwned {
    pub const FIXED_LEN: usize = 19;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "MassCancelRequest wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "MassCancelRequest appendage length mismatch"
        );
        Self {
            user_ref_num: be_u32(data, 1),
            firm: be_alpha(data, 5),
            symbol: be_alpha(data, 9),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `D` Disable Order Entry Request (inbound), fixed part 11B + appendage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisableOrderEntryRequestOwned {
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl DisableOrderEntryRequestOwned {
    pub const FIXED_LEN: usize = 11;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "DisableOrderEntryRequest wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "DisableOrderEntryRequest appendage length mismatch"
        );
        Self {
            user_ref_num: be_u32(data, 1),
            firm: be_alpha(data, 5),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `E` Enable Order Entry Request (inbound), fixed part 11B + appendage.
/// Same shape as [`DisableOrderEntryRequestOwned`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnableOrderEntryRequestOwned {
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl EnableOrderEntryRequestOwned {
    pub const FIXED_LEN: usize = 11;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "EnableOrderEntryRequest wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "EnableOrderEntryRequest appendage length mismatch"
        );
        Self {
            user_ref_num: be_u32(data, 1),
            firm: be_alpha(data, 5),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `Q` Account Query Request (inbound), dual form: bare 1B (type byte
/// only, no appendage) or 3B+ (Appendage Length present).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountQueryRequestOwned {
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl AccountQueryRequestOwned {
    pub const FIXED_CORE: usize = 1;
    pub const FIXED_LONG: usize = 3;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "AccountQueryRequest wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "AccountQueryRequest wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "AccountQueryRequest appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self { appendage }
    }
}

/// `S` System Event (outbound), exact 10B, no appendage at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemEventOwned {
    pub timestamp: u64,
    pub event_code: u8,
}

impl SystemEventOwned {
    pub const SIZE: usize = 10;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "SystemEvent wire row wrong length");
        Self {
            timestamp: be_u64(data, 1),
            event_code: data[9],
        }
    }
}

/// `A` Order Accepted (outbound), fixed part 64B + appendage. Appendage
/// Length field is mandatory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderAcceptedOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub side: u8,
    pub quantity: u32,
    pub symbol: [u8; 8],
    pub price: u64,
    pub time_in_force: u8,
    pub display: u8,
    pub order_reference_number: u64,
    pub capacity: u8,
    pub inter_market_sweep_eligibility: u8,
    pub cross_type: u8,
    pub order_state: u8,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderAcceptedOwned {
    pub const FIXED_LEN: usize = 64;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "OrderAccepted wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "OrderAccepted appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            side: data[13],
            quantity: be_u32(data, 14),
            symbol: be_alpha(data, 18),
            price: be_u64(data, 26),
            time_in_force: data[34],
            display: data[35],
            order_reference_number: be_u64(data, 36),
            capacity: data[44],
            inter_market_sweep_eligibility: data[45],
            cross_type: data[46],
            order_state: data[47],
            cl_ord_id: be_alpha(data, 48),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `U` Order Replaced (outbound), fixed part 68B + appendage. Appendage
/// Length field is mandatory (labeled Numeric here, unlike the
/// sibling Accepted message which labels the same field Integer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReplacedOwned {
    pub timestamp: u64,
    pub orig_user_ref_num: u32,
    pub user_ref_num: u32,
    pub side: u8,
    pub quantity: u32,
    pub symbol: [u8; 8],
    pub price: u64,
    pub time_in_force: u8,
    pub display: u8,
    pub order_reference_number: u64,
    pub capacity: u8,
    pub inter_market_sweep_eligibility: u8,
    pub cross_type: u8,
    pub order_state: u8,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderReplacedOwned {
    pub const FIXED_LEN: usize = 68;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "OrderReplaced wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "OrderReplaced appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            orig_user_ref_num: be_u32(data, 9),
            user_ref_num: be_u32(data, 13),
            side: data[17],
            quantity: be_u32(data, 18),
            symbol: be_alpha(data, 22),
            price: be_u64(data, 30),
            time_in_force: data[38],
            display: data[39],
            order_reference_number: be_u64(data, 40),
            capacity: data[48],
            inter_market_sweep_eligibility: data[49],
            cross_type: data[50],
            order_state: data[51],
            cl_ord_id: be_alpha(data, 52),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `C` Order Canceled (outbound), dual form: bare 18B or 20B+. Absent
/// unless the inbound message that triggered it carried a nonzero
/// UserRefIdx.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCanceledOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub quantity: u32,
    pub reason: u8,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderCanceledOwned {
    pub const FIXED_CORE: usize = 18;
    pub const FIXED_LONG: usize = 20;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "OrderCanceled wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "OrderCanceled wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "OrderCanceled appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            quantity: be_u32(data, 13),
            reason: data[17],
            appendage,
        }
    }
}

/// `D` AIQ Canceled (outbound), dual form: bare 32B or 34B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiqCanceledOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub decrement_shares: u32,
    pub reason: u8,
    pub quantity_prevented_from_trading: u32,
    pub execution_price: u64,
    pub liquidity_flag: u8,
    pub aiq_strategy: u8,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl AiqCanceledOwned {
    pub const FIXED_CORE: usize = 32;
    pub const FIXED_LONG: usize = 34;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "AiqCanceled wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "AiqCanceled wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "AiqCanceled appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            decrement_shares: be_u32(data, 13),
            reason: data[17],
            quantity_prevented_from_trading: be_u32(data, 18),
            execution_price: be_u64(data, 22),
            liquidity_flag: data[30],
            aiq_strategy: data[31],
            appendage,
        }
    }
}

/// `E` Order Executed (outbound), fixed part 36B + appendage. Appendage
/// Length field is mandatory (labeled Numeric here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderExecutedOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub quantity: u32,
    pub price: u64,
    pub liquidity_flag: u8,
    pub match_number: u64,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderExecutedOwned {
    pub const FIXED_LEN: usize = 36;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "OrderExecuted wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "OrderExecuted appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            quantity: be_u32(data, 13),
            price: be_u64(data, 17),
            liquidity_flag: data[25],
            match_number: be_u64(data, 26),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `B` Broken Trade (outbound), dual form: bare 36B or 38B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenTradeOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub match_number: u64,
    pub reason: u8,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl BrokenTradeOwned {
    pub const FIXED_CORE: usize = 36;
    pub const FIXED_LONG: usize = 38;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "BrokenTrade wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "BrokenTrade wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "BrokenTrade appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            match_number: be_u64(data, 13),
            reason: data[21],
            cl_ord_id: be_alpha(data, 22),
            appendage,
        }
    }
}

/// `J` Rejected (outbound), dual form: bare 29B or 31B+. Reason is a
/// 2-byte Numeric code (Appendix C hex reasons), unlike the
/// 1-byte Alpha Reason on Order Canceled and AIQ Canceled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub reason: u16,
    pub cl_ord_id: [u8; 14],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl RejectedOwned {
    pub const FIXED_CORE: usize = 29;
    pub const FIXED_LONG: usize = 31;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "Rejected wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "Rejected wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "Rejected appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            reason: be_u16(data, 13),
            cl_ord_id: be_alpha(data, 15),
            appendage,
        }
    }
}

/// `P` Cancel Pending (outbound), dual form: bare 13B or 15B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelPendingOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl CancelPendingOwned {
    pub const FIXED_CORE: usize = 13;
    pub const FIXED_LONG: usize = 15;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "CancelPending wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "CancelPending wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "CancelPending appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            appendage,
        }
    }
}

/// `I` Cancel Reject (outbound), dual form: bare 13B or 15B+. Same shape
/// as [`CancelPendingOwned`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRejectOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl CancelRejectOwned {
    pub const FIXED_CORE: usize = 13;
    pub const FIXED_LONG: usize = 15;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "CancelReject wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "CancelReject wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "CancelReject appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            appendage,
        }
    }
}

/// `T` Order Priority Update (outbound), dual form: bare 30B or 32B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPriorityUpdateOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub price: u64,
    pub display: u8,
    pub order_reference_number: u64,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderPriorityUpdateOwned {
    pub const FIXED_CORE: usize = 30;
    pub const FIXED_LONG: usize = 32;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "OrderPriorityUpdate wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "OrderPriorityUpdate wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "OrderPriorityUpdate appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            price: be_u64(data, 13),
            display: data[21],
            order_reference_number: be_u64(data, 22),
            appendage,
        }
    }
}

/// `M` Order Modified (outbound), dual form: bare 18B or 20B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderModifiedOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub side: u8,
    pub quantity: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderModifiedOwned {
    pub const FIXED_CORE: usize = 18;
    pub const FIXED_LONG: usize = 20;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "OrderModified wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "OrderModified wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "OrderModified appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            side: data[13],
            quantity: be_u32(data, 14),
            appendage,
        }
    }
}

/// `R` Order Restated (outbound), fixed part 16B + appendage. Appendage
/// Length field is mandatory (labeled Numeric here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRestatedOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub reason: u8,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl OrderRestatedOwned {
    pub const FIXED_LEN: usize = 16;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "OrderRestated wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "OrderRestated appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            reason: data[13],
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `X` Mass Cancel Response (outbound), fixed part 27B + appendage.
/// Appendage Length field is mandatory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MassCancelResponseOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub symbol: [u8; 8],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl MassCancelResponseOwned {
    pub const FIXED_LEN: usize = 27;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "MassCancelResponse wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "MassCancelResponse appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            firm: be_alpha(data, 13),
            symbol: be_alpha(data, 17),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `G` Disable Order Entry Response (outbound), fixed part 19B +
/// appendage. Appendage Length field is mandatory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisableOrderEntryResponseOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl DisableOrderEntryResponseOwned {
    pub const FIXED_LEN: usize = 19;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "DisableOrderEntryResponse wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "DisableOrderEntryResponse appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            firm: be_alpha(data, 13),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `K` Enable Order Entry Response (outbound), fixed part 19B +
/// appendage. Same shape as [`DisableOrderEntryResponseOwned`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnableOrderEntryResponseOwned {
    pub timestamp: u64,
    pub user_ref_num: u32,
    pub firm: [u8; 4],
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl EnableOrderEntryResponseOwned {
    pub const FIXED_LEN: usize = 19;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_LEN,
            "EnableOrderEntryResponse wire row shorter than fixed part"
        );
        let appendage_len = be_u16(data, Self::FIXED_LEN - 2) as usize;
        assert_eq!(
            data.len(),
            Self::FIXED_LEN + appendage_len,
            "EnableOrderEntryResponse appendage length mismatch"
        );
        Self {
            timestamp: be_u64(data, 1),
            user_ref_num: be_u32(data, 9),
            firm: be_alpha(data, 13),
            appendage: decode_appendage(&data[Self::FIXED_LEN..]),
        }
    }
}

/// `Q` Account Query Response (outbound), dual form: bare 13B or 15B+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountQueryResponseOwned {
    pub timestamp: u64,
    pub next_user_ref_num: u32,
    pub appendage: Vec<(u8, Vec<u8>)>,
}

impl AccountQueryResponseOwned {
    pub const FIXED_CORE: usize = 13;
    pub const FIXED_LONG: usize = 15;

    fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= Self::FIXED_CORE,
            "AccountQueryResponse wire row shorter than fixed core"
        );
        let appendage = if data.len() == Self::FIXED_CORE {
            Vec::new()
        } else {
            assert!(
                data.len() >= Self::FIXED_LONG,
                "AccountQueryResponse wire row too short for long form"
            );
            let appendage_len = be_u16(data, Self::FIXED_CORE) as usize;
            assert_eq!(
                data.len(),
                Self::FIXED_LONG + appendage_len,
                "AccountQueryResponse appendage length mismatch"
            );
            decode_appendage(&data[Self::FIXED_LONG..])
        };
        Self {
            timestamp: be_u64(data, 1),
            next_user_ref_num: be_u32(data, 9),
            appendage,
        }
    }
}
