//! parity baseline for NASDAQ TotalView-ITCH 5.0, all 23 message kinds. Hand
//! transcribed from the NASDAQ TotalView-ITCH 5.0 field tables. Never
//! generated, never read from schema yaml: this is the independent
//! reference a generated view layer gets checked against.
//!
//! Data Types section states all integer fields are big-endian and unsigned
//! unless otherwise noted; no field in this doc is noted signed, so every
//! integer here decodes to an unsigned width. Six-byte Timestamp fields
//! decode via local `be_u48`. Single-byte coded fields (event code, trading
//! state, market category, indicators, action codes) stay raw `u8` wire
//! bytes, not domain enums. Alpha/text fields stay untrimmed `[u8; N]` stack
//! arrays.

fn be_u16(data: &[u8], at: usize) -> u16 {
    u16::from_be_bytes(data[at..at + 2].try_into().unwrap())
}

fn be_u32(data: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(data[at..at + 4].try_into().unwrap())
}

fn be_u64(data: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(data[at..at + 8].try_into().unwrap())
}

// 6-byte Timestamp field (nanoseconds since midnight). No native u48, build
// via shift-or over the 6 wire bytes.
fn be_u48(data: &[u8], at: usize) -> u64 {
    let b = &data[at..at + 6];
    (b[0] as u64) << 40
        | (b[1] as u64) << 32
        | (b[2] as u64) << 24
        | (b[3] as u64) << 16
        | (b[4] as u64) << 8
        | (b[5] as u64)
}

fn be_alpha<const N: usize>(data: &[u8], at: usize) -> [u8; N] {
    data[at..at + N].try_into().unwrap()
}

/// dispatch fixed-size wire row to its owned decoder. `data` is the full row
/// including the leading msg_type byte at `data[0]`. Type byte is
/// case-sensitive: `H` (Stock Trading Action) and `h` (Operational Halt) are
/// distinct kinds. Unknown bytes return `None`.
pub fn decode_owned(msg_type: u8, data: &[u8]) -> Option<OwnedMessage> {
    match msg_type {
        b'S' => Some(OwnedMessage::SystemEvent(SystemEventOwned::decode(data))),
        b'R' => Some(OwnedMessage::StockDirectory(StockDirectoryOwned::decode(
            data,
        ))),
        b'H' => Some(OwnedMessage::StockTradingAction(
            StockTradingActionOwned::decode(data),
        )),
        b'Y' => Some(OwnedMessage::RegShoRestriction(
            RegShoRestrictionOwned::decode(data),
        )),
        b'L' => Some(OwnedMessage::MarketParticipantPosition(
            MarketParticipantPositionOwned::decode(data),
        )),
        b'V' => Some(OwnedMessage::MwcbDeclineLevel(
            MwcbDeclineLevelOwned::decode(data),
        )),
        b'W' => Some(OwnedMessage::MwcbStatus(MwcbStatusOwned::decode(data))),
        b'K' => Some(OwnedMessage::IpoQuotingPeriodUpdate(
            IpoQuotingPeriodUpdateOwned::decode(data),
        )),
        b'J' => Some(OwnedMessage::LuldAuctionCollar(
            LuldAuctionCollarOwned::decode(data),
        )),
        b'h' => Some(OwnedMessage::OperationalHalt(OperationalHaltOwned::decode(
            data,
        ))),
        b'A' => Some(OwnedMessage::AddOrderNoMpid(AddOrderNoMpidOwned::decode(
            data,
        ))),
        b'F' => Some(OwnedMessage::AddOrderMpidAttribution(
            AddOrderWithMpidOwned::decode(data),
        )),
        b'E' => Some(OwnedMessage::OrderExecuted(OrderExecutedOwned::decode(
            data,
        ))),
        b'C' => Some(OwnedMessage::OrderExecutedWithPrice(
            OrderExecutedWithPriceOwned::decode(data),
        )),
        b'X' => Some(OwnedMessage::OrderCancel(OrderCancelOwned::decode(data))),
        b'D' => Some(OwnedMessage::OrderDelete(OrderDeleteOwned::decode(data))),
        b'U' => Some(OwnedMessage::OrderReplace(OrderReplaceOwned::decode(data))),
        b'P' => Some(OwnedMessage::Trade(TradeOwned::decode(data))),
        b'Q' => Some(OwnedMessage::CrossTrade(CrossTradeOwned::decode(data))),
        b'B' => Some(OwnedMessage::BrokenTrade(BrokenTradeOwned::decode(data))),
        b'I' => Some(OwnedMessage::NetOrderImbalanceIndicator(NoiiOwned::decode(
            data,
        ))),
        b'N' => Some(OwnedMessage::RetailPriceImprovementIndicator(
            RetailPriceImprovementOwned::decode(data),
        )),
        b'O' => Some(OwnedMessage::DirectListingCapitalRaisePriceDiscovery(
            DirectListingCapitalRaiseOwned::decode(data),
        )),
        _ => None,
    }
}

/// owned message, one variant per fixed kind covered by this baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedMessage {
    SystemEvent(SystemEventOwned),
    StockDirectory(StockDirectoryOwned),
    StockTradingAction(StockTradingActionOwned),
    RegShoRestriction(RegShoRestrictionOwned),
    MarketParticipantPosition(MarketParticipantPositionOwned),
    MwcbDeclineLevel(MwcbDeclineLevelOwned),
    MwcbStatus(MwcbStatusOwned),
    IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdateOwned),
    LuldAuctionCollar(LuldAuctionCollarOwned),
    OperationalHalt(OperationalHaltOwned),
    AddOrderNoMpid(AddOrderNoMpidOwned),
    AddOrderMpidAttribution(AddOrderWithMpidOwned),
    OrderExecuted(OrderExecutedOwned),
    OrderExecutedWithPrice(OrderExecutedWithPriceOwned),
    OrderCancel(OrderCancelOwned),
    OrderDelete(OrderDeleteOwned),
    OrderReplace(OrderReplaceOwned),
    Trade(TradeOwned),
    CrossTrade(CrossTradeOwned),
    BrokenTrade(BrokenTradeOwned),
    NetOrderImbalanceIndicator(NoiiOwned),
    RetailPriceImprovementIndicator(RetailPriceImprovementOwned),
    DirectListingCapitalRaisePriceDiscovery(DirectListingCapitalRaiseOwned),
}

// every decode() below assumes data.len() == SIZE, asserted up front. slice
// ranges are in-bounds for that exact length by construction.

/// `S` System Event Message, 12B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemEventOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub event_code: u8,
}

impl SystemEventOwned {
    pub const SIZE: usize = 12;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "SystemEvent wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            event_code: data[11],
        }
    }
}

/// `R` Stock Directory, 39B. Doc renders this table's field/value columns
/// split across several markdown blocks (PDF conversion artifact); merged
/// back into one Name/Offset/Length/Type row set here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StockDirectoryOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub market_category: u8,
    pub financial_status_indicator: u8,
    pub round_lot_size: u32,
    pub round_lots_only: u8,
    pub issue_classification: u8,
    pub issue_sub_type: [u8; 2],
    pub authenticity: u8,
    pub short_sale_threshold_indicator: u8,
    pub ipo_flag: u8,
    pub luld_reference_price_tier: u8,
    pub etp_flag: u8,
    pub etp_leverage_factor: u32,
    pub inverse_indicator: u8,
}

impl StockDirectoryOwned {
    pub const SIZE: usize = 39;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "StockDirectory wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            market_category: data[19],
            financial_status_indicator: data[20],
            round_lot_size: be_u32(data, 21),
            round_lots_only: data[25],
            issue_classification: data[26],
            issue_sub_type: be_alpha(data, 27),
            authenticity: data[29],
            short_sale_threshold_indicator: data[30],
            ipo_flag: data[31],
            luld_reference_price_tier: data[32],
            etp_flag: data[33],
            etp_leverage_factor: be_u32(data, 34),
            inverse_indicator: data[38],
        }
    }
}

/// `H` Stock Trading Action, 25B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StockTradingActionOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub trading_state: u8,
    pub reserved: u8,
    pub reason: [u8; 4],
}

impl StockTradingActionOwned {
    pub const SIZE: usize = 25;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "StockTradingAction wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            trading_state: data[19],
            reserved: data[20],
            reason: be_alpha(data, 21),
        }
    }
}

/// `Y` Reg SHO Restriction (Reg SHO Short Sale Price Test Restricted
/// Indicator), 20B. Doc's own field label for the first field on this table
/// is "Locate Code", not "Stock Locate" used by every other kind; kept
/// literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegShoRestrictionOwned {
    pub locate_code: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub reg_sho_action: u8,
}

impl RegShoRestrictionOwned {
    pub const SIZE: usize = 20;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "RegShoRestriction wire row wrong length"
        );
        Self {
            locate_code: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            reg_sho_action: data[19],
        }
    }
}

/// `L` Market Participant Position, 26B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketParticipantPositionOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub mpid: [u8; 4],
    pub stock: [u8; 8],
    pub primary_market_maker: u8,
    pub market_maker_mode: u8,
    pub market_participant_state: u8,
}

impl MarketParticipantPositionOwned {
    pub const SIZE: usize = 26;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "MarketParticipantPosition wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            mpid: be_alpha(data, 11),
            stock: be_alpha(data, 15),
            primary_market_maker: data[23],
            market_maker_mode: data[24],
            market_participant_state: data[25],
        }
    }
}

/// `V` MWCB Decline Level Message, 35B. Levels are Price(8), decoded as
/// plain unsigned 64-bit integers per the Data Types section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MwcbDeclineLevelOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub level_1: u64,
    pub level_2: u64,
    pub level_3: u64,
}

impl MwcbDeclineLevelOwned {
    pub const SIZE: usize = 35;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "MwcbDeclineLevel wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            level_1: be_u64(data, 11),
            level_2: be_u64(data, 19),
            level_3: be_u64(data, 27),
        }
    }
}

/// `W` MWCB Status Message, 12B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MwcbStatusOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub breached_level: u8,
}

impl MwcbStatusOwned {
    pub const SIZE: usize = 12;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "MwcbStatus wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            breached_level: data[11],
        }
    }
}

/// `K` IPO Quoting Period Update, 28B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpoQuotingPeriodUpdateOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub ipo_quotation_release_time: u32,
    pub ipo_quotation_release_qualifier: u8,
    pub ipo_price: u32,
}

impl IpoQuotingPeriodUpdateOwned {
    pub const SIZE: usize = 28;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "IpoQuotingPeriodUpdate wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            ipo_quotation_release_time: be_u32(data, 19),
            ipo_quotation_release_qualifier: data[23],
            ipo_price: be_u32(data, 24),
        }
    }
}

/// `J` LULD Auction Collar, 35B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LuldAuctionCollarOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub auction_collar_reference_price: u32,
    pub upper_auction_collar_price: u32,
    pub lower_auction_collar_price: u32,
    pub auction_collar_extension: u32,
}

impl LuldAuctionCollarOwned {
    pub const SIZE: usize = 35;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "LuldAuctionCollar wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            auction_collar_reference_price: be_u32(data, 19),
            upper_auction_collar_price: be_u32(data, 23),
            lower_auction_collar_price: be_u32(data, 27),
            auction_collar_extension: be_u32(data, 31),
        }
    }
}

/// `h` Operational Halt, 21B. Lowercase type byte, distinct from `H` Stock
/// Trading Action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalHaltOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub market_code: u8,
    pub operational_halt_action: u8,
}

impl OperationalHaltOwned {
    pub const SIZE: usize = 21;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OperationalHalt wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            market_code: data[19],
            operational_halt_action: data[20],
        }
    }
}

/// `A` Add Order - No MPID Attribution, 36B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddOrderNoMpidOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub buy_sell_indicator: u8,
    pub shares: u32,
    pub stock: [u8; 8],
    pub price: u32,
}

impl AddOrderNoMpidOwned {
    pub const SIZE: usize = 36;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "AddOrderNoMpid wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            buy_sell_indicator: data[19],
            shares: be_u32(data, 20),
            stock: be_alpha(data, 24),
            price: be_u32(data, 32),
        }
    }
}

/// `F` Add Order with MPID Attribution, 40B. Same shape as `A` plus trailing
/// attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddOrderWithMpidOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub buy_sell_indicator: u8,
    pub shares: u32,
    pub stock: [u8; 8],
    pub price: u32,
    pub attribution: [u8; 4],
}

impl AddOrderWithMpidOwned {
    pub const SIZE: usize = 40;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "AddOrderWithMpid wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            buy_sell_indicator: data[19],
            shares: be_u32(data, 20),
            stock: be_alpha(data, 24),
            price: be_u32(data, 32),
            attribution: be_alpha(data, 36),
        }
    }
}

/// `E` Order Executed Message, 31B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderExecutedOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub executed_shares: u32,
    pub match_number: u64,
}

impl OrderExecutedOwned {
    pub const SIZE: usize = 31;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderExecuted wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            executed_shares: be_u32(data, 19),
            match_number: be_u64(data, 23),
        }
    }
}

/// `C` Order Executed With Price, 36B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderExecutedWithPriceOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub executed_shares: u32,
    pub match_number: u64,
    pub printable: u8,
    pub execution_price: u32,
}

impl OrderExecutedWithPriceOwned {
    pub const SIZE: usize = 36;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderExecutedWithPrice wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            executed_shares: be_u32(data, 19),
            match_number: be_u64(data, 23),
            printable: data[31],
            execution_price: be_u32(data, 32),
        }
    }
}

/// `X` Order Cancel Message, 23B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderCancelOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub cancelled_shares: u32,
}

impl OrderCancelOwned {
    pub const SIZE: usize = 23;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "OrderCancel wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            cancelled_shares: be_u32(data, 19),
        }
    }
}

/// `D` Order Delete Message, 19B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderDeleteOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
}

impl OrderDeleteOwned {
    pub const SIZE: usize = 19;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "OrderDelete wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
        }
    }
}

/// `U` Order Replace Message, 35B. Side, stock symbol, and attribution
/// carry forward from the original Add Order and are not present here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderReplaceOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub original_order_reference_number: u64,
    pub new_order_reference_number: u64,
    pub shares: u32,
    pub price: u32,
}

impl OrderReplaceOwned {
    pub const SIZE: usize = 35;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "OrderReplace wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            original_order_reference_number: be_u64(data, 11),
            new_order_reference_number: be_u64(data, 19),
            shares: be_u32(data, 27),
            price: be_u32(data, 31),
        }
    }
}

/// `P` Trade Message (Non-Cross), 44B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub order_reference_number: u64,
    pub buy_sell_indicator: u8,
    pub shares: u32,
    pub stock: [u8; 8],
    pub price: u32,
    pub match_number: u64,
}

impl TradeOwned {
    pub const SIZE: usize = 44;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "Trade wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            order_reference_number: be_u64(data, 11),
            buy_sell_indicator: data[19],
            shares: be_u32(data, 20),
            stock: be_alpha(data, 24),
            price: be_u32(data, 32),
            match_number: be_u64(data, 36),
        }
    }
}

/// `Q` Cross Trade Message, 40B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrossTradeOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub shares: u64,
    pub stock: [u8; 8],
    pub cross_price: u32,
    pub match_number: u64,
    pub cross_type: u8,
}

impl CrossTradeOwned {
    pub const SIZE: usize = 40;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "CrossTrade wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            shares: be_u64(data, 11),
            stock: be_alpha(data, 19),
            cross_price: be_u32(data, 27),
            match_number: be_u64(data, 31),
            cross_type: data[39],
        }
    }
}

/// `B` Broken Trade Message, 19B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokenTradeOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub match_number: u64,
}

impl BrokenTradeOwned {
    pub const SIZE: usize = 19;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "BrokenTrade wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            match_number: be_u64(data, 11),
        }
    }
}

/// `I` Net Order Imbalance Indicator, 50B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoiiOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub paired_shares: u64,
    pub imbalance_shares: u64,
    pub imbalance_direction: u8,
    pub stock: [u8; 8],
    pub far_price: u32,
    pub near_price: u32,
    pub current_reference_price: u32,
    pub cross_type: u8,
    pub price_variation_indicator: u8,
}

impl NoiiOwned {
    pub const SIZE: usize = 50;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "Noii wire row wrong length");
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            paired_shares: be_u64(data, 11),
            imbalance_shares: be_u64(data, 19),
            imbalance_direction: data[27],
            stock: be_alpha(data, 28),
            far_price: be_u32(data, 36),
            near_price: be_u32(data, 40),
            current_reference_price: be_u32(data, 44),
            cross_type: data[48],
            price_variation_indicator: data[49],
        }
    }
}

/// `N` Retail Price Improvement Indicator, 20B. Message-row Notes column
/// calls this a "Retail Interest message"; struct name follows the section
/// and table title instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetailPriceImprovementOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub interest_flag: u8,
}

impl RetailPriceImprovementOwned {
    pub const SIZE: usize = 20;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "RetailPriceImprovement wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            interest_flag: data[19],
        }
    }
}

/// `O` Direct Listing with Capital Raise Price Discovery Message, 48B.
/// Near Execution Time is an 8-byte plain Integer field, not the 6-byte
/// Timestamp used elsewhere in this doc, so it decodes via `be_u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectListingCapitalRaiseOwned {
    pub stock_locate: u16,
    pub tracking_number: u16,
    pub timestamp: u64,
    pub stock: [u8; 8],
    pub open_eligibility_status: u8,
    pub minimum_allowable_price: u32,
    pub maximum_allowable_price: u32,
    pub near_execution_price: u32,
    pub near_execution_time: u64,
    pub lower_price_range_collar: u32,
    pub upper_price_range_collar: u32,
}

impl DirectListingCapitalRaiseOwned {
    pub const SIZE: usize = 48;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "DirectListingCapitalRaise wire row wrong length"
        );
        Self {
            stock_locate: be_u16(data, 1),
            tracking_number: be_u16(data, 3),
            timestamp: be_u48(data, 5),
            stock: be_alpha(data, 11),
            open_eligibility_status: data[19],
            minimum_allowable_price: be_u32(data, 20),
            maximum_allowable_price: be_u32(data, 24),
            near_execution_price: be_u32(data, 28),
            near_execution_time: be_u64(data, 32),
            lower_price_range_collar: be_u32(data, 40),
            upper_price_range_collar: be_u32(data, 44),
        }
    }
}
