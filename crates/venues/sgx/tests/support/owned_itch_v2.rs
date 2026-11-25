//! Hand-transcribed parity baseline for SGX Titan ITCH & GLIMPSE protocol
//! v2.5, all 14 fixed-size kinds (13 ITCH type-byte messages plus GLIMPSE
//! End of Snapshot, section 4.2.3.1). Transcribed straight from the
//! specification's field tables (sections 3.3.2 through 3.3.7, 4.2.3.1).
//! Never generated, never read from schema yaml: independent reference the
//! generated view layer gets checked against.
//!
//! Single-byte coded/numeric fields stay raw `u8` wire bytes, not domain
//! enums. Alpha/text and reserved byte spans stay untrimmed fixed `[u8; N]`
//! arrays. Numeric fields are unsigned big-endian per the doc's own
//! data-type table; Price fields are signed 4-byte big-endian.

fn be_u16(data: &[u8], at: usize) -> u16 {
    u16::from_be_bytes(data[at..at + 2].try_into().unwrap())
}

fn be_u32(data: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(data[at..at + 4].try_into().unwrap())
}

fn be_u64(data: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(data[at..at + 8].try_into().unwrap())
}

fn be_i32(data: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(data[at..at + 4].try_into().unwrap())
}

fn be_alpha<const N: usize>(data: &[u8], at: usize) -> [u8; N] {
    data[at..at + N].try_into().unwrap()
}

/// dispatch fixed-size wire row to its owned decoder. `data` is the full row
/// including the leading msg_type byte at `data[0]`. Modify Order (prose
/// only, no wire table, no letter code) and unknown bytes return `None`.
pub fn decode_owned(msg_type: u8, data: &[u8]) -> Option<OwnedMessage> {
    match msg_type {
        b'T' => Some(OwnedMessage::Seconds(SecondsOwned::decode(data))),
        b'R' => Some(OwnedMessage::OrderBookDirectory(
            OrderBookDirectoryOwned::decode(data),
        )),
        b'M' => Some(OwnedMessage::CombinationOrderBookLeg(
            CombinationOrderBookLegOwned::decode(data),
        )),
        b'L' => Some(OwnedMessage::TickSizeTableEntry(
            TickSizeTableEntryOwned::decode(data),
        )),
        b'S' => Some(OwnedMessage::SystemEvent(SystemEventOwned::decode(data))),
        b'O' => Some(OwnedMessage::OrderBookState(OrderBookStateOwned::decode(
            data,
        ))),
        b'A' => Some(OwnedMessage::AddOrder(AddOrderOwned::decode(data))),
        b'E' => Some(OwnedMessage::OrderExecuted(OrderExecutedOwned::decode(
            data,
        ))),
        b'C' => Some(OwnedMessage::OrderExecutedWithPrice(
            OrderExecutedWithPriceOwned::decode(data),
        )),
        b'U' => Some(OwnedMessage::OrderReplace(OrderReplaceOwned::decode(data))),
        b'D' => Some(OwnedMessage::OrderDelete(OrderDeleteOwned::decode(data))),
        b'P' => Some(OwnedMessage::Trade(TradeOwned::decode(data))),
        b'Z' => Some(OwnedMessage::EquilibriumPriceUpdate(
            EquilibriumPriceUpdateOwned::decode(data),
        )),
        b'G' => Some(OwnedMessage::EndOfSnapshot(EndOfSnapshotOwned::decode(
            data,
        ))),
        _ => None,
    }
}

/// owned message, one variant per fixed kind covered by this baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedMessage {
    Seconds(SecondsOwned),
    OrderBookDirectory(OrderBookDirectoryOwned),
    CombinationOrderBookLeg(CombinationOrderBookLegOwned),
    TickSizeTableEntry(TickSizeTableEntryOwned),
    SystemEvent(SystemEventOwned),
    OrderBookState(OrderBookStateOwned),
    AddOrder(AddOrderOwned),
    OrderExecuted(OrderExecutedOwned),
    OrderExecutedWithPrice(OrderExecutedWithPriceOwned),
    OrderReplace(OrderReplaceOwned),
    OrderDelete(OrderDeleteOwned),
    Trade(TradeOwned),
    EquilibriumPriceUpdate(EquilibriumPriceUpdateOwned),
    EndOfSnapshot(EndOfSnapshotOwned),
}

// every decode() below assumes data.len() == SIZE, asserted up front. slice
// ranges are in-bounds for that exact length by construction.

/// `T` Seconds, 5B. section 3.3.2 Seconds message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondsOwned {
    pub second: u32,
}

impl SecondsOwned {
    pub const SIZE: usize = 5;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "Seconds wire row wrong length");
        Self {
            second: be_u32(data, 1),
        }
    }
}

/// `R` OrderBookDirectory, 129B, 20 fields. section 3.3.3.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderBookDirectoryOwned {
    pub timestamp_nanoseconds: u32,
    pub order_book_id: u32,
    pub symbol: [u8; 32],
    pub long_name: [u8; 32],
    pub isin: [u8; 12],
    pub financial_product: u8,
    pub trading_currency: [u8; 3],
    pub price_decimals: u16,
    pub nominal_value_decimals: u16,
    pub odd_lot_size: u32,
    pub round_lot_size: u32,
    pub block_lot_size: u32,
    pub nominal_value: u64,
    pub number_of_legs: u8,
    pub commodity_code: u32,
    pub strike_price: i32,
    pub expiration_date: u32,
    pub strike_price_decimals: u16,
    pub put_or_call: u8,
}

impl OrderBookDirectoryOwned {
    pub const SIZE: usize = 129;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderBookDirectory wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_book_id: be_u32(data, 5),
            symbol: be_alpha(data, 9),
            long_name: be_alpha(data, 41),
            isin: be_alpha(data, 73),
            financial_product: data[85],
            trading_currency: be_alpha(data, 86),
            price_decimals: be_u16(data, 89),
            nominal_value_decimals: be_u16(data, 91),
            odd_lot_size: be_u32(data, 93),
            round_lot_size: be_u32(data, 97),
            block_lot_size: be_u32(data, 101),
            nominal_value: be_u64(data, 105),
            number_of_legs: data[113],
            commodity_code: be_u32(data, 114),
            strike_price: be_i32(data, 118),
            expiration_date: be_u32(data, 122),
            strike_price_decimals: be_u16(data, 126),
            put_or_call: data[128],
        }
    }
}

/// `M` CombinationOrderBookLeg, 18B. section 3.3.3.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CombinationOrderBookLegOwned {
    pub timestamp_nanoseconds: u32,
    pub combination_order_book_id: u32,
    pub leg_order_book_id: u32,
    pub leg_side: u8,
    pub leg_ratio: u32,
}

impl CombinationOrderBookLegOwned {
    pub const SIZE: usize = 18;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "CombinationOrderBookLeg wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            combination_order_book_id: be_u32(data, 5),
            leg_order_book_id: be_u32(data, 9),
            leg_side: data[13],
            leg_ratio: be_u32(data, 14),
        }
    }
}

/// `L` TickSizeTableEntry, 25B. section 3.3.3.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickSizeTableEntryOwned {
    pub timestamp_nanoseconds: u32,
    pub order_book_id: u32,
    pub tick_size: u64,
    pub price_from: i32,
    pub price_to: i32,
}

impl TickSizeTableEntryOwned {
    pub const SIZE: usize = 25;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "TickSizeTableEntry wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_book_id: be_u32(data, 5),
            tick_size: be_u64(data, 9),
            price_from: be_i32(data, 17),
            price_to: be_i32(data, 21),
        }
    }
}

/// `S` SystemEvent, 6B. section 3.3.4.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemEventOwned {
    pub timestamp_nanoseconds: u32,
    pub event_code: u8,
}

impl SystemEventOwned {
    pub const SIZE: usize = 6;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "SystemEvent wire row wrong length");
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            event_code: data[5],
        }
    }
}

/// `O` OrderBookState, 29B. section 3.3.4.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderBookStateOwned {
    pub timestamp_nanoseconds: u32,
    pub order_book_id: u32,
    pub state_name: [u8; 20],
}

impl OrderBookStateOwned {
    pub const SIZE: usize = 29;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderBookState wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_book_id: be_u32(data, 5),
            state_name: be_alpha(data, 9),
        }
    }
}

/// `A` AddOrder, 37B. section 3.3.5.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddOrderOwned {
    pub timestamp_nanoseconds: u32,
    pub order_id: u64,
    pub order_book_id: u32,
    pub side: u8,
    pub order_book_position: u32,
    pub quantity: u64,
    pub price: i32,
    pub order_attributes: u16,
    pub lot_type: u8,
}

impl AddOrderOwned {
    pub const SIZE: usize = 37;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "AddOrder wire row wrong length");
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_id: be_u64(data, 5),
            order_book_id: be_u32(data, 13),
            side: data[17],
            order_book_position: be_u32(data, 18),
            quantity: be_u64(data, 22),
            price: be_i32(data, 30),
            order_attributes: be_u16(data, 34),
            lot_type: data[36],
        }
    }
}

/// `E` OrderExecuted, 52B. section 3.3.5.3. two 7-byte reserved spans kept
/// untrimmed, no meaning assigned by the doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderExecutedOwned {
    pub timestamp_nanoseconds: u32,
    pub order_id: u64,
    pub order_book_id: u32,
    pub side: u8,
    pub executed_quantity: u64,
    pub match_id: u64,
    pub combo_group_id: u32,
    pub reserved0: [u8; 7],
    pub reserved1: [u8; 7],
}

impl OrderExecutedOwned {
    pub const SIZE: usize = 52;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderExecuted wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_id: be_u64(data, 5),
            order_book_id: be_u32(data, 13),
            side: data[17],
            executed_quantity: be_u64(data, 18),
            match_id: be_u64(data, 26),
            combo_group_id: be_u32(data, 34),
            reserved0: be_alpha(data, 38),
            reserved1: be_alpha(data, 45),
        }
    }
}

/// `C` OrderExecutedWithPrice, 58B. section 3.3.5.4. same reserved spans as
/// `E` plus trailing trade_price/occurred_at_cross/printable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderExecutedWithPriceOwned {
    pub timestamp_nanoseconds: u32,
    pub order_id: u64,
    pub order_book_id: u32,
    pub side: u8,
    pub executed_quantity: u64,
    pub match_id: u64,
    pub combo_group_id: u32,
    pub reserved0: [u8; 7],
    pub reserved1: [u8; 7],
    pub trade_price: i32,
    pub occurred_at_cross: u8,
    pub printable: u8,
}

impl OrderExecutedWithPriceOwned {
    pub const SIZE: usize = 58;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "OrderExecutedWithPrice wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_id: be_u64(data, 5),
            order_book_id: be_u32(data, 13),
            side: data[17],
            executed_quantity: be_u64(data, 18),
            match_id: be_u64(data, 26),
            combo_group_id: be_u32(data, 34),
            reserved0: be_alpha(data, 38),
            reserved1: be_alpha(data, 45),
            trade_price: be_i32(data, 52),
            occurred_at_cross: data[56],
            printable: data[57],
        }
    }
}

/// `U` OrderReplace, 36B. section 3.3.5.5. doc notes no Order Replace is
/// actually seen on the ITCH feed (Delete+Add used instead), but the wire
/// table is fully specified so it is kept in this baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderReplaceOwned {
    pub timestamp_nanoseconds: u32,
    pub order_id: u64,
    pub order_book_id: u32,
    pub side: u8,
    pub new_order_book_position: u32,
    pub quantity: u64,
    pub price: i32,
    pub order_attributes: u16,
}

impl OrderReplaceOwned {
    pub const SIZE: usize = 36;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "OrderReplace wire row wrong length");
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_id: be_u64(data, 5),
            order_book_id: be_u32(data, 13),
            side: data[17],
            new_order_book_position: be_u32(data, 18),
            quantity: be_u64(data, 22),
            price: be_i32(data, 30),
            order_attributes: be_u16(data, 34),
        }
    }
}

/// `D` OrderDelete, 18B. section 3.3.5.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderDeleteOwned {
    pub timestamp_nanoseconds: u32,
    pub order_id: u64,
    pub order_book_id: u32,
    pub side: u8,
}

impl OrderDeleteOwned {
    pub const SIZE: usize = 18;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "OrderDelete wire row wrong length");
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_id: be_u64(data, 5),
            order_book_id: be_u32(data, 13),
            side: data[17],
        }
    }
}

/// `P` Trade, 50B. section 3.3.6.1. two 7-byte reserved spans kept
/// untrimmed, no meaning assigned by the doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeOwned {
    pub timestamp_nanoseconds: u32,
    pub match_id: u64,
    pub combo_group_id: u32,
    pub side: u8,
    pub quantity: u64,
    pub order_book_id: u32,
    pub trade_price: i32,
    pub reserved0: [u8; 7],
    pub reserved1: [u8; 7],
    pub printable: u8,
    pub occurred_at_cross: u8,
}

impl TradeOwned {
    pub const SIZE: usize = 50;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), Self::SIZE, "Trade wire row wrong length");
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            match_id: be_u64(data, 5),
            combo_group_id: be_u32(data, 13),
            side: data[17],
            quantity: be_u64(data, 18),
            order_book_id: be_u32(data, 26),
            trade_price: be_i32(data, 30),
            reserved0: be_alpha(data, 34),
            reserved1: be_alpha(data, 41),
            printable: data[48],
            occurred_at_cross: data[49],
        }
    }
}

/// `Z` EquilibriumPriceUpdate, 53B. section 3.3.7.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EquilibriumPriceUpdateOwned {
    pub timestamp_nanoseconds: u32,
    pub order_book_id: u32,
    pub available_bid_quantity_at_equilibrium_price: u64,
    pub available_ask_quantity_at_equilibrium_price: u64,
    pub equilibrium_price: i32,
    pub best_bid_price: i32,
    pub best_ask_price: i32,
    pub best_bid_quantity: u64,
    pub best_ask_quantity: u64,
}

impl EquilibriumPriceUpdateOwned {
    pub const SIZE: usize = 53;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "EquilibriumPriceUpdate wire row wrong length"
        );
        Self {
            timestamp_nanoseconds: be_u32(data, 1),
            order_book_id: be_u32(data, 5),
            available_bid_quantity_at_equilibrium_price: be_u64(data, 9),
            available_ask_quantity_at_equilibrium_price: be_u64(data, 17),
            equilibrium_price: be_i32(data, 25),
            best_bid_price: be_i32(data, 29),
            best_ask_price: be_i32(data, 33),
            best_bid_quantity: be_u64(data, 37),
            best_ask_quantity: be_u64(data, 45),
        }
    }
}

/// `G` EndOfSnapshot, 21B. GLIMPSE section 4.2.3.1, not ITCH proper.
/// Sequence Number is ASCII text carrying the ITCH SoupBinTCP sequence
/// number, kept as untrimmed bytes like other Alpha fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndOfSnapshotOwned {
    pub sequence_number: [u8; 20],
}

impl EndOfSnapshotOwned {
    pub const SIZE: usize = 21;

    fn decode(data: &[u8]) -> Self {
        assert_eq!(
            data.len(),
            Self::SIZE,
            "EndOfSnapshot wire row wrong length"
        );
        Self {
            sequence_number: be_alpha(data, 1),
        }
    }
}
