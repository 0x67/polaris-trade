//! ITCH 5.0 wire builders, shared by the integration test binaries. Every
//! message opens with its type byte, a stock locate, a tracking number, and
//! 48-bit nanoseconds since midnight; per-kind sizes come from the generated
//! views. One security throughout.

use proto_nasdaq::itch_v5;

pub const LOCATE: u16 = 1;
pub const STOCK: &[u8; 8] = b"AAPL    ";

fn message(kind: u8, len: usize, locate: u16, ts_ns: u64) -> Vec<u8> {
    let mut row = vec![0u8; len];
    row[0] = kind;
    row[1..3].copy_from_slice(&locate.to_be_bytes());
    row[5..11].copy_from_slice(&ts_ns.to_be_bytes()[2..]);
    row
}

/// `S` SystemEvent: event_code byte at offset 11; `O` starts the feed, `C` ends
/// it. Both map to a `Control` record. Market-wide, so the locate is zero.
pub fn system_event(code: u8, ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'S', itch_v5::SystemEventView::SIZE, 0, ts_ns);
    row[11] = code;
    row
}

/// `R` StockDirectory -> Definition, carrying the symbol and round lot size.
pub fn stock_directory(ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'R', itch_v5::StockDirectoryView::SIZE, LOCATE, ts_ns);
    row[11..19].copy_from_slice(STOCK);
    row[21..25].copy_from_slice(&100u32.to_be_bytes());
    row
}

/// `H` StockTradingAction -> Status; state `T` is trading.
pub fn trading_action(ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'H', itch_v5::StockTradingActionView::SIZE, LOCATE, ts_ns);
    row[11..19].copy_from_slice(STOCK);
    row[19] = b'T';
    row
}

/// `A` AddOrderNoMpid -> Mbo (+ derived Mbp10/Bbo). order reference u64, side,
/// shares u32, stock, price u32 in 1/10000 dollars.
pub fn add(order: u64, side: u8, shares: u32, price: u32, ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'A', itch_v5::AddOrderNoMpidView::SIZE, LOCATE, ts_ns);
    row[11..19].copy_from_slice(&order.to_be_bytes());
    row[19] = side;
    row[20..24].copy_from_slice(&shares.to_be_bytes());
    row[24..32].copy_from_slice(STOCK);
    row[32..36].copy_from_slice(&price.to_be_bytes());
    row
}

/// `E` OrderExecuted against a resting order -> Mbo(Fill) + Trade at the
/// resting order's price.
pub fn executed(order: u64, shares: u32, match_no: u64, ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'E', itch_v5::OrderExecutedView::SIZE, LOCATE, ts_ns);
    row[11..19].copy_from_slice(&order.to_be_bytes());
    row[19..23].copy_from_slice(&shares.to_be_bytes());
    row[23..31].copy_from_slice(&match_no.to_be_bytes());
    row
}

/// `D` OrderDelete -> Mbo, taking the resting order off the book.
pub fn delete(order: u64, ts_ns: u64) -> Vec<u8> {
    let mut row = message(b'D', itch_v5::OrderDeleteView::SIZE, LOCATE, ts_ns);
    row[11..19].copy_from_slice(&order.to_be_bytes());
    row
}
