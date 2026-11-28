//! Rebuilds a [`MarketEvent`] from record bytes a plugin handed out. The bytes
//! come from another build, so every check a bit pattern needs before it can
//! be read as a record runs here: the rtype names a [`MarketEvent`] variant,
//! the length is that record's size and agrees with the header, and every
//! enum field holds one of its variants.

use core::mem::{offset_of, size_of};

use md_events::{
    Action, AssetClass, BreakRefKind, Control, ControlKind, Definition, Fill, Imbalance, Level,
    LevelAction, Mbo, OptionType, Record, Side, StatType, Stats, Status, Trade, TradeBreak,
    TradingState, rtype,
};
use md_pipeline::MarketEvent;

/// One `is_<enum>(raw) -> bool` per md-events enum a [`MarketEvent`] record
/// carries. Each lists every variant in an exhaustive match, so a variant
/// added to the enum fails to compile here until it is listed.
macro_rules! variants {
    ($($check:ident: $ty:ident as $repr:ty { $($variant:ident),+ $(,)? })+) => {$(
        fn $check(raw: $repr) -> bool {
            const _: fn($ty) = |value| match value {
                $($ty::$variant)|+ => {}
            };
            [$($ty::$variant as $repr),+].contains(&raw)
        }
    )+};
}

variants! {
    is_action: Action as u8 { Add, Cancel, Modify, Fill, FillWithPrice, Trade, Reset }
    is_side: Side as u8 { Bid, Ask, None }
    is_level_action: LevelAction as u8 { New, Change, Delete, Clear }
    is_break_ref_kind: BreakRefKind as u8 { MatchNumber, SecondaryTradeId }
    is_control_kind: ControlKind as u8 {
        Heartbeat, FeedStart, FeedEnd, GapDetected, SeqReset, SnapshotBegin, SnapshotEnd,
        RecoveryStart, RecoveryComplete,
    }
    is_stat_type: StatType as u16 {
        PriceLimits, ReferencePrice, IndexPrice, OpeningPrice, ClosingPrice, TotalVolume,
        TotalValue, TradeCount, HighPrice, LowPrice, Vwap, FundingRate, OpenInterest, MarkPrice,
        SettlementPrice, Liquidation, InsuranceFund, NetAssetValue, VenueSpecific,
    }
    is_trading_state: TradingState as u8 {
        Unknown, PreOpen, OpeningAuction, Continuous, IntradayAuction, Halt, Pause, Break,
        ClosingAuction, PostClose, Closed, Suspended, NoTrade,
    }
    is_asset_class: AssetClass as u8 {
        Equity, Future, Option, Perpetual, Spot, Index, Fx, Bond, Commodity, Etf,
    }
    is_option_type: OptionType as u8 { None, Call, Put }
}

/// The event `bytes` holds, or `None` when they are not one whole
/// [`MarketEvent`] record: too short for a header, an rtype of no variant, a
/// length other than the record's size or its header's, or an enum field
/// outside its variants.
pub(crate) fn event(bytes: &[u8]) -> Option<MarketEvent> {
    match *bytes.get(1)? {
        rtype::MBO => read::<Mbo>(bytes, |b| {
            is_action(b[offset_of!(Mbo, action)]) && is_side(b[offset_of!(Mbo, side)])
        })
        .map(MarketEvent::Mbo),
        rtype::LEVEL => read::<Level>(bytes, |b| {
            is_side(b[offset_of!(Level, side)]) && is_level_action(b[offset_of!(Level, action)])
        })
        .map(MarketEvent::Level),
        rtype::TRADE => read::<Trade>(bytes, |b| is_side(b[offset_of!(Trade, aggressor)]))
            .map(MarketEvent::Trade),
        rtype::TRADE_BREAK => read::<TradeBreak>(bytes, |b| {
            is_break_ref_kind(b[offset_of!(TradeBreak, ref_kind)])
        })
        .map(MarketEvent::TradeBreak),
        rtype::IMBALANCE => {
            read::<Imbalance>(bytes, |b| is_side(b[offset_of!(Imbalance, imbalance_side)]))
                .map(MarketEvent::Imbalance)
        }
        rtype::FILL => read::<Fill>(bytes, |b| {
            is_break_ref_kind(b[offset_of!(Fill, ref_kind)])
                && is_side(b[offset_of!(Fill, aggressor)])
        })
        .map(MarketEvent::Fill),
        rtype::STATS => read::<Stats>(bytes, |b| {
            let at = offset_of!(Stats, stat_type);
            is_stat_type(u16::from_ne_bytes([b[at], b[at + 1]]))
        })
        .map(MarketEvent::Stats),
        rtype::STATUS => read::<Status>(bytes, |b| is_trading_state(b[offset_of!(Status, state)]))
            .map(MarketEvent::Status),
        rtype::CONTROL => read::<Control>(bytes, |b| is_control_kind(b[offset_of!(Control, kind)]))
            .map(MarketEvent::Control),
        rtype::DEFINITION => read::<Definition>(bytes, |b| {
            is_asset_class(b[offset_of!(Definition, asset_class)])
                && is_option_type(b[offset_of!(Definition, tail.option_type)])
        })
        .map(MarketEvent::Definition),
        _ => None,
    }
}

/// Reads record `T` out of `bytes` once the length checks and `enums_valid`,
/// which checks every enum field of `T`, pass.
fn read<T: Record>(bytes: &[u8], enums_valid: impl FnOnce(&[u8]) -> bool) -> Option<T> {
    // `length` counts 32-bit words, and every record is at least a header long
    let whole = bytes.len() == size_of::<T>() && usize::from(bytes[0]) * 4 == bytes.len();
    if !whole || !enums_valid(bytes) {
        return None;
    }
    // SAFETY: `bytes` holds `size_of::<T>()` bytes, read unaligned. `T` is a generated md-events
    // record, `repr(C)` and `Copy`, whose fields are integers, byte arrays and the enums the
    // caller's `enums_valid` checked, so every pattern that passed is a valid `T`. An enum
    // field added to a record must join the check in `event`.
    Some(unsafe { bytes.as_ptr().cast::<T>().read_unaligned() })
}

#[cfg(test)]
mod tests;
