//! Normalized market-data event contract.
//!
//! Records, flags, price convention, action vocabulary, trading-state enum.
//! Zero dependencies: the product surface every consumer builds against. Fixed
//! `#[repr(C)]` records with a DBN-style length/rtype prefix so capture files
//! stay skimmable by offset and record growth stays additive.
//!
//! The record data types (records, typed enums, rtype bands, `Record` impls) are
//! generated from `schemas/records/l0_l3.yaml` and live in `generated`. The
//! hand-written modules carry only the logic layered on them: `Price`, `RecordFlags`,
//! `decode_prefix`, `StatType::band`, `Definition::new`.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod action;
pub mod definition;
pub mod flags;
/// Generator-owned record data types, the product surface. Written by proto-codegen
/// from `schemas/records/l0_l3.yaml`; a drift gate keeps the committed tree fresh.
pub mod generated;
pub mod price;
pub mod record;

pub use action::StatBand;
pub use flags::RecordFlags;
pub use generated::{
    Action, AssetClass, Bbo, BidAskPair, BreakRefKind, Category, Control, ControlKind, Definition,
    DefinitionTail, Fill, Imbalance, Level, LevelAction, Mbo, Mbp10, Ohlcv, OptionType, REGISTERED,
    RecordHeader, Side, StatType, Stats, Status, Trade, TradeBreak, TradingState, rtype,
};
pub use price::{Price, SentinelScheme};
pub use record::{Record, decode_prefix};
