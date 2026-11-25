//! The normalized event enum every venue adapter emits.

use core::mem::size_of;

use md_events::{
    Control, Definition, Fill, Imbalance, Level, Mbo, Record, RecordHeader, Stats, Status, Trade,
    TradeBreak,
};

use crate::{ingest::StreamId, output::RecordRef};

/// One normalized record a venue adapter emitted.
///
/// Adapters emit events and engines derive snapshots, so the engine-derived
/// `Mbp10`, `Bbo`, and `Ohlcv` are not variants. `Mbp10` is 480 bytes and would
/// set the copy width of every emit, including the 96-byte [`Mbo`] path.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MarketEvent {
    /// Order-level book event.
    Mbo(Mbo),
    /// Price-level book event from an MBP-native feed.
    Level(Level),
    /// Trade print.
    Trade(Trade),
    /// Cancellation of an earlier trade.
    TradeBreak(TradeBreak),
    /// Auction imbalance.
    Imbalance(Imbalance),
    /// Participant-attributed fill.
    Fill(Fill),
    /// Venue-published statistic.
    Stats(Stats),
    /// Trading-state transition.
    Status(Status),
    /// Feed lifecycle event.
    Control(Control),
    /// Instrument definition.
    Definition(Definition),
}

// Widest variant is the 176-byte Definition, tail included; this pins the
// per-emit copy width every adapter pays, whatever the variant.
const _: () = assert!(size_of::<MarketEvent>() <= 176);

impl MarketEvent {
    /// The record's common header.
    #[inline]
    pub fn header(&self) -> &RecordHeader {
        match self {
            MarketEvent::Mbo(r) => r.header(),
            MarketEvent::Level(r) => r.header(),
            MarketEvent::Trade(r) => r.header(),
            MarketEvent::TradeBreak(r) => r.header(),
            MarketEvent::Imbalance(r) => r.header(),
            MarketEvent::Fill(r) => r.header(),
            MarketEvent::Stats(r) => r.header(),
            MarketEvent::Status(r) => r.header(),
            MarketEvent::Control(r) => r.header(),
            MarketEvent::Definition(r) => r.header(),
        }
    }

    /// The record's rtype byte, read from its header.
    #[inline]
    pub fn rtype(&self) -> u8 {
        self.header().rtype
    }

    /// Borrow the record as header plus body bytes on `stream`, the form sinks
    /// and the C ABI consume. No serialization: the `repr(C)` record already is
    /// its wire form. Egress fields hold whatever the header carries now.
    #[inline]
    pub fn as_record_ref(&self, stream: StreamId) -> RecordRef<'_> {
        match self {
            MarketEvent::Mbo(r) => record_ref(r, stream),
            MarketEvent::Level(r) => record_ref(r, stream),
            MarketEvent::Trade(r) => record_ref(r, stream),
            MarketEvent::TradeBreak(r) => record_ref(r, stream),
            MarketEvent::Imbalance(r) => record_ref(r, stream),
            MarketEvent::Fill(r) => record_ref(r, stream),
            MarketEvent::Stats(r) => record_ref(r, stream),
            MarketEvent::Status(r) => record_ref(r, stream),
            MarketEvent::Control(r) => record_ref(r, stream),
            MarketEvent::Definition(r) => record_ref(r, stream),
        }
    }
}

/// Split one md-events record into its header and the bytes after it.
fn record_ref<T: Record>(rec: &T, stream: StreamId) -> RecordRef<'_> {
    // SAFETY: callers pass only generated md-events records, `repr(C)` and `Copy`. The generator
    // spells every gap as an explicit `_pad` field and const-asserts each record's size, so no
    // implicit padding exists and every byte behind `rec` is initialized.
    let bytes =
        unsafe { core::slice::from_raw_parts((rec as *const T).cast::<u8>(), size_of::<T>()) };
    RecordRef {
        stream,
        header: rec.header(),
        body: &bytes[size_of::<RecordHeader>()..],
    }
}
