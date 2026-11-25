//! Adapter-side input to the instrument registry: a venue directory entry as
//! decoded, before minting. The emitted `md_events::Definition` record is the
//! consumer-facing output; this is the pre-mint input.

use md_events::AssetClass;

use super::registry::VenueId;

/// A venue directory entry handed to [`super::registry::InstrumentRegistry`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionEvent {
    /// Venue this entry came from.
    pub venue: VenueId,
    /// Venue symbol, zero-padded.
    pub raw_symbol: [u8; 20],
    /// Board code, zero-padded.
    pub board: [u8; 4],
    /// Venue-native orderbook id.
    pub venue_native_id: u64,
    /// Effective time of this directory entry.
    pub ts: u64,
    /// Asset class (metadata only).
    pub asset_class: AssetClass,
    /// Quantity scale exponent.
    pub qty_scale: i8,
    /// Round-lot size in the venue's minimal quantity unit.
    pub lot_size: u32,
}
