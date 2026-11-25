//! Venue-adapter seam: the contract between a venue's wire decoder and the
//! pipeline.
//!
//! An adapter turns wire bytes into [`MarketEvent`]s and declares the
//! [`SessionModel`] its trading day follows. [`VenueAdapter`] is the static
//! hot-path trait; [`DynAdapter`] is its object-safe erasure. Zone-derived
//! session anchors resolve through a [`TzResolver`], with the bundled-tzdb
//! implementation behind the `tz` feature.

mod contract;
mod event;
mod session;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(test)]
mod tests;
#[cfg(feature = "tz")]
mod tz;

pub use contract::{AdapterStats, DynAdapter, VenueAdapter};
pub use event::MarketEvent;
pub use session::{ClosureModel, NoTzdb, RollModel, SessionModel, TzResolver};
#[cfg(feature = "tz")]
pub use tz::BundledTzdb;
