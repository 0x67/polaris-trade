//! Instrument and participant identity.
//!
//! Minted stable ids with point-in-time alias resolution, and participant
//! attribution keyed on the alpha broker code. Both registries rebuild from
//! their own event streams, so no persisted mapping is needed across restart or
//! replay.

pub mod definition;
pub mod participant;
pub mod registry;

pub use definition::DefinitionEvent;
pub use participant::{AttributionCapability, ParticipantId, ParticipantRegistry};
pub use registry::{InstrumentId, InstrumentRegistry, VenueId};
