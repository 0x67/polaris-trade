//! Ingest and output pipeline.
//!
//! Source and sink traits, the venue-adapter seam with its session model,
//! egress sequencer (sole clock point), instrument identity registries,
//! versioned config, coverage matrix.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod adapter;
pub mod config;
pub mod coverage;
pub mod identity;
pub mod ingest;
pub mod output;

#[cfg(feature = "tz")]
pub use adapter::BundledTzdb;
pub use adapter::{
    AdapterFault, AdapterStats, ClosureModel, DynAdapter, MarketEvent, NoTzdb, RollModel,
    SessionModel, TzResolver, VenueAdapter,
};
pub use config::{
    BoardMap, BoardRule, CivilDate, ConfigError, DeclaresVocabulary, EffectiveRange,
    IndexComposition, IndexMember, Purpose, StatusFlagRule, StatusFlagVocab, Tier, TierTable,
    VersionedConfig, VocabKind, Vocabulary,
};
pub use coverage::{Coverage, CoverageMatrix};
pub use identity::{
    AttributionCapability, DefinitionEvent, InstrumentId, InstrumentRegistry, ParticipantId,
    ParticipantRegistry, VenueId,
};
pub use ingest::{IngestError, IngestFrame, IngestSource, StreamId};
pub use output::{
    EgressClock, EgressSequencer, OutputSink, RecordRef, SinkError, SystemClock, VirtualClock,
};
