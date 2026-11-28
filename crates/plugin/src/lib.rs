#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! Versioned C ABI for venue adapter plugins, so a closed adapter can ship as
//! a shared library that an open host loads.
//!
//! [`export_adapter!`] turns a [`VenueAdapter`](md_pipeline::VenueAdapter)
//! into the [`PolarisAdapterVtable`] a plugin exports as
//! `polaris_adapter_entry`. On the host side [`Plugin`] checks a vtable's
//! [`POLARIS_PLUGIN_ABI_VERSION`] and reads the venue it declares, and
//! [`PluginAdapter`] drives one adapter of it as a
//! [`DynAdapter`](md_pipeline::DynAdapter), rebuilding every record the plugin
//! hands out after checking its rtype, length and enum fields.
//!
//! The crate depends on the adapter seam and the record types only, so a
//! plugin crate links nothing of the datafeed runtime. A plugin catches every
//! panic at its entry points, which needs a build with `panic = "unwind"`,
//! such as the workspace `ffi` profile. The C view of these types is part of
//! `polaris_abi.h` in `polaris-datafeed-ffi`.

mod abi;
pub mod export;
mod host;

pub use abi::{
    POLARIS_CLOSURE_NONE, POLARIS_CLOSURE_WEEKEND, POLARIS_PLUGIN_ABI_VERSION,
    POLARIS_PLUGIN_ERR_CONFIG, POLARIS_PLUGIN_ERR_NULL, POLARIS_PLUGIN_ERR_PANIC,
    POLARIS_PLUGIN_ERR_SESSION, POLARIS_PLUGIN_OK, POLARIS_ROLL_CONTINUOUS,
    POLARIS_ROLL_DAILY_FIXED, POLARIS_ROLL_DAILY_ZONED, PolarisAdapter, PolarisAdapterEntry,
    PolarisAdapterStats, PolarisAdapterVtable, PolarisRecord, PolarisRecordHeader,
    PolarisSessionModel, PolarisVenue,
};
pub use host::{Plugin, PluginAdapter, PluginError, PluginVenue};

#[cfg(test)]
mod tests;
