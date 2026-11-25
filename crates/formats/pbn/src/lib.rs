//! pbn: Polaris Binary eNcoding.
//!
//! The customer-facing wire and file format for normalized market-data records:
//! a self-describing stream header (`PBN1`), the self-delimiting record frame
//! rule with a bounds-checked walk, running symbology, and the schema-version
//! gate. Records themselves are the md-events contract; pbn is the envelope and
//! codec around them, shared by the live feed, historical downloads, and offline
//! replay, so one decode path serves all three.
//!
//! Scope is the format only (the encoding). The historical and live API grammar
//! (queries, session frames, filters, auth) is a separate documented protocol
//! implemented by the gateway and the SDKs, not part of this crate.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod container;
pub mod dispatch;
pub mod error;
pub mod header;
pub mod stream;
pub mod symbology;
pub mod validate;
pub mod version;

pub use container::{compress, decompress, read_stream, write_stream};
pub use dispatch::{AnyRecord, decode_any, read_records};
pub use error::PbnError;
pub use header::{
    FLAG_SYMBOLOGY, FLAG_ZSTD_BODY, HEADER_LEN, KIND_NORMALIZED, MAGIC, PROTO_VERSION, StreamHeader,
};
// One import surface: consumers reach the record layer through pbn.
pub use md_events;
pub use stream::{Frame, Frames, MAX_FRAME_LEN, frames, write_record};
pub use symbology::Symbology;
pub use validate::validate;
pub use version::{UpgradePolicy, check_compat};
