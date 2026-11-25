//! Offline diagnostic and replay explorer for pbn market-data files.
//!
//! One crate, two faces: this library holds the read/index/query engine; the
//! `mdex` binary is a thin command-line front over it. Built on `pbn` directly
//! because diagnosis needs random access, which the forward-only SDK decoder
//! cannot give.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod cli;
pub mod error;
pub mod filter;
pub mod index;
pub mod query;
pub mod reader;
pub mod render;

pub use error::MdexError;
pub use index::{Entry, Fault, FaultKind, Index};
pub use reader::{READER_MAJOR, Reader};
