//! Shared error types and alpha-field helpers for generated venue crates.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod alpha;
pub mod appendage;
pub mod error;

pub use alpha::trim_alpha;
pub use appendage::AppendageIter;
pub use error::{ParseError, TrailingBytes, WrongLength};
