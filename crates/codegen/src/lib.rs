//! Schema DSL model and generator entry points for proto-codegen.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

pub mod cli;
pub mod emit;
pub mod record_emit;
pub mod record_schema;
pub mod schema;
