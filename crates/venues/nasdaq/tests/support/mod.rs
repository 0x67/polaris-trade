//! test support modules, hand-written, never generated.
//!
//! each parity test file is its own crate (rust integration test convention), so
//! whichever baseline module a given binary does not call into reads as dead code
//! from that binary's view. allow(dead_code) on both keeps clippy clean per binary
//! without silencing genuine unused-code hits inside proto-nasdaq's own lib target.

#[allow(dead_code)]
pub mod owned_itch_v5;
#[allow(dead_code)]
pub mod owned_ouch_v5;
