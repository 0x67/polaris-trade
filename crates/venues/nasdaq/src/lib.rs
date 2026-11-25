//! NASDAQ venue crate, generated wire-parse modules land under here per protocol major

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

/// Capture feed discriminator for this venue, stamped at MDC1 header byte 13.
/// Public-venue range is 1..=127. An on-disk contract across every capture
/// written, so it is never renumbered. see md_capture::CaptureMeta
pub const FEED_NASDAQ: u8 = 3;

#[cfg(feature = "adapter")]
pub mod adapter;
pub mod itch_v5;
pub mod ouch_v5;
