//! Replay containers and driver.
//!
//! Capture (our zstd container), pcap, and length-prefixed binary dump readers
//! feeding the same ingest contract as live sockets. Containers are named after
//! their format, not a venue.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

// Gzip backend for the .gz replay path. Default `miniz` is the current pure-Rust
// impl; `zlib-rs`/`zlib-ng` are opt-in via --no-default-features. When more than
// one is enabled (e.g. --all-features) flate2 selects by its own priority
// (zlib-ng > zlib-rs > miniz); `backend()` in the bench reports the same order.
#[cfg(not(any(feature = "miniz", feature = "zlib-rs", feature = "zlib-ng")))]
compile_error!("md-replay needs a gzip backend: `miniz` (default), `zlib-rs`, or `zlib-ng`");

pub mod binary;
pub mod capture;
pub mod detect;
pub mod driver;
pub mod pcap;

pub use binary::{BinaryFileError, BinaryFileFrame, BinaryFileReplay};
pub use capture::{CaptureError, CaptureFrame, CaptureReplay};
pub use detect::{ContainerKind, DetectError, detect_container};
pub use driver::{
    PaceWindow, PacedSource, Pacer, Pacing, PacingMode, PacingSchedule, PacingScheduleError,
    ReplayDriver, SharedClock, ThreadSleepPacer,
};
pub use pcap::{PcapError, PcapFrame, PcapReplay};
