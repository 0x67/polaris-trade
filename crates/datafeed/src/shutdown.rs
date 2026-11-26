//! Process shutdown flag. A sync SIGINT/SIGTERM handler in `main` sets it; a
//! live [`crate::FeedSession`] checks it at batch boundaries and ends, and the
//! binary then closes the session, finishing the capture. A replay never
//! watches it and runs to the end of its capture.

use std::sync::{Arc, OnceLock, atomic::AtomicBool};

static FLAG: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// Process-wide stop flag, lazily created on first access. `main` registers
/// SIGINT/SIGTERM against this `Arc`; [`crate::app::run_config`] passes a clone
/// to [`crate::FeedSessionBuilder::stop`] for a live source only. Shared here,
/// not threaded through `run_config`, so signal install stays a binary concern.
pub fn flag() -> &'static Arc<AtomicBool> {
    FLAG.get_or_init(|| Arc::new(AtomicBool::new(false)))
}
