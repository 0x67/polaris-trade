//! Process shutdown flag. A sync SIGINT/SIGTERM handler in `main` sets it; the
//! live session loop polls it at batch boundaries and finishes the capture
//! cleanly before exit. See `live::run`.

use std::sync::{Arc, OnceLock, atomic::AtomicBool};

static FLAG: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// Process-wide stop flag, lazily created on first access. `main` registers
/// SIGINT/SIGTERM against this `Arc`; `app::run_config` hands a borrow to the
/// live runner, which polls it. Shared here, not threaded through `run_config`,
/// so signal install stays a binary concern.
pub fn flag() -> &'static Arc<AtomicBool> {
    FLAG.get_or_init(|| Arc::new(AtomicBool::new(false)))
}
