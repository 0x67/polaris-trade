//! SIGINT and SIGTERM handlers that set the session's stop flag.

use std::{
    io,
    sync::{Arc, atomic::AtomicBool},
};

/// Register a sync SIGINT + SIGTERM handler that sets `stop`.
/// `signal_hook::flag::register` is async-signal-safe: it only stores into the
/// `Arc<AtomicBool>` the live loop polls.
pub fn install(stop: &Arc<AtomicBool>) -> io::Result<()> {
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(stop))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(stop))?;
    Ok(())
}
