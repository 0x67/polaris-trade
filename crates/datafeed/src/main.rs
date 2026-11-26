//! datafeed command-line entry point.

use std::{path::PathBuf, process::ExitCode, sync::Arc};

use datafeed::{
    app::{CliOverrides, run},
    shutdown,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--version") {
        println!("datafeed {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let Some(path) = args.iter().find(|a| !a.starts_with("--")).cloned() else {
        eprintln!(
            "usage: datafeed <config.yaml|config.json> [--ohlcv-resolutions=1s,5m,1h]\n       datafeed --version"
        );
        return ExitCode::FAILURE;
    };
    let overrides = CliOverrides::from_args(&args);

    // Arm shutdown before the runner starts: a live capture must drain, close
    // writers, and write manifests on SIGINT/SIGTERM instead of truncating.
    if let Err(err) = install_shutdown_signals() {
        eprintln!("datafeed: cannot install signal handlers: {err}");
        return ExitCode::FAILURE;
    }

    match run(PathBuf::from(path), overrides) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("datafeed: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Register a sync SIGINT + SIGTERM handler that sets the shared stop flag.
/// `signal_hook::flag::register` is async-signal-safe: it only stores into the
/// `Arc<AtomicBool>` the live loop polls.
fn install_shutdown_signals() -> std::io::Result<()> {
    let flag = shutdown::flag();
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(flag))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(flag))?;
    Ok(())
}
