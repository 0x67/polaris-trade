//! datafeed command-line entry point.
//!
//! Owns the process-global state the library leaves alone: the shutdown
//! signal handlers and the logging and metrics installs.

mod signals;
mod telemetry;

use std::{
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, atomic::AtomicBool},
};

use datafeed::{
    FeedError,
    app::{CliOverrides, load_config, run_config},
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

    // Arm shutdown before the session starts: a live capture must drain, close
    // writers, and write manifests on SIGINT/SIGTERM instead of truncating.
    let stop = Arc::new(AtomicBool::new(false));
    if let Err(err) = signals::install(&stop) {
        eprintln!("datafeed: cannot install signal handlers: {err}");
        return ExitCode::FAILURE;
    }

    match run(PathBuf::from(path), &overrides, stop) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("datafeed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(path: PathBuf, overrides: &CliOverrides, stop: Arc<AtomicBool>) -> Result<(), FeedError> {
    let config = load_config(&path, overrides)?;
    // Held to the end of the run: dropping it stops the exporter.
    let _telemetry = telemetry::install(config.observability.as_ref())?;
    run_config(config, stop)
}
