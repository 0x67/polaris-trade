//! mdex command-line entry point.

use tracing_subscriber::EnvFilter;

fn main() {
    // One subscriber, installed before any work; libs emit through `tracing`.
    // stderr sink, RUST_LOG-filtered, default warn. stdout stays product output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    // Arg parsing and dispatch land with the query engine; see mdex::cli.
    println!("mdex");
}
