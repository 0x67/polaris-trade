//! Per-stage latency and throughput bench for the NASDAQ ITCH 5.0 pipeline.
//! It owns the replay loop instead of using criterion, so it can run a capture
//! of many million messages once and report hdr percentiles per stage.
//!
//! Three passes over the same capped prefix of one capture:
//!  1. Decode only: `itch_v5::parse_match` with a visitor that reads every
//!     field of every view. Reported as a mean in ns per message, timed per
//!     batch, because one decode is shorter than a monotonic-clock tick.
//!  2. Native throughput: `session::run` with the metrics gate off and no
//!     recorder, so each stage timer costs one gate load and no clock read.
//!  3. Instrumented latency: the same run with a capturing recorder installed
//!     and the gate on, which fills the feed, book apply, book derive and
//!     egress histograms. Its wall time carries the timers' cost and is dropped.
//!
//! `normalize_mean_ns` is the feed mean minus the decode mean, since the
//! adapter fuses decode and record building in `feed`. Stage means cover
//! different populations (see `StageStats::unit`) and must not be summed.
//!
//! Environment:
//!  - `NASDAQ_ITCH_SAMPLE`: the capture, a BinaryFILE dump as `.bin.zst` or
//!    `.gz`. Required.
//!  - `BENCH_HOST`: host label for the report, `unknown` when unset. The
//!    regression test compares timings only between equal labels.
//!  - `BENCH_CPU`: free-text CPU name for the report.
//!  - `BENCH_MAX_MESSAGES`: message cap, [`DEFAULT_MAX_MESSAGES`] when unset.
//!    The regression test refuses a run whose cap differs from the baseline's.
//!  - `BENCH_OUT`: report path, `benches/results/nasdaq_metrics.json` under
//!    the crate when unset.
//!
//! Needs `--features observability`: without it the stage timers are no-ops.

#[path = "nasdaq_pipeline/passes.rs"]
mod passes;
#[path = "nasdaq_pipeline/recorder.rs"]
mod recorder;
#[path = "nasdaq_pipeline/report.rs"]
mod report;
#[path = "nasdaq_pipeline/visitor.rs"]
mod visitor;

use std::{
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use recorder::StageRecorder;
use report::{BenchReport, CaptureInfo, HostInfo, Layout, StageStats, Stages, Throughput};

/// Messages replayed per pass when `BENCH_MAX_MESSAGES` is unset. On the
/// 2020-01-30 capture the prefix ends at 10:35:23 exchange time, an hour past
/// the open, and a run takes two to three minutes on the baseline host.
const DEFAULT_MAX_MESSAGES: u64 = 100_000_000;

fn max_messages() -> u64 {
    match std::env::var("BENCH_MAX_MESSAGES") {
        Ok(value) => value.parse().expect("BENCH_MAX_MESSAGES must be a u64"),
        Err(_) => DEFAULT_MAX_MESSAGES,
    }
}

fn stage(recorder: &StageRecorder, name: &str, unit: &'static str) -> StageStats {
    let stats = StageStats::of(recorder.stage(name), unit);
    assert!(
        stats.count > 0,
        "{name} recorded no sample: the name no longer matches src/obs.rs, or the gate this \
         bench arms is not the one the session reads"
    );
    stats
}

fn write_report(out: &Path, report: &BenchReport) {
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).expect("create the report directory");
    }
    let mut file = File::create(out).expect("create the report file");
    serde_json::to_writer_pretty(&mut file, report).expect("write the report");
    file.write_all(b"\n")
        .expect("end the report with a newline");
}

fn main() {
    // First statement of `main`, so the address follows where the process stack
    // starts. `black_box` keeps the local in memory.
    let stack_local = 0u8;
    let stack_low14 = report::low14(std::hint::black_box(&stack_local));

    let capture = PathBuf::from(
        std::env::var_os("NASDAQ_ITCH_SAMPLE")
            .expect("NASDAQ_ITCH_SAMPLE must point to a NASDAQ ITCH 5.0 capture (.bin.zst or .gz)"),
    );
    let out = std::env::var_os("BENCH_OUT").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/results/nasdaq_metrics.json"),
        PathBuf::from,
    );
    let cap = max_messages();

    eprintln!("hashing {}", capture.display());
    let sha256 = passes::sha256_file(&capture);

    eprintln!("decode-only pass, up to {cap} messages");
    let (decode, parse_messages) = passes::decode_only_pass(&capture, cap);
    assert!(parse_messages > 0, "the capture yielded no message");
    let parse_mean_ns = decode.as_secs_f64() * 1e9 / parse_messages as f64;

    eprintln!("native throughput pass");
    let (native, wall, _) = passes::session_pass(&capture, cap);
    assert_eq!(
        native.frames, parse_messages,
        "the passes replayed different inputs"
    );

    // `observability_core` holds the gate the session's stage timers read (see
    // src/obs.rs); the refresh makes this thread see the flip.
    let recorder = StageRecorder::new();
    metrics::set_global_recorder(recorder.clone()).expect("no recorder is installed before this");
    observability_core::set_metrics_enabled(true);
    observability_core::refresh_thread_gate();

    eprintln!("instrumented latency pass");
    let (instrumented, _, adapter_low14) = passes::session_pass(&capture, cap);
    assert_eq!(
        instrumented.frames, parse_messages,
        "the passes replayed different inputs"
    );

    let feed = stage(&recorder, recorder::STAGE_FEED, "message");
    let normalize_mean_ns = (feed.mean_ns - parse_mean_ns).max(0.0);
    let wall_secs = wall.as_secs_f64();
    let report = BenchReport {
        venue: "nasdaq-itch-v5",
        generated_at_unix_s: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is past the epoch")
            .as_secs(),
        host: HostInfo {
            label: std::env::var("BENCH_HOST").unwrap_or_else(|_| "unknown".into()),
            cpu: std::env::var("BENCH_CPU").unwrap_or_default(),
            arch: std::env::consts::ARCH,
            os: std::env::consts::OS,
            logical_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        },
        capture: CaptureInfo {
            path: capture.display().to_string(),
            sha256,
            max_messages: cap,
            messages: native.frames,
        },
        batch: passes::BATCH,
        layout: Layout {
            stack_low14,
            adapter_low14,
        },
        throughput: Throughput {
            records: native.records,
            wall_ns: wall.as_nanos() as u64,
            msgs_per_sec: native.frames as f64 / wall_secs,
            ns_per_msg: wall_secs * 1e9 / native.frames as f64,
        },
        stages: Stages {
            parse_mean_ns,
            parse_messages,
            feed,
            book_apply: stage(&recorder, recorder::STAGE_BOOK_APPLY, "mbo-segment"),
            book_derive: stage(&recorder, recorder::STAGE_BOOK_DERIVE, "mbo-segment"),
            egress: stage(&recorder, recorder::STAGE_EGRESS, "published-record"),
            normalize_mean_ns,
        },
    };

    write_report(&out, &report);
    eprintln!(
        "wrote {} ({} msgs, {:.2}M msgs/s, decode {:.1}ns/msg, feed p99 {}ns, book.apply p99 {}ns, \
         book.derive p99 {}ns, egress p99 {}ns)",
        out.display(),
        report.capture.messages,
        report.throughput.msgs_per_sec / 1e6,
        report.stages.parse_mean_ns,
        report.stages.feed.p99_ns,
        report.stages.book_apply.p99_ns,
        report.stages.book_derive.p99_ns,
        report.stages.egress.p99_ns,
    );
}
