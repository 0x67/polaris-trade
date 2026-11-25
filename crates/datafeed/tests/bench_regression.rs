//! Provenance and regression gate for the NASDAQ pipeline bench.
//!
//! Reads three JSON files under the crate:
//!  - `benches/baseline/manifest.json` (committed): the capture sha256 and
//!    message cap a baseline is valid for, the host that recorded it, and the
//!    tolerances.
//!  - `benches/baseline/nasdaq_metrics.json` (committed): the reference run.
//!  - `benches/results/nasdaq_metrics.json` (git-ignored): a fresh run of
//!    `benches/nasdaq_pipeline`, when someone has produced one.
//!
//! Every run checks that the committed baseline has its manifest's input and
//! host and a relative capture path. With a fresh run present, a different
//! input (sha256, cap or message count) fails the test, and a different host
//! label skips the timing comparison. On the baseline host a stage p50 or p99,
//! the decode mean or the throughput outside the manifest tolerance fails it.
//! The test needs no capture and no feature, so it runs in the normal suite. CI
//! has no fresh run and checks the baseline alone.

use std::path::{Path, PathBuf};

use serde_json::Value;

fn crate_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

fn load(path: &Path) -> Option<Value> {
    let bytes = std::fs::read(path).ok()?;
    Some(serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", path.display())))
}

fn node_at<'a>(v: &'a Value, path: &[&str]) -> &'a Value {
    path.iter().fold(v, |node, key| {
        node.get(key)
            .unwrap_or_else(|| panic!("missing key {key} in path {path:?}"))
    })
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> &'a str {
    node_at(v, path)
        .as_str()
        .unwrap_or_else(|| panic!("no string at {path:?}"))
}

fn u64_at(v: &Value, path: &[&str]) -> u64 {
    node_at(v, path)
        .as_u64()
        .unwrap_or_else(|| panic!("no u64 at {path:?}"))
}

fn f64_at(v: &Value, path: &[&str]) -> f64 {
    node_at(v, path)
        .as_f64()
        .unwrap_or_else(|| panic!("no f64 at {path:?}"))
}

/// Every timing of `fresh` that is outside the manifest tolerance of
/// `baseline`, one line each.
fn timing_regressions(manifest: &Value, baseline: &Value, fresh: &Value) -> Vec<String> {
    let p50_pct = f64_at(manifest, &["tolerance", "stage_p50_pct"]);
    let p99_pct = f64_at(manifest, &["tolerance", "stage_p99_pct"]);
    let slack_ns = f64_at(manifest, &["tolerance", "stage_slack_ns"]);
    let throughput_pct = f64_at(manifest, &["tolerance", "throughput_pct"]);
    let mut regressions = Vec::new();

    // A stage percentile moves in whole clock ticks, and one tick can exceed the
    // percentage of a short stage, so the ceiling is the higher of the two.
    for stage in ["feed", "book_apply", "book_derive", "egress"] {
        for (field, pct) in [("p50_ns", p50_pct), ("p99_ns", p99_pct)] {
            let path = ["stages", stage, field];
            let base = u64_at(baseline, &path) as f64;
            let got = u64_at(fresh, &path) as f64;
            let ceiling = (base * (1.0 + pct / 100.0)).max(base + slack_ns);
            if got > ceiling {
                regressions.push(format!(
                    "{stage} {field}: fresh {got}ns > ceiling {ceiling:.1}ns (baseline {base}ns, \
                     {pct}% or {slack_ns}ns)"
                ));
            }
        }
    }

    // Decode is a mean timed per batch, with no percentiles and no tick steps,
    // so it takes the p50 percentage without the slack.
    let path = ["stages", "parse_mean_ns"];
    let base = f64_at(baseline, &path);
    let got = f64_at(fresh, &path);
    let ceiling = base * (1.0 + p50_pct / 100.0);
    if got > ceiling {
        regressions.push(format!(
            "parse mean: fresh {got:.2}ns > ceiling {ceiling:.2}ns (baseline {base:.2}ns, \
             {p50_pct}%)"
        ));
    }

    // Throughput regresses downward, so its bound is a floor.
    let path = ["throughput", "msgs_per_sec"];
    let base = f64_at(baseline, &path);
    let got = f64_at(fresh, &path);
    let floor = base * (1.0 - throughput_pct / 100.0);
    if got < floor {
        regressions.push(format!(
            "throughput: fresh {got:.0} msgs/s < floor {floor:.0} (baseline {base:.0}, \
             {throughput_pct}%)"
        ));
    }

    regressions
}

// @lat: [[tests#Bench timing]]
#[test]
fn nasdaq_bench_baseline_matches_its_manifest_and_gates_fresh_runs() {
    let manifest =
        load(&crate_path("benches/baseline/manifest.json")).expect("committed bench manifest");
    let baseline =
        load(&crate_path("benches/baseline/nasdaq_metrics.json")).expect("committed baseline");

    let sha256 = str_at(&manifest, &["capture", "sha256"]);
    let max_messages = u64_at(&manifest, &["capture", "max_messages"]);
    let baseline_host = str_at(&manifest, &["baseline_host"]);

    assert_eq!(
        str_at(&baseline, &["capture", "sha256"]),
        sha256,
        "the baseline was recorded from another capture than the manifest names"
    );
    assert_eq!(
        u64_at(&baseline, &["capture", "max_messages"]),
        max_messages,
        "the baseline was recorded with another message cap than the manifest names"
    );
    assert_eq!(
        str_at(&baseline, &["host", "label"]),
        baseline_host,
        "the baseline was recorded on another host than the manifest's baseline_host"
    );
    // The bench stores `NASDAQ_ITCH_SAMPLE` as given and the baseline is
    // committed, so an absolute path would publish a local directory name.
    assert!(
        Path::new(str_at(&baseline, &["capture", "path"])).is_relative(),
        "the baseline's capture.path is absolute: record it with a relative NASDAQ_ITCH_SAMPLE"
    );

    let Some(fresh) = load(&crate_path("benches/results/nasdaq_metrics.json")) else {
        eprintln!(
            "no benches/results/nasdaq_metrics.json: checked the baseline against its manifest only"
        );
        return;
    };

    assert_eq!(
        str_at(&fresh, &["capture", "sha256"]),
        sha256,
        "the fresh run read another capture, so it cannot be compared"
    );
    assert_eq!(
        u64_at(&fresh, &["capture", "max_messages"]),
        max_messages,
        "the fresh run used another message cap, so it cannot be compared"
    );
    assert_eq!(
        u64_at(&fresh, &["capture", "messages"]),
        u64_at(&baseline, &["capture", "messages"]),
        "the fresh run replayed another message count from the same capture and cap"
    );

    let fresh_host = str_at(&fresh, &["host", "label"]);
    if fresh_host != baseline_host {
        eprintln!(
            "fresh run is from host {fresh_host}, the baseline from {baseline_host}: input \
             matches, timing comparison skipped"
        );
        return;
    }

    let regressions = timing_regressions(&manifest, &baseline, &fresh);
    assert!(
        regressions.is_empty(),
        "bench timings regressed past the manifest tolerance:\n{}",
        regressions.join("\n")
    );
}
