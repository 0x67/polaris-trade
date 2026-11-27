//! The deployed path: runs the built `datafeed` binary on the NASDAQ order book
//! reference fixture with a raw capture tap and compares what it leaves on disk
//! with the library path and the committed seam baseline.
//!
//! The binary's sink discards records and the tap stores raw frames, so what
//! the binary normalizes is not observed. The digest check proves the capture
//! holds the baseline frames: replaying them through an adapter built here from
//! the same config values must reproduce the baseline digests.

mod support;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, atomic::AtomicBool},
};
#[cfg(unix)]
use std::{process::Stdio, thread::sleep, time::Duration};

use datafeed::app::{CliOverrides, load_config, run_config};
use md_replay::CaptureReplay;
use serde_json::{Value, json};
#[cfg(unix)]
use support::capture_tree::wait_until;
use support::{
    capture_tree::{assert_manifests_verify, output_tree},
    digest::{config, digest_lines, expected_digests, replay_digest},
    reference_parity::binary_dump,
};

const STREAM_HEX: &str = include_str!("fixtures/orderbook-reference/stream.hex");
const CAPTURE_DATE: u32 = 20260521;

/// A scratch directory holding configs and capture trees. Removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("datafeed_deploy_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create scratch dir");
        Scratch(dir)
    }

    /// Writes the reference fixture as a length-prefixed dump and returns its path.
    fn reference_dump(&self) -> PathBuf {
        let path = self.0.join("stream.bin");
        fs::write(&path, binary_dump(STREAM_HEX)).expect("write replay dump");
        path
    }

    /// Writes `<name>.json`: a replay of `dump` with its tap under `<name>_out`.
    fn config(&self, name: &str, dump: &Path, pacing: Value, step_ns: u64) -> (PathBuf, PathBuf) {
        let out = self.0.join(format!("{name}_out"));
        let config = json!({
            "venue": "nasdaq",
            "venue_id": 2,
            "session_midnight_ns": 0,
            "source": {
                "kind": "replay",
                "path": dump,
                "base_ts": 0,
                "step_ns": step_ns,
                "pacing": pacing,
            },
            "taps": {"raw_dir": out, "capture_date": CAPTURE_DATE, "partition": "1d"},
        });
        let path = self.0.join(format!("{name}.json"));
        fs::write(&path, config.to_string()).expect("write config");
        (path, out)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn binary(config: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_datafeed"));
    command.arg(config);
    command
}

/// Runs the config in process, the way a host embedding the library would.
fn run_library(config: &Path) {
    let config = load_config(config, &CliOverrides::default()).expect("config loads");
    run_config(config, Arc::new(AtomicBool::new(false))).expect("library run");
}

fn only_leaf(tree: &[(String, Vec<u8>)]) -> PathBuf {
    let leaves: Vec<_> = tree
        .iter()
        .filter(|(p, _)| p.ends_with(".mdc1.zst"))
        .collect();
    assert_eq!(leaves.len(), 1, "one capture leaf, got {leaves:?}");
    PathBuf::from(&leaves[0].0)
}

// @lat: [[tests#Deployed binary#Binary capture matches the library and the baseline]]
#[test]
fn binary_capture_tree_matches_the_library_path_and_the_seam_baseline() {
    let scratch = Scratch::new("parity");
    let dump = scratch.reference_dump();
    let (bin_config, bin_out) = scratch.config("bin", &dump, json!({}), 1);
    let (lib_config, lib_out) = scratch.config("lib", &dump, json!({}), 1);

    let run = binary(&bin_config).output().expect("run datafeed");
    assert!(
        run.status.success(),
        "datafeed exited {}: {}",
        run.status,
        String::from_utf8_lossy(&run.stderr)
    );
    run_library(&lib_config);

    let tree = output_tree(&bin_out);
    assert_eq!(
        tree,
        output_tree(&lib_out),
        "binary and library trees differ"
    );
    assert_eq!(
        assert_manifests_verify(&bin_out),
        1,
        "one listed capture file"
    );

    assert_capture_matches_baseline(
        &bin_out.join(only_leaf(&tree)),
        "nasdaq-orderbook-reference",
    );
}

/// The prefix spans many 64-frame batches, which the reference fixture does
/// not, so this checks the capture across batch boundaries. Needs
/// `NASDAQ_ITCH_PREFIX`; see the `seam_parity` module doc.
// @lat: [[tests#Deployed binary#Binary capture of the prefix matches the baseline]]
#[test]
#[ignore = "needs NASDAQ_ITCH_PREFIX; see the seam_parity module doc"]
fn binary_capture_of_the_itch_prefix_matches_the_seam_baseline() {
    let prefix = PathBuf::from(
        std::env::var_os("NASDAQ_ITCH_PREFIX").expect("NASDAQ_ITCH_PREFIX must be set"),
    );
    let scratch = Scratch::new("prefix");
    let (config, out) = scratch.config("prefix", &prefix, json!({}), 1);

    let run = binary(&config).output().expect("run datafeed");
    assert!(
        run.status.success(),
        "datafeed exited {}: {}",
        run.status,
        String::from_utf8_lossy(&run.stderr)
    );

    assert_manifests_verify(&out);
    assert_capture_matches_baseline(
        &out.join(only_leaf(&output_tree(&out))),
        "nasdaq-itch-prefix",
    );
}

/// Replays the capture leaf through the NASDAQ adapter and requires the
/// fixture's committed digest lines.
fn assert_capture_matches_baseline(leaf: &Path, fixture: &str) {
    let source = CaptureReplay::from_reader(fs::File::open(leaf).expect("open leaf"))
        .expect("capture decodes");
    let config = config("nasdaq", 2, 0);
    let (sink, _) = replay_digest(&config, &mut *(config.venue.make)(&config), source);
    assert_eq!(
        digest_lines(fixture, &sink),
        expected_digests(fixture),
        "records rebuilt from the binary's capture differ from the committed baseline"
    );
}

// A replay ignores the stop flag (see `app::run_config`), so a SIGINT in the
// middle of one must neither kill the process nor cut the capture short: the
// run finishes, closes its tap, and leaves the tree an undisturbed run leaves.
// @lat: [[tests#Deployed binary#SIGINT during a replay changes nothing]]
#[cfg(unix)]
#[test]
fn sigint_during_a_replay_leaves_the_tree_of_an_undisturbed_run() {
    let scratch = Scratch::new("sigint");
    // 33 steps of 100 ms in real time keep the run alive for about 3 s.
    let pacing = json!({"default_mode": {"mode": "real-time"}});
    let dump = scratch.reference_dump();
    let (slow_config, slow_out) = scratch.config("slow", &dump, pacing, 100_000_000);
    let (ref_config, ref_out) = scratch.config("ref", &dump, json!({}), 100_000_000);

    let mut child = binary(&slow_config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn datafeed");
    // Frames reach the tap only when the paced batch completes, so no file
    // marks the middle of the run; give the handlers time to install instead.
    sleep(Duration::from_secs(1));
    // An exited child still receives `kill` while unreaped, so a finished run
    // must fail here, before the signal.
    assert!(
        child.try_wait().expect("poll child").is_none(),
        "the run ended before SIGINT was sent"
    );
    // SAFETY: `kill` takes a pid and a signal number and touches no memory.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    assert_eq!(rc, 0, "SIGINT not delivered");
    wait_until("the binary to exit", || {
        child.try_wait().expect("poll child").is_some()
    });
    let run = child.wait_with_output().expect("collect output");
    assert!(
        run.status.success(),
        "datafeed exited {}: {}",
        run.status,
        String::from_utf8_lossy(&run.stderr)
    );

    run_library(&ref_config);
    assert_eq!(output_tree(&slow_out), output_tree(&ref_out));
    assert_eq!(assert_manifests_verify(&slow_out), 1);
}
