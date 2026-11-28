//! A C host built with the platform C compiler against `include/polaris_abi.h`
//! and this crate's cdylib, which cargo builds as this test's library dependency.
//! Warnings are errors, under `-std=c99 -pedantic` on GCC and Clang and
//! `/std:c11` on MSVC, which has no C99 mode.

use std::{
    env::{self, consts},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use datafeed::{FeedError, FeedSessionBuilder, config::DatafeedConfig};

const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
const SCRATCH: &str = env!("CARGO_TARGET_TMPDIR");

type Records = Vec<(u16, Vec<u8>)>;

/// `cc` needs a target triple, which cargo hands only to build scripts; this
/// rebuilds it for the hosts CI runs on.
fn target_triple() -> String {
    let vendor_os = if cfg!(target_os = "macos") {
        "apple-darwin"
    } else if cfg!(target_env = "msvc") {
        "pc-windows-msvc"
    } else {
        "unknown-linux-gnu"
    };
    format!("{}-{vendor_os}", consts::ARCH)
}

fn c_compiler() -> cc::Tool {
    let target = target_triple();
    let mut build = cc::Build::new();
    build
        .target(&target)
        .host(&target)
        .opt_level(0)
        .debug(false)
        .cargo_metadata(false)
        .cargo_warnings(false)
        .emit_rerun_if_env_changed(false)
        .warnings(true)
        .extra_warnings(true)
        .warnings_into_errors(true)
        .include(Path::new(MANIFEST_DIR).join("include"));
    if build.get_compiler().is_like_msvc() {
        // fopen is standard C; MSVC warns on it under /W4
        build.std("c11").define("_CRT_SECURE_NO_WARNINGS", None);
    } else {
        build.std("c99").flag("-pedantic");
    }
    build.get_compiler()
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = Path::new(SCRATCH).join(name);
    fs::create_dir_all(&dir).expect("create the scratch dir");
    dir
}

fn run(mut cmd: Command) {
    let output = cmd.output().expect("start the command");
    assert!(
        output.status.success(),
        "{cmd:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The cdylib file and, on MSVC, the import library a C host links instead,
/// which cargo builds with this test's library dependency. Older cargo puts
/// them beside this binary in `deps/`. Newer cargo gives every unit its own
/// `build/<package>/<hash>/out/`, so the newest library among this package's
/// units is taken: the one this run built, or the last build of the same
/// sources under another feature set.
fn cdylib() -> (PathBuf, PathBuf) {
    let name = format!("{}polaris_feed{}", consts::DLL_PREFIX, consts::DLL_SUFFIX);
    let exe = env::current_exe().expect("test binary path");
    let out = exe.parent().expect("test binary has a directory");
    let beside = out.join(&name);
    let dylib = if beside.is_file() {
        beside
    } else {
        let package = out
            .parent()
            .and_then(Path::parent)
            .expect("test binary sits in build/<package>/<hash>/out");
        let (_, newest) = fs::read_dir(package)
            .expect("list the package's build units")
            .filter_map(|unit| Some(unit.ok()?.path().join("out").join(&name)))
            .filter_map(|lib| Some((fs::metadata(&lib).ok()?.modified().ok()?, lib)))
            .max()
            .unwrap_or_else(|| panic!("no {name} under {}", package.display()));
        newest
    };
    let link = if cfg!(target_env = "msvc") {
        dylib.with_extension("dll.lib")
    } else {
        dylib.clone()
    };
    assert!(link.is_file(), "{} is not built", link.display());
    (dylib, link)
}

fn build_harness() -> (PathBuf, PathBuf) {
    let dir = scratch_dir("c_host");
    let source = Path::new(MANIFEST_DIR).join("tests/c/harness.c");
    let exe = dir.join(format!("harness{}", consts::EXE_SUFFIX));
    let (dylib, link) = cdylib();
    let lib_dir = dylib.parent().expect("library directory").to_path_buf();
    let tool = c_compiler();
    let mut cmd = tool.to_command();
    cmd.arg(&source).arg(&link);
    if tool.is_like_msvc() {
        cmd.arg(format!("-Fe{}", exe.display()))
            .arg(format!("-Fo{}\\", dir.display()));
    } else {
        cmd.arg("-o")
            .arg(&exe)
            .arg(format!("-Wl,-rpath,{}", lib_dir.display()));
    }
    run(cmd);
    (exe, lib_dir)
}

fn replay_config() -> String {
    let path =
        Path::new(MANIFEST_DIR).join("../venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin");
    serde_json::json!({
        "venue": "nasdaq",
        "venue_id": 2,
        "session_midnight_ns": 0,
        "source": {"kind": "replay", "path": path},
    })
    .to_string()
}

fn rust_records(config: &str) -> Records {
    let config: DatafeedConfig = serde_json::from_str(config).expect("replay config");
    let mut session = FeedSessionBuilder::from(config)
        .build()
        .expect("session builds");
    let mut out = Vec::new();
    loop {
        match session.poll(&mut out, usize::MAX) {
            Ok(_) => {}
            Err(FeedError::Closed) => break,
            Err(err) => panic!("replay failed: {err}"),
        }
    }
    session.close().expect("close with no capture tap");
    out.iter()
        .map(|rec| (rec.stream().0, rec.bytes().to_vec()))
        .collect()
}

/// Splits the harness output back into records.
fn c_records(mut file: &[u8]) -> Records {
    let mut records = Vec::new();
    while let Some((head, rest)) = file.split_first_chunk::<6>() {
        let stream = u16::from_le_bytes([head[0], head[1]]);
        let len = u32::from_le_bytes([head[2], head[3], head[4], head[5]]) as usize;
        let (bytes, rest) = rest.split_at(len);
        records.push((stream, bytes.to_vec()));
        file = rest;
    }
    assert!(file.is_empty(), "{} trailing bytes", file.len());
    records
}

#[test]
fn header_compiles_alone_under_strict_c() {
    let dir = scratch_dir("header_only");
    let source = Path::new(MANIFEST_DIR).join("tests/c/header_only.c");
    let tool = c_compiler();
    let mut cmd = tool.to_command();
    cmd.arg("-c").arg(&source);
    if tool.is_like_msvc() {
        cmd.arg(format!("-Fo{}", dir.join("header_only.obj").display()));
    } else {
        cmd.arg("-o").arg(dir.join("header_only.o"));
    }
    run(cmd);
}

// The harness polls 7 records at a time through the C ABI and must write, in order and byte
// for byte, the records `FeedSession::poll` hands a Rust host over the same config.
#[test]
fn c_host_reads_the_records_rust_polls() {
    let config = replay_config();
    let expected = rust_records(&config);
    assert!(expected.len() > 7, "the fixture fills more than one C poll");
    let (exe, lib_dir) = build_harness();
    let output = scratch_dir("c_host").join("records.bin");

    let mut cmd = Command::new(&exe);
    cmd.arg(&config).arg(&output);
    if cfg!(windows) {
        // the loader finds a DLL on PATH; elsewhere the rpath points at it
        let path = env::var_os("PATH").unwrap_or_default();
        let paths = std::iter::once(lib_dir).chain(env::split_paths(&path));
        cmd.env("PATH", env::join_paths(paths).expect("PATH entries"));
    }
    run(cmd);

    let records = c_records(&fs::read(&output).expect("read the harness output"));
    assert_eq!(records.len(), expected.len(), "record count");
    let first_diff = records
        .iter()
        .zip(&expected)
        .position(|(c, rust)| c != rust);
    assert_eq!(first_diff, None, "first record that differs");
}
