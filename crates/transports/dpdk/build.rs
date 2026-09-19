//! With feature `driver-dpdk`: compiles `csrc/shim.c`, then links libdpdk after it.
//! Without it: nothing, so default build needs no system library.
//!
//! Link order: static shim must precede `-lrte_*` or `--as-needed` drops them.
//! So pass 1 reads compile flags only, shim compiles (emitting its own link
//! directive), then pass 2 emits libdpdk link directives. Pass 1 runs
//! `pkg-config --cflags` directly: pkg-config crate keeps only `-I`/`-D`, and
//! `x86_64` DPDK headers need `-march` (inlined `rte_memcpy` uses SSSE3).

use std::{env, ffi::OsString, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if env::var_os("CARGO_FEATURE_DRIVER_DPDK").is_none() {
        return;
    }
    println!("cargo:rerun-if-changed=csrc/shim.c");
    assert!(
        env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux"),
        "feature `driver-dpdk` needs a Linux target with libdpdk installed"
    );

    let mut shim = cc::Build::new();
    shim.file("csrc/shim.c");
    for flag in cflags().split_whitespace() {
        shim.flag(flag);
    }
    shim.compile("polaris_dpdk_shim");

    if let Err(e) = pkg_config::Config::new().probe("libdpdk") {
        panic!("libdpdk link flags: {e}");
    }
}

// `pkg-config --cflags libdpdk`, honouring `PKG_CONFIG` like pkg-config crate
fn cflags() -> String {
    let exe = env::var_os("PKG_CONFIG").unwrap_or_else(|| OsString::from("pkg-config"));
    let out = Command::new(&exe)
        .args(["--cflags", "libdpdk"])
        .output()
        .unwrap_or_else(|e| panic!("run {}: {e}", exe.to_string_lossy()));
    assert!(
        out.status.success(),
        "libdpdk not found by pkg-config (install the DPDK dev package, e.g. libdpdk-dev): {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("pkg-config output is UTF-8")
}
