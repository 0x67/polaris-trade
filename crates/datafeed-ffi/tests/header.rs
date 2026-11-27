//! Drift gate for the committed C header: cbindgen regenerates it from
//! `src/lib.rs` with `cbindgen.toml`, and the output must equal
//! `include/polaris_abi.h` byte for byte.
//!
//! After an ABI change, rewrite the header with
//! `POLARIS_ABI_HEADER_WRITE=1 cargo nextest run -p polaris-datafeed-ffi --test header`
//! and commit it with the change.

use std::{env, fs, path::Path};

#[test]
fn committed_header_matches_cbindgen_output() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config =
        cbindgen::Config::from_file(dir.join("cbindgen.toml")).expect("read cbindgen.toml");
    let bindings = cbindgen::Builder::new()
        .with_config(config)
        .with_src(dir.join("src/lib.rs"))
        .generate()
        .expect("cbindgen parses src/lib.rs");
    let mut fresh = Vec::new();
    bindings.write(&mut fresh);

    let path = dir.join("include/polaris_abi.h");
    if env::var_os("POLARIS_ABI_HEADER_WRITE").is_some() {
        fs::write(&path, &fresh).expect("write include/polaris_abi.h");
    }
    let committed = fs::read(&path).expect("read include/polaris_abi.h");
    assert!(
        committed == fresh,
        "include/polaris_abi.h differs from cbindgen's output; see this file's doc to rewrite it"
    );
}
