//! Regenerates every schema under `schemas/` and asserts the committed output matches.
//!
//! Passes trivially with zero schema files (pre-authoring state). Tolerates a schema
//! still mid-edit by a concurrent author: an empty file, an unreadable file, or a file
//! whose bytes change across a 100ms read window is skipped, not failed. A schema that
//! is non-empty and stable (same bytes both reads) but still fails to load or validate
//! IS a real failure. A generated file that is missing on disk is drift, same as one
//! that differs: a layout mistake must fail here, not pass as "not generated yet".

use std::{thread::sleep, time::Duration};

use proto_codegen::{
    cli::{discover_schema_paths, repo_root, venue_crate_dir},
    emit::generate,
    schema::SchemaFile,
};

#[test]
fn committed_output_is_fresh() {
    let root = repo_root();
    let schema_paths = discover_schema_paths(&root.join("schemas"));

    for path in &schema_paths {
        let Ok(first) = std::fs::read(path) else {
            eprintln!("skip {}: unreadable (concurrent author)", path.display());
            continue;
        };
        sleep(Duration::from_millis(100));
        let Ok(second) = std::fs::read(path) else {
            eprintln!(
                "skip {}: unreadable on re-read (concurrent author)",
                path.display()
            );
            continue;
        };
        if first != second {
            eprintln!(
                "skip {}: content changed mid-read (concurrent author)",
                path.display()
            );
            continue;
        }
        if second.is_empty() {
            eprintln!(
                "skip {}: empty file (concurrent author placeholder)",
                path.display()
            );
            continue;
        }

        // bytes stable and non-empty: a load/validate failure from here is real.
        let schema = match SchemaFile::load(path) {
            Ok(s) => s,
            Err(e) => panic!("stable schema {} failed to load: {e}", path.display()),
        };

        let rel = path.strip_prefix(&root).unwrap_or(path);
        let crate_dir = venue_crate_dir(&schema);
        let files = match generate(&schema, &crate_dir, &rel.to_string_lossy(), &second) {
            Ok(f) => f,
            Err(e) => panic!("stable schema {} failed to generate: {e}", path.display()),
        };

        for f in &files {
            let disk_bytes = std::fs::read(root.join(&f.rel_path)).unwrap_or_else(|e| {
                panic!(
                    "drift: {} missing on disk ({e}); run `cargo run -p proto-codegen`",
                    f.rel_path.display()
                )
            });
            assert_eq!(
                disk_bytes,
                f.contents.as_bytes(),
                "drift: {} does not match generate() output",
                f.rel_path.display()
            );
        }
    }
}
