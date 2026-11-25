//! Regenerates every schema under `schemas/records/` and asserts the committed output
//! (the generated Rust tree plus the C header) matches byte-for-byte.
//!
//! Same concurrent-author tolerance as the wire `drift` gate: a schema still mid-edit
//! (empty, unreadable, or changing across a 100ms window) is skipped, not failed. A
//! stable, non-empty schema that fails to load/generate, or whose committed output on
//! disk differs or is missing, is a real failure.

use std::{thread::sleep, time::Duration};

use proto_codegen::{
    cli::{discover_record_schema_paths, repo_root},
    record_emit::generate_records,
    record_schema::RecordSchema,
};

#[test]
fn committed_records_are_fresh() {
    let root = repo_root();
    let paths = discover_record_schema_paths(&root.join("schemas/records"));

    for path in &paths {
        let Ok(first) = std::fs::read(path) else {
            eprintln!("skip {}: unreadable (concurrent author)", path.display());
            continue;
        };
        sleep(Duration::from_millis(100));
        let Ok(second) = std::fs::read(path) else {
            eprintln!("skip {}: unreadable on re-read", path.display());
            continue;
        };
        if first != second {
            eprintln!("skip {}: content changed mid-read", path.display());
            continue;
        }
        if second.is_empty() {
            eprintln!("skip {}: empty file", path.display());
            continue;
        }

        let schema = match RecordSchema::load(path) {
            Ok(s) => s,
            Err(e) => panic!(
                "stable record schema {} failed to load: {e}",
                path.display()
            ),
        };
        let rel = path.strip_prefix(&root).unwrap_or(path);
        let files = match generate_records(&schema, &rel.to_string_lossy(), &second) {
            Ok(f) => f,
            Err(e) => panic!(
                "stable record schema {} failed to generate: {e}",
                path.display()
            ),
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
                "drift: {} does not match generate_records() output",
                f.rel_path.display()
            );
        }
    }
}
