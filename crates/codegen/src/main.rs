// CLI entry point: generate committed venue-crate output from schemas/**.yaml.
// Write/check/discovery logic lives in proto_codegen::cli so tests share the same seam.

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use proto_codegen::{
    cli::{
        PruneError, check_outputs, discover_record_schema_paths, discover_schema_paths,
        ensure_venue_crate_exists, find_orphans, prune_orphans, repo_root, schema_rel_path,
        venue_crate_dir, write_outputs,
    },
    emit::{generate, owned_module_dir},
    record_emit::{GEN_DIR, generate_records},
    record_schema::RecordSchema,
    schema::SchemaFile,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut check = false;
    let mut explicit_schemas: Vec<PathBuf> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--check" => check = true,
            "--schema" => {
                i += 1;
                match args.get(i) {
                    Some(p) => explicit_schemas.push(PathBuf::from(p)),
                    None => {
                        eprintln!("--schema requires a path argument");
                        return ExitCode::from(2);
                    }
                }
            }
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::from(2);
            }
        }
        i += 1;
    }

    let root = repo_root();
    let schema_paths = if explicit_schemas.is_empty() {
        discover_schema_paths(&root.join("schemas"))
    } else {
        explicit_schemas
            .into_iter()
            .map(|p| if p.is_absolute() { p } else { root.join(p) })
            .collect()
    };

    if schema_paths.is_empty() {
        eprintln!("no schemas found");
        return ExitCode::SUCCESS;
    }

    let mut drifted_all: Vec<PathBuf> = Vec::new();
    let mut orphans_all: Vec<PathBuf> = Vec::new();

    for path in &schema_paths {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to read {}: {e}", path.display());
                return ExitCode::from(2);
            }
        };
        let schema = match SchemaFile::load(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        };

        let rel = schema_rel_path(&root, path);
        let crate_dir = venue_crate_dir(&schema);
        let files = match generate(&schema, &crate_dir, &rel, &bytes) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        };

        let owned = match owned_module_dir(&schema, &crate_dir) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        };

        if check {
            drifted_all.extend(check_outputs(&root, &files));
            // marker-blind: check mode treats any stray file as drift, generated or not.
            orphans_all.extend(find_orphans(&root, &owned, &files));
        } else {
            if let Err(e) = ensure_venue_crate_exists(&root, &schema) {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
            if let Err(e) = write_outputs(&root, &files) {
                eprintln!("failed to write generated output: {e}");
                return ExitCode::from(2);
            }

            let orphans = find_orphans(&root, &owned, &files);
            if !orphans.is_empty() {
                match prune_orphans(&root, &orphans) {
                    Ok(deleted) => {
                        for p in &deleted {
                            println!("pruned: {}", p.display());
                        }
                    }
                    Err(e @ PruneError::Foreign { .. }) => {
                        eprintln!("{e}");
                        return ExitCode::from(1);
                    }
                    Err(e @ PruneError::Io { .. }) => {
                        eprintln!("{e}");
                        return ExitCode::from(2);
                    }
                }
            }
        }
    }

    // normalized-record pass: a distinct DSL (record_schema) writing the generated
    // md-events tree under GEN_DIR plus the C header. Wire discovery already skips
    // schemas/records/, so the two passes never touch each other's schemas.
    for path in &discover_record_schema_paths(&root.join("schemas/records")) {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to read {}: {e}", path.display());
                return ExitCode::from(2);
            }
        };
        let schema = match RecordSchema::load(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        };
        let rel = schema_rel_path(&root, path);
        let files = match generate_records(&schema, &rel, &bytes) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        };

        if check {
            drifted_all.extend(check_outputs(&root, &files));
            orphans_all.extend(find_orphans(&root, Path::new(GEN_DIR), &files));
        } else {
            if let Err(e) = write_outputs(&root, &files) {
                eprintln!("failed to write generated output: {e}");
                return ExitCode::from(2);
            }
            let orphans = find_orphans(&root, Path::new(GEN_DIR), &files);
            if !orphans.is_empty() {
                match prune_orphans(&root, &orphans) {
                    Ok(deleted) => {
                        for p in &deleted {
                            println!("pruned: {}", p.display());
                        }
                    }
                    Err(e @ PruneError::Foreign { .. }) => {
                        eprintln!("{e}");
                        return ExitCode::from(1);
                    }
                    Err(e @ PruneError::Io { .. }) => {
                        eprintln!("{e}");
                        return ExitCode::from(2);
                    }
                }
            }
        }
    }

    if check && (!drifted_all.is_empty() || !orphans_all.is_empty()) {
        for p in &drifted_all {
            eprintln!("drifted: {}", p.display());
        }
        for p in &orphans_all {
            eprintln!("orphan: {}", p.display());
        }
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}
