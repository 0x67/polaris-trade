//! Code emission. Pure functions: schema in, generated Rust source text out.
//! No filesystem writes here; `main.rs` owns IO.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::schema::{Direction, Endianness, Kind, SchemaError, SchemaFile, visitor_method};

mod bench;
mod build;
mod dispatch;
mod naming;
mod parity;
mod view;

use bench::emit_bench_stubs;
use build::{emit_build_kind, emit_build_mod};
use dispatch::emit_dispatch;
use naming::{emit_appendage, emit_message_type, emit_mod_rs, to_hex};
use parity::emit_parity_tests;
use view::{emit_view_kind, emit_view_mod};

/// One generated file, path relative to repo root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    pub rel_path: PathBuf,
    pub contents: String,
}

/// module name inside the venue crate, e.g. `itch_v1` from protocol `itch` doc_version `1.16.0`.
pub fn module_name(s: &SchemaFile) -> Result<String, SchemaError> {
    let major = s.doc_major()?;
    Ok(format!("{}_v{major}", s.protocol))
}

/// directory the generator fully owns for this schema: `<crate_dir>/src/<ns>/`.
/// `tests/parity_<ns>.rs` and `benches/per_kind_<ns>.rs` live in shared `tests/`/`benches/`
/// dirs alongside hand-written siblings and are NOT part of this directory.
pub fn owned_module_dir(s: &SchemaFile, crate_dir: &Path) -> Result<PathBuf, SchemaError> {
    let ns = module_name(s)?;
    Ok(crate_dir.join("src").join(ns))
}

/// true when any kind declares a direction. `schema.rs::validate` enforces all-or-none
/// across a schema's kinds, so this one check also tells whether appendage/registry
/// keys are in play for the whole file.
fn is_directional(s: &SchemaFile) -> bool {
    s.kinds.iter().any(|k| k.direction.is_some())
}

/// kinds visible on the recv surface: on a directional schema, only from_exchange
/// kinds; on a directionless schema, every kind (today's behavior, unchanged).
fn recv_kinds(s: &SchemaFile) -> Vec<&Kind> {
    if is_directional(s) {
        s.kinds
            .iter()
            .filter(|k| k.direction == Some(Direction::FromExchange))
            .collect()
    } else {
        s.kinds.iter().collect()
    }
}

/// kinds the generator emits a send-side builder for: to_exchange kinds only, always
/// fixed-width (byte_len sizes the caller buffer). Empty on any directionless schema,
/// since no kind there can declare `direction: to_exchange`.
fn build_kinds(s: &SchemaFile) -> Vec<&Kind> {
    s.kinds
        .iter()
        .filter(|k| k.direction == Some(Direction::ToExchange) && !k.variable)
        .collect()
}

/// generate every file this schema produces. Pure: validates, then emits text, no IO.
/// Every path sits under `crate_dir`, the venue crate's repo-relative directory; the
/// caller owns layout (see `cli::venue_crate_dir`).
/// `schema_rel_path` and `schema_bytes` feed the provenance header (path + content hash).
pub fn generate(
    s: &SchemaFile,
    crate_dir: &Path,
    schema_rel_path: &str,
    schema_bytes: &[u8],
) -> Result<Vec<GeneratedFile>, SchemaError> {
    s.validate()?;
    if s.endianness == Endianness::Le {
        return Err(SchemaError::LeUnimplemented);
    }

    let ns = module_name(s)?;
    let hash = to_hex(&Sha256::digest(schema_bytes));
    let header = provenance_header(schema_rel_path, &hash);
    let ns_dir = crate_dir.join("src").join(&ns);
    let view_dir = ns_dir.join("view");

    let mut files = vec![
        GeneratedFile {
            rel_path: ns_dir.join("message_type.rs"),
            contents: emit_message_type(s, &header),
        },
        GeneratedFile {
            rel_path: view_dir.join("mod.rs"),
            contents: emit_view_mod(s, &header),
        },
    ];

    for kind in recv_kinds(s).into_iter().filter(|k| !k.variable) {
        let method = visitor_method(&kind.name);
        files.push(GeneratedFile {
            rel_path: view_dir.join(format!("{method}.rs")),
            contents: emit_view_kind(kind, s.endianness, &header),
        });
    }

    if !s.appendage_tags.is_empty() {
        files.push(GeneratedFile {
            rel_path: ns_dir.join("appendage.rs"),
            contents: emit_appendage(s, &header),
        });
    }

    let to_exchange = build_kinds(s);
    if !to_exchange.is_empty() {
        let build_dir = ns_dir.join("build");
        files.push(GeneratedFile {
            rel_path: build_dir.join("mod.rs"),
            contents: emit_build_mod(s, &header),
        });
        for kind in &to_exchange {
            let method = visitor_method(&kind.name);
            files.push(GeneratedFile {
                rel_path: build_dir.join(format!("{method}.rs")),
                contents: emit_build_kind(s, kind, &header),
            });
        }
    }

    files.push(GeneratedFile {
        rel_path: ns_dir.join("dispatch.rs"),
        contents: emit_dispatch(s, &header),
    });
    files.push(GeneratedFile {
        rel_path: ns_dir.join("mod.rs"),
        contents: emit_mod_rs(s, &ns, &header),
    });

    files.push(GeneratedFile {
        rel_path: crate_dir.join("tests").join(format!("parity_{ns}.rs")),
        contents: emit_parity_tests(s, &ns, &header),
    });
    files.push(GeneratedFile {
        rel_path: crate_dir.join("benches").join(format!("per_kind_{ns}.rs")),
        contents: emit_bench_stubs(s, &ns, &header),
    });

    Ok(files)
}

/// two GENERATED doc lines plus a blank doc-comment separator, ready to prefix a file body.
/// shared with `record_emit` so both generators stamp the same version-free provenance.
pub(crate) fn provenance_header(schema_rel_path: &str, sha256_hex: &str) -> String {
    // no generator version in the header: a release version bump would churn
    // every generated file and fail the drift gate inside the release PR
    // itself. git history records which generator version produced the tree.
    format!(
        "//! GENERATED by proto-codegen. Do not edit.\n\
         //! schema: {schema_rel_path} sha256:{sha256_hex}\n\
         //!\n",
    )
}

/// doc field trimmed and checked non-empty; whitespace-only doc counts as absent so
/// it never emits a stray separator line. shared with `record_emit`.
pub(crate) fn doc_text(doc: &Option<String>) -> Option<&str> {
    doc.as_deref().filter(|d| !d.trim().is_empty())
}

/// doc text -> `///` lines at `indent`. One line per input line; empty line becomes
/// bare `///`; trailing whitespace and any trailing blank lines dropped (YAML block
/// scalar artifact); output deterministic across runs. shared with `record_emit`.
pub(crate) fn doc_lines(doc: &str, indent: &str) -> String {
    let mut out = String::new();
    for line in doc.trim_end().split('\n') {
        let line = line.trim_end();
        out.push_str(indent);
        if line.is_empty() {
            out.push_str("///\n");
        } else {
            out.push_str("/// ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests;
