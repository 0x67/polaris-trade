//! Per-kind criterion bench emission: view-fold benches plus builder-drive benches.

use super::{
    build_kinds, is_directional,
    naming::{
        byte_literal, emit_field_setter_call, emit_field_synthetic_binding,
        emit_tag_synthetic_binding, find_appendage_tag, synthetic_alpha_fn, synthetic_bytes_fn,
        synthetic_row_fn, tag_builder_append_call,
    },
    recv_kinds,
};
use crate::schema::{Kind, SchemaFile, TypeClass, visitor_method};

/// `benches/per_kind_<ns>.rs` body: one criterion fold fn per fixed kind, folding every
/// accessor into a `black_box`-consumed checksum. Shape mirrors
/// `itch-parse-spike/crates/benches/benches/per_kind.rs` (generated-view candidate only).
/// Directionless schemas keep the shape below; directional schemas route through
/// `emit_bench_stubs_directional` (from_exchange kinds only, plus one builder bench
/// per to_exchange kind).
pub(super) fn emit_bench_stubs(s: &SchemaFile, ns: &str, header: &str) -> String {
    if is_directional(s) {
        emit_bench_stubs_directional(s, ns, header)
    } else {
        emit_bench_stubs_directionless(s, ns, header)
    }
}

/// folds every byte of an alpha (text) field into the running checksum, shared by
/// every bench file.
fn fold_bytes_fn() -> &'static str {
    "// folds every byte of an alpha (text) field into the running checksum.\n\
     fn fold_bytes(acc: i64, bytes: &[u8]) -> i64 {\n    bytes.iter().fold(acc, |a, &b| a.wrapping_add(b as i64))\n}\n\n"
}

/// one `fn view_<method>(row: &[u8]) -> i64` folding every field accessor of a fixed
/// or appendage kind's view into a checksum, shared by directionless and directional
/// bench emission.
fn emit_bench_view_fn(kind: &Kind) -> String {
    let method = visitor_method(&kind.name);
    let mut out = format!("fn view_{method}(row: &[u8]) -> i64 {{\n");
    out.push_str(&format!(
        "    let v = {}View::parse(row).unwrap();\n    let mut acc: i64 = 0;\n",
        kind.name
    ));
    for field in &kind.fields {
        match field.class {
            TypeClass::Alpha => {
                out.push_str(&format!("    acc = fold_bytes(acc, v.{}());\n", field.name))
            }
            // accessor already returns i64; a cast would be clippy unnecessary_cast
            TypeClass::I64 => out.push_str(&format!(
                "    acc = acc.wrapping_add(v.{}());\n",
                field.name
            )),
            _ => out.push_str(&format!(
                "    acc = acc.wrapping_add(v.{}() as i64);\n",
                field.name
            )),
        }
    }
    out.push_str("    acc\n}\n\n");
    out
}

/// `benches/per_kind_<ns>.rs` body for a directionless (feed) schema: unchanged shape
/// from before direction/builder support landed, byte-identical for every committed
/// feed namespace.
fn emit_bench_stubs_directionless(s: &SchemaFile, ns: &str, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Per-kind criterion bench: folds every generated-view accessor into a\n\
         //! checksum so field reads cannot be optimized away.\n\n",
    );
    out.push_str("use std::hint::black_box;\n\n");
    out.push_str("use criterion::{criterion_group, criterion_main, Criterion};\n");
    out.push_str(&format!("use proto_{}::{ns}::*;\n\n", s.venue));
    out.push_str(synthetic_row_fn());
    out.push_str(fold_bytes_fn());

    let fixed: Vec<_> = s.kinds.iter().filter(|k| !k.variable).collect();

    for kind in &fixed {
        out.push_str(&emit_bench_view_fn(kind));
    }

    out.push_str(&format!(
        "fn bench_per_kind(c: &mut Criterion) {{\n    let mut group = c.benchmark_group(\"per_kind_{ns}\");\n"
    ));
    for kind in &fixed {
        let method = visitor_method(&kind.name);
        let size = kind.byte_len.unwrap_or(0);
        out.push_str(&format!(
            "    group.bench_function(\"{method}\", |b| {{\n        let row = synthetic_row({}, {size});\n        b.iter(|| black_box(view_{method}(&row)));\n    }});\n",
            byte_literal(kind.byte)
        ));
    }
    out.push_str("    group.finish();\n}\n\n");
    out.push_str("criterion_group!(benches, bench_per_kind);\ncriterion_main!(benches);\n");

    out
}

/// `benches/per_kind_<ns>.rs` body for a directional schema: recv-view bench per
/// from_exchange kind (appendage kinds benched on the empty-tail wire form, the
/// cheapest valid parse), plus one builder bench per to_exchange kind measuring
/// construct-set-finish over a stack buffer.
fn emit_bench_stubs_directional(s: &SchemaFile, ns: &str, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Per-kind criterion bench: from_exchange kinds fold every generated-view\n\
         //! accessor into a checksum; to_exchange kinds construct, set every field,\n\
         //! and finish a builder over a stack buffer.\n\n",
    );
    out.push_str("use std::hint::black_box;\n\n");
    out.push_str("use criterion::{criterion_group, criterion_main, Criterion};\n");
    out.push_str(&format!("use proto_{}::{ns}::*;\n\n", s.venue));
    out.push_str(synthetic_row_fn());

    let recv_fixed: Vec<&Kind> = recv_kinds(s).into_iter().filter(|k| !k.variable).collect();
    let to_exchange = build_kinds(s);
    if !to_exchange.is_empty() {
        out.push_str(synthetic_bytes_fn());
        out.push_str(synthetic_alpha_fn());
    }
    out.push_str(fold_bytes_fn());

    for kind in &recv_fixed {
        out.push_str(&emit_bench_view_fn(kind));
    }

    out.push_str(&format!(
        "fn bench_per_kind(c: &mut Criterion) {{\n    let mut group = c.benchmark_group(\"per_kind_{ns}\");\n"
    ));
    for kind in &recv_fixed {
        let method = visitor_method(&kind.name);
        let size = kind.byte_len.unwrap_or(0);
        // mut only when the empty-tail rewrite below mutates the row
        let binding = if kind.appendage.is_empty() {
            "let row"
        } else {
            "let mut row"
        };
        out.push_str(&format!(
            "    group.bench_function(\"{method}\", |b| {{\n        {binding} = synthetic_row({}, {size});\n",
            byte_literal(kind.byte)
        ));
        if !kind.appendage.is_empty() {
            out.push_str(
                "        let n = row.len();\n        row[n - 2] = 0;\n        row[n - 1] = 0;\n",
            );
        }
        out.push_str(&format!(
            "        b.iter(|| black_box(view_{method}(&row)));\n    }});\n"
        ));
    }
    for kind in &to_exchange {
        out.push_str(&emit_bench_build_block(s, kind));
    }
    out.push_str("    group.finish();\n}\n\n");
    out.push_str("criterion_group!(benches, bench_per_kind);\ncriterion_main!(benches);\n");

    out
}

/// one `group.bench_function("<method>_build", ...)` block: synthetic values
/// computed once as bench setup, `Builder::new` + every setter + `finish()` measured
/// per iteration.
fn emit_bench_build_block(s: &SchemaFile, kind: &Kind) -> String {
    let method = visitor_method(&kind.name);
    let total_len: u16 = kind.byte_len.unwrap_or(0)
        + kind
            .appendage
            .iter()
            .map(|t| 2 + find_appendage_tag(s, t).width)
            .sum::<u16>();
    let mut out = format!("    group.bench_function(\"{method}_build\", |b| {{\n");
    let mut seed = kind.byte;
    for field in &kind.fields {
        out.push_str(&emit_field_synthetic_binding(field, seed));
        seed = seed.wrapping_add(23);
    }
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&emit_tag_synthetic_binding(tag, seed));
        seed = seed.wrapping_add(23);
    }
    out.push_str("        b.iter(|| {\n");
    out.push_str(&format!("            let mut buf = [0u8; {total_len}];\n"));
    out.push_str(&format!(
        "            let mut bb = {}Builder::new(&mut buf);\n",
        kind.name
    ));
    for field in &kind.fields {
        out.push_str(&emit_field_setter_call(field, "bb", "            "));
    }
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&tag_builder_append_call(tag, "bb", "            "));
    }
    // black_box the buffer, not just the returned length: finish() returns a
    // constant, and without an observed read of buf the optimizer dead-store
    // eliminates every setter write, benching only loop overhead
    out.push_str(
        "            let n = bb.finish();\n            black_box(&buf);\n            black_box(n)\n        });\n    });\n",
    );
    out
}
