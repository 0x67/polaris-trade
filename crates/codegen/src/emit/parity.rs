//! Parity test emission: generated view/builder output checked against the
//! hand-written owned baseline on synthetic rows.

use super::{
    build_kinds, is_directional,
    naming::{
        byte_literal, emit_field_setter_call, emit_field_synthetic_binding,
        emit_tag_synthetic_binding, find_appendage_tag, pascal_case, synthetic_alpha_fn,
        synthetic_bytes_fn, synthetic_row_fn, tag_builder_append_call,
    },
    recv_kinds,
};
use crate::schema::{AppendageTag, Field, Kind, SchemaFile, TypeClass, visitor_method};

/// `tests/parity_<ns>.rs` body: one parity test per fixed kind, generated view vs the
/// hand-written `support::owned_<ns>` baseline on a synthetic row. Baseline module is
/// per-namespace so protocol majors sharing one venue crate keep independent byte
/// spaces. Missing a baseline arm or a field name mismatch is a compile error in the
/// emitted test, by design. Directionless schemas keep the single-entry-point shape
/// below; directional schemas route through `emit_parity_tests_directional` (split
/// `decode_to_exchange`/`decode_from_exchange` entry points, appendage variants,
/// builder-driven to_exchange parity).
/// Shape mirrors `itch-parse-spike/crates/itch-v1/tests/parity_view.rs`.
pub(super) fn emit_parity_tests(s: &SchemaFile, ns: &str, header: &str) -> String {
    if is_directional(s) {
        emit_parity_tests_directional(s, ns, header)
    } else {
        emit_parity_tests_directionless(s, ns, header)
    }
}

/// `tests/parity_<ns>.rs` body for a directionless (feed) schema: unchanged shape
/// from before direction/builder support landed, byte-identical for every committed
/// feed namespace.
fn emit_parity_tests_directionless(s: &SchemaFile, ns: &str, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Per-kind parity of generated views vs the hand-written owned baseline, on\n\
         //! deterministic synthetic rows.\n\n",
    );
    out.push_str("mod support;\n\n");
    out.push_str(&format!("use proto_{}::{ns}::*;\n\n", s.venue));
    out.push_str(synthetic_row_fn());

    for kind in s.kinds.iter().filter(|k| !k.variable) {
        let method = visitor_method(&kind.name);
        out.push_str(&format!("#[test]\nfn parity_{method}() {{\n"));
        out.push_str(&format!(
            "    let row = synthetic_row({}, {}View::SIZE);\n",
            byte_literal(kind.byte),
            kind.name
        ));
        out.push_str(&format!(
            "    let owned = match support::owned_{ns}::decode_owned({}, &row).expect(\"baseline decodes\") {{\n",
            byte_literal(kind.byte)
        ));
        out.push_str(&format!(
            "        support::owned_{ns}::OwnedMessage::{}(m) => m,\n",
            kind.name
        ));
        out.push_str(&format!(
            "        other => panic!(\"expected {}, got {{other:?}}\"),\n",
            kind.name
        ));
        out.push_str("    };\n");
        out.push_str(&format!(
            "    let v = {}View::parse(&row).unwrap();\n",
            kind.name
        ));
        out.push_str(&emit_field_parity_asserts(&kind.fields));
        out.push_str("}\n\n");
    }

    out
}

/// `tests/parity_<ns>.rs` body for a directional schema: from_exchange kinds parity
/// against `support::owned_<ns>::decode_from_exchange`, to_exchange kinds build via
/// the generated builder then parity against `decode_to_exchange`. Appendage-carrying
/// from_exchange kinds get empty/full (and, when `appendage_optional`, bare) variants;
/// appendage-carrying to_exchange kinds get a base (no tags) and a full-tag variant.
fn emit_parity_tests_directional(s: &SchemaFile, ns: &str, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Per-kind parity of generated views/builders vs the hand-written owned\n\
         //! baseline, on deterministic synthetic rows and setter-driven values.\n\
         //! from_exchange kinds check `decode_from_exchange`; to_exchange kinds build\n\
         //! then check `decode_to_exchange`.\n\n",
    );
    out.push_str("mod support;\n\n");
    out.push_str(&format!("use proto_{}::{ns}::*;\n\n", s.venue));
    out.push_str(synthetic_row_fn());
    out.push_str(synthetic_bytes_fn());
    out.push_str(synthetic_alpha_fn());
    // append_tag is only called by from_exchange appendage rows (to_exchange
    // appendage parity drives the generated builder's own append_<tag> methods
    // instead), so gate strictly on that to avoid an unused free fn.
    if recv_kinds(s).iter().any(|k| !k.appendage.is_empty()) {
        out.push_str(append_tag_fn());
    }

    for kind in recv_kinds(s).into_iter().filter(|k| !k.variable) {
        if kind.appendage.is_empty() {
            out.push_str(&emit_parity_from_exchange_fixed(kind, ns));
        } else {
            out.push_str(&emit_parity_from_exchange_appendage(s, kind, ns));
        }
    }

    for kind in build_kinds(s) {
        out.push_str(&emit_parity_to_exchange(s, kind, ns));
    }

    out
}

/// one `assert_eq!` per field, comparing a generated view accessor to the baseline's
/// same-named owned struct field. Alpha fields compare via slice (view returns
/// `&[u8]`, baseline stores a fixed array); everything else compares by value.
fn emit_field_parity_asserts(fields: &[Field]) -> String {
    let mut out = String::new();
    for field in fields {
        match field.class {
            TypeClass::Alpha => out.push_str(&format!(
                "    assert_eq!(v.{0}(), &owned.{0}[..], \"{0}\");\n",
                field.name
            )),
            _ => out.push_str(&format!(
                "    assert_eq!(v.{0}(), owned.{0}, \"{0}\");\n",
                field.name
            )),
        }
    }
    out
}

/// `tests/parity_<ns>.rs` fragment for one from_exchange fixed (no appendage) kind:
/// unchanged shape from the directionless test, split entry point.
fn emit_parity_from_exchange_fixed(kind: &Kind, ns: &str) -> String {
    let method = visitor_method(&kind.name);
    let mut out = format!("#[test]\nfn parity_{method}() {{\n");
    out.push_str(&format!(
        "    let row = synthetic_row({}, {}View::SIZE);\n",
        byte_literal(kind.byte),
        kind.name
    ));
    out.push_str(&emit_from_exchange_decode_and_view(kind, ns));
    out.push_str(&emit_field_parity_asserts(&kind.fields));
    out.push_str("}\n\n");
    out
}

/// shared `decode_from_exchange` dispatch plus `View::parse` lines for a from_exchange
/// kind's parity test, over a caller-constructed `row`.
fn emit_from_exchange_decode_and_view(kind: &Kind, ns: &str) -> String {
    let mut out = format!(
        "    let owned = match support::owned_{ns}::decode_from_exchange(&row).expect(\"baseline decodes\") {{\n"
    );
    out.push_str(&format!(
        "        support::owned_{ns}::OwnedMessage::{}(m) => m,\n",
        kind.name
    ));
    out.push_str(&format!(
        "        other => panic!(\"expected {}, got {{other:?}}\"),\n",
        kind.name
    ));
    out.push_str("    };\n");
    out.push_str(&format!(
        "    let v = {}View::parse(&row).expect(\"view parses\");\n",
        kind.name
    ));
    out
}

/// `tests/parity_<ns>.rs` fragment for one from_exchange appendage kind: `_empty`
/// (length field present, zero tags), `_full` (every registered tag appended, typed
/// and raw tail equality both checked), and, for `appendage_optional` kinds, `_bare`
/// (length field absent entirely).
fn emit_parity_from_exchange_appendage(s: &SchemaFile, kind: &Kind, ns: &str) -> String {
    let method = visitor_method(&kind.name);
    // fixed_len excludes the length field (matches view's FIXED_LEN); byte_len
    // is the wire minimum INCLUDING it, see emit_view_kind_appendage.
    let fixed_len: u16 = 1 + kind.fields.iter().map(|f| f.width).sum::<u16>();
    let byte_len = kind.byte_len.unwrap_or(0);

    let mut out = String::new();

    // empty: length field present, value zero, tail empty.
    out.push_str(&format!("#[test]\nfn parity_{method}_empty() {{\n"));
    out.push_str(&format!(
        "    let mut row = synthetic_row({}, {byte_len});\n",
        byte_literal(kind.byte)
    ));
    out.push_str("    let n = row.len();\n    row[n - 2] = 0;\n    row[n - 1] = 0;\n");
    out.push_str(&emit_from_exchange_decode_and_view(kind, ns));
    out.push_str(&emit_field_parity_asserts(&kind.fields));
    out.push_str("    assert_eq!(owned.appendage.len(), 0, \"empty appendage baseline\");\n");
    out.push_str("    assert_eq!(v.appendage().count(), 0, \"empty appendage view\");\n");
    out.push_str("}\n\n");

    // full: length field present, every registered tag appended with a synthetic
    // value, typed `tags()` and raw `appendage()` both checked against the baseline.
    out.push_str(&format!("#[test]\nfn parity_{method}_full() {{\n"));
    out.push_str(&format!(
        "    let mut row = synthetic_row({}, {fixed_len});\n",
        byte_literal(kind.byte)
    ));
    out.push_str("    row.push(0);\n    row.push(0);\n");
    let mut seed = kind.byte;
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&emit_tag_synthetic_binding(tag, seed));
        seed = seed.wrapping_add(23);
    }
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&tag_row_append_call(tag));
    }
    out.push_str(&format!(
        "    let tail_len = (row.len() - {fixed_len} - 2) as u16;\n    row[{fixed_len}..{fixed_len} + 2].copy_from_slice(&tail_len.to_be_bytes());\n"
    ));
    out.push_str(&emit_from_exchange_decode_and_view(kind, ns));
    out.push_str(&emit_field_parity_asserts(&kind.fields));
    out.push_str("    let got: Vec<(u8, Vec<u8>)> = v.appendage().map(|(t, val)| (t, val.to_vec())).collect();\n");
    out.push_str("    assert_eq!(got, owned.appendage, \"appendage tail\");\n");
    out.push_str("    let mut tags = v.tags();\n");
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&format!(
            "    assert_eq!(tags.next(), Some({}), \"{tag_name} typed\");\n",
            tag_variant_expr(tag)
        ));
    }
    out.push_str("    assert_eq!(tags.next(), None, \"no extra tags\");\n");
    out.push_str("}\n\n");

    // bare: length field absent entirely, wire ends right after the fixed part.
    if kind.appendage_optional {
        out.push_str(&format!("#[test]\nfn parity_{method}_bare() {{\n"));
        out.push_str(&format!(
            "    let row = synthetic_row({}, {fixed_len});\n",
            byte_literal(kind.byte)
        ));
        out.push_str(&emit_from_exchange_decode_and_view(kind, ns));
        out.push_str(&emit_field_parity_asserts(&kind.fields));
        out.push_str(
            "    assert_eq!(owned.appendage.len(), 0, \"bare-form baseline appendage\");\n",
        );
        out.push_str("    assert_eq!(v.appendage().count(), 0, \"bare-form view appendage\");\n");
        out.push_str("}\n\n");
    }

    out
}

/// `tests/parity_<ns>.rs` fragment for one to_exchange kind: dispatches on whether
/// the kind declares an appendage.
fn emit_parity_to_exchange(s: &SchemaFile, kind: &Kind, ns: &str) -> String {
    if kind.appendage.is_empty() {
        emit_parity_to_exchange_fixed(kind, ns)
    } else {
        emit_parity_to_exchange_appendage(s, kind, ns)
    }
}

/// builder-driven parity for a to_exchange kind with no appendage: drive every
/// setter with a synthetic value, `finish()`, decode with the baseline, assert
/// field-for-field equality plus total length.
fn emit_parity_to_exchange_fixed(kind: &Kind, ns: &str) -> String {
    let method = visitor_method(&kind.name);
    let mut out = format!("#[test]\nfn parity_{method}_build() {{\n");
    out.push_str(&format!(
        "    let mut buf = [0u8; {}Builder::FIXED_LEN];\n    let mut b = {}Builder::new(&mut buf);\n",
        kind.name, kind.name
    ));
    let mut seed = kind.byte;
    for field in &kind.fields {
        out.push_str(&emit_field_synthetic_binding(field, seed));
        out.push_str(&emit_field_setter_call(field, "b", "    "));
        seed = seed.wrapping_add(23);
    }
    out.push_str("    let n = b.finish();\n");
    out.push_str(&format!(
        "    assert_eq!(n, {}Builder::FIXED_LEN, \"finish length\");\n",
        kind.name
    ));
    out.push_str(&emit_to_exchange_decode(kind, ns, "&buf[..n]"));
    out.push_str(&emit_field_build_parity_asserts(&kind.fields));
    out.push_str("}\n\n");
    out
}

/// builder-driven parity for a to_exchange kind carrying an appendage: a base variant
/// (fields only, zero tags appended) and a full variant (every registered tag
/// appended), both asserted against the baseline.
fn emit_parity_to_exchange_appendage(s: &SchemaFile, kind: &Kind, ns: &str) -> String {
    let method = visitor_method(&kind.name);
    let fixed_len = kind.byte_len.unwrap_or(0);
    let mut out = String::new();

    // base: fields only, no tags appended. mut only when setter calls follow
    // (appendage-only kinds have no setters, so the base variant never mutates).
    let binding = if kind.fields.is_empty() {
        "let b"
    } else {
        "let mut b"
    };
    out.push_str(&format!("#[test]\nfn parity_{method}_build() {{\n"));
    out.push_str(&format!(
        "    let mut buf = [0u8; {}Builder::FIXED_LEN];\n    {binding} = {}Builder::new(&mut buf);\n",
        kind.name, kind.name
    ));
    let mut seed = kind.byte;
    for field in &kind.fields {
        out.push_str(&emit_field_synthetic_binding(field, seed));
        out.push_str(&emit_field_setter_call(field, "b", "    "));
        seed = seed.wrapping_add(23);
    }
    out.push_str("    let n = b.finish();\n");
    out.push_str(&format!(
        "    assert_eq!(n, {}Builder::FIXED_LEN, \"finish length, no tags\");\n",
        kind.name
    ));
    out.push_str(&emit_to_exchange_decode(kind, ns, "&buf[..n]"));
    out.push_str(&emit_field_build_parity_asserts(&kind.fields));
    out.push_str("    assert_eq!(owned.appendage.len(), 0, \"no tags appended\");\n");
    out.push_str("}\n\n");

    // full: fields plus every registered tag, buffer sized for the worst case.
    let total_len: u16 = fixed_len
        + kind
            .appendage
            .iter()
            .map(|t| 2 + find_appendage_tag(s, t).width)
            .sum::<u16>();
    out.push_str(&format!("#[test]\nfn parity_{method}_build_full() {{\n"));
    out.push_str(&format!(
        "    let mut buf = [0u8; {total_len}];\n    let mut b = {}Builder::new(&mut buf);\n",
        kind.name
    ));
    let mut seed = kind.byte;
    for field in &kind.fields {
        out.push_str(&emit_field_synthetic_binding(field, seed));
        out.push_str(&emit_field_setter_call(field, "b", "    "));
        seed = seed.wrapping_add(23);
    }
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&emit_tag_synthetic_binding(tag, seed));
        out.push_str(&tag_builder_append_call(tag, "b", "    "));
        seed = seed.wrapping_add(23);
    }
    out.push_str("    let n = b.finish();\n");
    out.push_str(&format!(
        "    assert_eq!(n, {total_len}usize, \"finish length, all tags\");\n"
    ));
    out.push_str(&emit_to_exchange_decode(kind, ns, "&buf[..n]"));
    out.push_str(&emit_field_build_parity_asserts(&kind.fields));
    out.push_str("    let expected_appendage: Vec<(u8, Vec<u8>)> = vec![\n");
    for tag_name in &kind.appendage {
        let tag = find_appendage_tag(s, tag_name);
        out.push_str(&format!(
            "        ({}, tag_{tag_name}_bytes.to_vec()),\n",
            tag.tag
        ));
    }
    out.push_str("    ];\n");
    out.push_str("    assert_eq!(owned.appendage, expected_appendage, \"appendage tail\");\n");
    out.push_str("}\n\n");

    out
}

/// shared `decode_to_exchange` dispatch, over the built wire slice `built_expr`.
fn emit_to_exchange_decode(kind: &Kind, ns: &str, built_expr: &str) -> String {
    let mut out = format!(
        "    let owned = match support::owned_{ns}::decode_to_exchange({built_expr}).expect(\"baseline decodes\") {{\n"
    );
    out.push_str(&format!(
        "        support::owned_{ns}::OwnedMessage::{}(m) => m,\n",
        kind.name
    ));
    out.push_str(&format!(
        "        other => panic!(\"expected {}, got {{other:?}}\"),\n",
        kind.name
    ));
    out.push_str("    };\n");
    out
}

/// one `assert_eq!` per field, comparing the baseline's owned struct field to the
/// `<name>_v` local bound by `emit_field_synthetic_binding` and passed to the
/// matching setter: a field a setter forgot to write shows up as a zero-vs-synthetic
/// mismatch, not a silent pass.
fn emit_field_build_parity_asserts(fields: &[Field]) -> String {
    let mut out = String::new();
    for field in fields {
        match field.class {
            TypeClass::Alpha => out.push_str(&format!(
                "    assert_eq!(&owned.{0}[..], &{0}_v[..], \"{0}\");\n",
                field.name
            )),
            _ => out.push_str(&format!(
                "    assert_eq!(owned.{0}, {0}_v, \"{0}\");\n",
                field.name
            )),
        }
    }
    out
}
/// TagValue-element append call for the FROM_EXCHANGE raw row builder (`append_tag`,
/// see `append_tag_fn`): always the raw synthetic bytes, regardless of tag class.
fn tag_row_append_call(tag: &AppendageTag) -> String {
    format!(
        "    append_tag(&mut row, {}, &tag_{}_bytes);\n",
        tag.tag, tag.name
    )
}
/// expected `AppendageTag::<Variant>(...)` expression for one tag, referencing the
/// bindings `emit_tag_synthetic_binding` produced: alpha compares by borrowed array
/// reference, everything else by the typed decoded value.
fn tag_variant_expr(tag: &AppendageTag) -> String {
    match tag.class {
        TypeClass::Alpha => format!(
            "AppendageTag::{}(&tag_{}_bytes)",
            pascal_case(&tag.name),
            tag.name
        ),
        _ => format!(
            "AppendageTag::{}(tag_{}_v)",
            pascal_case(&tag.name),
            tag.name
        ),
    }
}
/// writes one TagValue element (length byte, tag byte, value bytes) onto `buf`,
/// matching the wire's appendage element shape: length byte covers the tag byte plus
/// the value, not the value alone.
fn append_tag_fn() -> &'static str {
    "fn append_tag(buf: &mut Vec<u8>, tag: u8, value: &[u8]) {\n    buf.push((1 + value.len()) as u8);\n    buf.push(tag);\n    buf.extend_from_slice(value);\n}\n\n"
}
