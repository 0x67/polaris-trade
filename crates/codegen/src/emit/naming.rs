//! Naming, formatting, and shared codegen helpers used across emission targets.

use super::{build_kinds, doc_lines, doc_text, recv_kinds};
use crate::schema::{AppendageTag, Field, Kind, SchemaFile, TypeClass};

/// `message_type.rs` body: enum + `from_byte`/`as_byte` + `BYTE_LEN` table.
/// Shape mirrors `itch-parse-spike/crates/itch-v1/src/message_type.rs`.
pub(super) fn emit_message_type(s: &SchemaFile, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(&format!(
        "//! `{}` `{}` message type byte to variant mapping, plus fixed wire-size table.\n\n",
        s.venue, s.protocol
    ));
    out.push_str("use std::num::NonZeroU16;\n\n");

    let recv: Vec<&Kind> = recv_kinds(s);

    out.push_str(&format!(
        "/// `{}` message type. One variant per wire type byte.\n",
        s.protocol
    ));
    out.push_str(
        "#[repr(u8)]\n#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\npub enum MessageType {\n",
    );
    for kind in recv.iter().copied() {
        if let Some(doc) = doc_text(&kind.doc) {
            out.push_str(&doc_lines(doc, "    "));
        }
        out.push_str(&format!(
            "    {} = {},\n",
            kind.name,
            byte_literal(kind.byte)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("impl MessageType {\n");
    out.push_str("    /// map wire type byte to variant. None if byte is not a known type.\n");
    out.push_str("    pub const fn from_byte(b: u8) -> Option<Self> {\n        Some(match b {\n");
    for kind in recv.iter().copied() {
        out.push_str(&format!(
            "            {} => Self::{},\n",
            byte_literal(kind.byte),
            kind.name
        ));
    }
    out.push_str("            _ => return None,\n        })\n    }\n\n");
    out.push_str("    /// wire type byte for this variant.\n");
    out.push_str("    pub const fn as_byte(self) -> u8 {\n        self as u8\n    }\n}\n\n");

    out.push_str("const fn nz(n: u16) -> Option<NonZeroU16> {\n    NonZeroU16::new(n)\n}\n\n");

    let appendage_kinds: Vec<&Kind> = recv
        .iter()
        .copied()
        .filter(|k| !k.variable && !k.appendage.is_empty())
        .collect();
    let dual_form_names: Vec<&str> = appendage_kinds
        .iter()
        .copied()
        .filter(|k| k.appendage_optional)
        .map(|k| k.name.as_str())
        .collect();

    out.push_str(
        "/// fixed wire size per type byte. None for unknown byte or variable-length kind.\n",
    );
    if !appendage_kinds.is_empty() {
        out.push_str(
            "///\n/// For a kind carrying a trailing appendage, this is the MINIMUM valid\n\
             /// wire length: fixed prefix plus the synthesized 2-byte appendage length\n\
             /// field. The exact wire length also depends on the tail; view `parse`\n\
             /// does the length-field-consistency check.\n",
        );
        if !dual_form_names.is_empty() {
            out.push_str(&format!(
                "///\n/// Dual-form kinds additionally accept a bare form 2 bytes shorter\n\
                 /// (no length field, empty appendage): {}.\n",
                dual_form_names.join(", ")
            ));
        }
    }
    out.push_str("pub const BYTE_LEN: [Option<NonZeroU16>; 256] = build_byte_len();\n\n");

    out.push_str("const fn build_byte_len() -> [Option<NonZeroU16>; 256] {\n    let mut table = [None; 256];\n");
    for kind in recv.iter().copied() {
        if kind.variable {
            out.push_str(&format!(
                "    // {} ({}) stays None: variable length on wire\n",
                byte_literal(kind.byte),
                kind.name
            ));
        } else {
            // validate() guarantees fixed kinds carry byte_len; 0 fallback never reached.
            let len = kind.byte_len.unwrap_or(0);
            out.push_str(&format!(
                "    table[{} as usize] = nz({len});\n",
                byte_literal(kind.byte)
            ));
        }
    }
    out.push_str("    table\n}\n");

    out
}
/// Rust type name for a numeric `TypeClass`. Caller must not pass `Code`/`Alpha`.
pub(super) fn numeric_type_name(class: TypeClass) -> &'static str {
    match class {
        TypeClass::U8 => "u8",
        TypeClass::U16 => "u16",
        TypeClass::U32 => "u32",
        TypeClass::U48 => "u64",
        TypeClass::U64 => "u64",
        TypeClass::I8 => "i8",
        TypeClass::I16 => "i16",
        TypeClass::I32 => "i32",
        TypeClass::I64 => "i64",
        TypeClass::Code | TypeClass::Alpha => {
            unreachable!("emit_numeric_accessor only called for numeric fields")
        }
    }
}
/// resolve a kind's appendage tag name to its registry entry. `validate()` guarantees
/// every name in `kind.appendage` resolves, so the panic here is unreachable once
/// `generate()`'s `s.validate()?` has run.
pub(super) fn find_appendage_tag<'a>(s: &'a SchemaFile, name: &str) -> &'a AppendageTag {
    s.appendage_tags
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("validate() guarantees appendage tag {name} resolves"))
}
/// deterministic synthetic-row builder text, shared by parity tests and bench stubs.
/// `row[0]` is the kind byte, `row[j] = byte ^ (j as u8).wrapping_mul(37)` for `j >= 1`:
/// non-uniform bytes so an all-zeros decode can't false-pass parity.
pub(super) fn synthetic_row_fn() -> &'static str {
    "fn synthetic_row(byte: u8, len: usize) -> Vec<u8> {\n    let mut row = vec![0u8; len];\n    row[0] = byte;\n    for (j, slot) in row.iter_mut().enumerate().skip(1) {\n        *slot = byte ^ (j as u8).wrapping_mul(37);\n    }\n    row\n}\n\n"
}
/// `let <name>_v = ...;` binding holding one field's synthetic value, computed at
/// TEST RUNTIME from `seed` via the shared `synthetic_bytes`/`synthetic_alpha`
/// helpers so the same value drives both the builder setter call and the parity
/// assertion against the baseline.
pub(super) fn emit_field_synthetic_binding(field: &Field, seed: u8) -> String {
    match field.class {
        TypeClass::Code => format!(
            "    let {0}_v: u8 = synthetic_bytes({seed}, 1)[0];\n",
            field.name
        ),
        TypeClass::Alpha => format!(
            "    let {0}_v: [u8; {1}] = synthetic_alpha({seed}, {1}).try_into().unwrap();\n",
            field.name, field.width
        ),
        TypeClass::U48 => format!(
            "    let {0}_bytes: [u8; 6] = synthetic_bytes({seed}, 6).try_into().unwrap();\n    let {0}_v = {{ let mut buf = [0u8; 8]; buf[2..8].copy_from_slice(&{0}_bytes); u64::from_be_bytes(buf) }};\n",
            field.name
        ),
        other => format!(
            "    let {0}_v: {1} = {1}::from_be_bytes(synthetic_bytes({seed}, {2}).try_into().unwrap());\n",
            field.name,
            numeric_type_name(other),
            field.width
        ),
    }
}

/// one `<builder>.set_<name>(...)` call passing the value bound by
/// `emit_field_synthetic_binding`, at `indent` spaces.
pub(super) fn emit_field_setter_call(field: &Field, builder: &str, indent: &str) -> String {
    match field.class {
        TypeClass::Alpha => format!("{indent}{builder}.set_{0}(&{0}_v);\n", field.name),
        _ => format!("{indent}{builder}.set_{0}({0}_v);\n", field.name),
    }
}

/// `let tag_<name>_bytes: [u8; W] = ...;` binding holding one appendage tag's
/// synthetic wire value, plus (for non-alpha classes) a `let tag_<name>_v = ...;`
/// typed binding decoded from those same bytes. `seed` differentiates tags within
/// one kind so no two tags on the same kind synthesize identical bytes.
pub(super) fn emit_tag_synthetic_binding(tag: &AppendageTag, seed: u8) -> String {
    let width = tag.width;
    let synth_fn = if tag.class == TypeClass::Alpha {
        "synthetic_alpha"
    } else {
        "synthetic_bytes"
    };
    let mut out = format!(
        "    let tag_{0}_bytes: [u8; {width}] = {synth_fn}({seed}, {width}).try_into().unwrap();\n",
        tag.name
    );
    match tag.class {
        TypeClass::Alpha => {}
        TypeClass::Code => out.push_str(&format!(
            "    let tag_{0}_v = tag_{0}_bytes[0];\n",
            tag.name
        )),
        TypeClass::U48 => out.push_str(&format!(
            "    let tag_{0}_v = {{ let mut buf = [0u8; 8]; buf[2..8].copy_from_slice(&tag_{0}_bytes); u64::from_be_bytes(buf) }};\n",
            tag.name
        )),
        other => out.push_str(&format!(
            "    let tag_{0}_v = {1}::from_be_bytes(tag_{0}_bytes);\n",
            tag.name,
            numeric_type_name(other)
        )),
    }
    out
}
/// generated builder `append_<tag>` call for TO_EXCHANGE builder-driven parity: typed
/// value for numeric/code/u48 classes (mirrors `appendage_param_type`), raw array
/// reference for alpha. At `indent` spaces.
pub(super) fn tag_builder_append_call(tag: &AppendageTag, builder: &str, indent: &str) -> String {
    let arg = match tag.class {
        TypeClass::Alpha => format!("&tag_{}_bytes", tag.name),
        _ => format!("tag_{}_v", tag.name),
    };
    format!("{indent}{builder}.append_{}({arg});\n", tag.name)
}
/// deterministic per-field/per-tag byte pattern, test-runtime counterpart of
/// `synthetic_row`'s xor pattern: forces the top bit of the first (most significant,
/// big-endian) byte so signed numeric fields see negative values and unsigned fields
/// see their high range, not just small positive ones.
pub(super) fn synthetic_bytes_fn() -> &'static str {
    "#[allow(dead_code)]\nfn synthetic_bytes(seed: u8, width: usize) -> Vec<u8> {\n    let mut v: Vec<u8> = (0..width).map(|i| seed ^ (i as u8).wrapping_mul(37)).collect();\n    if let Some(first) = v.first_mut() {\n        *first |= 0x80;\n    }\n    v\n}\n\n"
}

/// deterministic printable alpha synthetic value: cycles 'A'..='Z' at the requested
/// width, keyed by `seed` the same way `synthetic_bytes` is.
pub(super) fn synthetic_alpha_fn() -> &'static str {
    "#[allow(dead_code)]\nfn synthetic_alpha(seed: u8, width: usize) -> Vec<u8> {\n    (0..width)\n        .map(|i| b'A' + seed.wrapping_add(i as u8) % 26)\n        .collect()\n}\n\n"
}
/// `mod.rs` body: module wiring plus re-exports. Later emitters extend `mod_decls`/`reexport_lines`.
pub(super) fn emit_mod_rs(s: &SchemaFile, ns: &str, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(&format!(
        "//! `{ns}` generated module: message types, views, and dispatch for `{}` `{}`.\n\n",
        s.venue, s.protocol
    ));

    let has_appendage = !s.appendage_tags.is_empty();
    let has_build = !build_kinds(s).is_empty();

    let mut mod_decls = vec!["message_type", "view"];
    if has_appendage {
        mod_decls.push("appendage");
    }
    if has_build {
        mod_decls.push("build");
    }
    mod_decls.push("dispatch");

    let mut reexport_lines = vec![
        "pub use message_type::{MessageType, BYTE_LEN};",
        "pub use view::*;",
    ];
    if has_appendage {
        reexport_lines.push("pub use appendage::{AppendageTag, TagIter};");
    }
    if has_build {
        reexport_lines.push("pub use build::*;");
    }
    reexport_lines.push("pub use dispatch::{HotVisitor, parse_match, parse_lut, build_lut};");

    for m in mod_decls {
        out.push_str(&format!("pub mod {m};\n"));
    }
    out.push('\n');
    for line in reexport_lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}
/// `<ns>/appendage.rs` body: typed tag enum plus a borrowed iterator over it, generated
/// once per schema from the schema-level `appendage_tags` registry. Only emitted when
/// that registry is nonempty.
pub(super) fn emit_appendage(s: &SchemaFile, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Typed appendage tag registry, generated from the schema's `appendage_tags`\n\
         //! list. Unregistered tags and registered tags at the wrong wire width both\n\
         //! decode to `Unknown`, never panic.\n\n",
    );
    out.push_str("use proto_common::appendage::AppendageIter;\n\n");

    out.push_str(
        "/// one decoded appendage TagValue element, typed per the schema's tag registry.\n",
    );
    out.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n");
    out.push_str("pub enum AppendageTag<'a> {\n");
    for tag in &s.appendage_tags {
        if let Some(doc) = doc_text(&tag.doc) {
            out.push_str(&doc_lines(doc, "    "));
        }
        out.push_str(&format!(
            "    {}({}),\n",
            pascal_case(&tag.name),
            appendage_variant_type(tag)
        ));
    }
    out.push_str("    /// unregistered tag, or a registered tag whose value width does not\n");
    out.push_str("    /// match the registry: always readable as raw bytes, never panics.\n");
    out.push_str("    Unknown { tag: u8, value: &'a [u8] },\n");
    out.push_str("}\n\n");

    out.push_str("impl<'a> AppendageTag<'a> {\n");
    out.push_str(
        "    // width guard on each arm makes the try_into/unwrap calls below panic-free\n",
    );
    out.push_str("    fn from_raw(tag: u8, value: &'a [u8]) -> Self {\n        match tag {\n");
    for tag in &s.appendage_tags {
        out.push_str(&format!(
            "            {} if value.len() == {} => Self::{}({}),\n",
            tag.tag,
            tag.width,
            pascal_case(&tag.name),
            appendage_variant_ctor(tag)
        ));
    }
    out.push_str("            _ => Self::Unknown { tag, value },\n        }\n    }\n}\n\n");

    out.push_str("/// borrowed iterator over an appendage tail, typed per the tag registry.\n");
    out.push_str("pub struct TagIter<'a>(AppendageIter<'a>);\n\n");
    out.push_str("impl<'a> TagIter<'a> {\n");
    out.push_str("    #[inline]\n");
    out.push_str("    pub fn new(tail: &'a [u8]) -> Self {\n        Self(AppendageIter::new(tail))\n    }\n}\n\n");
    out.push_str("impl<'a> Iterator for TagIter<'a> {\n");
    out.push_str("    type Item = AppendageTag<'a>;\n\n");
    out.push_str("    #[inline]\n");
    out.push_str("    fn next(&mut self) -> Option<Self::Item> {\n");
    out.push_str("        self.0.next().map(|(tag, value)| AppendageTag::from_raw(tag, value))\n");
    out.push_str("    }\n}\n");

    out
}

/// Rust type for one registry tag's decoded value: alpha stays a borrowed fixed-size
/// slice, `code` reads as `u8`, everything else via `numeric_type_name`.
fn appendage_variant_type(tag: &AppendageTag) -> String {
    match tag.class {
        TypeClass::Alpha => format!("&'a [u8; {}]", tag.width),
        TypeClass::Code => "u8".to_string(),
        TypeClass::U48 => "u64".to_string(),
        other => numeric_type_name(other).to_string(),
    }
}

/// constructor expression for one registry tag's variant, given a `value: &'a [u8]`
/// binding already checked to the tag's declared width by the caller's match guard.
fn appendage_variant_ctor(tag: &AppendageTag) -> String {
    match tag.class {
        TypeClass::Alpha => "value.try_into().unwrap()".to_string(),
        TypeClass::Code => "value[0]".to_string(),
        TypeClass::U48 => {
            "{ let mut buf = [0u8; 8]; buf[2..8].copy_from_slice(value); u64::from_be_bytes(buf) }"
                .to_string()
        }
        other => format!(
            "{}::from_be_bytes(value.try_into().unwrap())",
            numeric_type_name(other)
        ),
    }
}
/// snake_case registry tag name to PascalCase enum variant name.
pub(super) fn pascal_case(snake: &str) -> String {
    snake
        .split('_')
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}
/// wire type byte as a Rust byte literal, escaping the two chars that would break `b'X'` syntax.
pub(super) fn byte_literal(b: u8) -> String {
    match b {
        b'\'' => "b'\\''".to_string(),
        b'\\' => "b'\\\\'".to_string(),
        _ => format!("b'{}'", b as char),
    }
}
/// lowercase hex encoding, no external hex crate needed for this one call site.
pub(super) fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}
