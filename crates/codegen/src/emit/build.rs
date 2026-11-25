//! Send-side builder emission: `build/mod.rs` wiring plus per-kind builder structs.

use super::{
    build_kinds, doc_lines, doc_text,
    naming::{byte_literal, find_appendage_tag, numeric_type_name},
};
use crate::schema::{AppendageTag, Endianness, Field, Kind, SchemaFile, TypeClass, visitor_method};

/// `build/mod.rs` body: private `mod <method>;` decl plus `pub use <method>::<Name>Builder;`
/// re-export per to_exchange kind, mirroring `emit_view_mod`'s shape for the send side.
pub(super) fn emit_build_mod(s: &SchemaFile, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Send-side builders: write venue wire bytes into a caller-provided buffer,\n\
         //! one `#[inline]` setter per field, no heap allocation.\n\n",
    );

    let to_exchange = build_kinds(s);
    for kind in &to_exchange {
        out.push_str(&format!("mod {};\n", visitor_method(&kind.name)));
    }
    out.push('\n');
    for kind in &to_exchange {
        out.push_str(&format!(
            "pub use {}::{}Builder;\n",
            visitor_method(&kind.name),
            kind.name
        ));
    }
    out
}

/// `build/<method>.rs` body for one to_exchange kind: dispatches on whether the kind
/// declares an appendage.
pub(super) fn emit_build_kind(s: &SchemaFile, kind: &Kind, header: &str) -> String {
    if kind.appendage.is_empty() {
        emit_build_kind_fixed(kind, s.endianness, header)
    } else {
        emit_build_kind_appendage(s, kind, header)
    }
}

/// `build/<method>.rs` body for a to_exchange kind with no appendage: `buf: &'a mut
/// [u8]`, one setter per field, `finish()` returns the constant `FIXED_LEN`.
fn emit_build_kind_fixed(kind: &Kind, endianness: Endianness, header: &str) -> String {
    let mut out = header.to_string();

    // validate() requires byte_len on every fixed kind; 0 fallback never reached.
    let fixed_len = kind.byte_len.unwrap_or(0);

    out.push_str(&format!(
        "/// `{}` {}, {fixed_len}B, writes into a caller-provided buffer.\n",
        kind.byte as char, kind.name
    ));
    if let Some(doc) = doc_text(&kind.doc) {
        out.push_str("///\n");
        out.push_str(&doc_lines(doc, ""));
    }
    out.push_str(&format!(
        "pub struct {}Builder<'a> {{\n    buf: &'a mut [u8],\n}}\n\n",
        kind.name
    ));

    out.push_str(&format!("impl<'a> {}Builder<'a> {{\n", kind.name));
    out.push_str(&format!(
        "    pub const FIXED_LEN: usize = {fixed_len};\n\n"
    ));
    out.push_str(
        "    /// `buf` must be at least `FIXED_LEN` bytes. Writes the type byte, zeros\n\
         /// every numeric field, and space-fills every alpha field: no stale caller\n\
         /// bytes survive in the fixed region.\n",
    );
    out.push_str("    pub fn new(buf: &'a mut [u8]) -> Self {\n");
    out.push_str(&format!(
        "        assert!(\n            buf.len() >= {fixed_len},\n            \"buf too short for {kind_name}Builder::new: need at least {fixed_len} bytes\"\n        );\n",
        kind_name = kind.name
    ));
    out.push_str(&format!("        buf[..{fixed_len}].fill(0);\n"));
    out.push_str(&format!("        buf[0] = {};\n", byte_literal(kind.byte)));
    out.push_str(&emit_alpha_space_fill(&kind.fields));
    out.push_str("        Self { buf }\n    }\n\n");

    out.push_str(&emit_field_setters(&kind.fields, endianness, "self.buf"));

    out.push_str("    /// total bytes written: always `FIXED_LEN`, this kind has no appendage.\n");
    out.push_str("    pub fn finish(self) -> usize {\n        Self::FIXED_LEN\n    }\n");
    out.push_str("}\n\n");

    out
}

/// `build/<method>.rs` body for a to_exchange kind carrying an appendage: `buf: &'a
/// mut [u8]` plus an `appendage_end` cursor, one `append_<tag>` per tag registered for
/// THIS kind (a tag outside `kind.appendage` gets no method: compile-time rejection),
/// `finish()` returns the cursor.
fn emit_build_kind_appendage(s: &SchemaFile, kind: &Kind, header: &str) -> String {
    let mut out = header.to_string();

    // FIXED_LEN matches kind.byte_len / the BYTE_LEN table value, so it INCLUDES the
    // synthesized 2-byte appendage length field; the view's fixed_len excludes it.
    let fixed_len = kind.byte_len.unwrap_or(0);

    out.push_str(&format!(
        "/// `{}` {}, {fixed_len}B fixed prefix (including the appendage length field)\n/// plus a trailing appendage, writes into a caller-provided buffer.\n",
        kind.byte as char, kind.name
    ));
    if let Some(doc) = doc_text(&kind.doc) {
        out.push_str("///\n");
        out.push_str(&doc_lines(doc, ""));
    }
    out.push_str(&format!(
        "pub struct {}Builder<'a> {{\n    buf: &'a mut [u8],\n    appendage_end: usize,\n}}\n\n",
        kind.name
    ));

    out.push_str(&format!("impl<'a> {}Builder<'a> {{\n", kind.name));
    out.push_str(&format!(
        "    pub const FIXED_LEN: usize = {fixed_len};\n\n"
    ));
    out.push_str(
        "    /// `buf` must be at least `FIXED_LEN` bytes, plus room for any tag that\n\
         /// will be appended. Writes the type byte, zeros every numeric field and the\n\
         /// appendage length field, and space-fills every alpha field.\n",
    );
    out.push_str("    pub fn new(buf: &'a mut [u8]) -> Self {\n");
    out.push_str(&format!(
        "        assert!(\n            buf.len() >= {fixed_len},\n            \"buf too short for {kind_name}Builder::new: need at least {fixed_len} bytes\"\n        );\n",
        kind_name = kind.name
    ));
    out.push_str(&format!("        buf[..{fixed_len}].fill(0);\n"));
    out.push_str(&format!("        buf[0] = {};\n", byte_literal(kind.byte)));
    out.push_str(&emit_alpha_space_fill(&kind.fields));
    out.push_str(&format!(
        "        Self {{\n            buf,\n            appendage_end: {fixed_len},\n        }}\n    }}\n\n"
    ));

    out.push_str(&emit_field_setters(&kind.fields, s.endianness, "self.buf"));

    for tag_name in &kind.appendage {
        out.push_str(&emit_append_method(s, kind, tag_name, fixed_len));
    }

    out.push_str("    /// total bytes written: `FIXED_LEN` plus every appended tag's bytes.\n");
    out.push_str("    pub fn finish(self) -> usize {\n        self.appendage_end\n    }\n");
    out.push_str("}\n\n");

    out
}

/// space-fill lines for a builder's `new()`: one `buf[range].fill(b' ')` per alpha
/// field, run after the whole-region zero-fill so no stale caller bytes survive.
fn emit_alpha_space_fill(fields: &[Field]) -> String {
    let mut out = String::new();
    let mut offset: u16 = 1;
    for field in fields {
        if field.class == TypeClass::Alpha {
            let end = offset + field.width;
            out.push_str(&format!("        buf[{offset}..{end}].fill(b' ');\n"));
        }
        offset += field.width;
    }
    out
}

/// one `#[inline] set_<name>` per field, offsets accumulated the same way as
/// `emit_field_accessors` so setter and accessor always agree on layout. `recv` is
/// the receiver expression indexing into the write buffer (`self.buf` for both
/// builder shapes).
fn emit_field_setters(fields: &[Field], endianness: Endianness, recv: &str) -> String {
    let mut out = String::new();
    let mut offset: u16 = 1;
    for field in fields {
        match field.class {
            TypeClass::Code => {
                out.push_str(&format!(
                    "    #[inline]\n    pub fn set_{0}(&mut self, v: u8) -> &mut Self {{\n        {recv}[{offset}] = v;\n        self\n    }}\n\n",
                    field.name
                ));
                offset += field.width;
            }
            TypeClass::Alpha => {
                let end = offset + field.width;
                let width = field.width;
                out.push_str(&format!(
                    "    #[inline]\n    pub fn set_{0}(&mut self, v: &[u8]) -> &mut Self {{\n        debug_assert!(v.len() <= {width}, \"{0} overflows its {width}-byte field\");\n        let n = v.len().min({width});\n        {recv}[{offset}..{offset} + n].copy_from_slice(&v[..n]);\n        {recv}[{offset} + n..{end}].fill(b' ');\n        self\n    }}\n\n",
                    field.name
                ));
                offset = end;
            }
            _ => {
                out.push_str(&emit_numeric_setter(field, offset, endianness, recv));
                offset += field.width;
            }
        }
    }
    out
}

/// one numeric field setter: `copy_from_slice(&v.to_be_bytes())` over the const slice
/// range, the write-side mirror of `emit_numeric_accessor`. `u48` is special-cased the
/// same way as its accessor: caller passes `u64`, only the low 6 bytes are written.
fn emit_numeric_setter(field: &Field, offset: u16, endianness: Endianness, recv: &str) -> String {
    let end = offset + field.width;
    let ty = numeric_type_name(field.class);
    let write_fn = match endianness {
        Endianness::Be => "to_be_bytes",
        Endianness::Le => "to_be_bytes", // NOTE: unreachable, see emit_numeric_accessor
    };
    let mut out = String::new();
    if field.class == TypeClass::U48 {
        out.push_str(&format!(
            "    #[inline]\n    pub fn set_{0}(&mut self, v: u64) -> &mut Self {{\n        debug_assert!(v <= 0x0000_ffff_ffff_ffff, \"{0} overflows its 48-bit field\");\n        {recv}[{offset}..{end}].copy_from_slice(&v.{write_fn}()[2..8]);\n        self\n    }}\n\n",
            field.name
        ));
    } else {
        out.push_str(&format!(
            "    #[inline]\n    pub fn set_{0}(&mut self, v: {ty}) -> &mut Self {{\n        {recv}[{offset}..{end}].copy_from_slice(&v.{write_fn}());\n        self\n    }}\n\n",
            field.name
        ));
    }
    out
}

/// one `#[inline] append_<tag>` method for a tag registered on this kind: writes the
/// length byte (`1 + value width`), tag byte, and value at the cursor, advances the
/// cursor, and rewrites the 2-byte appendage length field with the new tail length.
fn emit_append_method(s: &SchemaFile, kind: &Kind, tag_name: &str, fixed_len: u16) -> String {
    let tag = find_appendage_tag(s, tag_name);
    let param_ty = appendage_param_type(tag);
    let len_byte = 1u16 + tag.width; // tag byte + value bytes: the wire length field
    let elem_len = 2 + tag.width; // len byte + tag byte + value bytes: total on wire

    let mut out = String::new();
    if let Some(doc) = doc_text(&tag.doc) {
        out.push_str(&doc_lines(doc, "    "));
    }
    out.push_str(&format!(
        "    #[inline]\n    pub fn append_{tag_name}(&mut self, v: {param_ty}) -> &mut Self {{\n"
    ));
    out.push_str(&format!(
        "        assert!(\n            self.buf.len() >= self.appendage_end + {elem_len},\n            \"buf too short to append {tag_name} to {kind_name}\"\n        );\n",
        kind_name = kind.name
    ));
    out.push_str(&format!(
        "        self.buf[self.appendage_end] = {len_byte};\n"
    ));
    out.push_str(&format!(
        "        self.buf[self.appendage_end + 1] = {};\n",
        tag.tag
    ));
    out.push_str(&emit_append_value_write(tag));
    out.push_str(&format!("        self.appendage_end += {elem_len};\n"));
    out.push_str(&format!(
        "        let tail_len = (self.appendage_end - Self::FIXED_LEN) as u16;\n        self.buf[{}..{fixed_len}].copy_from_slice(&tail_len.to_be_bytes());\n",
        fixed_len - 2
    ));
    out.push_str("        self\n    }\n\n");
    out
}

/// value-bytes write for one appendage tag, at `self.appendage_end + 2` (past the
/// length and tag bytes already written by the caller).
fn emit_append_value_write(tag: &AppendageTag) -> String {
    let start = "self.appendage_end + 2";
    match tag.class {
        TypeClass::Alpha => format!(
            "        self.buf[{start}..{start} + {}].copy_from_slice(v);\n",
            tag.width
        ),
        TypeClass::Code => format!("        self.buf[{start}] = v;\n"),
        TypeClass::U48 => format!(
            "        debug_assert!(v <= 0x0000_ffff_ffff_ffff, \"{} overflows its 48-bit field\");\n        self.buf[{start}..{start} + 6].copy_from_slice(&v.to_be_bytes()[2..8]);\n",
            tag.name
        ),
        _ => format!(
            "        self.buf[{start}..{start} + {}].copy_from_slice(&v.to_be_bytes());\n",
            tag.width
        ),
    }
}

/// Rust parameter type for one registry tag's `append_<tag>` setter value: alpha
/// takes a borrowed fixed-size array (no lifetime needed, only borrowed for the
/// call), `code` takes `u8`, everything else via `numeric_type_name`.
fn appendage_param_type(tag: &AppendageTag) -> String {
    match tag.class {
        TypeClass::Alpha => format!("&[u8; {}]", tag.width),
        TypeClass::Code => "u8".to_string(),
        TypeClass::U48 => "u64".to_string(),
        other => numeric_type_name(other).to_string(),
    }
}
