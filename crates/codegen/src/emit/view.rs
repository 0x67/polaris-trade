//! View-safe accessor emission: `view/mod.rs` wiring plus per-kind view structs.

use super::{doc_lines, doc_text, naming::numeric_type_name, recv_kinds};
use crate::schema::{Endianness, Field, Kind, SchemaFile, TypeClass, visitor_method};

/// `view/mod.rs` body: provenance header, the view-safe intro doc (moved verbatim from
/// the pre-split single-file `view.rs`), then per fixed kind in schema order a private
/// `mod <method>;` decl and a `pub use <method>::<Name>View;` re-export, so
/// `<ns>::view::<Name>View` resolves exactly as it did before the split.
pub(super) fn emit_view_mod(s: &SchemaFile, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! View-safe accessors over `&'a [u8; N]`: one length check at `parse`,\n\
         //! `#[inline]` const-index reads, zero `unsafe`. Coded single-byte fields\n\
         //! stay raw `u8`, alpha fields return a borrowed untrimmed sub-slice.\n\n",
    );

    let recv = recv_kinds(s);
    let fixed: Vec<&Kind> = recv.iter().copied().filter(|k| !k.variable).collect();

    for kind in &fixed {
        out.push_str(&format!("mod {};\n", visitor_method(&kind.name)));
    }
    out.push('\n');
    for kind in &fixed {
        out.push_str(&format!(
            "pub use {}::{}View;\n",
            visitor_method(&kind.name),
            kind.name
        ));
    }

    out
}

/// `view/<method>.rs` body: one `<Name>View<'a>` struct/impl with `#[inline]` field
/// accessors, self-contained (own `WrongLength` import). Transcribed byte-exact from
/// the pre-split per-kind emission. Shape mirrors
/// `itch-parse-spike/crates/itch-v1/src/view.rs::AddOrderView`.
pub(super) fn emit_view_kind(kind: &Kind, endianness: Endianness, header: &str) -> String {
    if kind.appendage.is_empty() {
        emit_view_kind_fixed(kind, endianness, header)
    } else {
        emit_view_kind_appendage(kind, endianness, header)
    }
}

/// `view/<method>.rs` body for a kind with no appendage: unchanged shape from before
/// direction/appendage support landed, byte-identical for every directionless schema.
fn emit_view_kind_fixed(kind: &Kind, endianness: Endianness, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str("use proto_common::WrongLength;\n\n");

    // validate() guarantees fixed kinds carry byte_len; 0 fallback never reached.
    let size = kind.byte_len.unwrap_or(0);

    out.push_str(&format!(
        "/// `{}` {}, {size}B.\n",
        kind.byte as char, kind.name
    ));
    if let Some(doc) = doc_text(&kind.doc) {
        out.push_str("///\n");
        out.push_str(&doc_lines(doc, ""));
    }
    out.push_str(&format!(
        "pub struct {}View<'a>(&'a [u8; {size}]);\n\n",
        kind.name
    ));
    out.push_str(&format!("impl<'a> {}View<'a> {{\n", kind.name));
    out.push_str(&format!("    pub const SIZE: usize = {size};\n\n"));
    out.push_str("    #[inline]\n");
    out.push_str("    pub fn parse(data: &'a [u8]) -> Result<Self, WrongLength> {\n");
    out.push_str("        data.try_into().map(Self).map_err(|_| WrongLength {\n");
    out.push_str("            expected: Self::SIZE,\n");
    out.push_str("            got: data.len(),\n");
    out.push_str("        })\n    }\n\n");

    out.push_str(&emit_field_accessors(&kind.fields, endianness, "self.0"));

    out.push_str("}\n\n");

    out
}

/// `view/<method>.rs` body for a kind carrying a trailing appendage: fixed/tail split
/// so field accessors stay const-offset reads on `&[u8; N]` (RULE-1) while the tail is
/// borrowed and dynamically sized. `N` excludes the synthesized 2-byte appendage length
/// field so a dual-form kind's bare wire form (no length field at all) still borrows.
fn emit_view_kind_appendage(kind: &Kind, endianness: Endianness, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str("use proto_common::appendage::AppendageIter;\n\n");
    out.push_str("use super::super::appendage::TagIter;\n\n");

    // type byte + declared field widths, excluding the synthesized 2-byte
    // appendage length field (kept out of the const-sized fixed array).
    let fixed_len: u16 = 1 + kind.fields.iter().map(|f| f.width).sum::<u16>();

    out.push_str(&format!(
        "/// `{}` {}, {fixed_len}B fixed prefix plus a trailing appendage.\n",
        kind.byte as char, kind.name
    ));
    if let Some(doc) = doc_text(&kind.doc) {
        out.push_str("///\n");
        out.push_str(&doc_lines(doc, ""));
    }
    if kind.appendage_optional {
        out.push_str(
            "///\n/// Dual-form wire: also accepts a bare message with the appendage\n\
             /// length field absent entirely, read as an empty appendage.\n",
        );
    }
    out.push_str(&format!(
        "pub struct {}View<'a> {{\n    fixed: &'a [u8; {fixed_len}],\n    tail: &'a [u8],\n}}\n\n",
        kind.name
    ));

    out.push_str(&format!("impl<'a> {}View<'a> {{\n", kind.name));
    out.push_str(&format!(
        "    pub const FIXED_LEN: usize = {fixed_len};\n\n"
    ));
    out.push_str(
        "    /// `data` must be at least `FIXED_LEN` bytes. When more bytes follow, the\n",
    );
    out.push_str("    /// next 2 (big-endian) must equal the remaining tail length. `None`\n");
    out.push_str("    /// on any length or length-field mismatch.\n");
    out.push_str("    #[inline]\n");
    out.push_str("    pub fn parse(data: &'a [u8]) -> Option<Self> {\n");
    out.push_str(&format!(
        "        if data.len() < {fixed_len} {{\n            return None;\n        }}\n"
    ));
    out.push_str(&format!(
        "        let (fixed_bytes, rest) = data.split_at({fixed_len});\n"
    ));
    out.push_str("        // fixed-length split above guarantees this conversion succeeds\n");
    out.push_str(&format!(
        "        let fixed: &'a [u8; {fixed_len}] = fixed_bytes.try_into().unwrap();\n"
    ));
    if kind.appendage_optional {
        out.push_str(
            "        if rest.is_empty() {\n            return Some(Self { fixed, tail: &[] });\n        }\n",
        );
    }
    out.push_str("        if rest.len() < 2 {\n            return None;\n        }\n");
    out.push_str("        let len_field = u16::from_be_bytes([rest[0], rest[1]]);\n");
    out.push_str("        let tail = &rest[2..];\n");
    out.push_str(
        "        if len_field as usize != tail.len() {\n            return None;\n        }\n",
    );
    out.push_str("        Some(Self { fixed, tail })\n    }\n\n");

    out.push_str(&emit_field_accessors(
        &kind.fields,
        endianness,
        "self.fixed",
    ));

    out.push_str("    /// raw walk over the tail: `(tag, value bytes)` per element, stops\n");
    out.push_str("    /// cleanly on a truncated element, never panics.\n");
    out.push_str("    #[inline]\n");
    out.push_str("    pub fn appendage(&self) -> AppendageIter<'a> {\n");
    out.push_str("        AppendageIter::new(self.tail)\n    }\n\n");

    out.push_str("    /// typed walk over the tail via the schema's tag registry.\n");
    out.push_str("    #[inline]\n");
    out.push_str("    pub fn tags(&self) -> TagIter<'a> {\n");
    out.push_str("        TagIter::new(self.tail)\n    }\n");
    out.push_str("}\n\n");

    out
}

/// per-field accessor block, offsets starting after the type byte at index 0. `recv`
/// is the receiver expression indexing into the byte array (`self.0` for a plain
/// tuple-struct view, `self.fixed` for an appendage view's fixed prefix).
fn emit_field_accessors(fields: &[Field], endianness: Endianness, recv: &str) -> String {
    let mut out = String::new();
    let mut offset: u16 = 1;
    for field in fields {
        match field.class {
            TypeClass::Code => {
                if let Some(doc) = doc_text(&field.doc) {
                    out.push_str(&doc_lines(doc, "    "));
                }
                out.push_str(&format!(
                    "    #[inline]\n    pub fn {}(&self) -> u8 {{\n        {recv}[{offset}]\n    }}\n\n",
                    field.name
                ));
                offset += field.width;
            }
            TypeClass::Alpha => {
                let end = offset + field.width;
                if let Some(doc) = doc_text(&field.doc) {
                    out.push_str(&doc_lines(doc, "    "));
                }
                out.push_str(&format!(
                    "    #[inline]\n    pub fn {}(&self) -> &'a [u8] {{\n        &{recv}[{offset}..{end}]\n    }}\n\n",
                    field.name
                ));
                offset = end;
            }
            _ => {
                out.push_str(&emit_numeric_accessor(field, offset, endianness, recv));
                offset += field.width;
            }
        }
    }
    out
}

/// one numeric field accessor: `<class>::from_be_bytes` over the const slice range.
/// `generate()` rejects `Endianness::Le` before any emitter runs, so the `Le` arm
/// below is unreachable in practice; kept only so the match stays exhaustive. `recv`
/// is the receiver expression indexing into the byte array (see `emit_field_accessors`).
fn emit_numeric_accessor(field: &Field, offset: u16, endianness: Endianness, recv: &str) -> String {
    let end = offset + field.width;
    let ty = numeric_type_name(field.class);
    let read_fn = match endianness {
        Endianness::Be => "from_be_bytes",
        Endianness::Le => "from_be_bytes", // NOTE: unreachable, see doc comment above
    };
    let mut out = String::new();
    if let Some(doc) = doc_text(&field.doc) {
        out.push_str(&doc_lines(doc, "    "));
    }
    if field.class == TypeClass::U48 {
        // 6 wire bytes cannot ride u64::from_be_bytes directly: zero-extend
        // through a padded stack buffer. two moves, no branch, no alloc.
        out.push_str(&format!(
            "    #[inline]\n    pub fn {}(&self) -> u64 {{\n        let mut buf = [0u8; 8];\n        buf[2..8].copy_from_slice(&{recv}[{offset}..{end}]);\n        u64::{read_fn}(buf)\n    }}\n\n",
            field.name
        ));
    } else {
        out.push_str(&format!(
            "    #[inline]\n    pub fn {}(&self) -> {ty} {{\n        {ty}::{read_fn}({recv}[{offset}..{end}].try_into().unwrap())\n    }}\n\n",
            field.name
        ));
    }
    out
}
