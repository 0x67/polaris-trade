use super::*;

// --- Rust emitters (workstream B1) -----------------------------------------------

pub(super) fn emit_rust_mod(s: &RecordSchema, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str(
        "//! Generated normalized-record data types. Do not edit; see schemas/records/.\n\n",
    );

    out.push_str("pub mod bands;\n");
    out.push_str("pub mod enums;\n");
    out.push_str("pub mod header;\n");
    out.push_str("pub mod records;\n");
    if !s.structs.is_empty() {
        out.push_str("pub mod structs;\n");
    }
    out.push('\n');

    let mut enums: Vec<&str> = s.enums.iter().map(|e| e.name.as_str()).collect();
    enums.sort_unstable();
    out.push_str(&format!("pub use enums::{{{}}};\n", enums.join(", ")));
    out.push_str(&format!("pub use header::{};\n", s.header.name));
    let mut recs: Vec<&str> = s.records.iter().map(|r| r.name.as_str()).collect();
    recs.sort_unstable();
    out.push_str(&format!("pub use records::{{{}}};\n", recs.join(", ")));
    out.push_str("pub use bands::{Category, REGISTERED, rtype};\n");
    if !s.structs.is_empty() {
        let mut structs: Vec<&str> = s.structs.iter().map(|st| st.name.as_str()).collect();
        structs.sort_unstable();
        out.push_str(&format!("pub use structs::{{{}}};\n", structs.join(", ")));
    }
    out
}

pub(super) fn emit_rust_enums(s: &RecordSchema, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str("//! Typed field enums. Generated; see schemas/records/.\n\n");
    for e in &s.enums {
        if let Some(d) = doc_text(&e.doc) {
            out.push_str(&doc_lines(d, ""));
        }
        let repr = match e.repr {
            EnumRepr::U8 => "u8",
            EnumRepr::U16 => "u16",
        };
        out.push_str(&format!("#[repr({repr})]\n"));
        if e.default.is_some() {
            out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]\n");
        } else {
            out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]\n");
        }
        out.push_str(&format!("pub enum {} {{\n", e.name));
        for v in &e.variants {
            if let Some(d) = doc_text(&v.doc) {
                out.push_str(&doc_lines(d, "    "));
            }
            if e.default.as_deref() == Some(v.name.as_str()) {
                out.push_str("    #[default]\n");
            }
            let value = match e.repr {
                EnumRepr::U8 => v.value.to_string(),
                EnumRepr::U16 => format!("{:#06X}", v.value),
            };
            out.push_str(&format!("    {} = {value},\n", v.name));
        }
        out.push_str("}\n\n");
    }
    out
}

/// shared struct emitter for the header and nested structs (no `hdr` prefix).
fn emit_rust_plain_struct(def: &StructDef, layout: &Layout, out: &mut String) {
    if let Some(d) = doc_text(&def.doc) {
        out.push_str(&doc_lines(d, ""));
    }
    out.push_str("#[repr(C)]\n");
    if def.derive_default {
        out.push_str("#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]\n");
    } else {
        out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq)]\n");
    }
    out.push_str(&format!("pub struct {} {{\n", def.name));
    emit_rust_fields(layout, out);
    out.push_str("}\n");
    out.push_str(&size_assert(&def.name, layout.size));
}

pub(super) fn emit_rust_header(s: &RecordSchema, layouts: &Layouts, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str("//! The shared record header. Generated; see schemas/records/.\n\n");
    let (enums, structs, has_flags) = refs_of([s.header.fields.as_slice()]);
    if has_flags {
        out.push_str("use crate::flags::RecordFlags;\n");
    }
    if !enums.is_empty() {
        out.push_str(&format!("use super::enums::{{{}}};\n", enums.join(", ")));
    }
    if !structs.is_empty() {
        out.push_str(&format!(
            "use super::structs::{{{}}};\n",
            structs.join(", ")
        ));
    }
    out.push('\n');
    emit_rust_plain_struct(&s.header, &layouts.header, &mut out);
    out
}

pub(super) fn emit_rust_structs(s: &RecordSchema, layouts: &Layouts, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str("//! Nested record structs. Generated; see schemas/records/.\n\n");
    let (enums, _structs, has_flags) = refs_of(s.structs.iter().map(|st| st.fields.as_slice()));
    if has_flags {
        out.push_str("use crate::flags::RecordFlags;\n");
    }
    if !enums.is_empty() {
        out.push_str(&format!("use super::enums::{{{}}};\n", enums.join(", ")));
    }
    if has_flags || !enums.is_empty() {
        out.push('\n');
    }
    for st in &s.structs {
        emit_rust_plain_struct(st, &layouts.structs[&st.name], &mut out);
        out.push('\n');
    }
    // drop the trailing blank line for byte-stable output.
    if out.ends_with("\n\n") {
        out.pop();
    }
    out
}

pub(super) fn emit_rust_records(s: &RecordSchema, layouts: &Layouts, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str("//! Header-carrying records. Generated; see schemas/records/.\n\n");

    let (enums, structs, _has_flags) = refs_of(s.records.iter().map(|r| r.fields.as_slice()));
    out.push_str("use crate::RecordHeader;\n");
    if !enums.is_empty() {
        out.push_str(&format!("use super::enums::{{{}}};\n", enums.join(", ")));
    }
    if !structs.is_empty() {
        out.push_str(&format!(
            "use super::structs::{{{}}};\n",
            structs.join(", ")
        ));
    }
    out.push('\n');

    for r in &s.records {
        if let Some(d) = doc_text(&r.doc) {
            out.push_str(&doc_lines(d, ""));
        }
        out.push_str("#[repr(C)]\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\n");
        out.push_str(&format!("pub struct {} {{\n", r.name));
        out.push_str("    pub hdr: RecordHeader,\n");
        emit_rust_fields(&layouts.records[&r.name], &mut out);
        out.push_str("}\n");
        out.push_str(&size_assert(&r.name, layouts.records[&r.name].size));
        out.push('\n');
    }
    if out.ends_with("\n\n") {
        out.pop();
    }
    out
}

pub(super) fn emit_rust_rtype(s: &RecordSchema, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str(
        "//! rtype bytes, category bands, and Record impls. Generated; see schemas/records/.\n\n",
    );

    let mut recs: Vec<&crate::record_schema::RecordDef> = s.records.iter().collect();
    recs.sort_by_key(|r| r.rtype);

    let mut names: Vec<&str> = s.records.iter().map(|r| r.name.as_str()).collect();
    names.sort_unstable();
    out.push_str("use crate::{Record, RecordHeader};\n");
    out.push_str(&format!(
        "use super::records::{{{}}};\n\n",
        names.join(", ")
    ));

    out.push_str("/// rtype byte assignments, allocated from the reserved category bands.\n");
    out.push_str("pub mod rtype {\n");
    for r in &recs {
        out.push_str(&format!("    /// rtype byte for [`super::{}`].\n", r.name));
        out.push_str(&format!(
            "    pub const {}: u8 = {:#04x};\n",
            const_name(&r.name),
            r.rtype
        ));
    }
    out.push_str("}\n\n");

    out.push_str(
        "/// Reserved rtype category. Bands are fixed so every future record family is an\n\
         /// additive allocation, never a layout break.\n\
         #[derive(Clone, Copy, Debug, PartialEq, Eq)]\n\
         pub enum Category {\n\
         \x20   Control,\n\
         \x20   BookTape,\n\
         \x20   Stats,\n\
         \x20   Reference,\n\
         \x20   ExecReserved,\n\
         \x20   QuoteRfqReserved,\n\
         \x20   AmmReserved,\n\
         \x20   Unassigned,\n\
         }\n\n\
         impl Category {\n\
         \x20   /// Category owning `rt`.\n\
         \x20   pub const fn of_rtype(rt: u8) -> Category {\n\
         \x20       match rt {\n\
         \x20           0x00..=0x0F => Category::Control,\n\
         \x20           0x10..=0x2F => Category::BookTape,\n\
         \x20           0x30..=0x3F => Category::Stats,\n\
         \x20           0x40..=0x4F => Category::Reference,\n\
         \x20           0x50..=0x5F => Category::ExecReserved,\n\
         \x20           0x60..=0x6F => Category::QuoteRfqReserved,\n\
         \x20           0x70..=0x7F => Category::AmmReserved,\n\
         \x20           _ => Category::Unassigned,\n\
         \x20       }\n\
         \x20   }\n\
         }\n\n",
    );

    out.push_str("/// Every rtype this crate defines, with the category it must fall in.\n");
    out.push_str("pub const REGISTERED: &[(u8, Category)] = &[\n");
    for r in &recs {
        out.push_str(&format!(
            "    (rtype::{}, Category::{}),\n",
            const_name(&r.name),
            band_name(r.band)
        ));
    }
    out.push_str("];\n\n");

    for r in &recs {
        out.push_str(&format!(
            "impl Record for {} {{\n    const RTYPE: u8 = rtype::{};\n    fn header(&self) -> &RecordHeader {{\n        &self.hdr\n    }}\n}}\n",
            r.name,
            const_name(&r.name)
        ));
    }
    out
}
