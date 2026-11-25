//! Dispatch emission: `HotVisitor` trait, per-kind thunks, match and LUT dispatch.

use super::{doc_lines, doc_text, naming::byte_literal, recv_kinds};
use crate::schema::{Kind, SchemaFile, visitor_method};

/// `dispatch.rs` body: `HotVisitor` trait, per-kind thunks, `parse_match`, `build_lut`/`parse_lut`.
/// Shape mirrors `itch-parse-spike/crates/itch-v1/src/dispatch.rs`.
pub(super) fn emit_dispatch(s: &SchemaFile, header: &str) -> String {
    let mut out = header.to_string();
    out.push_str(
        "//! Dispatch shapes over the view-safe accessors: match-on-byte vs a function\n\
         //! lookup table, both behind one visitor trait.\n\
         //!\n\
         //! `parse_lut`'s table is baked per-`V` via an associated const (`LutTable`\n\
         //! below), not rebuilt per call: a `static` cannot depend on a generic\n\
         //! parameter and a fn-local `const` cannot reference an enclosing generic\n\
         //! either, but an associated const on a blanket impl can depend on `Self`.\n\n",
    );

    let recv = recv_kinds(s);
    let fixed: Vec<&Kind> = recv.iter().copied().filter(|k| !k.variable).collect();
    let variable: Vec<&Kind> = recv.iter().copied().filter(|k| k.variable).collect();

    if !fixed.is_empty() {
        let view_names: Vec<String> = fixed.iter().map(|k| format!("{}View", k.name)).collect();
        out.push_str(&format!(
            "use super::view::{{{}}};\n\n",
            view_names.join(", ")
        ));
    }

    out.push_str(
        "/// Per-kind hot-path callbacks: fixed kinds use view-safe accessors;\n\
         /// variable kinds borrow the complete wire message. `cold` catches bytes without a\n\
         /// dedicated method (default: no-op; override to count/inspect unimplemented\n\
         /// or unknown kinds).\n",
    );
    out.push_str("pub trait HotVisitor {\n");
    for kind in &fixed {
        let method = visitor_method(&kind.name);
        if let Some(doc) = doc_text(&kind.doc) {
            out.push_str(&doc_lines(doc, "    "));
        }
        out.push_str(&format!(
            "    fn {method}(&mut self, v: {}View<'_>);\n",
            kind.name
        ));
    }
    for kind in &variable {
        let method = visitor_method(&kind.name);
        if let Some(doc) = doc_text(&kind.doc) {
            out.push_str(&doc_lines(doc, "    "));
        }
        out.push_str(&format!("    fn {method}(&mut self, _data: &[u8]) {{}}\n"));
    }
    out.push_str("\n    fn cold(&mut self, _msg_type: u8, _data: &[u8]) {}\n}\n\n");

    let has_appendage = fixed.iter().any(|k| !k.appendage.is_empty());
    if has_appendage {
        out.push_str(
            "// per-kind thunks: construct view-safe accessor, hand to visitor. Exact-length\n\
             // kinds: `data` is pre-sliced to exact BYTE_LEN, so `parse` can't fail. Appendage\n\
             // kinds: `parse` returns `None` on a length/length-field mismatch, routed to `cold`.\n",
        );
    } else {
        out.push_str(
            "// per-kind thunks: construct view-safe accessor, hand to visitor. `data` is\n\
             // pre-sliced to exact BYTE_LEN by the caller, so `parse`'s check can't fail.\n",
        );
    }
    for kind in &fixed {
        let method = visitor_method(&kind.name);
        if kind.appendage.is_empty() {
            out.push_str(&format!(
                "fn dispatch_{method}<V: HotVisitor>(data: &[u8], v: &mut V) {{\n    v.{method}({}View::parse(data).expect(\"caller pre-slices exact BYTE_LEN bytes\"));\n}}\n\n",
                kind.name
            ));
        } else {
            out.push_str(&format!(
                "fn dispatch_{method}<V: HotVisitor>(data: &[u8], v: &mut V) {{\n    match {}View::parse(data) {{\n        Some(view) => v.{method}(view),\n        None => v.cold({}, data),\n    }}\n}}\n\n",
                kind.name,
                byte_literal(kind.byte)
            ));
        }
    }
    for kind in &variable {
        let method = visitor_method(&kind.name);
        out.push_str(&format!(
            "fn dispatch_{method}<V: HotVisitor>(data: &[u8], v: &mut V) {{\n    v.{method}(data);\n}}\n\n"
        ));
    }

    out.push_str(
        "/// Match-on-byte dispatch. Fixed-kind `data` must match `BYTE_LEN`; variable-kind\n\
         /// `data` is the caller-validated complete message. Unimplemented or unknown kinds\n\
         /// go to `HotVisitor::cold`.\n",
    );
    out.push_str("pub fn parse_match<V: HotVisitor>(msg_type: u8, data: &[u8], v: &mut V) {\n    match msg_type {\n");
    for kind in &fixed {
        let method = visitor_method(&kind.name);
        out.push_str(&format!(
            "        {} => dispatch_{method}(data, v),\n",
            byte_literal(kind.byte)
        ));
    }
    for kind in &variable {
        let method = visitor_method(&kind.name);
        out.push_str(&format!(
            "        {} => dispatch_{method}(data, v),\n",
            byte_literal(kind.byte)
        ));
    }
    out.push_str("        _ => v.cold(msg_type, data),\n    }\n}\n\n");

    out.push_str(
        "type DispatchFn<V> = fn(&[u8], &mut V);\ntype DispatchTable<V> = [Option<DispatchFn<V>>; 256];\n\n",
    );

    out.push_str(
        "/// per-`V` 256-entry function table. `None` entries fall to `HotVisitor::cold`.\n",
    );
    out.push_str("pub const fn build_lut<V: HotVisitor>() -> DispatchTable<V> {\n    let mut table: DispatchTable<V> = [None; 256];\n");
    for kind in &fixed {
        let method = visitor_method(&kind.name);
        out.push_str(&format!(
            "    table[{} as usize] = Some(dispatch_{method}::<V>);\n",
            byte_literal(kind.byte)
        ));
    }
    for kind in &variable {
        let method = visitor_method(&kind.name);
        out.push_str(&format!(
            "    table[{} as usize] = Some(dispatch_{method}::<V>);\n",
            byte_literal(kind.byte)
        ));
    }
    out.push_str("    table\n}\n\n");

    out.push_str(
        "// sealed: hangs a per-`Self` associated const off `build_lut`. See module\n\
         // doc for why this needs a trait, not a static or a fn-local const.\n",
    );
    out.push_str("trait LutTable: HotVisitor + Sized {\n    const TABLE: DispatchTable<Self> = build_lut::<Self>();\n}\nimpl<V: HotVisitor> LutTable for V {}\n\n");

    out.push_str("/// lookup-table dispatch, same contract as `parse_match`.\n");
    out.push_str("pub fn parse_lut<V: HotVisitor>(msg_type: u8, data: &[u8], v: &mut V) {\n    match V::TABLE[msg_type as usize] {\n        Some(f) => f(data, v),\n        None => v.cold(msg_type, data),\n    }\n}\n");

    out
}
