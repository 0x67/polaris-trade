//! Record-schema emission. Pure: `RecordSchema` in, generated Rust + C header text
//! out; no IO (cli.rs owns writes). Mirrors `emit.rs` discipline and reuses its
//! provenance header and doc helpers.
//!
//! Rust output lands under `crates/events/src/generated/`; the C header at
//! `crates/events/include/md_events.h`. Generated records reference `crate::RecordHeader`
//! and `crate::Record` (the `md-events` re-exports), so the tree compiles alongside the
//! hand-written types during the transition and again once the swap re-points those
//! re-exports at this generated tree.

use std::{collections::BTreeSet, path::PathBuf};

use sha2::{Digest, Sha256};

use crate::{
    emit::{GeneratedFile, doc_lines, doc_text, provenance_header},
    record_schema::{
        Category, EnumRepr, FieldClass, Layout, Layouts, RecordField, RecordSchema,
        RecordSchemaError, Slot, StructDef,
    },
    schema::visitor_method,
};

mod c_header;
mod rust_emit;

use c_header::emit_c_header;
use rust_emit::{
    emit_rust_enums, emit_rust_header, emit_rust_mod, emit_rust_records, emit_rust_rtype,
    emit_rust_structs,
};

/// generator-owned Rust output directory, relative to repo root. The record pass
/// orphan-prunes `.rs` files under this dir; the CLI passes it to `find_orphans`.
pub const GEN_DIR: &str = "crates/events/src/generated";
/// generated C header, relative to repo root.
const C_HEADER: &str = "crates/events/include/md_events.h";

/// generate every file the record schema produces: the Rust `generated/` tree plus the
/// C header. Pure (validates, then emits text, no IO). `schema_rel_path` and
/// `schema_bytes` feed the shared provenance header.
pub fn generate_records(
    s: &RecordSchema,
    schema_rel_path: &str,
    schema_bytes: &[u8],
) -> Result<Vec<GeneratedFile>, RecordSchemaError> {
    s.validate()?;
    let layouts = s.layouts()?;
    let prov = provenance_header(schema_rel_path, &sha256_hex(schema_bytes));
    let gen_dir = PathBuf::from(GEN_DIR);

    Ok(vec![
        GeneratedFile {
            rel_path: gen_dir.join("mod.rs"),
            contents: emit_rust_mod(s, &prov),
        },
        GeneratedFile {
            rel_path: gen_dir.join("enums.rs"),
            contents: emit_rust_enums(s, &prov),
        },
        GeneratedFile {
            rel_path: gen_dir.join("header.rs"),
            contents: emit_rust_header(s, &layouts, &prov),
        },
        GeneratedFile {
            rel_path: gen_dir.join("structs.rs"),
            contents: emit_rust_structs(s, &layouts, &prov),
        },
        GeneratedFile {
            rel_path: gen_dir.join("records.rs"),
            contents: emit_rust_records(s, &layouts, &prov),
        },
        GeneratedFile {
            rel_path: gen_dir.join("bands.rs"),
            contents: emit_rust_rtype(s, &prov),
        },
        GeneratedFile {
            rel_path: PathBuf::from(C_HEADER),
            contents: emit_c_header(s, &layouts, &prov),
        },
    ])
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// SCREAMING_SNAKE rtype constant name for a record (`TradeBreak` -> `TRADE_BREAK`).
fn const_name(record: &str) -> String {
    visitor_method(record).to_uppercase()
}

fn band_name(c: Category) -> &'static str {
    match c {
        Category::Control => "Control",
        Category::BookTape => "BookTape",
        Category::Stats => "Stats",
        Category::Reference => "Reference",
        Category::ExecReserved => "ExecReserved",
        Category::QuoteRfqReserved => "QuoteRfqReserved",
        Category::AmmReserved => "AmmReserved",
        Category::Unassigned => "Unassigned",
    }
}

// --- Rust type mapping -----------------------------------------------------------

/// Rust field type for a class. `price` is `i64`; `flags` is the hand-written
/// `RecordFlags`; a nested struct/enum is its own name.
fn rust_type(c: &FieldClass) -> String {
    match c {
        FieldClass::U8 => "u8".to_string(),
        FieldClass::U16 => "u16".to_string(),
        FieldClass::U32 => "u32".to_string(),
        FieldClass::U64 => "u64".to_string(),
        FieldClass::I8 => "i8".to_string(),
        FieldClass::I16 => "i16".to_string(),
        FieldClass::I32 => "i32".to_string(),
        FieldClass::I64 | FieldClass::Price => "i64".to_string(),
        FieldClass::Flags => "RecordFlags".to_string(),
        FieldClass::Bytes { width } => format!("[u8; {width}]"),
        FieldClass::Enum { ty } | FieldClass::Struct { ty } => ty.clone(),
        FieldClass::Array { elem, count } => format!("[{}; {count}]", rust_type(elem)),
    }
}

/// accumulate the enum names, struct names, and whether `flags` appears, referenced by a
/// field class (recursing through arrays), for import generation.
fn collect_refs(
    c: &FieldClass,
    enums: &mut BTreeSet<String>,
    structs: &mut BTreeSet<String>,
    flags: &mut bool,
) {
    match c {
        FieldClass::Enum { ty } => {
            enums.insert(ty.clone());
        }
        FieldClass::Struct { ty } => {
            structs.insert(ty.clone());
        }
        FieldClass::Flags => *flags = true,
        FieldClass::Array { elem, .. } => collect_refs(elem, enums, structs, flags),
        _ => {}
    }
}

fn refs_of<'a>(
    field_lists: impl IntoIterator<Item = &'a [RecordField]>,
) -> (Vec<String>, Vec<String>, bool) {
    let mut enums = BTreeSet::new();
    let mut structs = BTreeSet::new();
    let mut flags = false;
    for fields in field_lists {
        for f in fields {
            collect_refs(&f.class, &mut enums, &mut structs, &mut flags);
        }
    }
    (
        enums.into_iter().collect(),
        structs.into_iter().collect(),
        flags,
    )
}

fn size_assert(name: &str, size: usize) -> String {
    format!("const _: () = assert!(core::mem::size_of::<{name}>() == {size});\n")
}

/// emit a laid-out field list (fields plus generator pads) as struct body lines.
fn emit_rust_fields(layout: &Layout, out: &mut String) {
    for slot in &layout.slots {
        match slot {
            Slot::Field {
                name, class, doc, ..
            } => {
                if let Some(d) = doc_text(doc) {
                    out.push_str(&doc_lines(d, "    "));
                }
                out.push_str(&format!("    pub {name}: {},\n", rust_type(class)));
            }
            Slot::Pad { name, width, .. } => {
                out.push_str(
                    "    /// Generator-inserted alignment pad; zeroed by every constructor so\n\
                     \x20   /// verbatim serialization never ships uninitialized bytes.\n",
                );
                out.push_str(&format!("    pub {name}: [u8; {width}],\n"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generated() -> Vec<GeneratedFile> {
        let path = crate::cli::repo_root().join("schemas/records/l0_l3.yaml");
        let bytes = std::fs::read(&path).expect("read committed schema");
        let s = RecordSchema::load(&path).expect("load committed schema");
        generate_records(&s, "schemas/records/l0_l3.yaml", &bytes).expect("generate")
    }

    fn body<'a>(files: &'a [GeneratedFile], suffix: &str) -> &'a str {
        files
            .iter()
            .find(|f| f.rel_path.ends_with(suffix))
            .map(|f| f.contents.as_str())
            .unwrap_or_else(|| panic!("generated {suffix}"))
    }

    // The generated Mbp10 carries the interior `_pad: [u8; 5]` and it sits before the
    // `levels` array, reproducing the committed interior-before-aggregate layout.
    #[test]
    fn mbp10_interior_pad() {
        let files = generated();
        let records = body(&files, "records.rs");
        let mbp10 = records
            .split("pub struct Mbp10")
            .nth(1)
            .expect("Mbp10 struct present");
        let pad = mbp10.find("pub _pad: [u8; 5],").expect("interior pad");
        let levels = mbp10
            .find("pub levels: [BidAskPair; 10],")
            .expect("levels array");
        assert!(pad < levels, "interior pad must precede levels");
        assert!(records.contains("const _: () = assert!(core::mem::size_of::<Mbp10>() == 480);"));
    }

    // Enums emit the right repr, explicit discriminants (ASCII Side, sparse StatType),
    // and Default plus #[default] only where the schema marks it.
    #[test]
    fn enums_repr_and_discriminants() {
        let enums = generated();
        let enums = body(&enums, "enums.rs");
        assert!(enums.contains("#[repr(u8)]"));
        assert!(enums.contains("Bid = 66,"));
        assert!(enums.contains("#[repr(u16)]"));
        assert!(enums.contains("VenueSpecific = 0xFF00,"));
        assert!(enums.contains("#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]"));
        assert!(enums.contains("    #[default]\n    Equity = 0,"));
    }

    // rtype module, Category + of_rtype, REGISTERED table, and one Record impl per record
    // are reproduced.
    #[test]
    fn rtype_surface() {
        let rtype = generated();
        let rtype = body(&rtype, "bands.rs");
        assert!(rtype.contains("pub const MBO: u8 = 0x10;"));
        assert!(rtype.contains("pub const TRADE_BREAK: u8 = 0x15;"));
        assert!(rtype.contains("pub const fn of_rtype(rt: u8) -> Category"));
        assert!(rtype.contains("(rtype::MBO, Category::BookTape),"));
        assert!(rtype.contains("impl Record for Mbo {\n    const RTYPE: u8 = rtype::MBO;"));
    }

    // The C header carries width-exact enum typedefs, flag #defines, and a
    // _Static_assert per struct/record pinning the size.
    #[test]
    fn c_header_static_assert() {
        let files = generated();
        let header = body(&files, "md_events.h");
        assert!(header.contains("typedef uint8_t Side;"));
        assert!(header.contains("Side_Bid = 66,"));
        assert!(header.contains("typedef uint16_t StatType;"));
        assert!(header.contains("#define MD_FLAG_SNAPSHOT 0x0001"));
        assert!(header.contains("#define MD_FLAG_SUSPECT 0x0040"));
        assert!(header.contains("MD_STATIC_ASSERT(sizeof(RecordHeader) == 56"));
        assert!(header.contains("MD_STATIC_ASSERT(sizeof(Mbp10) == 480"));
        assert!(header.contains("uint8_t _pad[5];"));
        assert!(header.contains("BidAskPair levels[10];"));
        // C++ consumability: extern "C" guard, and no system includes so header-parsing
        // binding tools stay clean.
        assert!(header.contains("#ifdef __cplusplus\nextern \"C\" {"));
        assert!(!header.contains("#include <assert.h>"));
    }

    // Output is byte-identical across runs and the provenance header is version-free, so
    // the drift gate is stable and a release bump never churns the tree.
    #[test]
    fn deterministic() {
        let a = generated();
        let b = generated();
        assert_eq!(a, b);
        let modrs = body(&a, "mod.rs");
        assert!(modrs.starts_with("//! GENERATED by proto-codegen. Do not edit."));
        assert!(!modrs.to_lowercase().contains("version 0"));
        assert!(!modrs.contains("proto-codegen v"));
    }

    // The emitted C header compiles under both a C11 and a C++17 front end, and every
    // static_assert holds (the compiler agrees with the generator's computed sizes).
    // The C++ leg exercises the extern "C" guard and the static_assert portability.
    // Each leg is skipped if its compiler is unavailable.
    #[test]
    fn c_header_compiles() {
        let files = generated();
        let header = body(&files, "md_events.h");
        let dir = std::env::temp_dir().join(format!("md-events-h-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("md_events.h");
        std::fs::write(&path, header).expect("write header");

        for (cmd, std_flag, lang) in [("cc", "-std=c11", "c"), ("c++", "-std=c++17", "c++")] {
            match std::process::Command::new(cmd)
                .arg(std_flag)
                .arg("-fsyntax-only")
                .arg("-x")
                .arg(lang)
                .arg(&path)
                .output()
            {
                Ok(out) => assert!(
                    out.status.success(),
                    "{cmd} rejected the header: {}",
                    String::from_utf8_lossy(&out.stderr)
                ),
                Err(e) => eprintln!("skip {cmd} leg: compiler unavailable ({e})"),
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
