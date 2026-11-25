//! Normalized-record schema DSL: load YAML, validate layout invariants, compute
//! alignment padding. Distinct from the venue-wire `schema` model (no type-letter
//! byte, no `byte_len`/`direction`/`appendage`); carries records (header + rtype),
//! plain nestable structs, and an enum registry. Shares only the doc-hygiene check
//! (`schema::banned_doc_token`) with the wire DSL.
//!
//! The generator owns padding: an author writes no `_pad`; `compute_padding` inserts
//! it and rejects a field order that would force avoidable interior padding. That is
//! why hand-authored pad cannot be wrong. See `record_emit` for the emission side.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

use crate::schema::banned_doc_token;

/// Storage width of a generated enum. `#[repr(u8)]` or `#[repr(u16)]`; C emits the
/// matching `uint8_t`/`uint16_t` storage typedef.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumRepr {
    U8,
    U16,
}

impl EnumRepr {
    /// alignment and size in bytes of this repr.
    pub const fn width(self) -> usize {
        match self {
            EnumRepr::U8 => 1,
            EnumRepr::U16 => 2,
        }
    }

    /// largest discriminant representable in this repr.
    const fn max_value(self) -> u16 {
        match self {
            EnumRepr::U8 => u8::MAX as u16,
            EnumRepr::U16 => u16::MAX,
        }
    }
}

/// Reserved rtype category. Bands are fixed so a future record family is an additive
/// allocation, never a layout break. Mirrors the emitted `md-events` `Category`;
/// `of_rtype` here is the source the generator projects into `generated/rtype.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Category {
    Control,
    BookTape,
    Stats,
    Reference,
    ExecReserved,
    QuoteRfqReserved,
    AmmReserved,
    Unassigned,
}

impl Category {
    /// Category owning `rt`. Status is placed in the stats band as session data.
    pub const fn of_rtype(rt: u8) -> Category {
        match rt {
            0x00..=0x0F => Category::Control,
            0x10..=0x2F => Category::BookTape,
            0x30..=0x3F => Category::Stats,
            0x40..=0x4F => Category::Reference,
            0x50..=0x5F => Category::ExecReserved,
            0x60..=0x6F => Category::QuoteRfqReserved,
            0x70..=0x7F => Category::AmmReserved,
            _ => Category::Unassigned,
        }
    }

    /// bands held open for future record families; no record may occupy one.
    pub const fn is_reserved(self) -> bool {
        matches!(
            self,
            Category::ExecReserved | Category::QuoteRfqReserved | Category::AmmReserved
        )
    }
}

/// Field type in a record or struct. `Price` is `i64` with a 1e-9 scale tag; `Flags`
/// is the hand-written `RecordFlags` (u16 storage). No author-facing pad: alignment
/// padding is generator-owned. Bytes reserved for a future field are a named `Bytes`.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldClass {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    /// `i64`, tagged 1e-9 scale (`i64::MAX` null); logic type `Price` layers on it.
    Price,
    /// `RecordFlags` (u16 storage); hand-written logic type, referenced not redefined.
    Flags,
    /// fixed `[u8; width]`.
    Bytes {
        width: u16,
    },
    /// names an `EnumDef`.
    Enum {
        ty: String,
    },
    /// names a `StructDef`.
    Struct {
        ty: String,
    },
    /// `[elem; count]`, e.g. `[BidAskPair; 10]`.
    Array {
        elem: Box<FieldClass>,
        count: u16,
    },
}

impl FieldClass {
    /// byte alignment under `repr(C)`. Aggregates take their element/struct alignment.
    fn align(&self, structs: &HashMap<String, Layout>, enums: &HashMap<String, EnumRepr>) -> usize {
        match self {
            FieldClass::U8 | FieldClass::I8 | FieldClass::Bytes { .. } => 1,
            FieldClass::U16 | FieldClass::I16 | FieldClass::Flags => 2,
            FieldClass::U32 | FieldClass::I32 => 4,
            FieldClass::U64 | FieldClass::I64 | FieldClass::Price => 8,
            FieldClass::Enum { ty } => enums.get(ty).map_or(1, |r| r.width()),
            FieldClass::Struct { ty } => structs.get(ty).map_or(1, |l| l.align),
            FieldClass::Array { elem, .. } => elem.align(structs, enums),
        }
    }

    /// byte size under `repr(C)` (aggregates include their own interior/trailing pad).
    fn size(&self, structs: &HashMap<String, Layout>, enums: &HashMap<String, EnumRepr>) -> usize {
        match self {
            FieldClass::U8 | FieldClass::I8 => 1,
            FieldClass::U16 | FieldClass::I16 | FieldClass::Flags => 2,
            FieldClass::U32 | FieldClass::I32 => 4,
            FieldClass::U64 | FieldClass::I64 | FieldClass::Price => 8,
            FieldClass::Bytes { width } => *width as usize,
            FieldClass::Enum { ty } => enums.get(ty).map_or(1, |r| r.width()),
            FieldClass::Struct { ty } => structs.get(ty).map_or(0, |l| l.size),
            FieldClass::Array { elem, count } => *count as usize * elem.size(structs, enums),
        }
    }

    /// true for the field classes that may sit behind a generator interior pad: only
    /// an array or nested struct. A scalar behind interior pad means the order forces
    /// avoidable padding and is rejected.
    fn is_aggregate(&self) -> bool {
        matches!(self, FieldClass::Array { .. } | FieldClass::Struct { .. })
    }
}

/// One ordered field. `doc` renders as rustdoc / C comment above the field.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordField {
    pub name: String,
    pub class: FieldClass,
    pub doc: Option<String>,
}

/// One enum variant with its explicit, pinned discriminant.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EnumVariant {
    pub name: String,
    pub value: u16,
    #[serde(default)]
    pub doc: Option<String>,
}

/// A generated enum: a fixed repr plus explicit discriminants, sparse or ASCII-valued.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EnumDef {
    pub name: String,
    pub repr: EnumRepr,
    /// variant marked `#[default]` (e.g. `AssetClass::Equity`); absent = no `Default`.
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub doc: Option<String>,
    pub variants: Vec<EnumVariant>,
}

/// One `RecordFlags` bit. The flag storage type stays the hand-written `RecordFlags`
/// (u16); these are the bit definitions, emitted as C `#define`s (and downstream
/// constants) so every language reads one source. Bit `i` means `1 << i`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FlagDef {
    pub name: String,
    pub bit: u8,
    #[serde(default)]
    pub doc: Option<String>,
}

/// A plain, headerless struct, nestable inside a record (`BidAskPair` in `Mbp10`).
#[derive(Debug, Clone, PartialEq)]
pub struct StructDef {
    pub name: String,
    pub derive_default: bool,
    pub doc: Option<String>,
    pub fields: Vec<RecordField>,
}

/// A header-carrying, rtype-bearing record. Every record opens with the shared
/// `RecordHeader` (`hdr`), emitted by the generator, so `fields` holds body only.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordDef {
    pub name: String,
    pub rtype: u8,
    pub band: Category,
    pub doc: Option<String>,
    pub fields: Vec<RecordField>,
}

/// One normalized-record schema file. `header` is emitted once and prefixes every
/// record; `structs` are nestable; `enums` back typed fields; `records` are the wire
/// record set.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordSchema {
    pub schema_version: String,
    pub enums: Vec<EnumDef>,
    /// `RecordFlags` bit registry; emitted to the C header (and downstream), not Rust
    /// (the `RecordFlags` type stays hand-written logic).
    pub flags: Vec<FlagDef>,
    pub structs: Vec<StructDef>,
    pub header: StructDef,
    pub records: Vec<RecordDef>,
}

/// Every computed layout for a schema: header, each struct, each record, plus the enum
/// repr map. Recomputes what `validate` checks; the emitter consumes it.
#[derive(Debug, Clone)]
pub struct Layouts {
    pub enum_reprs: HashMap<String, EnumRepr>,
    pub header: Layout,
    pub structs: HashMap<String, Layout>,
    pub records: HashMap<String, Layout>,
}

/// A field or generator-inserted pad, with its computed offset. The ordered `slots`
/// of a `Layout` are exactly what the emitter renders, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Slot {
    Field {
        name: String,
        class: FieldClass,
        doc: Option<String>,
        offset: usize,
    },
    Pad {
        name: String,
        width: usize,
        offset: usize,
    },
}

/// Result of walking a field list: the ordered slots (fields + inserted pads), the
/// total `repr(C)` size, and the alignment.
#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    pub slots: Vec<Slot>,
    pub size: usize,
    pub align: usize,
}

/// Load or validation failure. Every variant names the offending record/field/type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecordSchemaError {
    #[error("failed to read record schema {}: {source}", .path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse record schema {}: {source}", .path.display())]
    Yaml {
        path: PathBuf,
        source: serde_yaml_ng::Error,
    },

    #[error("field {field}: {detail}")]
    MalformedField { field: String, detail: String },

    #[error("{ctx} field {field}: {kind} names undeclared type {ty}")]
    UnknownType {
        ctx: String,
        field: String,
        kind: &'static str,
        ty: String,
    },

    #[error(
        "{ctx} field {field}: avoidable interior padding before a scalar; put 8-byte fields first"
    )]
    AvoidablePadding { ctx: String, field: String },

    #[error("{ctx}: size {size} is not a multiple of 4 (record length counts 32-bit words)")]
    NotWordAligned { ctx: String, size: usize },

    #[error(
        "record {record}: rtype {rtype:#04x} band {declared:?} does not match its range {actual:?}"
    )]
    RtypeBand {
        record: String,
        rtype: u8,
        declared: Category,
        actual: Category,
    },

    #[error("record {record}: rtype {rtype:#04x} occupies reserved band {band:?}")]
    RtypeReserved {
        record: String,
        rtype: u8,
        band: Category,
    },

    #[error("duplicate rtype {rtype:#04x} used by {first} and {second}")]
    RtypeDup {
        rtype: u8,
        first: String,
        second: String,
    },

    #[error("{ctx}: {detail}")]
    BadName { ctx: String, detail: String },

    #[error("{ctx}{}: doc contains banned token {token}",
        .field.as_ref().map(|f| format!(" field {f}")).unwrap_or_default())]
    DocBannedToken {
        ctx: String,
        field: Option<String>,
        token: String,
    },
}

/// full strict + reserved keyword set through edition 2024. A field name landing here
/// would break the generated `pub <name>:` struct field or accessor. Copied from
/// `schema.rs` (import scoped to `banned_doc_token`), not shared, so this DSL stays
/// independent of the wire model's internals.
const RUST_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

fn is_pascal_case(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric())
}

fn is_snake_ident(s: &str) -> bool {
    if RUST_KEYWORDS.contains(&s) {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_screaming_snake(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Walk `fields` from `base_offset`, inserting alignment pad where `repr(C)` forces
/// it. Interior pad is allowed only before an array/struct; a scalar behind interior
/// pad is rejected as avoidable (`AvoidablePadding`) with a reorder hint. Trailing pad
/// rounds the total up to `max(min_align, widest field align)`. The final size must be
/// a multiple of 4 (`NotWordAligned`). `min_align` is the header alignment for records
/// (so the record aligns to 8 even if its body is narrower) and 1 for a plain struct.
///
/// A single pad is named `_pad`; two or more are `_pad0`, `_pad1`, ... . No record in
/// the current set needs more than one, so every generated pad field is `_pad`.
pub fn compute_padding(
    fields: &[RecordField],
    base_offset: usize,
    min_align: usize,
    ctx: &str,
    structs: &HashMap<String, Layout>,
    enums: &HashMap<String, EnumRepr>,
) -> Result<Layout, RecordSchemaError> {
    let mut slots: Vec<Slot> = Vec::with_capacity(fields.len() + 1);
    let mut offset = base_offset;
    let mut align = min_align.max(1);

    for field in fields {
        let fa = field.class.align(structs, enums);
        align = align.max(fa);
        let need = (fa - offset % fa) % fa;
        if need > 0 {
            if !field.class.is_aggregate() {
                return Err(RecordSchemaError::AvoidablePadding {
                    ctx: ctx.to_string(),
                    field: field.name.clone(),
                });
            }
            slots.push(Slot::Pad {
                name: String::new(),
                width: need,
                offset,
            });
            offset += need;
        }
        slots.push(Slot::Field {
            name: field.name.clone(),
            class: field.class.clone(),
            doc: field.doc.clone(),
            offset,
        });
        offset += field.class.size(structs, enums);
    }

    let trailing = (align - offset % align) % align;
    if trailing > 0 {
        slots.push(Slot::Pad {
            name: String::new(),
            width: trailing,
            offset,
        });
        offset += trailing;
    }

    if !offset.is_multiple_of(4) {
        return Err(RecordSchemaError::NotWordAligned {
            ctx: ctx.to_string(),
            size: offset,
        });
    }

    name_pads(&mut slots);
    Ok(Layout {
        slots,
        size: offset,
        align,
    })
}

/// name each inserted pad: `_pad` when there is one, `_pad0`/`_pad1`/... when several.
fn name_pads(slots: &mut [Slot]) {
    let count = slots
        .iter()
        .filter(|s| matches!(s, Slot::Pad { .. }))
        .count();
    let mut i = 0;
    for slot in slots.iter_mut() {
        if let Slot::Pad { name, .. } = slot {
            *name = if count == 1 {
                "_pad".to_string()
            } else {
                format!("_pad{i}")
            };
            i += 1;
        }
    }
}

impl RecordSchema {
    /// read `path` and parse it; does not validate (compose `load` + `validate`).
    pub fn load(path: &Path) -> Result<Self, RecordSchemaError> {
        let text = std::fs::read_to_string(path).map_err(|source| RecordSchemaError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let raw: raw::RawSchema =
            serde_yaml_ng::from_str(&text).map_err(|source| RecordSchemaError::Yaml {
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_raw(raw)
    }

    /// parse from a YAML string; the in-memory path used by tests and `load`.
    pub fn parse(text: &str) -> Result<Self, RecordSchemaError> {
        let raw: raw::RawSchema =
            serde_yaml_ng::from_str(text).map_err(|source| RecordSchemaError::Yaml {
                path: PathBuf::from("<memory>"),
                source,
            })?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: raw::RawSchema) -> Result<Self, RecordSchemaError> {
        Ok(RecordSchema {
            schema_version: raw.schema_version,
            enums: raw.enums,
            flags: raw.flags,
            structs: raw
                .structs
                .into_iter()
                .map(StructDef::from_raw)
                .collect::<Result<_, _>>()?,
            header: StructDef::from_raw(raw.header)?,
            records: raw
                .records
                .into_iter()
                .map(RecordDef::from_raw)
                .collect::<Result<_, _>>()?,
        })
    }

    /// enum name -> repr, for padding/size lookups.
    fn enum_reprs(&self) -> HashMap<String, EnumRepr> {
        self.enums
            .iter()
            .map(|e| (e.name.clone(), e.repr))
            .collect()
    }

    /// compute every layout (header, structs, records) for the emitter. Structs are
    /// walked in declaration order so a nested struct is already known. Call after
    /// `validate`; on a malformed schema this returns the same padding error.
    pub fn layouts(&self) -> Result<Layouts, RecordSchemaError> {
        let enum_reprs = self.enum_reprs();
        let mut structs: HashMap<String, Layout> = HashMap::new();
        for s in &self.structs {
            let l = compute_padding(&s.fields, 0, 1, &s.name, &structs, &enum_reprs)?;
            structs.insert(s.name.clone(), l);
        }
        let header = compute_padding(
            &self.header.fields,
            0,
            1,
            &self.header.name,
            &structs,
            &enum_reprs,
        )?;
        let mut records: HashMap<String, Layout> = HashMap::new();
        for r in &self.records {
            let l = compute_padding(
                &r.fields,
                header.size,
                header.align,
                &r.name,
                &structs,
                &enum_reprs,
            )?;
            records.insert(r.name.clone(), l);
        }
        Ok(Layouts {
            enum_reprs,
            header,
            structs,
            records,
        })
    }
}

/// Raw serde shapes: the flat YAML form of a field (`class` tag plus sibling
/// `ty`/`width`/`count`/`elem`) before resolution into the typed `FieldClass`.
mod raw;
mod validation;

#[cfg(test)]
mod tests;
