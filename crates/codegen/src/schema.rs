//! Wire-schema DSL model: load YAML, validate against the emission invariants.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

/// Byte order a schema declares for its numeric fields.
///
/// `Le` parses fine but generation rejects it (`SchemaError::LeUnimplemented`)
/// until a little-endian venue needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Endianness {
    Be,
    Le,
}

/// Wire type of one field within a kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TypeClass {
    U8,
    U16,
    U32,
    /// 6-byte big-endian unsigned; accessor zero-extends into u64.
    U48,
    U64,
    I8,
    I16,
    I32,
    I64,
    Code,
    Alpha,
}

impl TypeClass {
    /// fixed wire width for numeric and code classes; `None` for alpha (width set by schema).
    fn fixed_width(self) -> Option<u16> {
        match self {
            TypeClass::U8 | TypeClass::I8 | TypeClass::Code => Some(1),
            TypeClass::U16 | TypeClass::I16 => Some(2),
            TypeClass::U32 | TypeClass::I32 => Some(4),
            TypeClass::U48 => Some(6),
            TypeClass::U64 | TypeClass::I64 => Some(8),
            TypeClass::Alpha => None,
        }
    }
}

/// One ordered field within a kind's fixed-size wire layout.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Field {
    pub name: String,
    pub width: u16,
    pub class: TypeClass,
    /// venue-doc prose for this field; rendered as rustdoc above the accessor. absent -> `None`.
    pub doc: Option<String>,
}

/// wire direction of an order-entry kind, exchange's perspective.
///
/// `ToExchange` = client sends (Enter/Replace/Cancel...); `FromExchange` = client
/// receives (Accepted/Rejected/Executed...). Absent on directionless feed schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    ToExchange,
    FromExchange,
}

/// one entry in a schema-level appendage tag registry (NASDAQ-style TagValue TLV).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AppendageTag {
    pub tag: u8,
    pub name: String,
    pub width: u16,
    pub class: TypeClass,
    /// venue-doc prose for this tag; rendered as rustdoc above the generated enum variant.
    pub doc: Option<String>,
}

/// One message kind: a wire type byte plus either a fixed field layout or `variable: true`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Kind {
    pub name: String,
    #[serde(deserialize_with = "deserialize_byte")]
    pub byte: u8,
    pub byte_len: Option<u16>,
    #[serde(default)]
    pub variable: bool,
    pub since: Option<String>,
    pub defect: Option<String>,
    /// venue-doc prose for this kind; rendered as rustdoc at three emission sites. absent -> `None`.
    pub doc: Option<String>,
    #[serde(default)]
    pub fields: Vec<Field>,
    /// order-entry wire direction; absent on feed (directionless) schemas.
    #[serde(default)]
    pub direction: Option<Direction>,
    /// registry tag names carried by this kind's trailing TLV appendage, empty = none.
    #[serde(default)]
    pub appendage: Vec<String>,
    /// dual-form wire: venue may omit the appendage length field entirely.
    #[serde(default)]
    pub appendage_optional: bool,
}

/// Root of one (venue, protocol, major) YAML schema file.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaFile {
    pub venue: String,
    pub protocol: String,
    pub doc_version: String,
    /// protocol major for the module namespace when it differs from the doc's
    /// iteration version (e.g. a v2 platform documented by a v4.0 doc).
    /// Absent = derive from doc_version's leading integer.
    #[serde(default)]
    pub major: Option<u32>,
    pub endianness: Endianness,
    pub kinds: Vec<Kind>,
    /// schema-level appendage tag registry; referenced by name from `Kind::appendage`.
    #[serde(default)]
    pub appendage_tags: Vec<AppendageTag>,
}

/// Schema load or validation failure. Every variant names the offending kind/field/path.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaError {
    #[error("failed to read schema {}: {source}", .path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse schema {}: {source}", .path.display())]
    Yaml {
        path: PathBuf,
        source: serde_yaml_ng::Error,
    },

    #[error("kind {kind}: byte_len mismatch, declared {declared}, derived {derived}")]
    WidthSum {
        kind: String,
        declared: u32,
        derived: u32,
    },

    #[error("duplicate type byte {byte:?} used by kinds {kinds}")]
    DuplicateByte { byte: char, kinds: String },

    #[error("kind {kind}: {detail}")]
    BadName { kind: String, detail: String },

    #[error("kind {kind} field {field}: width does not match class")]
    ClassWidth { kind: String, field: String },

    #[error(
        "kind {kind}: variable kinds must omit byte_len and fields; fixed kinds require byte_len, and fields unless an appendage is declared"
    )]
    VariableConflict { kind: String },

    #[error("doc_version {doc_version:?} has no parseable leading integer")]
    BadDocVersion { doc_version: String },

    #[error("little-endian emission not implemented")]
    LeUnimplemented,

    #[error("venue crate directory missing: {}", .dir.display())]
    MissingVenueCrate { dir: PathBuf },

    #[error("kind {kind}{}: doc contains banned token {token}",
        .field.as_ref().map(|f| format!(" field {f}")).unwrap_or_default())]
    DocBannedToken {
        kind: String,
        field: Option<String>,
        token: String,
    },

    #[error("mixed direction presence: kinds {kinds} must all declare direction or none must")]
    DirectionMixed { kinds: String },

    #[error("kind {kind}: {detail}")]
    AppendageInvalid { kind: String, detail: String },
}

/// full strict + reserved keyword set through edition 2024; rejects field names and derived
/// kind visitor-method/module names. reserved-but-unused keywords (final, try, yield, ...)
/// are listed too: they pass no `fn` position, so generated accessors would not compile.
/// contextual keywords (union, raw) stay legal identifiers and are deliberately absent.
const RUST_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// deserialize `byte` from a one-char YAML string ("A", "d"); case matters, wire types are letters
fn deserialize_byte<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) if c.is_ascii() && !c.is_ascii_control() => Ok(c as u8),
        _ => Err(serde::de::Error::custom(format!(
            "byte must be exactly one printable ascii char, got {s:?}"
        ))),
    }
}

/// first banned token in doc text, None if clean. stems composed at runtime (`stem + '-'`)
/// so this file stays clean under the workspace hygiene grep. shared with
/// `record_schema::RecordSchema::validate` so both DSLs enforce one doc rule.
pub(crate) fn banned_doc_token(text: &str) -> Option<String> {
    for stem in ["REQ", "TASK", "AC"] {
        let tok = format!("{stem}-");
        if text.contains(&tok) {
            return Some(tok);
        }
    }
    if text.contains('\u{2014}') {
        return Some("em dash (U+2014)".to_string());
    }
    None
}

fn is_lower_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

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

/// YAML spelling of a `Direction`, for error messages naming a directional byte collision.
fn direction_str(dir: Direction) -> &'static str {
    match dir {
        Direction::ToExchange => "to_exchange",
        Direction::FromExchange => "from_exchange",
    }
}

/// PascalCase kind name -> snake_case visitor method name.
///
/// Underscore inserted before an uppercase char that follows a lowercase/digit, or before
/// an uppercase char that follows another uppercase but precedes a lowercase (acronym
/// boundary). Two distinct valid kind names can still collide here (`"Ab"` and `"AB"` both
/// map to `"ab"`); `SchemaFile::validate` catches that as `SchemaError::BadName`.
pub fn visitor_method(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(chars.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() {
            let prev_lower_or_digit =
                i > 0 && (chars[i - 1].is_ascii_lowercase() || chars[i - 1].is_ascii_digit());
            let acronym_boundary = i > 0
                && chars[i - 1].is_ascii_uppercase()
                && i + 1 < chars.len()
                && chars[i + 1].is_ascii_lowercase();
            if i > 0 && (prev_lower_or_digit || acronym_boundary) {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

impl SchemaFile {
    /// read `path` and parse it as YAML. Does not validate; callers compose `load` + `validate`.
    pub fn load(path: &Path) -> Result<Self, SchemaError> {
        let text = std::fs::read_to_string(path).map_err(|source| SchemaError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        serde_yaml_ng::from_str(&text).map_err(|source| SchemaError::Yaml {
            path: path.to_path_buf(),
            source,
        })
    }

    /// module major: explicit `major` key when present, else the leading integer of
    /// `doc_version` (e.g. `"1.16.0"` -> `1`). doc_version always validates either way
    /// so a typo'd doc_version cannot hide behind an override.
    pub fn doc_major(&self) -> Result<u32, SchemaError> {
        let digits: String = self
            .doc_version
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        let derived = digits
            .parse::<u32>()
            .map_err(|_| SchemaError::BadDocVersion {
                doc_version: self.doc_version.clone(),
            })?;
        Ok(self.major.unwrap_or(derived))
    }

    /// run every rejection rule: idents, doc_version, byte uniqueness, name shape/uniqueness,
    /// variable/fixed shape, class width, width sum, direction, appendage. See design
    /// section 2.2 rule table.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if !is_lower_ident(&self.venue) {
            return Err(SchemaError::BadName {
                kind: self.venue.clone(),
                detail: "venue must be a lowercase ascii ident".to_string(),
            });
        }
        if !is_lower_ident(&self.protocol) {
            return Err(SchemaError::BadName {
                kind: self.protocol.clone(),
                detail: "protocol must be a lowercase ascii ident".to_string(),
            });
        }
        self.doc_major()?;

        // direction is all-or-none across kinds; a schema with even one directioned
        // kind is "directional" and unlocks appendage/registry use for every kind.
        let any_direction = self.kinds.iter().any(|k| k.direction.is_some());
        let all_direction = self.kinds.iter().all(|k| k.direction.is_some());
        if any_direction && !all_direction {
            let missing: Vec<&str> = self
                .kinds
                .iter()
                .filter(|k| k.direction.is_none())
                .map(|k| k.name.as_str())
                .collect();
            return Err(SchemaError::DirectionMixed {
                kinds: missing.join(", "),
            });
        }

        let mut tag_by_name: HashMap<&str, &AppendageTag> = HashMap::new();
        let mut seen_tag_numbers: HashMap<u8, String> = HashMap::new();
        for tag in &self.appendage_tags {
            if !is_snake_ident(&tag.name) {
                return Err(SchemaError::AppendageInvalid {
                    kind: "schema".to_string(),
                    detail: format!(
                        "appendage tag name {} is not a valid snake_case identifier",
                        tag.name
                    ),
                });
            }
            if let Some(doc) = &tag.doc
                && let Some(token) = banned_doc_token(doc)
            {
                return Err(SchemaError::DocBannedToken {
                    kind: "schema".to_string(),
                    field: Some(tag.name.clone()),
                    token,
                });
            }
            match tag.class.fixed_width() {
                Some(expected) if tag.width != expected => {
                    return Err(SchemaError::AppendageInvalid {
                        kind: "schema".to_string(),
                        detail: format!(
                            "appendage tag {} width {} does not match class",
                            tag.name, tag.width
                        ),
                    });
                }
                None if tag.width == 0 => {
                    return Err(SchemaError::AppendageInvalid {
                        kind: "schema".to_string(),
                        detail: format!("appendage tag {} alpha width must be nonzero", tag.name),
                    });
                }
                _ => {}
            }
            if let Some(existing) = seen_tag_numbers.get(&tag.tag) {
                return Err(SchemaError::AppendageInvalid {
                    kind: "schema".to_string(),
                    detail: format!(
                        "duplicate appendage tag number {} used by {existing} and {}",
                        tag.tag, tag.name
                    ),
                });
            }
            seen_tag_numbers.insert(tag.tag, tag.name.clone());
            if tag_by_name.insert(tag.name.as_str(), tag).is_some() {
                return Err(SchemaError::AppendageInvalid {
                    kind: "schema".to_string(),
                    detail: format!("duplicate appendage tag name {}", tag.name),
                });
            }
        }
        if !self.appendage_tags.is_empty() && !any_direction {
            return Err(SchemaError::AppendageInvalid {
                kind: "schema".to_string(),
                detail: "appendage_tags present on a directionless schema".to_string(),
            });
        }

        let mut seen_bytes: HashMap<(Option<Direction>, u8), String> = HashMap::new();
        let mut seen_kind_names: HashSet<String> = HashSet::new();
        let mut seen_visitor_methods: HashMap<String, String> = HashMap::new();
        let mut referenced_tags: HashSet<String> = HashSet::new();

        for kind in &self.kinds {
            if !is_pascal_case(&kind.name) {
                return Err(SchemaError::BadName {
                    kind: kind.name.clone(),
                    detail: "kind name must be PascalCase: first char uppercase, rest ascii alphanumeric, no underscore".to_string(),
                });
            }
            if !seen_kind_names.insert(kind.name.clone()) {
                return Err(SchemaError::BadName {
                    kind: kind.name.clone(),
                    detail: "duplicate kind name".to_string(),
                });
            }

            if let Some(doc) = &kind.doc
                && let Some(token) = banned_doc_token(doc)
            {
                return Err(SchemaError::DocBannedToken {
                    kind: kind.name.clone(),
                    field: None,
                    token,
                });
            }

            let method = visitor_method(&kind.name);
            if RUST_KEYWORDS.contains(&method.as_str()) {
                return Err(SchemaError::BadName {
                    kind: kind.name.clone(),
                    detail: format!(
                        "derived visitor method/module name `{method}` is a Rust keyword"
                    ),
                });
            }
            if let Some(other) = seen_visitor_methods.insert(method.clone(), kind.name.clone()) {
                return Err(SchemaError::BadName {
                    kind: kind.name.clone(),
                    detail: format!("visitor method `{method}` collides with kind {other}"),
                });
            }

            if !kind.appendage.is_empty() {
                if !any_direction {
                    return Err(SchemaError::AppendageInvalid {
                        kind: kind.name.clone(),
                        detail: "appendage requires the kind to declare a direction".to_string(),
                    });
                }
                if kind.variable {
                    return Err(SchemaError::AppendageInvalid {
                        kind: kind.name.clone(),
                        detail: "appendage not allowed on a variable kind".to_string(),
                    });
                }
                for tag_name in &kind.appendage {
                    if tag_by_name.contains_key(tag_name.as_str()) {
                        referenced_tags.insert(tag_name.clone());
                    } else {
                        return Err(SchemaError::AppendageInvalid {
                            kind: kind.name.clone(),
                            detail: format!("appendage references unknown tag {tag_name}"),
                        });
                    }
                }
            }
            if kind.appendage_optional && kind.appendage.is_empty() {
                return Err(SchemaError::AppendageInvalid {
                    kind: kind.name.clone(),
                    detail: "appendage_optional set without a nonempty appendage".to_string(),
                });
            }

            let byte_key = (kind.direction, kind.byte);
            if let Some(existing) = seen_bytes.get(&byte_key) {
                let kinds = match kind.direction {
                    Some(dir) => format!(
                        "{existing} ({}), {} ({})",
                        direction_str(dir),
                        kind.name,
                        direction_str(dir)
                    ),
                    None => format!("{existing}, {}", kind.name),
                };
                return Err(SchemaError::DuplicateByte {
                    byte: kind.byte as char,
                    kinds,
                });
            }
            seen_bytes.insert(byte_key, kind.name.clone());

            if kind.variable {
                if kind.byte_len.is_some() || !kind.fields.is_empty() {
                    return Err(SchemaError::VariableConflict {
                        kind: kind.name.clone(),
                    });
                }
                continue;
            }

            let Some(byte_len) = kind.byte_len else {
                return Err(SchemaError::VariableConflict {
                    kind: kind.name.clone(),
                });
            };
            // appendage-only kinds are legal: type byte + synthesized length field +
            // TLV tail, no fixed rows (NASDAQ OUCH AccountQueryRequest is 3 bytes)
            if kind.fields.is_empty() && kind.appendage.is_empty() {
                return Err(SchemaError::VariableConflict {
                    kind: kind.name.clone(),
                });
            }

            let mut seen_field_names: HashSet<String> = HashSet::new();
            let mut derived: u32 = 1; // type byte prefix
            for field in &kind.fields {
                if !is_snake_ident(&field.name) {
                    return Err(SchemaError::BadName {
                        kind: kind.name.clone(),
                        detail: format!(
                            "field {} is not a valid snake_case identifier",
                            field.name
                        ),
                    });
                }
                if !seen_field_names.insert(field.name.clone()) {
                    return Err(SchemaError::BadName {
                        kind: kind.name.clone(),
                        detail: format!("duplicate field name {}", field.name),
                    });
                }

                if let Some(doc) = &field.doc
                    && let Some(token) = banned_doc_token(doc)
                {
                    return Err(SchemaError::DocBannedToken {
                        kind: kind.name.clone(),
                        field: Some(field.name.clone()),
                        token,
                    });
                }

                match field.class.fixed_width() {
                    Some(expected) if field.width != expected => {
                        return Err(SchemaError::ClassWidth {
                            kind: kind.name.clone(),
                            field: field.name.clone(),
                        });
                    }
                    None if field.width == 0 => {
                        return Err(SchemaError::ClassWidth {
                            kind: kind.name.clone(),
                            field: field.name.clone(),
                        });
                    }
                    _ => {}
                }

                derived += u32::from(field.width);
            }
            // 2-byte appendage length field is synthesized by the generator, never
            // listed in `fields`, so its width is added here, not walked above.
            if !kind.appendage.is_empty() {
                derived += 2;
            }

            if derived != u32::from(byte_len) {
                return Err(SchemaError::WidthSum {
                    kind: kind.name.clone(),
                    declared: u32::from(byte_len),
                    derived,
                });
            }
        }

        for tag in &self.appendage_tags {
            if !referenced_tags.contains(&tag.name) {
                return Err(SchemaError::AppendageInvalid {
                    kind: "schema".to_string(),
                    detail: format!("appendage tag {} is never referenced by any kind", tag.name),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
