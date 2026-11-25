use serde::Deserialize;

use super::{Category, EnumDef, FieldClass, FlagDef, RecordSchemaError};

#[derive(Deserialize)]
pub struct RawSchema {
    pub schema_version: String,
    #[serde(default)]
    pub enums: Vec<EnumDef>,
    #[serde(default)]
    pub flags: Vec<FlagDef>,
    #[serde(default)]
    pub structs: Vec<RawStruct>,
    pub header: RawStruct,
    #[serde(default)]
    pub records: Vec<RawRecord>,
}

#[derive(Deserialize)]
pub struct RawStruct {
    pub name: String,
    #[serde(default)]
    pub derive_default: bool,
    #[serde(default)]
    pub doc: Option<String>,
    pub fields: Vec<RawField>,
}

#[derive(Deserialize)]
pub struct RawRecord {
    pub name: String,
    pub rtype: u8,
    pub band: Category,
    #[serde(default)]
    pub doc: Option<String>,
    pub fields: Vec<RawField>,
}

#[derive(Deserialize)]
pub struct RawField {
    pub name: String,
    pub class: String,
    #[serde(default)]
    pub ty: Option<String>,
    #[serde(default)]
    pub width: Option<u16>,
    #[serde(default)]
    pub count: Option<u16>,
    #[serde(default)]
    pub elem: Option<Box<RawElem>>,
    #[serde(default)]
    pub doc: Option<String>,
}

#[derive(Deserialize)]
pub struct RawElem {
    pub class: String,
    #[serde(default)]
    pub ty: Option<String>,
    #[serde(default)]
    pub width: Option<u16>,
}

impl super::StructDef {
    pub(super) fn from_raw(raw: RawStruct) -> Result<Self, RecordSchemaError> {
        Ok(super::StructDef {
            name: raw.name,
            derive_default: raw.derive_default,
            doc: raw.doc,
            fields: raw
                .fields
                .into_iter()
                .map(resolve_field)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl super::RecordDef {
    pub(super) fn from_raw(raw: RawRecord) -> Result<Self, RecordSchemaError> {
        Ok(super::RecordDef {
            name: raw.name,
            rtype: raw.rtype,
            band: raw.band,
            doc: raw.doc,
            fields: raw
                .fields
                .into_iter()
                .map(resolve_field)
                .collect::<Result<_, _>>()?,
        })
    }
}

fn resolve_field(raw: RawField) -> Result<super::RecordField, RecordSchemaError> {
    let class = resolve_class(
        &raw.class,
        raw.ty.as_deref(),
        raw.width,
        raw.count,
        raw.elem,
        &raw.name,
    )?;
    Ok(super::RecordField {
        name: raw.name,
        class,
        doc: raw.doc,
    })
}

fn resolve_class(
    tag: &str,
    ty: Option<&str>,
    width: Option<u16>,
    count: Option<u16>,
    elem: Option<Box<RawElem>>,
    field: &str,
) -> Result<FieldClass, RecordSchemaError> {
    let missing = |what: &str| RecordSchemaError::MalformedField {
        field: field.to_string(),
        detail: format!("class {tag} requires {what}"),
    };
    Ok(match tag {
        "u8" => FieldClass::U8,
        "u16" => FieldClass::U16,
        "u32" => FieldClass::U32,
        "u64" => FieldClass::U64,
        "i8" => FieldClass::I8,
        "i16" => FieldClass::I16,
        "i32" => FieldClass::I32,
        "i64" => FieldClass::I64,
        "price" => FieldClass::Price,
        "flags" => FieldClass::Flags,
        "bytes" => FieldClass::Bytes {
            width: width.ok_or_else(|| missing("width"))?,
        },
        "enum" => FieldClass::Enum {
            ty: ty.ok_or_else(|| missing("ty"))?.to_string(),
        },
        "struct" => FieldClass::Struct {
            ty: ty.ok_or_else(|| missing("ty"))?.to_string(),
        },
        "array" => {
            let elem = elem.ok_or_else(|| missing("elem"))?;
            let count = count.ok_or_else(|| missing("count"))?;
            FieldClass::Array {
                elem: Box::new(resolve_elem(*elem, field)?),
                count,
            }
        }
        other => {
            return Err(RecordSchemaError::MalformedField {
                field: field.to_string(),
                detail: format!("unknown field class {other}"),
            });
        }
    })
}

fn resolve_elem(raw: RawElem, field: &str) -> Result<FieldClass, RecordSchemaError> {
    // an array element is a scalar, price/flags, bytes, or a nested struct; no
    // array-of-array in the current record set.
    resolve_class(&raw.class, raw.ty.as_deref(), raw.width, None, None, field)
}
