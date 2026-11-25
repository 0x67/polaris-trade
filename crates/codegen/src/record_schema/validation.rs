use super::*;

impl RecordSchema {
    /// run every rejection rule: idents, doc hygiene, enum discriminants, type refs,
    /// padding (via `compute_padding`), rtype band and uniqueness. Computes struct
    /// layouts in declaration order (a struct may nest only an earlier struct).
    pub fn validate(&self) -> Result<(), RecordSchemaError> {
        let enum_reprs = self.enum_reprs();
        let enum_names: HashSet<&str> = self.enums.iter().map(|e| e.name.as_str()).collect();

        for e in &self.enums {
            self.validate_enum(e)?;
        }

        // RecordFlags bit registry: SCREAMING_SNAKE names, bits inside the u16 storage,
        // unique name and bit. Emitted to the C header (and downstream), never to Rust.
        let mut seen_flag_names: HashSet<&str> = HashSet::new();
        let mut seen_flag_bits: HashSet<u8> = HashSet::new();
        for fl in &self.flags {
            if !is_screaming_snake(&fl.name) {
                return Err(RecordSchemaError::BadName {
                    ctx: "flags".to_string(),
                    detail: format!("flag {} must be SCREAMING_SNAKE_CASE", fl.name),
                });
            }
            if fl.bit >= 16 {
                return Err(RecordSchemaError::BadName {
                    ctx: "flags".to_string(),
                    detail: format!("flag {} bit {} exceeds the u16 storage", fl.name, fl.bit),
                });
            }
            if !seen_flag_names.insert(fl.name.as_str()) {
                return Err(RecordSchemaError::BadName {
                    ctx: "flags".to_string(),
                    detail: format!("duplicate flag name {}", fl.name),
                });
            }
            if !seen_flag_bits.insert(fl.bit) {
                return Err(RecordSchemaError::BadName {
                    ctx: "flags".to_string(),
                    detail: format!("duplicate flag bit {}", fl.bit),
                });
            }
            check_doc(&fl.doc, "flags", Some(&fl.name))?;
        }

        // struct layouts computed as we go so a nested struct is already known.
        let mut struct_layouts: HashMap<String, Layout> = HashMap::new();
        let mut struct_names: HashSet<&str> = HashSet::new();
        for s in &self.structs {
            self.validate_struct_fields(s, &struct_names, &enum_names)?;
            let layout = compute_padding(&s.fields, 0, 1, &s.name, &struct_layouts, &enum_reprs)?;
            struct_layouts.insert(s.name.clone(), layout);
            struct_names.insert(s.name.as_str());
        }

        // header: a plain struct emitted once; its size is the record base offset.
        self.validate_struct_fields(&self.header, &struct_names, &enum_names)?;
        let header_layout = compute_padding(
            &self.header.fields,
            0,
            1,
            &self.header.name,
            &struct_layouts,
            &enum_reprs,
        )?;

        let mut seen_rtype: HashMap<u8, String> = HashMap::new();
        let mut seen_record_names: HashSet<&str> = HashSet::new();
        for r in &self.records {
            if !is_pascal_case(&r.name) {
                return Err(RecordSchemaError::BadName {
                    ctx: r.name.clone(),
                    detail: "record name must be PascalCase".to_string(),
                });
            }
            if !seen_record_names.insert(r.name.as_str()) {
                return Err(RecordSchemaError::BadName {
                    ctx: r.name.clone(),
                    detail: "duplicate record name".to_string(),
                });
            }
            check_doc(&r.doc, &r.name, None)?;

            let actual = Category::of_rtype(r.rtype);
            if actual.is_reserved() {
                return Err(RecordSchemaError::RtypeReserved {
                    record: r.name.clone(),
                    rtype: r.rtype,
                    band: actual,
                });
            }
            if actual != r.band {
                return Err(RecordSchemaError::RtypeBand {
                    record: r.name.clone(),
                    rtype: r.rtype,
                    declared: r.band,
                    actual,
                });
            }
            if let Some(first) = seen_rtype.insert(r.rtype, r.name.clone()) {
                return Err(RecordSchemaError::RtypeDup {
                    rtype: r.rtype,
                    first,
                    second: r.name.clone(),
                });
            }

            self.check_fields(&r.fields, &r.name, &struct_names, &enum_names)?;
            compute_padding(
                &r.fields,
                header_layout.size,
                header_layout.align,
                &r.name,
                &struct_layouts,
                &enum_reprs,
            )?;
        }

        Ok(())
    }

    fn validate_enum(&self, e: &EnumDef) -> Result<(), RecordSchemaError> {
        if !is_pascal_case(&e.name) {
            return Err(RecordSchemaError::BadName {
                ctx: e.name.clone(),
                detail: "enum name must be PascalCase".to_string(),
            });
        }
        check_doc(&e.doc, &e.name, None)?;
        let mut seen_names: HashSet<&str> = HashSet::new();
        let mut seen_values: HashSet<u16> = HashSet::new();
        for v in &e.variants {
            if !is_pascal_case(&v.name) {
                return Err(RecordSchemaError::BadName {
                    ctx: e.name.clone(),
                    detail: format!("variant {} must be PascalCase", v.name),
                });
            }
            if v.value > e.repr.max_value() {
                return Err(RecordSchemaError::BadName {
                    ctx: e.name.clone(),
                    detail: format!(
                        "variant {} discriminant {} exceeds {:?}",
                        v.name, v.value, e.repr
                    ),
                });
            }
            if !seen_names.insert(v.name.as_str()) {
                return Err(RecordSchemaError::BadName {
                    ctx: e.name.clone(),
                    detail: format!("duplicate variant name {}", v.name),
                });
            }
            if !seen_values.insert(v.value) {
                return Err(RecordSchemaError::BadName {
                    ctx: e.name.clone(),
                    detail: format!("duplicate variant discriminant {}", v.value),
                });
            }
            check_doc(&v.doc, &e.name, Some(&v.name))?;
        }
        if let Some(def) = &e.default
            && !e.variants.iter().any(|v| &v.name == def)
        {
            return Err(RecordSchemaError::BadName {
                ctx: e.name.clone(),
                detail: format!("default variant {def} is not declared"),
            });
        }
        Ok(())
    }

    fn validate_struct_fields(
        &self,
        s: &StructDef,
        struct_names: &HashSet<&str>,
        enum_names: &HashSet<&str>,
    ) -> Result<(), RecordSchemaError> {
        if !is_pascal_case(&s.name) {
            return Err(RecordSchemaError::BadName {
                ctx: s.name.clone(),
                detail: "struct name must be PascalCase".to_string(),
            });
        }
        check_doc(&s.doc, &s.name, None)?;
        self.check_fields(&s.fields, &s.name, struct_names, enum_names)
    }

    /// per-field checks shared by structs, the header, and records: snake_case unique
    /// names, doc hygiene, declared enum/struct type refs, nonzero bytes/array extents.
    fn check_fields(
        &self,
        fields: &[RecordField],
        ctx: &str,
        struct_names: &HashSet<&str>,
        enum_names: &HashSet<&str>,
    ) -> Result<(), RecordSchemaError> {
        let mut seen: HashSet<&str> = HashSet::new();
        for f in fields {
            if !is_snake_ident(&f.name) {
                return Err(RecordSchemaError::BadName {
                    ctx: ctx.to_string(),
                    detail: format!("field {} is not a snake_case identifier", f.name),
                });
            }
            if !seen.insert(f.name.as_str()) {
                return Err(RecordSchemaError::BadName {
                    ctx: ctx.to_string(),
                    detail: format!("duplicate field name {}", f.name),
                });
            }
            check_doc(&f.doc, ctx, Some(&f.name))?;
            check_class(&f.class, ctx, &f.name, struct_names, enum_names)?;
        }
        Ok(())
    }
}

/// enum/struct type refs resolve to a declared type; bytes/array extents are nonzero.
fn check_class(
    class: &FieldClass,
    ctx: &str,
    field: &str,
    struct_names: &HashSet<&str>,
    enum_names: &HashSet<&str>,
) -> Result<(), RecordSchemaError> {
    match class {
        FieldClass::Enum { ty } if !enum_names.contains(ty.as_str()) => {
            Err(RecordSchemaError::UnknownType {
                ctx: ctx.to_string(),
                field: field.to_string(),
                kind: "enum",
                ty: ty.clone(),
            })
        }
        FieldClass::Struct { ty } if !struct_names.contains(ty.as_str()) => {
            Err(RecordSchemaError::UnknownType {
                ctx: ctx.to_string(),
                field: field.to_string(),
                kind: "struct",
                ty: ty.clone(),
            })
        }
        FieldClass::Bytes { width } if *width == 0 => Err(RecordSchemaError::MalformedField {
            field: field.to_string(),
            detail: "bytes width must be nonzero".to_string(),
        }),
        FieldClass::Array { elem, count } => {
            if *count == 0 {
                return Err(RecordSchemaError::MalformedField {
                    field: field.to_string(),
                    detail: "array count must be nonzero".to_string(),
                });
            }
            check_class(elem, ctx, field, struct_names, enum_names)
        }
        _ => Ok(()),
    }
}

/// reject a banned token in any doc field, reusing the wire DSL's check.
fn check_doc(
    doc: &Option<String>,
    ctx: &str,
    field: Option<&str>,
) -> Result<(), RecordSchemaError> {
    if let Some(text) = doc
        && let Some(token) = banned_doc_token(text)
    {
        return Err(RecordSchemaError::DocBannedToken {
            ctx: ctx.to_string(),
            field: field.map(str::to_string),
            token,
        });
    }
    Ok(())
}
