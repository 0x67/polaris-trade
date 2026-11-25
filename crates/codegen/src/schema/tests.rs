use super::*;

fn minimal_kind_yaml(name: &str, byte: &str) -> String {
    format!(
        r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: {name}
    byte: "{byte}"
    byte_len: 2
    fields:
      - {{ name: v, width: 1, class: code }}
"#
    )
}

fn field_schema_yaml(class: &str, width: u16, byte_len: u16) -> String {
    format!(
        r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Case
    byte: "C"
    byte_len: {byte_len}
    fields:
      - {{ name: v, width: {width}, class: {class} }}
"#
    )
}

fn write_temp_yaml(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "proto-codegen-test-{}-{name}.yaml",
        std::process::id()
    ));
    std::fs::write(&path, contents).expect("write temp schema");
    path
}

const TOY_YAML: &str = r#"
venue: toyv
protocol: toyp
doc_version: "1.2.0"
endianness: be

kinds:
  - name: Mixed
    byte: "M"
    byte_len: 16
    fields:
      - { name: seq, width: 4, class: u32 }
      - { name: side, width: 1, class: code }
      - { name: symbol, width: 8, class: alpha }
      - { name: qty, width: 2, class: u16 }
  - name: News
    byte: "N"
    variable: true
"#;

#[test]
fn full_feature_toy_schema_loads() {
    let schema: SchemaFile = serde_yaml_ng::from_str(TOY_YAML).expect("parses");
    assert_eq!(schema.venue, "toyv");
    assert_eq!(schema.protocol, "toyp");
    assert_eq!(schema.endianness, Endianness::Be);
    assert_eq!(schema.doc_major().unwrap(), 1);
    schema.validate().expect("valid toy schema");

    assert_eq!(schema.kinds.len(), 2);
    let mixed = &schema.kinds[0];
    assert_eq!(mixed.byte, b'M');
    assert_eq!(mixed.byte_len, Some(16));
    assert_eq!(mixed.fields.len(), 4);
    assert_eq!(mixed.fields[0].name, "seq");
    assert_eq!(mixed.fields[0].width, 4);
    assert_eq!(mixed.fields[0].class, TypeClass::U32);
    assert_eq!(mixed.fields[2].class, TypeClass::Alpha);
    assert_eq!(mixed.fields[2].width, 8);

    let news = &schema.kinds[1];
    assert!(news.variable);
    assert!(news.fields.is_empty());
    assert_eq!(news.byte_len, None);
}

#[test]
fn rejects_width_sum_mismatch() {
    let yaml = field_schema_yaml("u32", 4, 99);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::WidthSum {
            declared, derived, ..
        } => {
            assert_eq!(declared, 99);
            assert_eq!(derived, 5); // 1 type byte + 4
        }
        other => panic!("expected WidthSum, got {other:?}"),
    }
}

#[test]
fn rejects_duplicate_type_byte() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Alpha
    byte: "X"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
  - name: Beta
    byte: "X"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::DuplicateByte { byte, kinds } => {
            assert_eq!(byte, 'X');
            assert!(kinds.contains("Alpha"));
            assert!(kinds.contains("Beta"));
        }
        other => panic!("expected DuplicateByte, got {other:?}"),
    }
}

#[test]
fn rejects_bad_field_ident() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Bad
    byte: "B"
    byte_len: 2
    fields:
      - { name: BadName, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::BadName { .. }
    ));
}

#[test]
fn rejects_duplicate_visitor_method() {
    // "Ab" and "AB" both pass the kind-name PascalCase rule (first char uppercase,
    // rest ascii alphanumeric, no underscore) but collapse to the same snake_case
    // visitor method "ab" -- proves collision is possible under this weak PascalCase
    // check, so validate() must catch it as BadName.
    assert_eq!(visitor_method("Ab"), "ab");
    assert_eq!(visitor_method("AB"), "ab");

    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Ab
    byte: "1"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
  - name: AB
    byte: "2"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::BadName { .. }
    ));
}

#[test]
fn major_key_overrides_doc_version() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "4.0"
major: 2
endianness: be
kinds:
  - name: Case
    byte: "C"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    schema.validate().expect("validates");
    assert_eq!(schema.doc_major().unwrap(), 2);
    assert_eq!(schema.doc_version, "4.0");
}

#[test]
fn absent_major_derives_from_doc_version() {
    let yaml = field_schema_yaml("code", 1, 2);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    assert_eq!(schema.major, None);
    assert_eq!(schema.doc_major().unwrap(), 1);
}

#[test]
fn u48_loads_with_width_6() {
    let yaml = field_schema_yaml("u48", 6, 7);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    schema.validate().expect("u48 width 6 validates");
    assert_eq!(schema.kinds[0].fields[0].class, TypeClass::U48);
}

#[test]
fn u48_rejects_wrong_width() {
    let yaml = field_schema_yaml("u48", 5, 6);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::ClassWidth { .. }
    ));
}

#[test]
fn rejects_class_width_mismatch() {
    let yaml = field_schema_yaml("u32", 8, 9);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::ClassWidth { .. }
    ));

    let yaml = field_schema_yaml("alpha", 0, 1);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::ClassWidth { .. }
    ));

    let yaml = field_schema_yaml("code", 2, 3);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::ClassWidth { .. }
    ));
}

#[test]
fn rejects_variable_with_fields() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: News
    byte: "N"
    variable: true
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::VariableConflict { .. }
    ));
}

#[test]
fn rejects_missing_endianness() {
    let path = write_temp_yaml(
        "missing-endianness",
        r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
kinds: []
"#,
    );
    let err = SchemaFile::load(&path).unwrap_err();
    assert!(matches!(err, SchemaError::Yaml { .. }));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn rejects_bad_doc_version() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "abc"
endianness: be
kinds: []
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::BadDocVersion { .. }
    ));
}

#[test]
fn byte_deserializer_distinguishes_case() {
    let lower: SchemaFile = serde_yaml_ng::from_str(&minimal_kind_yaml("Lower", "d")).unwrap();
    let upper: SchemaFile = serde_yaml_ng::from_str(&minimal_kind_yaml("Upper", "D")).unwrap();
    assert_eq!(lower.kinds[0].byte, b'd');
    assert_eq!(upper.kinds[0].byte, b'D');
    assert_ne!(lower.kinds[0].byte, upper.kinds[0].byte);
}

#[test]
fn byte_deserializer_rejects_multi_char() {
    let result: Result<SchemaFile, _> = serde_yaml_ng::from_str(&minimal_kind_yaml("Bad", "AB"));
    assert!(result.is_err());
}

#[test]
fn doc_keys_roundtrip() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Doced
    byte: "D"
    byte_len: 2
    doc: |
      line one
      line two
    fields:
      - { name: v, width: 1, class: code, doc: "field meaning" }
  - name: Bare
    byte: "B"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).expect("parses");
    schema.validate().expect("doc text clean");

    let doced = &schema.kinds[0];
    assert_eq!(doced.doc.as_deref(), Some("line one\nline two\n"));
    assert_eq!(doced.fields[0].doc.as_deref(), Some("field meaning"));

    let bare = &schema.kinds[1];
    assert_eq!(bare.doc, None);
    assert_eq!(bare.fields[0].doc, None);
}

#[test]
fn rejects_banned_doc_token_kind() {
    // stems composed at runtime so this test source stays hygiene-grep clean.
    for stem in ["REQ", "TASK", "AC"] {
        let token = format!("{stem}-001 needs doc");
        let yaml = format!(
            r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Doced
    byte: "D"
    byte_len: 2
    doc: "{token}"
    fields:
      - {{ name: v, width: 1, class: code }}
"#
        );
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        match schema.validate().unwrap_err() {
            SchemaError::DocBannedToken {
                kind,
                field,
                token: matched,
            } => {
                assert_eq!(kind, "Doced");
                assert_eq!(field, None);
                assert!(matched.starts_with(stem));
            }
            other => panic!("expected DocBannedToken, got {other:?}"),
        }
    }

    let em_dash_doc = format!("bad{}text", '\u{2014}');
    let yaml = format!(
        r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Doced
    byte: "D"
    byte_len: 2
    doc: "{em_dash_doc}"
    fields:
      - {{ name: v, width: 1, class: code }}
"#
    );
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::DocBannedToken { kind, field, token } => {
            assert_eq!(kind, "Doced");
            assert_eq!(field, None);
            assert!(token.contains("U+2014"));
        }
        other => panic!("expected DocBannedToken, got {other:?}"),
    }
}

#[test]
fn rejects_banned_doc_token_field() {
    for stem in ["REQ", "TASK", "AC"] {
        let token = format!("{stem}-002 field note");
        let yaml = format!(
            r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Doced
    byte: "D"
    byte_len: 2
    fields:
      - {{ name: v, width: 1, class: code, doc: "{token}" }}
"#
        );
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        match schema.validate().unwrap_err() {
            SchemaError::DocBannedToken {
                kind,
                field,
                token: matched,
            } => {
                assert_eq!(kind, "Doced");
                assert_eq!(field.as_deref(), Some("v"));
                assert!(matched.starts_with(stem));
            }
            other => panic!("expected DocBannedToken, got {other:?}"),
        }
    }

    let em_dash_doc = format!("bad{}text", '\u{2014}');
    let yaml = format!(
        r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Doced
    byte: "D"
    byte_len: 2
    fields:
      - {{ name: v, width: 1, class: code, doc: "{em_dash_doc}" }}
"#
    );
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::DocBannedToken { kind, field, token } => {
            assert_eq!(kind, "Doced");
            assert_eq!(field.as_deref(), Some("v"));
            assert!(token.contains("U+2014"));
        }
        other => panic!("expected DocBannedToken, got {other:?}"),
    }
}

#[test]
fn rejects_keyword_visitor_method() {
    for name in ["Match", "Mod"] {
        let yaml = format!(
            r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: {name}
    byte: "K"
    byte_len: 2
    fields:
      - {{ name: v, width: 1, class: code }}
"#
        );
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        assert!(matches!(
            schema.validate().unwrap_err(),
            SchemaError::BadName { .. }
        ));
    }
}

#[test]
fn rejects_keyword_field_name() {
    // strict (type), 2018 reserved (try), reserved-but-unused (final, yield),
    // 2024 reserved (gen): all would break the generated `pub fn <name>` accessor
    for name in ["type", "try", "final", "yield", "gen"] {
        let yaml = format!(
            r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Kw
    byte: "K"
    byte_len: 3
    fields:
      - {{ name: {name}, width: 2, class: u16 }}
"#
        );
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        match schema.validate().unwrap_err() {
            SchemaError::BadName { detail, .. } => {
                assert!(
                    detail.contains(name),
                    "detail should name the field: {detail}"
                );
            }
            other => panic!("expected BadName for field `{name}`, got {other:?}"),
        }
    }
}

#[test]
fn u8_and_i8_load_with_width_one() {
    let yaml = field_schema_yaml("u8", 1, 2);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    schema.validate().expect("u8 width 1 validates");
    assert_eq!(schema.kinds[0].fields[0].class, TypeClass::U8);

    let yaml = field_schema_yaml("i8", 1, 2);
    let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
    schema.validate().expect("i8 width 1 validates");
    assert_eq!(schema.kinds[0].fields[0].class, TypeClass::I8);
}

#[test]
fn u8_and_i8_reject_wrong_width() {
    for class in ["u8", "i8"] {
        let yaml = field_schema_yaml(class, 2, 3);
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        assert!(matches!(
            schema.validate().unwrap_err(),
            SchemaError::ClassWidth { .. }
        ));
    }
}

#[test]
fn feed_schema_without_direction_keys_validates_unchanged() {
    let schema: SchemaFile = serde_yaml_ng::from_str(TOY_YAML).expect("parses");
    assert!(schema.kinds.iter().all(|k| k.direction.is_none()));
    schema
        .validate()
        .expect("directionless feed schema still validates unchanged");
}

#[test]
fn direction_rejects_unknown_string() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: sideways
    fields:
      - { name: v, width: 1, class: code }
"#;
    let result: Result<SchemaFile, _> = serde_yaml_ng::from_str(yaml);
    assert!(result.is_err());
}

#[test]
fn direction_collision_same_byte_opposite_directions_loads() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    fields:
      - { name: v, width: 1, class: code }
  - name: OrderReplaced
    byte: "U"
    byte_len: 2
    direction: from_exchange
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    schema
        .validate()
        .expect("cross-direction byte reuse allowed");
}

#[test]
fn same_direction_duplicate_byte_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    fields:
      - { name: v, width: 1, class: code }
  - name: ReplaceOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::DuplicateByte { byte, kinds } => {
            assert_eq!(byte, 'U');
            assert!(kinds.contains("EnterOrder"));
            assert!(kinds.contains("ReplaceOrder"));
            assert!(kinds.contains("to_exchange"));
        }
        other => panic!("expected DuplicateByte, got {other:?}"),
    }
}

#[test]
fn mixed_direction_presence_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    fields:
      - { name: v, width: 1, class: code }
  - name: Bare
    byte: "B"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::DirectionMixed { kinds } => {
            assert!(kinds.contains("Bare"));
        }
        other => panic!("expected DirectionMixed, got {other:?}"),
    }
}

#[test]
fn empty_appendage_vec_means_no_appendage_in_byte_len() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    appendage: []
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    schema
        .validate()
        .expect("empty appendage vec does not add the 2-byte length field");
}

#[test]
fn appendage_full_round_trip_validates() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
  - { tag: 2, name: min_qty, width: 4, class: u32 }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm, min_qty]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).expect("parses");
    schema
        .validate()
        .expect("registered appendage on directional kind validates");
    assert_eq!(schema.appendage_tags.len(), 2);
    assert_eq!(
        schema.kinds[0].appendage,
        vec!["firm".to_string(), "min_qty".to_string()]
    );
}

#[test]
fn appendage_optional_dual_form_kind_validates() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
kinds:
  - name: OrderCanceled
    byte: "C"
    byte_len: 4
    direction: from_exchange
    appendage: [firm]
    appendage_optional: true
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    schema
        .validate()
        .expect("appendage_optional with nonempty appendage validates");
}

#[test]
fn appendage_unresolved_tag_name_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm, ghost]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, detail } => {
            assert_eq!(kind, "EnterOrder");
            assert!(detail.contains("ghost"));
        }
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_duplicate_registry_tag_number_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
  - { tag: 1, name: min_qty, width: 4, class: u32 }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::AppendageInvalid { .. }
    ));
}

#[test]
fn appendage_duplicate_registry_tag_name_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
  - { tag: 2, name: firm, width: 4, class: alpha }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::AppendageInvalid { .. }
    ));
}

#[test]
fn appendage_registry_name_must_be_snake_ident() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: Firm, width: 4, class: alpha }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [Firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, detail } => {
            assert_eq!(kind, "schema");
            assert!(detail.contains("Firm"));
        }
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_optional_without_appendage_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 2
    direction: to_exchange
    appendage_optional: true
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::AppendageInvalid { .. }
    ));
}

#[test]
fn appendage_on_variable_kind_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
kinds:
  - name: News
    byte: "N"
    variable: true
    direction: from_exchange
    appendage: [firm]
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, .. } => assert_eq!(kind, "News"),
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_tags_registry_on_directionless_schema_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
kinds:
  - name: Bare
    byte: "B"
    byte_len: 2
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, detail } => {
            assert_eq!(kind, "schema");
            assert!(detail.contains("directionless"));
        }
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_on_directionless_kind_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Bare
    byte: "B"
    byte_len: 4
    appendage: [firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, detail } => {
            assert_eq!(kind, "Bare");
            assert!(detail.contains("direction"));
        }
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_tags_dead_registry_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 4, class: alpha }
  - { tag: 2, name: min_qty, width: 4, class: u32 }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    match schema.validate().unwrap_err() {
        SchemaError::AppendageInvalid { kind, detail } => {
            assert_eq!(kind, "schema");
            assert!(detail.contains("min_qty"));
        }
        other => panic!("expected AppendageInvalid, got {other:?}"),
    }
}

#[test]
fn appendage_tag_doc_banned_token_rejected() {
    for stem in ["REQ", "TASK", "AC"] {
        let token = format!("{stem}-003 tag note");
        let yaml = format!(
            r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - {{ tag: 1, name: firm, width: 4, class: alpha, doc: "{token}" }}
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm]
    fields:
      - {{ name: v, width: 1, class: code }}
"#
        );
        let schema: SchemaFile = serde_yaml_ng::from_str(&yaml).unwrap();
        match schema.validate().unwrap_err() {
            SchemaError::DocBannedToken {
                kind,
                field,
                token: matched,
            } => {
                assert_eq!(kind, "schema");
                assert_eq!(field.as_deref(), Some("firm"));
                assert!(matched.starts_with(stem));
            }
            other => panic!("expected DocBannedToken, got {other:?}"),
        }
    }
}

#[test]
fn appendage_registry_width_class_mismatch_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: min_qty, width: 5, class: u32 }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [min_qty]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::AppendageInvalid { .. }
    ));

    let yaml_alpha_zero = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 1, name: firm, width: 0, class: alpha }
kinds:
  - name: EnterOrder
    byte: "U"
    byte_len: 4
    direction: to_exchange
    appendage: [firm]
    fields:
      - { name: v, width: 1, class: code }
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml_alpha_zero).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::AppendageInvalid { .. }
    ));
}

#[test]
fn appendage_only_kind_valid_without_fields() {
    // type byte + synthesized 2-byte length + tail, zero fixed rows:
    // byte_len 3. NASDAQ OUCH AccountQueryRequest shape.
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
appendage_tags:
  - { tag: 28, name: user_ref_idx, width: 1, class: u8 }
kinds:
  - name: AccountQueryRequest
    byte: "Q"
    byte_len: 3
    direction: to_exchange
    appendage: [user_ref_idx]
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    schema
        .validate()
        .expect("appendage-only kind must validate");
}

#[test]
fn fixed_kind_without_fields_or_appendage_still_rejected() {
    let yaml = r#"
venue: toyv
protocol: toyp
doc_version: "1.0.0"
endianness: be
kinds:
  - name: Empty
    byte: "E"
    byte_len: 1
"#;
    let schema: SchemaFile = serde_yaml_ng::from_str(yaml).unwrap();
    assert!(matches!(
        schema.validate().unwrap_err(),
        SchemaError::VariableConflict { .. }
    ));
}
