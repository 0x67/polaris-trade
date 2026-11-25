//! Two `generate()` runs over identical schema input produce byte-identical output.

use std::path::Path;

use proto_codegen::{
    emit::generate,
    schema::{Endianness, Field, Kind, SchemaFile, TypeClass},
};

fn toy_schema() -> SchemaFile {
    SchemaFile {
        venue: "toyv".to_string(),
        protocol: "toyp".to_string(),
        doc_version: "1.0.0".to_string(),
        major: None,
        endianness: Endianness::Be,
        kinds: vec![
            Kind {
                name: "Mixed".to_string(),
                byte: b'M',
                byte_len: Some(9),
                variable: false,
                since: None,
                defect: None,
                doc: None,
                fields: vec![
                    Field {
                        name: "seq".to_string(),
                        width: 4,
                        class: TypeClass::U32,
                        doc: None,
                    },
                    Field {
                        name: "flag".to_string(),
                        width: 1,
                        class: TypeClass::Code,
                        doc: None,
                    },
                    Field {
                        name: "tag".to_string(),
                        width: 3,
                        class: TypeClass::Alpha,
                        doc: None,
                    },
                ],
                direction: None,
                appendage: Vec::new(),
                appendage_optional: false,
            },
            Kind {
                name: "News".to_string(),
                byte: b'N',
                byte_len: None,
                variable: true,
                since: None,
                defect: None,
                doc: None,
                fields: Vec::new(),
                direction: None,
                appendage: Vec::new(),
                appendage_optional: false,
            },
        ],
        appendage_tags: Vec::new(),
    }
}

#[test]
fn same_input_same_bytes() {
    let schema = toy_schema();
    let bytes = b"toy schema fixture bytes";

    let run1 = generate(&schema, Path::new("crates/toyv"), "toy.yaml", bytes).expect("generates");
    let run2 = generate(&schema, Path::new("crates/toyv"), "toy.yaml", bytes).expect("generates");

    assert_eq!(run1.len(), run2.len());
    for (a, b) in run1.iter().zip(run2.iter()) {
        assert_eq!(a.rel_path, b.rel_path);
        assert_eq!(a.contents, b.contents);
    }
}
