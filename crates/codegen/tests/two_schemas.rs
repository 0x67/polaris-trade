//! Two schemas sharing a venue but different protocols (and colliding wire bytes)
//! must generate into disjoint sibling modules, and the write-mode venue-crate gate
//! must refuse to write when the venue crate shell does not exist yet.

use std::{collections::HashSet, path::PathBuf};

use proto_codegen::{
    cli::{ensure_venue_crate_exists, venue_crate_dir, write_outputs},
    emit::{generate, module_name},
    schema::{Endianness, Field, Kind, SchemaError, SchemaFile, TypeClass},
};

fn toy_schema(protocol: &str) -> SchemaFile {
    SchemaFile {
        venue: "toyv".to_string(),
        protocol: protocol.to_string(),
        doc_version: "1.0.0".to_string(),
        major: None,
        endianness: Endianness::Be,
        kinds: vec![Kind {
            name: "Thing".to_string(),
            byte: b'A',
            byte_len: Some(2),
            variable: false,
            since: None,
            defect: None,
            doc: None,
            fields: vec![Field {
                name: "v".to_string(),
                width: 1,
                class: TypeClass::Code,
                doc: None,
            }],
            direction: None,
            appendage: Vec::new(),
            appendage_optional: false,
        }],
        appendage_tags: Vec::new(),
    }
}

#[test]
fn toy_schemas_coexist() {
    let alpha = toy_schema("alpha");
    let beta = toy_schema("beta");

    // same venue crate, distinct module namespace, despite the colliding wire byte 'A'.
    assert_eq!(venue_crate_dir(&alpha), venue_crate_dir(&beta));
    let ns_alpha = module_name(&alpha).unwrap();
    let ns_beta = module_name(&beta).unwrap();
    assert_ne!(ns_alpha, ns_beta);

    let files_alpha = generate(
        &alpha,
        &venue_crate_dir(&alpha),
        "schemas/toyv/alpha/v1.yaml",
        b"alpha bytes",
    )
    .unwrap();
    let files_beta = generate(
        &beta,
        &venue_crate_dir(&beta),
        "schemas/toyv/beta/v1.yaml",
        b"beta bytes",
    )
    .unwrap();

    let paths_alpha: HashSet<PathBuf> = files_alpha.iter().map(|f| f.rel_path.clone()).collect();
    let paths_beta: HashSet<PathBuf> = files_beta.iter().map(|f| f.rel_path.clone()).collect();
    assert!(
        paths_alpha.is_disjoint(&paths_beta),
        "rel_path collision between sibling protocols: {:?}",
        paths_alpha.intersection(&paths_beta).collect::<Vec<_>>()
    );

    // every output path is namespaced under its own module: zero shared generated symbols.
    for f in &files_alpha {
        assert!(f.rel_path.to_string_lossy().contains(&ns_alpha));
    }
    for f in &files_beta {
        assert!(f.rel_path.to_string_lossy().contains(&ns_beta));
    }
}

#[test]
fn missing_venue_crate_errors() {
    let tmp = std::env::temp_dir().join(format!(
        "proto-codegen-two-schemas-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).expect("create tempdir repo root");

    let schema = toy_schema("alpha");

    // no crates/venues/toyv under tmp yet: the write-mode gate must refuse.
    let err = ensure_venue_crate_exists(&tmp, &schema).unwrap_err();
    assert!(matches!(err, SchemaError::MissingVenueCrate { .. }));

    // once the venue crate shell exists, the same gate passes and write_outputs lands bytes.
    std::fs::create_dir_all(tmp.join("crates/venues/toyv")).unwrap();
    ensure_venue_crate_exists(&tmp, &schema).expect("venue crate now present");

    let files = generate(
        &schema,
        &venue_crate_dir(&schema),
        "schemas/toyv/alpha/v1.yaml",
        b"alpha bytes",
    )
    .unwrap();
    write_outputs(&tmp, &files).expect("write generated files");
    for f in &files {
        let written = std::fs::read(tmp.join(&f.rel_path)).expect("file written");
        assert_eq!(written, f.contents.as_bytes());
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
