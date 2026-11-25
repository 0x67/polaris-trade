use super::*;
use crate::{
    cli::venue_crate_dir,
    schema::{AppendageTag, Field, Kind, TypeClass},
};

/// crate dir the toy schemas generate into; tests locate files by suffix, not by it.
const TOY_CRATE: &str = "crates/toyv";

fn toy_schema(
    venue: &str,
    protocol: &str,
    doc_version: &str,
    endianness: Endianness,
    kinds: Vec<Kind>,
) -> SchemaFile {
    SchemaFile {
        venue: venue.to_string(),
        protocol: protocol.to_string(),
        doc_version: doc_version.to_string(),
        major: None,
        endianness,
        appendage_tags: Vec::new(),
        kinds,
    }
}

fn code_field(name: &str) -> Field {
    Field {
        name: name.to_string(),
        width: 1,
        class: TypeClass::Code,
        doc: None,
    }
}

fn fixed_kind(name: &str, byte: u8, byte_len: u16, fields: Vec<Field>) -> Kind {
    Kind {
        name: name.to_string(),
        byte,
        byte_len: Some(byte_len),
        variable: false,
        since: None,
        defect: None,
        doc: None,
        direction: None,
        appendage: Vec::new(),
        appendage_optional: false,
        fields,
    }
}

fn variable_kind(name: &str, byte: u8) -> Kind {
    Kind {
        name: name.to_string(),
        byte,
        byte_len: None,
        variable: true,
        since: None,
        defect: None,
        doc: None,
        direction: None,
        appendage: Vec::new(),
        appendage_optional: false,
        fields: Vec::new(),
    }
}

fn with_doc(mut kind: Kind, doc: &str) -> Kind {
    kind.doc = Some(doc.to_string());
    kind
}

fn with_meta(mut kind: Kind, since: &str, defect: &str) -> Kind {
    kind.since = Some(since.to_string());
    kind.defect = Some(defect.to_string());
    kind
}

fn with_direction(mut kind: Kind, dir: Direction) -> Kind {
    kind.direction = Some(dir);
    kind
}

fn with_appendage(mut kind: Kind, tags: &[&str], optional: bool) -> Kind {
    kind.appendage = tags.iter().map(|t| t.to_string()).collect();
    kind.appendage_optional = optional;
    kind
}

fn toy_appendage_tag(tag: u8, name: &str, width: u16, class: TypeClass) -> AppendageTag {
    AppendageTag {
        tag,
        name: name.to_string(),
        width,
        class,
        doc: None,
    }
}

/// same shape as `toy_schema` but with a caller-supplied `appendage_tags` registry,
/// for direction/appendage tests that must not touch every other `toy_schema` call site.
fn directional_toy_schema(kinds: Vec<Kind>, appendage_tags: Vec<AppendageTag>) -> SchemaFile {
    SchemaFile {
        venue: "toyv".to_string(),
        protocol: "toyp".to_string(),
        doc_version: "1.0.0".to_string(),
        major: None,
        endianness: Endianness::Be,
        appendage_tags,
        kinds,
    }
}

// one numeric + one code + one alpha + one variable kind, widths sum to byte_len 16.
fn toy_schema_full() -> SchemaFile {
    toy_schema(
        "toyv",
        "toyp",
        "1.2.0",
        Endianness::Be,
        vec![
            fixed_kind(
                "Mixed",
                b'M',
                16,
                vec![
                    Field {
                        name: "seq".to_string(),
                        width: 4,
                        class: TypeClass::U32,
                        doc: None,
                    },
                    code_field("side"),
                    Field {
                        name: "symbol".to_string(),
                        width: 8,
                        class: TypeClass::Alpha,
                        doc: None,
                    },
                    Field {
                        name: "qty".to_string(),
                        width: 2,
                        class: TypeClass::U16,
                        doc: None,
                    },
                ],
            ),
            variable_kind("News", b'N'),
        ],
    )
}

fn find(files: &[GeneratedFile], suffix: &str) -> GeneratedFile {
    files
        .iter()
        .find(|f| f.rel_path.ends_with(suffix))
        .unwrap_or_else(|| panic!("no generated file ending in {suffix}"))
        .clone()
}

fn assert_no_banned_tokens(files: &[GeneratedFile]) {
    let banned = [
        concat!("RE", "Q-"),
        concat!("TA", "SK-"),
        concat!("A", "C-"),
    ];
    for f in files {
        for token in banned {
            assert!(
                !f.contents.contains(token),
                "banned token {token} in {:?}",
                f.rel_path
            );
        }
        assert!(
            !f.contents.contains('\u{2014}'),
            "em dash in {:?}",
            f.rel_path
        );
    }
}

#[test]
fn venue_and_module_derivation() {
    let toy_v1 = toy_schema("toy", "itch", "1.16.0", Endianness::Be, vec![]);
    assert_eq!(venue_crate_dir(&toy_v1), PathBuf::from("crates/venues/toy"));
    assert_eq!(module_name(&toy_v1).unwrap(), "itch_v1");

    let toy_v4 = toy_schema("toy", "itch", "4.0", Endianness::Be, vec![]);
    assert_eq!(module_name(&toy_v4).unwrap(), "itch_v4");
}

#[test]
fn provenance_header_present() {
    let schema = toy_schema_full();
    let schema_bytes = b"toy schema fixture bytes";
    let files = generate(
        &schema,
        Path::new(TOY_CRATE),
        "schemas/toy/toy/v1.yaml",
        schema_bytes,
    )
    .expect("generates");
    assert!(!files.is_empty());

    let expected_hash = to_hex(&Sha256::digest(schema_bytes));
    assert_eq!(expected_hash.len(), 64);
    assert!(
        expected_hash
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );

    for f in &files {
        assert!(
            f.contents.starts_with("//! GENERATED by proto-codegen."),
            "{:?}",
            f.rel_path
        );
        assert!(f.contents.contains("Do not edit."));
        assert!(f.contents.contains(&format!(
            "schema: schemas/toy/toy/v1.yaml sha256:{expected_hash}"
        )));
    }
}

#[test]
fn message_type_matches_schema() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![
            fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
            fixed_kind("Lower", b'd', 2, vec![code_field("v")]),
            fixed_kind("Beta", b'B', 3, vec![code_field("v"), code_field("w")]),
            variable_kind("News", b'N'),
        ],
    );
    let files =
        generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").expect("generates");
    let mt = find(&files, "message_type.rs");

    assert!(mt.contents.contains("Alpha = b'A',"));
    assert!(mt.contents.contains("Lower = b'd',"));
    assert!(mt.contents.contains("Beta = b'B',"));
    assert!(mt.contents.contains("News = b'N',"));

    assert!(mt.contents.contains("table[b'A' as usize] = nz(2);"));
    assert!(mt.contents.contains("table[b'd' as usize] = nz(2);"));
    assert!(mt.contents.contains("table[b'B' as usize] = nz(3);"));
    assert!(mt.contents.contains("stays None: variable length on wire"));
    assert!(!mt.contents.contains("table[b'N' as usize]"));
}

#[test]
fn le_schema_unimplemented() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Le,
        vec![fixed_kind("Alpha", b'A', 2, vec![code_field("v")])],
    );
    let err = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap_err();
    assert!(matches!(err, SchemaError::LeUnimplemented));
}

#[test]
fn generated_text_compiles_shape_sanity() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let mt = find(&files, "message_type.rs");
    for needle in ["#[repr(u8)]", "from_byte", "as_byte", "BYTE_LEN"] {
        assert!(mt.contents.contains(needle), "missing {needle}");
    }
}

#[test]
fn no_banned_tokens_in_generated_output() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert_no_banned_tokens(&files);
}

#[test]
fn view_accessor_offsets_accumulate() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind(
            "Mixed",
            b'M',
            21,
            vec![
                Field {
                    name: "seq".to_string(),
                    width: 4,
                    class: TypeClass::U32,
                    doc: None,
                },
                Field {
                    name: "big".to_string(),
                    width: 8,
                    class: TypeClass::I64,
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
                Field {
                    name: "tail".to_string(),
                    width: 4,
                    class: TypeClass::I32,
                    doc: None,
                },
            ],
        )],
    );
    let files =
        generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").expect("generates");
    let view = find(&files, "view/mixed.rs");

    assert!(
        view.contents
            .contains("u32::from_be_bytes(self.0[1..5].try_into().unwrap())")
    );
    assert!(
        view.contents
            .contains("i64::from_be_bytes(self.0[5..13].try_into().unwrap())")
    );
    assert!(
        view.contents
            .contains("pub fn flag(&self) -> u8 {\n        self.0[13]\n    }")
    );
    assert!(
        view.contents
            .contains("pub fn tag(&self) -> &'a [u8] {\n        &self.0[14..17]\n    }")
    );
    assert!(
        view.contents
            .contains("i32::from_be_bytes(self.0[17..21].try_into().unwrap())")
    );
}

#[test]
fn be_emits_from_be_bytes() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/mixed.rs");
    assert!(view.contents.contains("from_be_bytes"));
    assert!(!view.contents.contains("from_le_bytes"));
}

#[test]
fn variable_kind_gets_no_view_struct() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![
            fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
            variable_kind("News", b'N'),
        ],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let alpha_view = find(&files, "view/alpha.rs");
    assert!(alpha_view.contents.contains("pub struct AlphaView"));

    // variable kind gets no per-kind file, and NewsView never appears under view/.
    assert!(!files.iter().any(|f| f.rel_path.ends_with("view/news.rs")));
    for f in &files {
        if f.rel_path.components().any(|c| c.as_os_str() == "view") {
            assert!(!f.contents.contains("NewsView"));
        }
    }
}

#[test]
fn parse_shape_matches_spike() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind(
            "AddOrder",
            b'A',
            31,
            vec![
                Field {
                    name: "elapsed_nanos".to_string(),
                    width: 4,
                    class: TypeClass::U32,
                    doc: None,
                },
                Field {
                    name: "order_number".to_string(),
                    width: 8,
                    class: TypeClass::I64,
                    doc: None,
                },
                Field {
                    name: "order_verb".to_string(),
                    width: 1,
                    class: TypeClass::Code,
                    doc: None,
                },
                Field {
                    name: "quantity".to_string(),
                    width: 8,
                    class: TypeClass::I64,
                    doc: None,
                },
                Field {
                    name: "orderbook".to_string(),
                    width: 4,
                    class: TypeClass::I32,
                    doc: None,
                },
                Field {
                    name: "price".to_string(),
                    width: 4,
                    class: TypeClass::I32,
                    doc: None,
                },
                Field {
                    name: "domicile".to_string(),
                    width: 1,
                    class: TypeClass::Code,
                    doc: None,
                },
            ],
        )],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/add_order.rs");

    assert!(
        view.contents
            .contains("pub struct AddOrderView<'a>(&'a [u8; 31]);")
    );
    assert!(view.contents.contains("pub const SIZE: usize = 31;"));
    assert!(
        view.contents
            .contains("data.try_into().map(Self).map_err(|_| WrongLength {")
    );
}

#[test]
fn dispatch_arms_cover_fixed_and_variable_kinds() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let dispatch = find(&files, "dispatch.rs");

    assert!(
        dispatch
            .contents
            .contains("fn mixed(&mut self, v: MixedView<'_>);")
    );
    assert!(
        dispatch
            .contents
            .contains("b'M' => dispatch_mixed(data, v),")
    );
    assert!(
        dispatch
            .contents
            .contains("table[b'M' as usize] = Some(dispatch_mixed::<V>);")
    );
    assert!(dispatch.contents.contains("_ => v.cold(msg_type, data),"));

    assert!(
        dispatch
            .contents
            .contains("fn news(&mut self, _data: &[u8]) {}")
    );
    assert!(
        dispatch
            .contents
            .contains("b'N' => dispatch_news(data, v),")
    );
    assert!(
        dispatch
            .contents
            .contains("table[b'N' as usize] = Some(dispatch_news::<V>);")
    );
}

#[test]
fn visitor_method_names_snake_case() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind("AddOrder", b'A', 2, vec![code_field("v")])],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let dispatch = find(&files, "dispatch.rs");

    assert!(
        dispatch
            .contents
            .contains("fn add_order(&mut self, v: AddOrderView<'_>);")
    );
    assert!(
        dispatch
            .contents
            .contains("fn dispatch_add_order<V: HotVisitor>(data: &[u8], v: &mut V) {")
    );
}

#[test]
fn parity_text_references_every_kind() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");

    assert!(parity.contents.contains("mod support;"));
    assert!(parity.contents.contains("fn parity_mixed() {"));
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::decode_owned(b'M', &row)")
    );
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::OwnedMessage::Mixed(m) => m,")
    );
    assert!(
        !parity.contents.contains("support::owned::"),
        "un-namespaced baseline path leaked"
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(v.seq(), owned.seq, \"seq\");")
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(v.side(), owned.side, \"side\");")
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(v.symbol(), &owned.symbol[..], \"symbol\");")
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(v.qty(), owned.qty, \"qty\");")
    );

    // variable kind gets no parity test.
    assert!(!parity.contents.contains("fn parity_news"));
}

#[test]
fn bench_text_folds_every_field() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let bench = find(&files, "per_kind_toyp_v1.rs");

    assert!(
        bench
            .contents
            .contains("fn view_mixed(row: &[u8]) -> i64 {")
    );
    assert!(
        bench
            .contents
            .contains("acc = acc.wrapping_add(v.seq() as i64);")
    );
    assert!(
        bench
            .contents
            .contains("acc = acc.wrapping_add(v.side() as i64);")
    );
    assert!(
        bench
            .contents
            .contains("acc = fold_bytes(acc, v.symbol());")
    );
    assert!(
        bench
            .contents
            .contains("acc = acc.wrapping_add(v.qty() as i64);")
    );
    assert!(bench.contents.contains("black_box(view_mixed(&row))"));
    assert!(bench.contents.contains("criterion_main!(benches);"));

    assert!(!bench.contents.contains("fn view_news"));
}

// i64 accessor already returns i64; emitted fold must not cast (clippy unnecessary_cast).
#[test]
fn bench_i64_field_folds_without_cast() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind(
            "Big",
            b'B',
            9,
            vec![Field {
                name: "order_number".to_string(),
                width: 8,
                class: TypeClass::I64,
                doc: None,
            }],
        )],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let bench = find(&files, "per_kind_toyp_v1.rs");
    assert!(
        bench
            .contents
            .contains("acc = acc.wrapping_add(v.order_number());")
    );
    assert!(!bench.contents.contains("v.order_number() as i64"));
    // synthetic row builder emits iterator loop, not indexed range loop.
    assert!(
        bench
            .contents
            .contains("row.iter_mut().enumerate().skip(1)")
    );
}

#[test]
fn view_split_file_list() {
    let schema = toy_schema_full(); // Mixed (fixed) + News (variable)
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    let fixed_count = schema.kinds.iter().filter(|k| !k.variable).count();
    assert_eq!(files.len(), 6 + fixed_count);

    assert!(files.iter().any(|f| f.rel_path.ends_with("view/mod.rs")));
    assert!(files.iter().any(|f| f.rel_path.ends_with("view/mixed.rs")));
    assert!(!files.iter().any(|f| f.rel_path.ends_with("view/news.rs")));

    // no plain view.rs file anywhere: the split replaces it entirely.
    assert!(
        !files
            .iter()
            .any(|f| f.rel_path.file_name().and_then(|n| n.to_str()) == Some("view.rs"))
    );
}

#[test]
fn view_mod_private_mods_pub_use() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view_mod = find(&files, "view/mod.rs");

    assert!(view_mod.contents.contains("mod mixed;"));
    assert!(!view_mod.contents.contains("pub mod mixed;"));
    assert!(view_mod.contents.contains("pub use mixed::MixedView;"));
}

#[test]
fn view_kind_self_contained() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let mixed_view = find(&files, "view/mixed.rs");

    assert!(
        mixed_view
            .contents
            .starts_with("//! GENERATED by proto-codegen.")
    );
    assert!(
        mixed_view
            .contents
            .contains("use proto_common::WrongLength;")
    );
}

#[test]
fn dispatch_and_mod_paths_unchanged() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let ns = module_name(&schema).unwrap();

    // top-level mod.rs, disambiguated from view/mod.rs by the ns segment.
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    assert!(mod_rs.contents.contains("pub use view::*;"));

    let dispatch = find(&files, "dispatch.rs");
    assert!(dispatch.contents.contains("use super::view::{"));
}

#[test]
fn owned_module_dir_value() {
    let toy_v1 = toy_schema("toy", "itch", "1.16.0", Endianness::Be, vec![]);
    assert_eq!(
        owned_module_dir(&toy_v1, Path::new("crates/toy")).unwrap(),
        PathBuf::from("crates/toy/src/itch_v1")
    );

    let schema = toy_schema_full();
    let owned = owned_module_dir(&schema, Path::new(TOY_CRATE)).unwrap();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    let parity = find(&files, "parity_toyp_v1.rs");
    let bench = find(&files, "per_kind_toyp_v1.rs");
    assert!(!parity.rel_path.starts_with(&owned));
    assert!(!bench.rel_path.starts_with(&owned));
}

#[test]
fn generate_honors_crate_dir() {
    // a layout other than crates/<venue>: every path family must follow the caller
    let crate_dir = Path::new("crates/venues/toyv");
    let files = generate(&toy_schema_full(), crate_dir, "toy.yaml", b"toy bytes").unwrap();
    for suffix in [
        "message_type.rs",
        "parity_toyp_v1.rs",
        "per_kind_toyp_v1.rs",
    ] {
        find(&files, suffix);
    }
    for f in &files {
        assert!(
            f.rel_path.starts_with(crate_dir),
            "{} outside {}",
            f.rel_path.display(),
            crate_dir.display()
        );
    }
}

#[test]
fn doc_lines_rendering() {
    // multi-line, empty interior line -> bare `///`.
    assert_eq!(
        doc_lines("first\n\nthird", ""),
        "/// first\n///\n/// third\n"
    );

    // trailing whitespace on a line trimmed; single trailing newline (YAML block
    // scalar artifact) ignored entirely.
    assert_eq!(
        doc_lines("line one   \nline two\n", ""),
        "/// line one\n/// line two\n"
    );

    // multiple trailing blank lines collapse away too, not just one.
    assert_eq!(doc_lines("only line\n\n\n", ""), "/// only line\n");

    // indent applies to every rendered line, bare separators included.
    assert_eq!(
        doc_lines("a\n\nb", "    "),
        "    /// a\n    ///\n    /// b\n"
    );
}

#[test]
fn kind_doc_on_view_struct() {
    let kind = with_doc(
        fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
        "first line\nsecond line",
    );
    let schema = toy_schema("toyv", "toyp", "1.0.0", Endianness::Be, vec![kind]);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/alpha.rs");

    let expected_block = "/// `A` Alpha, 2B.\n\
///\n\
/// first line\n\
/// second line\n\
pub struct AlphaView<'a>(&'a [u8; 2]);\n";
    assert!(view.contents.contains(expected_block), "{}", view.contents);
}

#[test]
fn kind_doc_on_message_type_variant() {
    let fixed = with_doc(
        fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
        "alpha kind doc",
    );
    // variable kinds carry no view/visitor site, but the variant site must still render.
    let variable = with_doc(variable_kind("News", b'N'), "variable-length news item");

    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed, variable],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let mt = find(&files, "message_type.rs");

    assert!(
        mt.contents
            .contains("    /// alpha kind doc\n    Alpha = b'A',\n")
    );
    assert!(
        mt.contents
            .contains("    /// variable-length news item\n    News = b'N',\n")
    );
}

#[test]
fn kind_doc_on_visitor_method() {
    let fixed = with_doc(
        fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
        "alpha visitor doc",
    );
    let schema = toy_schema("toyv", "toyp", "1.0.0", Endianness::Be, vec![fixed]);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let dispatch = find(&files, "dispatch.rs");

    assert!(
        dispatch
            .contents
            .contains("    /// alpha visitor doc\n    fn alpha(&mut self, v: AlphaView<'_>);\n")
    );
}

#[test]
fn field_doc_on_accessor() {
    let kind = fixed_kind(
        "Mixed2",
        b'X',
        13,
        vec![
            Field {
                name: "seq".to_string(),
                width: 4,
                class: TypeClass::U32,
                doc: Some("sequence number, wraps at u32::MAX".to_string()),
            },
            Field {
                name: "symbol".to_string(),
                width: 8,
                class: TypeClass::Alpha,
                doc: Some("untrimmed ticker".to_string()),
            },
        ],
    );
    let schema = toy_schema("toyv", "toyp", "1.0.0", Endianness::Be, vec![kind]);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/mixed2.rs");

    assert!(view.contents.contains(
        "    /// sequence number, wraps at u32::MAX\n    #[inline]\n    pub fn seq(&self) -> u32 {"
    ));
    assert!(view.contents.contains(
        "    /// untrimmed ticker\n    #[inline]\n    pub fn symbol(&self) -> &'a [u8] {"
    ));
}

#[test]
fn docless_schema_byte_identical() {
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind("Alpha", b'A', 2, vec![code_field("v")])],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/alpha.rs");

    let hash = to_hex(&Sha256::digest(b"toy bytes"));
    let mut expected = String::new();
    expected.push_str("//! GENERATED by proto-codegen. Do not edit.\n");
    expected.push_str(&format!("//! schema: toy.yaml sha256:{hash}\n"));
    expected.push_str("//!\n");
    expected.push_str("use proto_common::WrongLength;\n\n");
    expected.push_str("/// `A` Alpha, 2B.\n");
    expected.push_str("pub struct AlphaView<'a>(&'a [u8; 2]);\n\n");
    expected.push_str("impl<'a> AlphaView<'a> {\n");
    expected.push_str("    pub const SIZE: usize = 2;\n\n");
    expected.push_str("    #[inline]\n");
    expected.push_str("    pub fn parse(data: &'a [u8]) -> Result<Self, WrongLength> {\n");
    expected.push_str("        data.try_into().map(Self).map_err(|_| WrongLength {\n");
    expected.push_str("            expected: Self::SIZE,\n");
    expected.push_str("            got: data.len(),\n");
    expected.push_str("        })\n    }\n\n");
    expected.push_str("    #[inline]\n    pub fn v(&self) -> u8 {\n        self.0[1]\n    }\n\n");
    expected.push_str("}\n\n");

    assert_eq!(view.contents, expected);
}

fn u48_toy_schema() -> SchemaFile {
    toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind(
            "Stamp",
            b'Z',
            8,
            vec![
                Field {
                    name: "timestamp".to_string(),
                    width: 6,
                    class: TypeClass::U48,
                    doc: None,
                },
                code_field("v"),
            ],
        )],
    )
}

#[test]
fn u48_accessor_padded_read() {
    let files = generate(
        &u48_toy_schema(),
        Path::new(TOY_CRATE),
        "toy.yaml",
        b"toy bytes",
    )
    .unwrap();
    let view = find(&files, "view/stamp.rs");
    assert!(view.contents.contains("pub fn timestamp(&self) -> u64 {"));
    assert!(view.contents.contains("let mut buf = [0u8; 8];"));
    assert!(
        view.contents
            .contains("buf[2..8].copy_from_slice(&self.0[1..7]);")
    );
    assert!(view.contents.contains("u64::from_be_bytes(buf)"));
    assert!(view.contents.contains("#[inline]"));
}

#[test]
fn u48_parity_default_arm() {
    let files = generate(
        &u48_toy_schema(),
        Path::new(TOY_CRATE),
        "toy.yaml",
        b"toy bytes",
    )
    .unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");
    assert!(
        parity
            .contents
            .contains("assert_eq!(v.timestamp(), owned.timestamp, \"timestamp\");")
    );
}

#[test]
fn u48_bench_default_fold() {
    let files = generate(
        &u48_toy_schema(),
        Path::new(TOY_CRATE),
        "toy.yaml",
        b"toy bytes",
    )
    .unwrap();
    let bench = find(&files, "per_kind_toyp_v1.rs");
    assert!(
        bench
            .contents
            .contains("acc = acc.wrapping_add(v.timestamp() as i64);")
    );
}

#[test]
fn since_defect_never_emitted() {
    let marker = "internal issue 9142 duplicate seq on replay";
    let kind = with_meta(
        fixed_kind("Alpha", b'A', 2, vec![code_field("v")]),
        "9.9.9",
        marker,
    );
    let schema = toy_schema("toyv", "toyp", "1.0.0", Endianness::Be, vec![kind]);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    for f in &files {
        assert!(
            !f.contents.contains("9.9.9"),
            "since leaked in {:?}",
            f.rel_path
        );
        assert!(
            !f.contents.contains(marker),
            "defect leaked in {:?}",
            f.rel_path
        );
    }
}

#[test]
fn to_exchange_absent_from_recv_surface() {
    let enter = with_direction(
        fixed_kind("EnterOrder", b'O', 2, vec![code_field("v")]),
        Direction::ToExchange,
    );
    let accepted = with_direction(
        fixed_kind("OrderAccepted", b'A', 2, vec![code_field("v")]),
        Direction::FromExchange,
    );
    let schema = directional_toy_schema(vec![enter, accepted], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    let mt = find(&files, "message_type.rs");
    assert!(mt.contents.contains("OrderAccepted = b'A',"));
    assert!(!mt.contents.contains("EnterOrder = b'O',"));
    assert!(mt.contents.contains("table[b'A' as usize]"));
    assert!(!mt.contents.contains("table[b'O' as usize]"));

    let dispatch = find(&files, "dispatch.rs");
    assert!(
        dispatch
            .contents
            .contains("fn order_accepted(&mut self, v: OrderAcceptedView<'_>);")
    );
    assert!(!dispatch.contents.contains("fn enter_order"));
    assert!(!dispatch.contents.contains("EnterOrderView"));
    assert!(!dispatch.contents.contains("b'O' =>"));

    assert!(
        files
            .iter()
            .any(|f| f.rel_path.ends_with("view/order_accepted.rs"))
    );
    assert!(
        !files
            .iter()
            .any(|f| f.rel_path.ends_with("view/enter_order.rs"))
    );
}

#[test]
fn directional_schema_emits_split_parity_and_bench() {
    let enter = with_direction(
        fixed_kind("EnterOrder", b'O', 2, vec![code_field("v")]),
        Direction::ToExchange,
    );
    let accepted = with_direction(
        fixed_kind("OrderAccepted", b'A', 2, vec![code_field("v")]),
        Direction::FromExchange,
    );
    let schema = directional_toy_schema(vec![enter, accepted], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    let parity = find(&files, "parity_toyp_v1.rs");
    assert!(parity.contents.contains("mod support;"));
    // from_exchange kind: split entry point, no byte param.
    assert!(parity.contents.contains("fn parity_order_accepted() {"));
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::decode_from_exchange(&row)")
    );
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::OwnedMessage::OrderAccepted(m) => m,")
    );
    // to_exchange kind: builder-driven, decode_to_exchange, no view/BYTE_LEN trace.
    assert!(parity.contents.contains("fn parity_enter_order_build() {"));
    assert!(parity.contents.contains("EnterOrderBuilder::new(&mut buf)"));
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::decode_to_exchange(&buf[..n])")
    );
    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::OwnedMessage::EnterOrder(m) => m,")
    );
    // un-namespaced feed-style baseline call never leaks into the directional path.
    assert!(!parity.contents.contains("decode_owned("));

    let bench = find(&files, "per_kind_toyp_v1.rs");
    assert!(
        bench
            .contents
            .contains("fn view_order_accepted(row: &[u8]) -> i64 {")
    );
    assert!(bench.contents.contains("\"enter_order_build\""));
    assert!(!bench.contents.contains("fn view_enter_order"));
    assert!(bench.contents.contains("EnterOrderBuilder::new(&mut buf)"));
    assert!(
        bench.contents.contains(
            "let n = bb.finish();\n            black_box(&buf);\n            black_box(n)"
        )
    );
}

#[test]
fn directionless_schema_parity_and_bench_byte_identical() {
    // guards the dispatcher split in emit_parity_tests/emit_bench_stubs: a
    // directionless schema must still take the unchanged code path.
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");
    let bench = find(&files, "per_kind_toyp_v1.rs");

    assert!(
        parity
            .contents
            .contains("support::owned_toyp_v1::decode_owned(b'M', &row)")
    );
    assert!(!parity.contents.contains("decode_from_exchange"));
    assert!(!parity.contents.contains("decode_to_exchange"));
    assert!(
        bench
            .contents
            .contains("fn view_mixed(row: &[u8]) -> i64 {")
    );
    assert!(!bench.contents.contains("_build"));
}

#[test]
fn from_exchange_appendage_parity_has_empty_full_bare_variants() {
    let optional_kind = appendage_kind("OrderCanceled", b'C', true);
    let mandatory_kind = appendage_kind("OrderAccepted", b'A', false);
    let schema = directional_toy_schema(vec![optional_kind, mandatory_kind], appendage_registry());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");

    // dual-form kind: all three variants present.
    assert!(
        parity
            .contents
            .contains("fn parity_order_canceled_empty() {")
    );
    assert!(
        parity
            .contents
            .contains("fn parity_order_canceled_full() {")
    );
    assert!(
        parity
            .contents
            .contains("fn parity_order_canceled_bare() {")
    );
    // mandatory-appendage kind: empty/full present, no bare variant.
    assert!(
        parity
            .contents
            .contains("fn parity_order_accepted_empty() {")
    );
    assert!(
        parity
            .contents
            .contains("fn parity_order_accepted_full() {")
    );
    assert!(
        !parity
            .contents
            .contains("fn parity_order_accepted_bare() {")
    );

    // empty variant zeroes the length field on a full-byte_len row.
    assert!(
            parity
                .contents
                .contains("let mut row = synthetic_row(b'A', 7);\n    let n = row.len();\n    row[n - 2] = 0;\n    row[n - 1] = 0;")
        );
    // full variant: every registered tag appended, typed and raw tail both checked.
    assert!(
        parity
            .contents
            .contains("append_tag(&mut row, 1, &tag_firm_bytes);")
    );
    assert!(
        parity
            .contents
            .contains("append_tag(&mut row, 2, &tag_min_qty_bytes);")
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(got, owned.appendage, \"appendage tail\");")
    );
    assert!(parity.contents.contains(
        "assert_eq!(tags.next(), Some(AppendageTag::Firm(&tag_firm_bytes)), \"firm typed\");"
    ));
    assert!(parity.contents.contains(
        "assert_eq!(tags.next(), Some(AppendageTag::MinQty(tag_min_qty_v)), \"min_qty typed\");"
    ));
    assert!(
        parity
            .contents
            .contains("assert_eq!(tags.next(), None, \"no extra tags\");")
    );

    // bare variant: fixed-len row only, no length field at all.
    assert!(
        parity
            .contents
            .contains("fn parity_order_canceled_bare() {\n    let row = synthetic_row(b'C', 5);")
    );
}

#[test]
fn to_exchange_appendage_parity_has_base_and_full_variants() {
    let kind = to_exchange_appendage_kind(
        "EnterOrder",
        b'O',
        7,
        vec![Field {
            name: "seq".to_string(),
            width: 4,
            class: TypeClass::U32,
            doc: None,
        }],
        &["firm", "min_qty"],
    );
    let registry = vec![
        toy_appendage_tag(1, "firm", 4, TypeClass::Alpha),
        toy_appendage_tag(2, "min_qty", 4, TypeClass::U32),
    ];
    let schema = directional_toy_schema(vec![kind], registry);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");

    assert!(parity.contents.contains("fn parity_enter_order_build() {"));
    assert!(
        parity
            .contents
            .contains("fn parity_enter_order_build_full() {")
    );
    assert!(
        parity
            .contents
            .contains("assert_eq!(owned.appendage.len(), 0, \"no tags appended\");")
    );
    assert!(parity.contents.contains("b.append_firm(&tag_firm_bytes);"));
    assert!(parity.contents.contains("b.append_min_qty(tag_min_qty_v);"));
    assert!(
        parity
            .contents
            .contains("assert_eq!(owned.appendage, expected_appendage, \"appendage tail\");")
    );
}

#[test]
fn to_exchange_builder_parity_drives_every_setter() {
    let kind = to_exchange_kind(
        "EnterOrder",
        b'O',
        7,
        vec![
            Field {
                name: "user_ref_num".to_string(),
                width: 4,
                class: TypeClass::U32,
                doc: None,
            },
            code_field("side"),
            Field {
                name: "qty".to_string(),
                width: 1,
                class: TypeClass::U8,
                doc: None,
            },
        ],
    );
    let schema = directional_toy_schema(vec![kind], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let parity = find(&files, "parity_toyp_v1.rs");

    let set_calls = parity.contents.matches("b.set_").count();
    // one setter call per field: user_ref_num, side, qty.
    assert_eq!(set_calls, 3);
    assert!(
        parity
            .contents
            .contains("b.set_user_ref_num(user_ref_num_v);")
    );
    assert!(parity.contents.contains("b.set_side(side_v);"));
    assert!(parity.contents.contains("b.set_qty(qty_v);"));
    assert!(
        parity
            .contents
            .contains("assert_eq!(owned.user_ref_num, user_ref_num_v, \"user_ref_num\");")
    );
}

#[test]
fn directional_parity_and_bench_no_banned_tokens() {
    let optional_kind = appendage_kind("OrderCanceled", b'C', true);
    let build_kind = to_exchange_appendage_kind(
        "EnterOrder",
        b'O',
        7,
        vec![Field {
            name: "seq".to_string(),
            width: 4,
            class: TypeClass::U32,
            doc: None,
        }],
        &["firm", "min_qty"],
    );
    let schema = directional_toy_schema(vec![optional_kind, build_kind], appendage_registry());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert_no_banned_tokens(&files);
}

fn appendage_registry() -> Vec<AppendageTag> {
    vec![
        toy_appendage_tag(1, "firm", 4, TypeClass::Alpha),
        toy_appendage_tag(2, "min_qty", 4, TypeClass::U32),
    ]
}

fn appendage_kind(name: &str, byte: u8, optional: bool) -> Kind {
    with_appendage(
        with_direction(
            fixed_kind(
                name,
                byte,
                7,
                vec![Field {
                    name: "seq".to_string(),
                    width: 4,
                    class: TypeClass::U32,
                    doc: None,
                }],
            ),
            Direction::FromExchange,
        ),
        &["firm", "min_qty"],
        optional,
    )
}

#[test]
fn appendage_view_parse_shape() {
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderAccepted", b'A', false)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/order_accepted.rs");

    assert!(view.contents.contains("fixed: &'a [u8; 5],"));
    assert!(view.contents.contains("pub const FIXED_LEN: usize = 5;"));
    assert!(view.contents.contains("if data.len() < 5 {"));
    assert!(
        view.contents
            .contains("let (fixed_bytes, rest) = data.split_at(5);")
    );
    assert!(view.contents.contains("if rest.len() < 2 {"));
    assert!(
        view.contents
            .contains("let len_field = u16::from_be_bytes([rest[0], rest[1]]);")
    );
    assert!(
        view.contents
            .contains("if len_field as usize != tail.len() {")
    );
    assert!(
        view.contents
            .contains("pub fn appendage(&self) -> AppendageIter<'a>")
    );
    assert!(view.contents.contains("pub fn tags(&self) -> TagIter<'a>"));
    assert!(
        view.contents
            .contains("u32::from_be_bytes(self.fixed[1..5].try_into().unwrap())")
    );
    // non-optional: no bare-form early return
    assert!(!view.contents.contains("rest.is_empty()"));
}

#[test]
fn appendage_optional_view_accepts_bare_form() {
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderCanceled", b'C', true)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/order_canceled.rs");

    assert!(view.contents.contains("if rest.is_empty() {"));
    assert!(
        view.contents
            .contains("return Some(Self { fixed, tail: &[] });")
    );
    assert!(view.contents.contains("Dual-form wire"));
}

#[test]
fn appendage_only_kind_view_has_no_field_accessors() {
    let kind = with_appendage(
        with_direction(
            fixed_kind("AccountQueryRequest", b'Q', 3, Vec::new()),
            Direction::FromExchange,
        ),
        &["user_ref_idx"],
        true,
    );
    let registry = vec![toy_appendage_tag(1, "user_ref_idx", 1, TypeClass::U8)];
    let schema = directional_toy_schema(vec![kind], registry);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/account_query_request.rs");

    assert!(view.contents.contains("pub const FIXED_LEN: usize = 1;"));
    assert!(!view.contents.contains("pub fn seq"));
    assert!(view.contents.contains("pub fn appendage(&self)"));
    assert!(view.contents.contains("pub fn tags(&self)"));
}

#[test]
fn appendage_dispatch_thunk_routes_none_to_cold() {
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderAccepted", b'A', false)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let dispatch = find(&files, "dispatch.rs");

    assert!(
        dispatch
            .contents
            .contains("match OrderAcceptedView::parse(data) {")
    );
    assert!(
        dispatch
            .contents
            .contains("Some(view) => v.order_accepted(view),")
    );
    assert!(dispatch.contents.contains("None => v.cold(b'A', data),"));
}

#[test]
fn appendage_rs_enum_content() {
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderAccepted", b'A', false)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let appendage = find(&files, "appendage.rs");

    assert!(appendage.contents.contains("pub enum AppendageTag<'a> {"));
    assert!(appendage.contents.contains("Firm(&'a [u8; 4]),"));
    assert!(appendage.contents.contains("MinQty(u32),"));
    assert!(
        appendage
            .contents
            .contains("Unknown { tag: u8, value: &'a [u8] },")
    );
    assert!(
        appendage
            .contents
            .contains("1 if value.len() == 4 => Self::Firm(value.try_into().unwrap()),")
    );
    assert!(appendage.contents.contains(
        "2 if value.len() == 4 => Self::MinQty(u32::from_be_bytes(value.try_into().unwrap())),"
    ));
    assert!(
        appendage
            .contents
            .contains("pub struct TagIter<'a>(AppendageIter<'a>);")
    );

    let ns = module_name(&schema).unwrap();
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    assert!(mod_rs.contents.contains("pub mod appendage;"));
    assert!(
        mod_rs
            .contents
            .contains("pub use appendage::{AppendageTag, TagIter};")
    );
}

#[test]
fn appendage_rs_absent_without_registry() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert!(!files.iter().any(|f| f.rel_path.ends_with("appendage.rs")));

    let ns = module_name(&schema).unwrap();
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    assert!(!mod_rs.contents.contains("appendage"));
}

#[test]
fn appendage_output_no_banned_tokens() {
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderAccepted", b'A', false)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert_no_banned_tokens(&files);
}

#[test]
fn directionless_schema_still_byte_identical_view() {
    // guards emit_view_kind's dispatch: unchanged fixed-view path for empty `appendage`.
    // byte-identity proof: crates/codegen/tests/drift.rs::committed_output_is_fresh
    let schema = toy_schema(
        "toyv",
        "toyp",
        "1.0.0",
        Endianness::Be,
        vec![fixed_kind("Alpha", b'A', 2, vec![code_field("v")])],
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let view = find(&files, "view/alpha.rs");
    assert!(
        view.contents
            .contains("pub struct AlphaView<'a>(&'a [u8; 2]);")
    );
    assert!(!view.contents.contains("fixed:"));
    assert!(!view.contents.contains("FIXED_LEN"));
}

// -- builder emission (workstream C) --

fn to_exchange_kind(name: &str, byte: u8, byte_len: u16, fields: Vec<Field>) -> Kind {
    with_direction(
        fixed_kind(name, byte, byte_len, fields),
        Direction::ToExchange,
    )
}

fn to_exchange_appendage_kind(
    name: &str,
    byte: u8,
    byte_len: u16,
    fields: Vec<Field>,
    tags: &[&str],
) -> Kind {
    with_appendage(
        with_direction(
            fixed_kind(name, byte, byte_len, fields),
            Direction::ToExchange,
        ),
        tags,
        false,
    )
}

#[test]
fn appendage_kind_byte_len_table_value_is_fixed_len_plus_two() {
    // appendage_kind: view's fixed_len (type byte + seq field) is 5; kind.byte_len
    // (declared 7) adds the synthesized 2-byte appendage length field on top.
    let schema = directional_toy_schema(
        vec![appendage_kind("OrderAccepted", b'A', false)],
        appendage_registry(),
    );
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let mt = find(&files, "message_type.rs");
    assert!(mt.contents.contains("table[b'A' as usize] = nz(7);"));
}

#[test]
fn build_emission_basic_shape() {
    let kind = to_exchange_kind(
        "EnterOrder",
        b'O',
        7,
        vec![
            Field {
                name: "user_ref_num".to_string(),
                width: 4,
                class: TypeClass::U32,
                doc: None,
            },
            code_field("side"),
            Field {
                name: "qty".to_string(),
                width: 1,
                class: TypeClass::U8,
                doc: None,
            },
        ],
    );
    let schema = directional_toy_schema(vec![kind], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let build = find(&files, "build/enter_order.rs");

    assert!(
        build
            .contents
            .contains("pub struct EnterOrderBuilder<'a> {")
    );
    assert!(build.contents.contains("buf: &'a mut [u8],"));
    assert!(build.contents.contains("pub const FIXED_LEN: usize = 7;"));
    assert!(
        build
            .contents
            .contains("pub fn new(buf: &'a mut [u8]) -> Self {")
    );
    assert!(build.contents.contains("buf[..7].fill(0);"));
    assert!(build.contents.contains("buf[0] = b'O';"));
    assert!(
        build
            .contents
            .contains("pub fn set_user_ref_num(&mut self, v: u32) -> &mut Self {")
    );
    assert!(
        build
            .contents
            .contains("self.buf[1..5].copy_from_slice(&v.to_be_bytes());")
    );
    assert!(
        build
            .contents
            .contains("pub fn set_side(&mut self, v: u8) -> &mut Self {")
    );
    assert!(build.contents.contains("self.buf[5] = v;"));
    assert!(
        build
            .contents
            .contains("pub fn set_qty(&mut self, v: u8) -> &mut Self {")
    );
    assert!(
        build
            .contents
            .contains("self.buf[6..7].copy_from_slice(&v.to_be_bytes());")
    );
    assert!(
        build
            .contents
            .contains("pub fn finish(self) -> usize {\n        Self::FIXED_LEN\n    }")
    );
    // no appendage on this kind: no cursor field, no append methods
    assert!(!build.contents.contains("appendage_end"));
    assert!(!build.contents.contains("fn append_"));

    let build_mod = find(&files, "build/mod.rs");
    assert!(build_mod.contents.contains("mod enter_order;"));
    assert!(!build_mod.contents.contains("pub mod enter_order;"));
    assert!(
        build_mod
            .contents
            .contains("pub use enter_order::EnterOrderBuilder;")
    );

    let ns = module_name(&schema).unwrap();
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    assert!(mod_rs.contents.contains("pub mod build;"));
    assert!(mod_rs.contents.contains("pub use build::*;"));
}

#[test]
fn alpha_setter_space_pads_and_truncates_in_release() {
    let kind = to_exchange_kind(
        "EnterOrder",
        b'O',
        9,
        vec![Field {
            name: "symbol".to_string(),
            width: 8,
            class: TypeClass::Alpha,
            doc: None,
        }],
    );
    let schema = directional_toy_schema(vec![kind], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let build = find(&files, "build/enter_order.rs");

    assert!(build.contents.contains("buf[1..9].fill(b' ');"));
    assert!(
        build
            .contents
            .contains("pub fn set_symbol(&mut self, v: &[u8]) -> &mut Self {")
    );
    assert!(
        build
            .contents
            .contains("debug_assert!(v.len() <= 8, \"symbol overflows its 8-byte field\");")
    );
    assert!(build.contents.contains("let n = v.len().min(8);"));
    assert!(
        build
            .contents
            .contains("self.buf[1..1 + n].copy_from_slice(&v[..n]);")
    );
    assert!(build.contents.contains("self.buf[1 + n..9].fill(b' ');"));
}

#[test]
fn u48_setter_writes_low_six_bytes_with_overflow_guard() {
    let kind = to_exchange_kind(
        "Stamp",
        b'Z',
        7,
        vec![Field {
            name: "timestamp".to_string(),
            width: 6,
            class: TypeClass::U48,
            doc: None,
        }],
    );
    let schema = directional_toy_schema(vec![kind], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let build = find(&files, "build/stamp.rs");

    assert!(
        build
            .contents
            .contains("pub fn set_timestamp(&mut self, v: u64) -> &mut Self {")
    );
    assert!(build.contents.contains(
        "debug_assert!(v <= 0x0000_ffff_ffff_ffff, \"timestamp overflows its 48-bit field\");"
    ));
    assert!(
        build
            .contents
            .contains("self.buf[1..7].copy_from_slice(&v.to_be_bytes()[2..8]);")
    );
}

#[test]
fn build_appendage_kind_has_scoped_append_methods() {
    let registry = vec![
        toy_appendage_tag(1, "firm", 4, TypeClass::Alpha),
        toy_appendage_tag(2, "min_qty", 4, TypeClass::U32),
    ];
    let seq_field = || Field {
        name: "seq".to_string(),
        width: 4,
        class: TypeClass::U32,
        doc: None,
    };
    let narrow = to_exchange_appendage_kind("EnterOrder", b'O', 7, vec![seq_field()], &["firm"]);
    let wide = to_exchange_appendage_kind(
        "ReplaceOrder",
        b'U',
        7,
        vec![seq_field()],
        &["firm", "min_qty"],
    );
    let schema = directional_toy_schema(vec![narrow, wide], registry);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    let enter = find(&files, "build/enter_order.rs");
    assert!(enter.contents.contains("appendage_end: usize,"));
    assert!(
        enter
            .contents
            .contains("pub fn append_firm(&mut self, v: &[u8; 4]) -> &mut Self {")
    );
    // min_qty is registered schema-wide but not on EnterOrder's appendage list:
    // no method at all, compile-time rejection of tags outside the kind's list.
    assert!(!enter.contents.contains("append_min_qty"));
    assert!(
        enter
            .contents
            .contains("pub fn finish(self) -> usize {\n        self.appendage_end\n    }")
    );
    assert!(enter.contents.contains(
            "assert!(\n            self.buf.len() >= self.appendage_end + 6,\n            \"buf too short to append firm to EnterOrder\"\n        );"
        ));
    assert!(enter.contents.contains("self.buf[self.appendage_end] = 5;"));
    assert!(
        enter
            .contents
            .contains("self.buf[self.appendage_end + 1] = 1;")
    );
    assert!(enter.contents.contains(
        "self.buf[self.appendage_end + 2..self.appendage_end + 2 + 4].copy_from_slice(v);"
    ));
    assert!(enter.contents.contains("self.appendage_end += 6;"));
    assert!(
        enter
            .contents
            .contains("self.buf[5..7].copy_from_slice(&tail_len.to_be_bytes());")
    );

    let replace = find(&files, "build/replace_order.rs");
    assert!(
        replace
            .contents
            .contains("pub fn append_firm(&mut self, v: &[u8; 4]) -> &mut Self {")
    );
    assert!(
        replace
            .contents
            .contains("pub fn append_min_qty(&mut self, v: u32) -> &mut Self {")
    );
}

#[test]
fn appendage_only_kind_builder_no_setters() {
    let registry = vec![toy_appendage_tag(28, "user_ref_idx", 1, TypeClass::U8)];
    let kind = to_exchange_appendage_kind(
        "AccountQueryRequest",
        b'Q',
        3,
        Vec::new(),
        &["user_ref_idx"],
    );
    let schema = directional_toy_schema(vec![kind], registry);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    let build = find(&files, "build/account_query_request.rs");

    assert!(build.contents.contains("pub const FIXED_LEN: usize = 3;"));
    assert!(!build.contents.contains("fn set_"));
    assert!(
        build
            .contents
            .contains("pub fn append_user_ref_idx(&mut self, v: u8) -> &mut Self {")
    );
    assert!(build.contents.contains("buf[0] = b'Q';"));
    // no fields to space-fill: whole-region zero-fill alone covers this kind
    assert!(!build.contents.contains(".fill(b' ')"));
}

#[test]
fn no_build_files_when_only_from_exchange_kinds() {
    let accepted = with_direction(
        fixed_kind("OrderAccepted", b'A', 2, vec![code_field("v")]),
        Direction::FromExchange,
    );
    let schema = directional_toy_schema(vec![accepted], Vec::new());
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();

    assert!(
        !files
            .iter()
            .any(|f| f.rel_path.to_string_lossy().contains("build"))
    );
    let ns = module_name(&schema).unwrap();
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    // "build_lut" (dispatch reexport) is always present; only the build MODULE
    // wiring is conditional, so check for the exact decl/reexport, not a substring.
    assert!(!mod_rs.contents.contains("mod build;"));
    assert!(!mod_rs.contents.contains("pub use build::*;"));
}

#[test]
fn directionless_schema_emits_no_build_tree() {
    let schema = toy_schema_full();
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert!(
        !files
            .iter()
            .any(|f| f.rel_path.to_string_lossy().contains("build"))
    );
    let ns = module_name(&schema).unwrap();
    let mod_rs = find(&files, &format!("{ns}/mod.rs"));
    // "build_lut" (dispatch reexport) is always present; only the build MODULE
    // wiring is conditional, so check for the exact decl/reexport, not a substring.
    assert!(!mod_rs.contents.contains("mod build;"));
    assert!(!mod_rs.contents.contains("pub use build::*;"));
}

#[test]
fn build_output_no_banned_tokens() {
    let registry = vec![toy_appendage_tag(1, "firm", 4, TypeClass::Alpha)];
    let kind = to_exchange_appendage_kind(
        "EnterOrder",
        b'O',
        7,
        vec![Field {
            name: "seq".to_string(),
            width: 4,
            class: TypeClass::U32,
            doc: None,
        }],
        &["firm"],
    );
    let schema = directional_toy_schema(vec![kind], registry);
    let files = generate(&schema, Path::new(TOY_CRATE), "toy.yaml", b"toy bytes").unwrap();
    assert_no_banned_tokens(&files);
}
