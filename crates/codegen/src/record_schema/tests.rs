use super::*;

fn f(name: &str, class: FieldClass) -> RecordField {
    RecordField {
        name: name.to_string(),
        class,
        doc: None,
    }
}

/// struct + enum registries matching the committed record set, for padding tests.
fn registries() -> (HashMap<String, Layout>, HashMap<String, EnumRepr>) {
    let mut enums = HashMap::new();
    for n in [
        "Action",
        "Side",
        "LevelAction",
        "BreakRefKind",
        "ControlKind",
        "AssetClass",
    ] {
        enums.insert(n.to_string(), EnumRepr::U8);
    }
    enums.insert("StatType".to_string(), EnumRepr::U16);
    enums.insert("TradingState".to_string(), EnumRepr::U8);

    let bap = [
        f("bid_px", FieldClass::I64),
        f("ask_px", FieldClass::I64),
        f("bid_sz", FieldClass::U64),
        f("ask_sz", FieldClass::U64),
        f("bid_ct", FieldClass::U32),
        f("ask_ct", FieldClass::U32),
    ];
    let mut structs = HashMap::new();
    let bap_layout =
        compute_padding(&bap, 0, 1, "BidAskPair", &structs, &enums).expect("bap layout");
    assert_eq!(bap_layout.size, 40);
    structs.insert("BidAskPair".to_string(), bap_layout);
    (structs, enums)
}

fn pad_widths(layout: &Layout) -> Vec<usize> {
    layout
        .slots
        .iter()
        .filter_map(|s| match s {
            Slot::Pad { width, .. } => Some(*width),
            Slot::Field { .. } => None,
        })
        .collect()
}

const TOY: &str = r#"
schema_version: "1"
enums:
  - name: Side
    repr: u8
    variants:
      - { name: Bid, value: 66 }
      - { name: Ask, value: 65 }
      - { name: None, value: 78 }
structs:
  - name: Pair
    derive_default: true
    fields:
      - { name: px, class: i64 }
      - { name: sz, class: u64 }
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
    - { name: ts_event, class: u64 }
    - { name: flags, class: flags }
records:
  - name: Sample
    rtype: 0x10
    band: BookTape
    fields:
      - { name: price, class: price }
      - { name: side, class: enum, ty: Side }
      - { name: pair, class: struct, ty: Pair }
"#;

// Model parses records, structs, enums, and each field class from the flat YAML
// form; this is the load-and-shape contract the emitters build on.
#[test]
fn model_shape() {
    let s = RecordSchema::parse(TOY).expect("parses");
    assert_eq!(s.schema_version, "1");
    assert_eq!(s.enums.len(), 1);
    assert_eq!(s.structs.len(), 1);
    assert_eq!(s.header.name, "RecordHeader");
    assert_eq!(s.records.len(), 1);

    let sample = &s.records[0];
    assert_eq!(sample.rtype, 0x10);
    assert_eq!(sample.band, Category::BookTape);
    assert_eq!(sample.fields[0].class, FieldClass::Price);
    assert_eq!(
        sample.fields[1].class,
        FieldClass::Enum {
            ty: "Side".to_string()
        }
    );
    assert_eq!(
        sample.fields[2].class,
        FieldClass::Struct {
            ty: "Pair".to_string()
        }
    );
    assert!(s.structs[0].derive_default);
    s.validate().expect("toy schema validates");
}

// Sparse and ASCII-valued discriminants ride through verbatim (Side letters,
// StatType's high venue-escape value): the generator never renumbers a variant.
#[test]
fn enum_explicit_discriminants() {
    let yaml = r#"
schema_version: "1"
enums:
  - name: Side
    repr: u8
    variants:
      - { name: Bid, value: 66 }
      - { name: Ask, value: 65 }
  - name: StatType
    repr: u16
    variants:
      - { name: PriceLimits, value: 0x0001 }
      - { name: VenueSpecific, value: 0xFF00 }
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
"#;
    let s = RecordSchema::parse(yaml).expect("parses");
    assert_eq!(s.enums[0].variants[0].value, 66);
    assert_eq!(s.enums[0].variants[1].value, 65);
    assert_eq!(s.enums[1].repr, EnumRepr::U16);
    assert_eq!(s.enums[1].variants[1].value, 0xFF00);
}

// The padding walk over the committed field orders yields exactly the committed
// `_pad` widths and sizes: Mbo trailing 7 (96), Level trailing 3 (80), Mbp10
// interior 5 before the level array (480), Fill no pad (96, ends flush).
#[test]
fn padding_walk_matches_committed() {
    let (structs, enums) = registries();
    let base = 56; // RecordHeader size
    let hdr_align = 8;

    let mbo = [
        f("order_id", FieldClass::U64),
        f("price", FieldClass::I64),
        f("qty", FieldClass::U64),
        f(
            "action",
            FieldClass::Enum {
                ty: "Action".into(),
            },
        ),
        f("side", FieldClass::Enum { ty: "Side".into() }),
        f("participant", FieldClass::Bytes { width: 7 }),
    ];
    let mbo_l = compute_padding(&mbo, base, hdr_align, "Mbo", &structs, &enums).unwrap();
    assert_eq!(mbo_l.size, 96);
    assert_eq!(pad_widths(&mbo_l), vec![7]);

    let level = [
        f("price", FieldClass::I64),
        f("qty", FieldClass::U64),
        f("num_deletes", FieldClass::U16),
        f("side", FieldClass::Enum { ty: "Side".into() }),
        f("level", FieldClass::U8),
        f(
            "action",
            FieldClass::Enum {
                ty: "LevelAction".into(),
            },
        ),
    ];
    let level_l = compute_padding(&level, base, hdr_align, "Level", &structs, &enums).unwrap();
    assert_eq!(level_l.size, 80);
    assert_eq!(pad_widths(&level_l), vec![3]);

    let mbp10 = [
        f("price", FieldClass::I64),
        f("qty", FieldClass::U64),
        f(
            "action",
            FieldClass::Enum {
                ty: "Action".into(),
            },
        ),
        f("side", FieldClass::Enum { ty: "Side".into() }),
        f("depth", FieldClass::U8),
        f(
            "levels",
            FieldClass::Array {
                elem: Box::new(FieldClass::Struct {
                    ty: "BidAskPair".into(),
                }),
                count: 10,
            },
        ),
    ];
    let mbp10_l = compute_padding(&mbp10, base, hdr_align, "Mbp10", &structs, &enums).unwrap();
    assert_eq!(mbp10_l.size, 480);
    assert_eq!(pad_widths(&mbp10_l), vec![5]);
    // the single interior pad sits before `levels`, at offset 75.
    match &mbp10_l.slots[mbp10_l.slots.len() - 2] {
        Slot::Pad {
            name,
            width,
            offset,
        } => {
            assert_eq!(name, "_pad");
            assert_eq!(*width, 5);
            assert_eq!(*offset, 75);
        }
        other => panic!("expected interior pad, got {other:?}"),
    }

    let fill = [
        f("price", FieldClass::I64),
        f("qty", FieldClass::U64),
        f("fill_ref", FieldClass::U64),
        f(
            "ref_kind",
            FieldClass::Enum {
                ty: "BreakRefKind".into(),
            },
        ),
        f("buyer", FieldClass::Bytes { width: 7 }),
        f("seller", FieldClass::Bytes { width: 7 }),
        f("aggressor", FieldClass::Enum { ty: "Side".into() }),
    ];
    let fill_l = compute_padding(&fill, base, hdr_align, "Fill", &structs, &enums).unwrap();
    assert_eq!(fill_l.size, 96);
    assert!(pad_widths(&fill_l).is_empty(), "Fill ends flush, no pad");
}

// A scalar forced behind interior padding is avoidable (reorder 8-byte-first) and
// rejected; the same fields aggregate-last would be fine.
#[test]
fn rejects_avoidable_padding() {
    let (structs, enums) = registries();
    // u8 then u64 scalar: the u64 needs 7 interior bytes before it.
    let bad = [f("tag", FieldClass::U8), f("big", FieldClass::U64)];
    let err = compute_padding(&bad, 0, 1, "Bad", &structs, &enums).unwrap_err();
    assert!(matches!(err, RecordSchemaError::AvoidablePadding { .. }));
}

// A struct whose fields sum to a non-word size (three bytes) is rejected: record
// `length` counts 32-bit words and could not encode it.
#[test]
fn rejects_non_word_size() {
    let (structs, enums) = registries();
    let bad = [
        f("a", FieldClass::U8),
        f("b", FieldClass::U8),
        f("c", FieldClass::U8),
    ];
    let err = compute_padding(&bad, 0, 1, "Bad", &structs, &enums).unwrap_err();
    assert!(matches!(
        err,
        RecordSchemaError::NotWordAligned { size: 3, .. }
    ));
}

// An enum/struct field naming an undeclared type is rejected before emission.
#[test]
fn rejects_unknown_type() {
    let yaml = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
records:
  - name: Sample
    rtype: 0x10
    band: BookTape
    fields:
      - { name: side, class: enum, ty: Ghost }
"#;
    let s = RecordSchema::parse(yaml).expect("parses");
    match s.validate().unwrap_err() {
        RecordSchemaError::UnknownType { ty, kind, .. } => {
            assert_eq!(ty, "Ghost");
            assert_eq!(kind, "enum");
        }
        other => panic!("expected UnknownType, got {other:?}"),
    }
}

// Non-snake field and non-Pascal type names are rejected.
#[test]
fn rejects_bad_ident() {
    let bad_field = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: BadField, class: u8 }
"#;
    assert!(matches!(
        RecordSchema::parse(bad_field)
            .unwrap()
            .validate()
            .unwrap_err(),
        RecordSchemaError::BadName { .. }
    ));

    let bad_type = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
records:
  - name: lowerName
    rtype: 0x10
    band: BookTape
    fields:
      - { name: qty, class: u64 }
"#;
    assert!(matches!(
        RecordSchema::parse(bad_type)
            .unwrap()
            .validate()
            .unwrap_err(),
        RecordSchemaError::BadName { .. }
    ));
}

// An rtype in the wrong band or a reserved band, or a duplicate rtype, is rejected;
// this reproduces the `REGISTERED` invariant the hand-written tests lock.
#[test]
fn rejects_rtype_out_of_band() {
    let wrong_band = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
records:
  - name: Sample
    rtype: 0x30
    band: BookTape
    fields:
      - { name: qty, class: u64 }
"#;
    match RecordSchema::parse(wrong_band)
        .unwrap()
        .validate()
        .unwrap_err()
    {
        RecordSchemaError::RtypeBand {
            actual, declared, ..
        } => {
            assert_eq!(actual, Category::Stats);
            assert_eq!(declared, Category::BookTape);
        }
        other => panic!("expected RtypeBand, got {other:?}"),
    }

    let reserved = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
records:
  - name: Sample
    rtype: 0x55
    band: ExecReserved
    fields:
      - { name: qty, class: u64 }
"#;
    assert!(matches!(
        RecordSchema::parse(reserved)
            .unwrap()
            .validate()
            .unwrap_err(),
        RecordSchemaError::RtypeReserved { .. }
    ));

    let dup = r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - { name: length, class: u8 }
    - { name: rtype, class: u8 }
    - { name: venue, class: u16 }
    - { name: instrument_id, class: u32 }
records:
  - name: A
    rtype: 0x10
    band: BookTape
    fields:
      - { name: qty, class: u64 }
  - name: B
    rtype: 0x10
    band: BookTape
    fields:
      - { name: qty, class: u64 }
"#;
    assert!(matches!(
        RecordSchema::parse(dup).unwrap().validate().unwrap_err(),
        RecordSchemaError::RtypeDup { rtype: 0x10, .. }
    ));
}

// Banned tokens in any doc field are rejected, reusing the wire DSL's check.
#[test]
fn rejects_banned_doc_token() {
    for stem in ["REQ", "TASK", "AC"] {
        let token = format!("{stem}-001 note");
        let yaml = format!(
            r#"
schema_version: "1"
header:
  name: RecordHeader
  fields:
    - {{ name: length, class: u8, doc: "{token}" }}
"#
        );
        match RecordSchema::parse(&yaml).unwrap().validate().unwrap_err() {
            RecordSchemaError::DocBannedToken { token: matched, .. } => {
                assert!(matched.starts_with(stem));
            }
            other => panic!("expected DocBannedToken, got {other:?}"),
        }
    }
}

// The committed schema loads, validates, and its padding walk reproduces the
// shipped md-events sizes: RecordHeader 56, Mbo 96, Mbp10 480 (interior pad before
// levels), Fill 96 (flush), Definition 176 (pad before the nested tail). No
// avoidable-padding / non-word errors, proving the committed field order is pad-clean.
#[test]
fn committed_schema_validates() {
    let path = crate::cli::repo_root().join("schemas/records/l0_l3.yaml");
    let s = RecordSchema::load(&path).expect("committed schema loads");
    s.validate().expect("committed schema validates");

    let enums = s.enum_reprs();
    let mut structs = HashMap::new();
    for st in &s.structs {
        let l =
            compute_padding(&st.fields, 0, 1, &st.name, &structs, &enums).expect("struct layout");
        structs.insert(st.name.clone(), l);
    }
    let header = compute_padding(&s.header.fields, 0, 1, &s.header.name, &structs, &enums)
        .expect("header layout");
    assert_eq!(header.size, 56, "RecordHeader");

    let size_of = |name: &str| {
        let r = s
            .records
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("record {name} present"));
        compute_padding(&r.fields, header.size, header.align, name, &structs, &enums)
            .unwrap_or_else(|e| panic!("{name} layout: {e}"))
            .size
    };
    assert_eq!(size_of("Mbo"), 96);
    assert_eq!(size_of("Level"), 80);
    assert_eq!(size_of("Mbp10"), 480);
    assert_eq!(size_of("Bbo"), 96);
    assert_eq!(size_of("Trade"), 88);
    assert_eq!(size_of("TradeBreak"), 72);
    assert_eq!(size_of("Ohlcv"), 120);
    assert_eq!(size_of("Imbalance"), 120);
    assert_eq!(size_of("Fill"), 96);
    assert_eq!(size_of("Stats"), 88);
    assert_eq!(size_of("Status"), 80);
    assert_eq!(size_of("Control"), 72);
    assert_eq!(size_of("Definition"), 176);
}
