//! Seed corpus generator for the pbn targets. Builds valid frames and
//! containers with pbn's own writers; all synthetic, no capture bytes.
//! Deterministic and idempotent: `cargo test` from `fuzz/` regenerates the
//! committed `stream_walk`, `record_roundtrip` and `container` seeds
//! byte-identical.

use std::{fs, path::PathBuf};

use arbitrary::Unstructured;
use pbn::{
    FLAG_ZSTD_BODY, KIND_NORMALIZED, PROTO_VERSION, StreamHeader,
    md_events::{AssetClass, Definition},
    write_record, write_stream,
};
use polaris_fuzz::{mbo, trade};

fn seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join(target);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), bytes).unwrap();
}

/// Fixed pseudo-byte pattern feeding the record builders.
fn pattern(seed: u8, n: usize) -> Vec<u8> {
    (0..n)
        .map(|i| seed.wrapping_add(i as u8).wrapping_mul(31))
        .collect()
}

/// Record body: alternating trades and mbos from deterministic bytes.
fn body(seed_byte: u8, pairs: usize) -> Vec<u8> {
    let raw = pattern(seed_byte, pairs * 160);
    let mut u = Unstructured::new(&raw);
    let mut b = Vec::new();
    for _ in 0..pairs {
        write_record(&mut b, &trade(&mut u).unwrap()).unwrap();
        write_record(&mut b, &mbo(&mut u).unwrap()).unwrap();
    }
    b
}

/// One definition frame as a writer built before the tail emitted it: the
/// first 104 bytes of the record, length word 26.
fn pre_tail_definition() -> Vec<u8> {
    let def = Definition::new(7, 1, 0, 7, [b'P'; 20], *b"MAIN", AssetClass::Equity, 0, 100);
    let mut b = Vec::new();
    write_record(&mut b, &def).unwrap();
    b.truncate(Definition::PRE_TAIL_LEN);
    b[0] = (Definition::PRE_TAIL_LEN / 4) as u8;
    b
}

fn header(flags: u16) -> StreamHeader {
    StreamHeader {
        proto_version: PROTO_VERSION,
        body_kind: KIND_NORMALIZED,
        schema_major: 1,
        schema_minor: 0,
        venue: 1,
        flags,
        dataset: 0,
        start_ts: 0,
        end_ts: 0,
    }
}

#[test]
fn write_seed_corpus() {
    // stream_walk input: bare record body
    seed("stream_walk", "trades-mbos", &body(3, 3));
    seed("stream_walk", "single-trade", &body(9, 1)[..88]);
    seed("stream_walk", "pre-tail-definition", &pre_tail_definition());

    // record_roundtrip input: unstructured field bytes
    seed("record_roundtrip", "zeros", &[0u8; 160]);
    seed("record_roundtrip", "pattern", &pattern(7, 160));

    // container input: full stream, plain and zstd body
    seed(
        "container",
        "uncompressed",
        &write_stream(&header(0), &body(5, 2)).unwrap(),
    );
    seed(
        "container",
        "zstd",
        &write_stream(&header(FLAG_ZSTD_BODY), &body(11, 4)).unwrap(),
    );
}
