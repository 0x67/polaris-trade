//! Offline replay: read a downloaded pbn file, decode its records, resolve
//! symbology, and print a tally. No gateway, no network. This is the offline
//! half of the SDK: the same decode path a live consumer uses, pointed at a file.
//!
//!   cargo run --example replay [path/to/file.pbn]
//!
//! With no path, it writes a synthetic stream to a temp file and replays that,
//! so the full write -> file -> read -> decode path runs end to end.

use std::{collections::BTreeMap, path::PathBuf};

use pbn::{
    AnyRecord, PbnError, StreamHeader, Symbology,
    header::{FLAG_SYMBOLOGY, FLAG_ZSTD_BODY, KIND_NORMALIZED, PROTO_VERSION},
    md_events::{
        AssetClass, Control, ControlKind, Definition, RecordFlags, RecordHeader, Side, Trade, rtype,
    },
    read_records, read_stream, write_record, write_stream,
};

// The reader's own compiled schema major. A stream with a different major is
// rejected fail-closed by read_stream before any record decodes.
const READER_MAJOR: u8 = 1;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path: PathBuf = match std::env::args().nth(1) {
        Some(p) => PathBuf::from(p),
        None => {
            let p = std::env::temp_dir().join("pbn_replay_demo.pbn");
            std::fs::write(&p, synth_stream()?)?;
            eprintln!("no path given; wrote a synthetic stream to {}", p.display());
            p
        }
    };

    let bytes = std::fs::read(&path)?;
    let (header, body) = read_stream(&bytes, READER_MAJOR)?;
    println!("file:   {}", path.display());
    println!(
        "header: venue={} schema={}.{} dataset={} zstd={} bytes={}",
        header.venue,
        header.schema_major,
        header.schema_minor,
        header.dataset,
        header.flags & FLAG_ZSTD_BODY != 0,
        bytes.len(),
    );

    let mut symbology = Symbology::new();
    let mut counts: BTreeMap<u8, usize> = BTreeMap::new();
    let mut gaps = 0usize;

    for rec in read_records(&body) {
        let rec = rec?;
        *counts.entry(rec.rtype()).or_default() += 1;
        match rec {
            AnyRecord::Definition(d) => symbology.ingest(d),
            AnyRecord::Control(c) if c.kind == ControlKind::GapDetected => gaps += 1,
            _ => {}
        }
    }

    println!("records: {}", counts.values().sum::<usize>());
    for (rt, n) in &counts {
        println!("  rtype {rt:#04x}  x{n}");
    }
    println!("instruments: {}", symbology.len());
    for d in symbology.definitions() {
        println!(
            "  id={} symbol={} board={}",
            d.hdr.instrument_id,
            ascii(&d.raw_symbol),
            ascii(&d.board),
        );
    }
    println!("gaps: {gaps}");
    Ok(())
}

/// Trim trailing NULs and render a fixed byte field as text.
fn ascii(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// Build a small self-describing stream: a Definition preamble for two
/// instruments, some trades, and a gap Control, zstd-compressed.
fn synth_stream() -> Result<Vec<u8>, PbnError> {
    let header = StreamHeader {
        proto_version: PROTO_VERSION,
        body_kind: KIND_NORMALIZED,
        schema_major: 1,
        schema_minor: 0,
        venue: 1,
        flags: FLAG_ZSTD_BODY | FLAG_SYMBOLOGY,
        dataset: 42,
        start_ts: 0,
        end_ts: 0,
    };

    let mut body = Vec::new();
    for (id, sym, board) in [(777u32, b"CCCC", b"MAIN"), (778, b"DDDD", b"AUX1")] {
        let mut raw = [0u8; 20];
        raw[..4].copy_from_slice(sym);
        let def = Definition::new(
            id,
            1,
            0,
            u64::from(id),
            raw,
            *board,
            AssetClass::Equity,
            0,
            100,
        );
        write_record(&mut body, &def)?;
    }
    for seq in 0..5u64 {
        write_record(&mut body, &trade(if seq % 2 == 0 { 777 } else { 778 }, seq))?;
    }
    write_record(&mut body, &gap())?;

    write_stream(&header, &body)
}

fn trade(instrument_id: u32, seq: u64) -> Trade {
    let hdr = RecordHeader {
        length: (core::mem::size_of::<Trade>() / 4) as u8,
        rtype: rtype::TRADE,
        venue: 1,
        instrument_id,
        ts_event: 100 + seq,
        ts_recv: 100 + seq,
        ts_egress: 0,
        venue_seq: seq,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    Trade {
        hdr,
        price: 1_000_000_000 + seq as i64,
        qty: 50,
        trade_ref: seq,
        aggressor: Side::Bid,
        _pad: [0u8; 7],
    }
}

fn gap() -> Control {
    let hdr = RecordHeader {
        length: (core::mem::size_of::<Control>() / 4) as u8,
        rtype: rtype::CONTROL,
        venue: 1,
        instrument_id: 0,
        ts_event: 0,
        ts_recv: 0,
        ts_egress: 0,
        venue_seq: 0,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    Control {
        hdr,
        aux: 3,
        kind: ControlKind::GapDetected,
        _pad: [0u8; 7],
    }
}
