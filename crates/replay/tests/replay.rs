//! Container-level replay behavior: eager vs streaming load equivalence and
//! container kind auto-detection.

use std::{fs, io::Write as _, path::PathBuf};

use flate2::{Compression, write::GzEncoder};
use md_pipeline::{IngestFrame, IngestSource};
use md_replay::{BinaryFileReplay, ContainerKind, DetectError, detect_container};

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("md_replay_it_{name}"))
}

fn drain<S: IngestSource>(mut source: S) -> Vec<S::Frame> {
    let mut out = Vec::new();
    while let Ok(n) = source.recv_batch(&mut out, 8) {
        if n == 0 {
            break;
        }
    }
    out
}

// length-prefixed body: [len:u16 BE][payload] per message
fn framed(messages: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for m in messages {
        bytes.extend_from_slice(&(m.len() as u16).to_be_bytes());
        bytes.extend_from_slice(m);
    }
    bytes
}

// Eager constructors must yield frame streams identical to the streaming
// constructors over the same fixture, for both plain and gzip containers.
#[test]
fn eager_matches_stream() {
    let body = framed(&[b"alpha", b"bravo", b"charlie"]);

    let bin = tmp("eager_matches_stream.bin");
    fs::write(&bin, &body).expect("write .bin fixture");
    let streamed = drain(BinaryFileReplay::from_path(&bin, 1_000, 5).expect("from_path"));
    let eager = drain(BinaryFileReplay::from_path_eager(&bin, 1_000, 5).expect("from_path_eager"));
    assert_eq!(streamed.len(), 3);
    assert_eq!(streamed, eager, "plain: eager identical to streaming");

    let gz = tmp("eager_matches_stream.gz");
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&body).expect("gzip write");
    fs::write(&gz, enc.finish().expect("gzip finish")).expect("write .gz fixture");
    let streamed = drain(BinaryFileReplay::from_gz(&gz, 1_000, 5).expect("from_gz"));
    let eager = drain(BinaryFileReplay::from_gz_eager(&gz, 1_000, 5).expect("from_gz_eager"));
    assert_eq!(streamed.len(), 3);
    assert_eq!(streamed, eager, "gzip: eager identical to streaming");

    let _ = fs::remove_file(&bin);
    let _ = fs::remove_file(&gz);
}

// gzip one body into one member
fn gz_member(body: &[u8]) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(body).expect("gzip write");
    enc.finish().expect("gzip finish")
}

// Concatenated multi-member gzip (rsyslog/pigz style rotation) must inflate
// through every member; a single-member decoder reports clean EOF after the
// first member and silently drops the rest of the day.
#[test]
fn multi_member_gzip_decodes_all_members() {
    let mut concatenated = gz_member(&framed(&[b"alpha", b"bravo"]));
    concatenated.extend_from_slice(&gz_member(&framed(&[b"charlie"])));
    let gz = tmp("multi_member.gz");
    fs::write(&gz, &concatenated).expect("write .gz fixture");

    let streamed = drain(BinaryFileReplay::from_gz(&gz, 1_000, 5).expect("from_gz"));
    let eager = drain(BinaryFileReplay::from_gz_eager(&gz, 1_000, 5).expect("from_gz_eager"));
    let _ = fs::remove_file(&gz);

    let payloads: Vec<&[u8]> = streamed.iter().map(|f| f.payload()).collect();
    assert_eq!(
        payloads,
        vec![b"alpha".as_slice(), b"bravo", b"charlie"],
        "second member's messages must survive inflate"
    );
    assert_eq!(streamed, eager, "gzip: eager identical to streaming");
}

// Every shipped magic resolves, extensions cover the magic-less binary
// framing, and unrecognized input refuses with `DetectError::Unknown`.
#[test]
fn detect_container_magics() {
    let case = |name: &str, bytes: &[u8]| {
        let path = tmp(name);
        fs::write(&path, bytes).expect("write detect fixture");
        let got = detect_container(&path);
        let _ = fs::remove_file(&path);
        got
    };

    assert!(matches!(
        case("capture.dat", b"MDC1rest"),
        Ok(ContainerKind::Capture)
    ));

    let pcap_magics: [[u8; 4]; 4] = [
        [0xd4, 0xc3, 0xb2, 0xa1],
        [0xa1, 0xb2, 0xc3, 0xd4],
        [0x4d, 0x3c, 0xb2, 0xa1],
        [0xa1, 0xb2, 0x3c, 0x4d],
    ];
    for (i, magic) in pcap_magics.iter().enumerate() {
        assert!(
            matches!(
                case(&format!("pcap_{i}.dat"), magic),
                Ok(ContainerKind::Pcap)
            ),
            "pcap magic variant {i}"
        );
    }

    assert!(matches!(
        case("member.dat", &[0x1f, 0x8b, 0x08, 0x00]),
        Ok(ContainerKind::Gzip)
    ));

    // extension fallback: length-prefixed framing carries no magic to sniff
    assert!(matches!(
        case("dump.bin", &[0x00, 0x05]),
        Ok(ContainerKind::BinaryFramed)
    ));
    assert!(matches!(
        case("dump.itch", &[0x00, 0x05]),
        Ok(ContainerKind::BinaryFramed)
    ));
    // empty .gz: no magic readable, extension still resolves
    assert!(matches!(case("empty.gz", b""), Ok(ContainerKind::Gzip)));

    assert!(matches!(
        case("garbage.dat", b"????"),
        Err(DetectError::Unknown)
    ));
}
