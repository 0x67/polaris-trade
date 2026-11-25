//! Container auto-detection: magic bytes first, extension fallback for the
//! magic-less length-prefixed binary framing (unsniffable by content).

use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

// capture header magic, owned by the container crate. see md_capture::MAGIC
use md_capture::MAGIC as MAGIC_CAPTURE;
// four classic pcap magics (LE/BE x usec/nanos). see crate::pcap
const MAGIC_PCAP: [[u8; 4]; 4] = [
    [0xd4, 0xc3, 0xb2, 0xa1],
    [0xa1, 0xb2, 0xc3, 0xd4],
    [0x4d, 0x3c, 0xb2, 0xa1],
    [0xa1, 0xb2, 0x3c, 0x4d],
];
// gzip member magic, first two bytes
const MAGIC_GZIP: [u8; 2] = [0x1f, 0x8b];

/// Replay container kinds [`detect_container`] resolves. Maps one-to-one
/// onto this crate's readers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerKind {
    /// `MDC1` capture container. see [`crate::capture::CaptureReplay`]
    Capture,
    /// Classic pcap, any of the four magic variants. see [`crate::pcap::PcapReplay`]
    Pcap,
    /// Gzip member wrapping the length-prefixed binary framing.
    Gzip,
    /// Magic-less length-prefixed binary framing, resolved by extension.
    BinaryFramed,
}

/// Failure resolving a container kind. `Unknown` must refuse startup:
/// replaying a misdetected container decodes garbage silently.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DetectError {
    /// Opening or reading the file head failed.
    #[error("container detect io: {0}")]
    Io(#[from] std::io::Error),
    /// No known magic and extension not in the fallback map.
    #[error("unrecognized replay container: no known magic, unmapped extension")]
    Unknown,
}

/// Resolve `path`'s container kind: first 4 bytes against the capture, pcap,
/// and gzip magics, then extension fallback (`.bin`/`.itch` binary framed,
/// `.gz` gzip). [`DetectError::Unknown`] when neither resolves.
pub fn detect_container<P: AsRef<Path>>(path: P) -> Result<ContainerKind, DetectError> {
    let path = path.as_ref();
    let mut head = [0u8; 4];
    let got = read_head(&mut File::open(path)?, &mut head)?;
    if got >= 4 {
        if head == MAGIC_CAPTURE {
            return Ok(ContainerKind::Capture);
        }
        if MAGIC_PCAP.contains(&head) {
            return Ok(ContainerKind::Pcap);
        }
    }
    if got >= 2 && head[..2] == MAGIC_GZIP {
        return Ok(ContainerKind::Gzip);
    }
    match path.extension().and_then(|e| e.to_str()) {
        Some("bin") | Some("itch") => Ok(ContainerKind::BinaryFramed),
        Some("gz") => Ok(ContainerKind::Gzip),
        _ => Err(DetectError::Unknown),
    }
}

// read up to buf.len() head bytes; short file is a short count, not an error
fn read_head(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(got)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn write_tmp(name: &str, head: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("mdreplay-detect-{name}"));
        std::fs::File::create(&path)
            .expect("create fixture")
            .write_all(head)
            .expect("write fixture");
        path
    }

    // Every positive kind is already pinned by the replay integration test
    // `detect_container_magics`, against literal magics rather than the
    // constants under test. Only the refusal path is uncovered there.

    // An unrecognized container must refuse rather than guess: replaying a
    // misdetected file is the failure this whole function exists to prevent.
    #[test]
    fn unknown_container_refuses() {
        let path = write_tmp("mystery.dat", &[0x00, 0x01, 0x02, 0x03]);
        let err = detect_container(&path).expect_err("unmapped extension must error");
        assert!(matches!(err, DetectError::Unknown));
        let _ = std::fs::remove_file(path);
    }
}
