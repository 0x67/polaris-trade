//! Fixture-parity gate for hand-written adapters.
//!
//! A fixture is a directory holding two text files. `frames.txt` records the
//! venue frames fed to the adapter, one per line as
//! `<stream> <ts_recv> <venue_seq> <payload hex>` (`-` for an empty payload,
//! `#` starts a comment line). `expected.txt` holds the normalized output: one
//! line per emitted event tagged with the 1-based frame that emitted it, then
//! a final line with the adapter counters. A mapping change fails the gate with
//! the first differing line, so the record that changed is named.
//!
//! An intended mapping change is accepted by running the failing test with
//! `POLARIS_REWRITE_EXPECTED=1`, which rewrites `expected.txt` from the
//! current adapter. The same variable creates `expected.txt` for a new
//! fixture. Review the diff of that file in the same commit. Any other value,
//! including `0`, leaves the gate checking.

use std::{
    ffi::OsStr,
    fmt::Write as _,
    fs, io,
    path::{Path, PathBuf},
};

use crate::{adapter::VenueAdapter, ingest::StreamId};

/// Environment variable that makes the gate rewrite `expected.txt`.
const REWRITE_ENV: &str = "POLARIS_REWRITE_EXPECTED";

const FRAMES_FILE: &str = "frames.txt";
const EXPECTED_FILE: &str = "expected.txt";

/// One recorded venue frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParityFrame {
    /// Stream the frame arrived on.
    stream: StreamId,
    /// Receive time handed to the adapter.
    ts_recv: u64,
    /// Wire sequence number handed to the adapter.
    venue_seq: u64,
    /// The whole wire message.
    payload: Vec<u8>,
}

/// Why a fixture check failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(super) enum ParityError {
    /// A fixture file could not be read or written.
    #[error("fixture file {path}: {source}")]
    Io {
        /// The file involved.
        path: PathBuf,
        /// The underlying failure.
        #[source]
        source: io::Error,
    },
    /// A line of `frames.txt` is not `<stream> <ts_recv> <venue_seq> <hex>`.
    #[error("{path} line {line}: {reason}")]
    Frame {
        /// The frames file.
        path: PathBuf,
        /// 1-based line number.
        line: usize,
        /// What is wrong with the line.
        reason: &'static str,
    },
    /// The adapter's output differs from `expected.txt`.
    #[error(
        "{path} differs at line {line}\n  expected: {expected}\n  actual:   {actual}\n\
         rewrite it for an intended mapping change with {REWRITE_ENV}=1"
    )]
    Mismatch {
        /// The expected file.
        path: PathBuf,
        /// 1-based line of the first difference.
        line: usize,
        /// The expected line, or `<end of file>`.
        expected: String,
        /// The line the adapter produced, or `<end of output>`.
        actual: String,
    },
}

/// Parse the text form of `frames.txt`; `path` only labels errors.
///
/// # Errors
///
/// [`ParityError::Frame`] naming the first malformed line.
pub(super) fn parse_frames(path: &Path, text: &str) -> Result<Vec<ParityFrame>, ParityError> {
    let mut frames = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let bad = |reason| ParityError::Frame {
            path: path.to_path_buf(),
            line: index + 1,
            reason,
        };
        let mut fields = line.split_whitespace();
        let (Some(stream), Some(ts_recv), Some(venue_seq), Some(hex), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return Err(bad("want four fields"));
        };
        frames.push(ParityFrame {
            stream: StreamId(stream.parse().map_err(|_| bad("stream is not a u16"))?),
            ts_recv: ts_recv.parse().map_err(|_| bad("ts_recv is not a u64"))?,
            venue_seq: venue_seq
                .parse()
                .map_err(|_| bad("venue_seq is not a u64"))?,
            payload: decode_hex(hex).ok_or_else(|| bad("payload is not hex"))?,
        });
    }
    Ok(frames)
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text == "-" {
        return Some(Vec::new());
    }
    if !text.bytes().all(|b| b.is_ascii_hexdigit()) || !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).ok())
        .collect()
}

/// Feed `frames` to a fresh `A` built from `cfg`, draining after each frame,
/// and render the output in the `expected.txt` form.
#[must_use]
fn render_output<A: VenueAdapter>(cfg: A::Config, frames: &[ParityFrame]) -> String {
    let mut adapter = A::new(cfg);
    let mut out = String::new();
    for (index, frame) in frames.iter().enumerate() {
        adapter.feed(frame.stream, &frame.payload, frame.ts_recv, frame.venue_seq);
        for event in adapter.drain() {
            // writing to a String cannot fail
            let _ = writeln!(out, "frame {} {event:?}", index + 1);
        }
    }
    let _ = writeln!(out, "stats {:?}", adapter.stats());
    out
}

/// Replay `<dir>/frames.txt` through `A` and render the output.
fn replay<A: VenueAdapter>(cfg: A::Config, dir: &Path) -> Result<String, ParityError> {
    let frames_path = dir.join(FRAMES_FILE);
    let frames = parse_frames(&frames_path, &read(&frames_path)?)?;
    Ok(render_output::<A>(cfg, &frames))
}

/// Compare the replay of `<dir>/frames.txt` with `<dir>/expected.txt`. Never
/// writes, so a deliberately defective adapter can be run against a shared
/// fixture.
///
/// # Errors
///
/// [`ParityError`] on an unreadable file, a malformed frame line, or the first
/// output line that differs.
pub(super) fn check_fixture_parity<A: VenueAdapter>(
    cfg: A::Config,
    dir: &Path,
) -> Result<(), ParityError> {
    let actual = replay::<A>(cfg, dir)?;
    let expected_path = dir.join(EXPECTED_FILE);
    first_difference(&expected_path, &read(&expected_path)?, &actual)
}

pub(super) fn rewrite_requested(value: Option<&OsStr>) -> bool {
    value == Some(OsStr::new("1"))
}

/// Fail unless `A` reproduces `<dir>/expected.txt` from `<dir>/frames.txt`.
/// With `POLARIS_REWRITE_EXPECTED=1` it writes `expected.txt` instead.
///
/// # Panics
///
/// With the failure text, which names the first differing line.
#[track_caller]
pub fn assert_fixture_parity<A: VenueAdapter>(cfg: A::Config, dir: &Path) {
    let result = if rewrite_requested(std::env::var_os(REWRITE_ENV).as_deref()) {
        replay::<A>(cfg, dir).and_then(|actual| {
            let path = dir.join(EXPECTED_FILE);
            fs::write(&path, actual).map_err(|source| ParityError::Io { path, source })
        })
    } else {
        check_fixture_parity::<A>(cfg, dir)
    };
    let Err(err) = result else { return };
    let missing_expected = matches!(
        &err,
        ParityError::Io { path, source }
            if source.kind() == io::ErrorKind::NotFound
                && path.file_name().is_some_and(|name| name == EXPECTED_FILE)
    );
    if missing_expected {
        panic!("fixture parity: {err}\ncreate it with {REWRITE_ENV}=1");
    }
    panic!("fixture parity: {err}");
}

fn read(path: &Path) -> Result<String, ParityError> {
    fs::read_to_string(path).map_err(|source| ParityError::Io {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn first_difference(
    path: &Path,
    expected: &str,
    actual: &str,
) -> Result<(), ParityError> {
    let mut want = expected.lines();
    let mut got = actual.lines();
    for line in 1.. {
        let (e, a) = (want.next(), got.next());
        if e == a {
            if e.is_none() {
                break;
            }
            continue;
        }
        return Err(ParityError::Mismatch {
            path: path.to_path_buf(),
            line,
            expected: e.unwrap_or("<end of file>").to_owned(),
            actual: a.unwrap_or("<end of output>").to_owned(),
        });
    }
    Ok(())
}
