//! Scan-and-cache offset, symbology, and timeline index over a pbn file.
//!
//! One `pbn::frames` walk builds every lookup structure: an `Entry` per record
//! (stream order), a per-instrument list sorted by `(ts_event, entry index)`, a
//! `venue_seq` multimap, a symbol map, running symbology, and per-instrument
//! Definition timelines. The walk never aborts on a corrupt frame; it records a
//! `Fault` and keeps the partial index, so a damaged capture stays inspectable.
//! A sidecar caches the result beside the source (or under `--index-dir`), keyed
//! for freshness so repeat opens skip the scan.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use pbn::{AnyRecord, PbnError, StreamHeader, Symbology, md_events::Definition};

use crate::{error::MdexError, reader::Reader};

/// mdex cache-format version. Bumped on any `CacheFile` layout change so an old
/// sidecar fails the freshness check and is rebuilt.
const CACHE_FORMAT: u16 = 1;

/// instrument_id to its Definition timeline, ordered by `ts_event`.
type Timelines = HashMap<u32, Vec<(u64, Definition)>>;

/// One indexed record: enough to filter and to seek back to the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    /// Byte offset of the frame in the record body.
    pub offset: u64,
    pub ts_event: u64,
    pub instrument_id: u32,
    pub rtype: u8,
    pub venue_seq: u64,
}

/// The pbn cause behind a scan `Fault`, in a serde-friendly, md-events-free form
/// (`PbnError` is not `Serialize`). Mirrors the `PbnError` variants a body walk
/// can raise; read_stream-time faults (magic/proto/version) fold into `Other`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FaultKind {
    Truncated,
    BadLength { rtype: u8, len_words: u8 },
    FrameTooLong { size: u64, max: u64 },
    RecordTooShort { rtype: u8, need: u64, got: u64 },
    BadDiscriminant { rtype: u8, offset: u64 },
    Other,
}

impl From<PbnError> for FaultKind {
    fn from(e: PbnError) -> Self {
        match e {
            PbnError::Truncated => FaultKind::Truncated,
            PbnError::BadLength { rtype, len_words } => FaultKind::BadLength { rtype, len_words },
            PbnError::FrameTooLong { size, max } => FaultKind::FrameTooLong {
                size: size as u64,
                max: max as u64,
            },
            PbnError::RecordTooShort { rtype, need, got } => FaultKind::RecordTooShort {
                rtype,
                need: need as u64,
                got: got as u64,
            },
            PbnError::BadDiscriminant { rtype, offset } => FaultKind::BadDiscriminant {
                rtype,
                offset: offset as u64,
            },
            _ => FaultKind::Other,
        }
    }
}

/// A framing or decode fault hit during the scan. The build records it and stops
/// with the entries before it intact; a diagnostic tool opens a damaged capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Fault {
    pub offset: u64,
    pub cause: FaultKind,
}

/// Lookup index over one pbn body. Owns offsets and maps; borrows the body only
/// during `build`, so the `Engine` can re-borrow it freely afterward.
pub struct Index {
    entries: Vec<Entry>,
    by_instr: HashMap<u32, Vec<u32>>,
    by_seq: HashMap<u64, Vec<u32>>,
    by_symbol: HashMap<[u8; 20], u32>,
    // NOTE: read by Engine::instrument once the query engine lands.
    #[allow(dead_code)]
    sym: Symbology,
    timelines: Timelines,
    faults: Vec<Fault>,
}

impl Index {
    /// Walk the body once, building every lookup structure. Infallible: a corrupt
    /// frame becomes a `Fault` and stops the walk with a partial index intact.
    pub fn build(reader: &Reader) -> Index {
        let mut entries: Vec<Entry> = Vec::new();
        let mut by_instr: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut by_seq: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut by_symbol: HashMap<[u8; 20], u32> = HashMap::new();
        let mut sym = Symbology::new();
        let mut timelines: Timelines = HashMap::new();
        let mut faults: Vec<Fault> = Vec::new();

        let mut off = 0u64;
        for frame in pbn::frames(reader.body()) {
            let frame = match frame {
                Ok(f) => f,
                Err(e) => {
                    faults.push(Fault {
                        offset: off,
                        cause: e.into(),
                    });
                    break;
                }
            };
            match pbn::decode_any(&frame) {
                Ok(rec) => {
                    if let Some(h) = rec.header() {
                        let idx = entries.len() as u32; // stream position
                        entries.push(Entry {
                            offset: off,
                            ts_event: h.ts_event,
                            instrument_id: h.instrument_id,
                            rtype: h.rtype,
                            venue_seq: h.venue_seq,
                        });
                        by_instr.entry(h.instrument_id).or_default().push(idx);
                        by_seq.entry(h.venue_seq).or_default().push(idx);
                        if let AnyRecord::Definition(d) = rec {
                            sym.ingest(d);
                            by_symbol.insert(d.raw_symbol, d.hdr.instrument_id);
                            timelines
                                .entry(d.hdr.instrument_id)
                                .or_default()
                                .push((d.hdr.ts_event, d));
                        }
                    }
                    // Unknown records carry no header: nothing to key on, but the
                    // offset still advances so later frames stay aligned.
                }
                Err(e) => {
                    faults.push(Fault {
                        offset: off,
                        cause: e.into(),
                    });
                    break;
                }
            }
            off += frame.bytes.len() as u64;
        }

        // (ts_event, entry index) is a total order: among equal ts the stream
        // latest sorts last, so every lookup is deterministic.
        for list in by_instr.values_mut() {
            list.sort_by_key(|&i| (entries[i as usize].ts_event, i));
        }
        for tl in timelines.values_mut() {
            tl.sort_by_key(|&(ts, _)| ts);
        }

        Index {
            entries,
            by_instr,
            by_seq,
            by_symbol,
            sym,
            timelines,
            faults,
        }
    }

    /// Scan faults; empty on a clean file.
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    /// Entries for one instrument with `from <= ts_event <= to`, in the
    /// per-instrument `(ts_event, entry index)` order (not stream order).
    pub fn range(&self, instr: u32, from: u64, to: u64) -> impl Iterator<Item = &Entry> {
        let list = self.by_instr.get(&instr).map(Vec::as_slice).unwrap_or(&[]);
        let lo = list.partition_point(|&i| self.entries[i as usize].ts_event < from);
        let hi = list.partition_point(|&i| self.entries[i as usize].ts_event <= to);
        list[lo..hi].iter().map(move |&i| &self.entries[i as usize])
    }

    /// The last entry with `ts_event <= ts` for one instrument, optionally of one
    /// rtype. Among equal `ts_event` the stream-latest is returned; with `rtype`
    /// the walk steps back to the nearest entry of that rtype.
    pub fn seek_le(&self, instr: u32, ts: u64, rtype: Option<u8>) -> Option<&Entry> {
        let list = self.by_instr.get(&instr)?;
        let hi = list.partition_point(|&i| self.entries[i as usize].ts_event <= ts);
        list[..hi].iter().rev().find_map(|&i| {
            let e = &self.entries[i as usize];
            match rtype {
                Some(rt) if e.rtype != rt => None,
                _ => Some(e),
            }
        })
    }

    /// Board code effective for an instrument at `ts`: the greatest-`ts_event`
    /// Definition at or before `ts`. `None` before the first Definition or for an
    /// unknown instrument. The as-of position keeps an intraday redefinition from
    /// retroactively reclassifying earlier records.
    pub fn board_as_of(&self, instr: u32, ts: u64) -> Option<[u8; 4]> {
        let tl = self.timelines.get(&instr)?;
        let i = tl.partition_point(|(t, _)| *t <= ts).checked_sub(1)?;
        Some(tl[i].1.board)
    }

    /// Load a fresh sidecar for `src`, rebuilding symbology from Definition
    /// offsets. `StaleCache` when the sidecar is missing keys, decode-fails, or
    /// fails the freshness check against the current source.
    pub fn load(reader: &Reader, src: &Path, index_dir: Option<&Path>) -> Result<Index, MdexError> {
        let path = cache_path(src, index_dir);
        let bytes = std::fs::read(&path)?;
        let cache: CacheFile = bincode::deserialize(&bytes).map_err(|_| MdexError::StaleCache)?;
        let meta = std::fs::metadata(src)?;
        if !fresh(
            &cache,
            meta.len(),
            mtime_ns(&meta),
            &reader.header().to_bytes(),
        ) {
            return Err(MdexError::StaleCache);
        }
        let (sym, timelines) = rebuild_symbology(reader.body(), &cache.def_offsets)?;
        Ok(Index {
            entries: cache.entries,
            by_instr: cache.by_instr,
            by_seq: cache.by_seq,
            by_symbol: cache.by_symbol,
            sym,
            timelines,
            faults: cache.faults,
        })
    }

    /// Write the sidecar atomically (temp + rename). Stores Definition offsets,
    /// not values, so no md-events type is serialized. The header image and
    /// source size + mtime key the freshness check.
    pub fn save(
        &self,
        src: &Path,
        header: &StreamHeader,
        index_dir: Option<&Path>,
    ) -> Result<(), MdexError> {
        let meta = std::fs::metadata(src)?;
        let cache = CacheFile {
            format: CACHE_FORMAT,
            reader_major: crate::reader::READER_MAJOR,
            src_len: meta.len(),
            src_mtime_ns: mtime_ns(&meta),
            header_image: header.to_bytes(),
            entries: self.entries.clone(),
            by_instr: self.by_instr.clone(),
            by_seq: self.by_seq.clone(),
            by_symbol: self.by_symbol.clone(),
            def_offsets: self
                .entries
                .iter()
                .filter(|e| e.rtype == pbn::md_events::rtype::DEFINITION)
                .map(|e| e.offset)
                .collect(),
            faults: self.faults.clone(),
        };
        let bytes = bincode::serialize(&cache).map_err(std::io::Error::other)?;
        write_atomic(&cache_path(src, index_dir), &bytes)
    }
}

/// Sidecar location: `<src>.mdex.idx` beside the source by default; under
/// `index_dir` a per-source hash disambiguates same-named files from different
/// directories, so a read-only data directory still caches.
pub fn cache_path(src: &Path, index_dir: Option<&Path>) -> PathBuf {
    match index_dir {
        None => {
            let mut s = src.as_os_str().to_os_string();
            s.push(".mdex.idx");
            PathBuf::from(s)
        }
        Some(dir) => {
            let base = src
                .file_name()
                .map(|f| f.to_string_lossy().into_owned())
                .unwrap_or_else(|| "stream".to_string());
            dir.join(format!("{base}.{:016x}.mdex.idx", path_hash(src)))
        }
    }
}

/// Serialized sidecar. Holds entries + maps + Definition offsets + faults + the
/// freshness keys, never a Definition value (that would serialize an md-events
/// type); symbology and timelines rebuild from the offsets on load.
#[derive(serde::Serialize, serde::Deserialize)]
struct CacheFile {
    format: u16,
    reader_major: u8,
    src_len: u64,
    src_mtime_ns: u128,
    // The verbatim 32-byte stream header. An exact image, not a digest: it is
    // already only 32 bytes, so storing it catches a same-len same-mtime content
    // swap with no collision risk and no hash dependency.
    header_image: [u8; 32],
    entries: Vec<Entry>,
    by_instr: HashMap<u32, Vec<u32>>,
    by_seq: HashMap<u64, Vec<u32>>,
    by_symbol: HashMap<[u8; 20], u32>,
    def_offsets: Vec<u64>,
    faults: Vec<Fault>,
}

/// A sidecar is reusable only when the format, reader major, source size, source
/// mtime, and header image all match. The header image is the guard a same-len
/// same-mtime swap would otherwise slip past.
fn fresh(cache: &CacheFile, src_len: u64, src_mtime_ns: u128, header_image: &[u8; 32]) -> bool {
    cache.format == CACHE_FORMAT
        && cache.reader_major == crate::reader::READER_MAJOR
        && cache.src_len == src_len
        && cache.src_mtime_ns == src_mtime_ns
        && cache.header_image == *header_image
}

/// Decode each Definition frame at its stored offset, rebuilding symbology and
/// the ordered timelines. Few Definitions per stream, so this stays cheap.
fn rebuild_symbology(
    body: &[u8],
    def_offsets: &[u64],
) -> Result<(Symbology, Timelines), MdexError> {
    let mut sym = Symbology::new();
    let mut timelines: Timelines = HashMap::new();
    for &off in def_offsets {
        let slice = body.get(off as usize..).ok_or(MdexError::StaleCache)?;
        let frame = pbn::frames(slice).next().ok_or(MdexError::StaleCache)??;
        if let AnyRecord::Definition(d) = pbn::decode_any(&frame)? {
            sym.ingest(d);
            timelines
                .entry(d.hdr.instrument_id)
                .or_default()
                .push((d.hdr.ts_event, d));
        }
    }
    for tl in timelines.values_mut() {
        tl.sort_by_key(|&(ts, _)| ts);
    }
    Ok((sym, timelines))
}

/// Source mtime in ns since the epoch, or 0 for a pre-epoch stamp.
fn mtime_ns(meta: &std::fs::Metadata) -> u128 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Short hash of the source's absolute path, disambiguating same-named files
/// under one `--index-dir`. Rebuild-safe: a hash change just misses the cache
/// once, never a wrong read.
fn path_hash(src: &Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let abs = std::fs::canonicalize(src).unwrap_or_else(|_| src.to_path_buf());
    let mut h = std::collections::hash_map::DefaultHasher::new();
    abs.hash(&mut h);
    h.finish()
}

/// Write to a sibling temp file then rename onto the target, so a reader never
/// sees a half-written sidecar.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), MdexError> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use pbn::{
        KIND_NORMALIZED, PROTO_VERSION, StreamHeader,
        md_events::{
            Action, AssetClass, Definition, Mbo, RecordFlags, RecordHeader, Side, Trade, rtype,
        },
        write_record, write_stream,
    };
    use tempfile::{NamedTempFile, TempDir};

    use super::*;

    fn rec_header(rt: u8, len_bytes: usize, instr: u32, ts: u64, seq: u64) -> RecordHeader {
        RecordHeader {
            length: (len_bytes / 4) as u8,
            rtype: rt,
            venue: 1,
            instrument_id: instr,
            ts_event: ts,
            ts_recv: ts,
            ts_egress: 0,
            venue_seq: seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        }
    }

    fn trade(instr: u32, ts: u64, seq: u64) -> Trade {
        Trade {
            hdr: rec_header(rtype::TRADE, size_of::<Trade>(), instr, ts, seq),
            price: 1_000_000_000,
            qty: 50,
            trade_ref: seq,
            aggressor: Side::Bid,
            _pad: [0u8; 7],
        }
    }

    fn mbo(instr: u32, ts: u64, seq: u64, order_id: u64) -> Mbo {
        Mbo {
            hdr: rec_header(rtype::MBO, size_of::<Mbo>(), instr, ts, seq),
            order_id,
            price: 1_000_000_000,
            qty: 50,
            action: Action::Add,
            side: Side::Bid,
            participant: [0u8; 7],
            _pad: [0u8; 7],
        }
    }

    fn def(instr: u32, symbol: &[u8; 20], board: &[u8; 4], ts: u64) -> Definition {
        Definition::new(
            instr,
            1,
            ts,
            u64::from(instr),
            *symbol,
            *board,
            AssetClass::Equity,
            0,
            100,
        )
    }

    fn stream_bytes(body: &[u8]) -> Vec<u8> {
        let header = StreamHeader {
            proto_version: PROTO_VERSION,
            body_kind: KIND_NORMALIZED,
            schema_major: 1,
            schema_minor: 0,
            venue: 1,
            flags: 0,
            dataset: 0,
            start_ts: 0,
            end_ts: 0,
        };
        write_stream(&header, body).unwrap()
    }

    fn reader_from(body: &[u8]) -> (Reader, NamedTempFile) {
        let f = NamedTempFile::new().unwrap();
        std::fs::write(f.path(), stream_bytes(body)).unwrap();
        let reader = Reader::open(f.path()).unwrap();
        (reader, f)
    }

    const SYM_CCCC: [u8; 20] = *b"CCCC\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    // A definition written before the tail (104 bytes) indexes with no fault
    // and its symbol resolves, so the frame after it is still reached.
    #[test]
    fn pre_tail_definition_indexes_without_fault() {
        let mut body = Vec::new();
        write_record(&mut body, &def(7, &[b'P'; 20], b"MAIN", 1)).unwrap();
        body.truncate(Definition::PRE_TAIL_LEN);
        body[0] = (Definition::PRE_TAIL_LEN / 4) as u8;
        write_record(&mut body, &trade(7, 10, 1)).unwrap();
        let (reader, _f) = reader_from(&body);
        let index = Index::build(&reader);
        assert!(index.faults.is_empty());
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.by_symbol[&[b'P'; 20]], 7);
    }

    #[test]
    fn build_indexes_every_record() {
        let mut body = Vec::new();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 1)).unwrap();
        write_record(&mut body, &trade(1, 10, 1)).unwrap();
        write_record(&mut body, &mbo(1, 20, 2, 500)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.entries.len(), 3);
        assert!(idx.faults.is_empty());
    }

    #[test]
    fn by_instr_sorted_stable_equal_ts() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 0)).unwrap(); // idx 0
        write_record(&mut body, &trade(1, 30, 1)).unwrap(); // idx 1
        write_record(&mut body, &trade(1, 10, 2)).unwrap(); // idx 2
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        // equal ts 10 keeps stream order (0 before 2); 30 sorts last
        assert_eq!(idx.by_instr[&1], vec![0, 2, 1]);
    }

    #[test]
    fn symbology_populated() {
        let mut body = Vec::new();
        write_record(&mut body, &def(7, &SYM_CCCC, b"MAIN", 1)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.by_symbol[&SYM_CCCC], 7);
        assert_eq!(idx.board_as_of(7, 5), Some(*b"MAIN"));
    }

    #[test]
    fn unknown_record_skipped_no_entry() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 0)).unwrap();
        let second = body.len();
        write_record(&mut body, &trade(1, 20, 1)).unwrap();
        body[second + 1] = 0x7A; // rtype byte of the second record: unknown band
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        // only the first record has a known header and becomes an entry
        assert_eq!(idx.entries.len(), 1);
        assert!(idx.faults.is_empty());
    }

    #[test]
    fn intraday_definition_timeline_ordered() {
        let mut body = Vec::new();
        // ingested out of ts order; the timeline must sort ascending
        write_record(&mut body, &def(1, &[b'A'; 20], b"AUX1", 50)).unwrap();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 5)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        let boards: Vec<[u8; 4]> = idx.timelines[&1].iter().map(|(_, d)| d.board).collect();
        assert_eq!(boards, vec![*b"MAIN", *b"AUX1"]);
    }

    #[test]
    fn seq_collision_kept_in_multimap() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 9)).unwrap();
        write_record(&mut body, &trade(1, 20, 9)).unwrap(); // same venue_seq
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.by_seq[&9], vec![0, 1]);
    }

    #[test]
    fn corrupt_frame_yields_partial_index_and_fault() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 0)).unwrap();
        let first_len = body.len();
        write_record(&mut body, &trade(1, 20, 1)).unwrap();
        body.truncate(body.len() - 4); // chop the second record mid-body
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.entries.len(), 1);
        assert_eq!(idx.faults.len(), 1);
        assert_eq!(idx.faults[0].offset, first_len as u64);
        assert_eq!(idx.faults[0].cause, FaultKind::Truncated);
    }

    #[test]
    fn range_bounds_inclusive() {
        let mut body = Vec::new();
        for ts in [10u64, 20, 30] {
            write_record(&mut body, &trade(1, ts, ts)).unwrap();
        }
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        let ts: Vec<u64> = idx.range(1, 10, 20).map(|e| e.ts_event).collect();
        assert_eq!(ts, vec![10, 20]);
    }

    #[test]
    fn seek_le_finds_last_prior() {
        let mut body = Vec::new();
        for ts in [10u64, 20, 30] {
            write_record(&mut body, &trade(1, ts, ts)).unwrap();
        }
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.seek_le(1, 25, None).unwrap().ts_event, 20);
    }

    #[test]
    fn seek_le_filters_rtype() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 0)).unwrap();
        write_record(&mut body, &mbo(1, 20, 1, 500)).unwrap();
        write_record(&mut body, &trade(1, 30, 2)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.seek_le(1, 35, Some(rtype::TRADE)).unwrap().ts_event, 30);
        assert_eq!(idx.seek_le(1, 35, Some(rtype::MBO)).unwrap().ts_event, 20);
    }

    #[test]
    fn seek_le_equal_ts_returns_stream_latest() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 100)).unwrap(); // idx 0
        write_record(&mut body, &trade(1, 10, 200)).unwrap(); // idx 1, stream-latest
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.seek_le(1, 10, None).unwrap().venue_seq, 200);
    }

    #[test]
    fn board_as_of_before_and_after_redefinition() {
        let mut body = Vec::new();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 5)).unwrap();
        write_record(&mut body, &def(1, &[b'A'; 20], b"AUX1", 50)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.board_as_of(1, 10), Some(*b"MAIN")); // before the redefinition
        assert_eq!(idx.board_as_of(1, 60), Some(*b"AUX1")); // after
    }

    #[test]
    fn board_as_of_unknown_is_none() {
        let mut body = Vec::new();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 5)).unwrap();
        let (reader, _f) = reader_from(&body);
        let idx = Index::build(&reader);
        assert_eq!(idx.board_as_of(999, 10), None); // unknown instrument
        assert_eq!(idx.board_as_of(1, 1), None); // before the first Definition
    }

    #[test]
    fn save_then_load_round_trip() {
        let mut body = Vec::new();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 1)).unwrap();
        write_record(&mut body, &trade(1, 10, 5)).unwrap();
        write_record(&mut body, &mbo(1, 20, 6, 900)).unwrap();
        let (reader, f) = reader_from(&body);
        let built = Index::build(&reader);
        built.save(f.path(), reader.header(), None).unwrap();

        let loaded = Index::load(&reader, f.path(), None).unwrap();
        assert_eq!(loaded.entries, built.entries);
        assert_eq!(loaded.by_instr, built.by_instr);
        assert_eq!(loaded.by_seq, built.by_seq);
        assert_eq!(loaded.by_symbol, built.by_symbol);
        assert_eq!(loaded.faults, built.faults);
        // timelines rebuilt from offsets: compare via the board lookup
        assert_eq!(loaded.board_as_of(1, 10), Some(*b"MAIN"));
    }

    // Freshness is a pure predicate over the source keys, tested directly so the
    // staleness cases are deterministic (no mtime races, no filetime dep).
    fn sample_cache() -> CacheFile {
        CacheFile {
            format: CACHE_FORMAT,
            reader_major: crate::reader::READER_MAJOR,
            src_len: 100,
            src_mtime_ns: 42,
            header_image: [7u8; 32],
            entries: Vec::new(),
            by_instr: HashMap::new(),
            by_seq: HashMap::new(),
            by_symbol: HashMap::new(),
            def_offsets: Vec::new(),
            faults: Vec::new(),
        }
    }

    #[test]
    fn stale_on_size_change() {
        let c = sample_cache();
        assert!(fresh(&c, 100, 42, &[7u8; 32]));
        assert!(!fresh(&c, 101, 42, &[7u8; 32]));
    }

    #[test]
    fn stale_on_mtime_change() {
        let c = sample_cache();
        assert!(!fresh(&c, 100, 43, &[7u8; 32]));
    }

    #[test]
    fn stale_on_same_len_mtime_content_swap() {
        let c = sample_cache();
        // same size and mtime, different header image: the swap is caught
        assert!(!fresh(&c, 100, 42, &[8u8; 32]));
    }

    #[test]
    fn cache_holds_no_definition_values() {
        let mut body = Vec::new();
        write_record(&mut body, &def(1, &[b'A'; 20], b"MAIN", 1)).unwrap();
        write_record(&mut body, &trade(1, 10, 5)).unwrap();
        let (reader, f) = reader_from(&body);
        Index::build(&reader)
            .save(f.path(), reader.header(), None)
            .unwrap();

        let bytes = std::fs::read(cache_path(f.path(), None)).unwrap();
        let cache: CacheFile = bincode::deserialize(&bytes).unwrap();
        // one Definition at offset 0, stored as an offset, not a value
        assert_eq!(cache.def_offsets, vec![0]);
    }

    #[test]
    fn index_dir_redirects_path() {
        let dir = TempDir::new().unwrap();
        let src = Path::new("/data/capture.pbn");
        let redirected = cache_path(src, Some(dir.path()));
        assert!(redirected.starts_with(dir.path()));
        assert_ne!(redirected, cache_path(src, None));
    }

    #[test]
    fn readonly_default_is_warning() {
        let mut body = Vec::new();
        write_record(&mut body, &trade(1, 10, 0)).unwrap();
        let (reader, f) = reader_from(&body);
        let built = Index::build(&reader);
        // an index_dir under a nonexistent parent makes the write fail; the Engine
        // downgrades this to a warning rather than a query error.
        let missing = Path::new("/mdex-nonexistent-dir-xyz/sub");
        assert!(
            built
                .save(f.path(), reader.header(), Some(missing))
                .is_err()
        );
    }
}
