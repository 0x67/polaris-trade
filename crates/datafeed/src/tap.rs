//! Capture tap seam: raw venue bytes tapped before the adapter, written to
//! self-describing MDC1 files split by time window. Raw capture is the replay
//! source of truth, so an adapter re-run over it reproduces the normalized
//! stream without re-tapping the wire.

use std::{
    collections::HashMap,
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use md_pipeline::{BundledTzdb, StreamId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    FeedError,
    session_window::{OpenSession, add_days, days_from_civil, resolve_session},
    venue::VenueDescriptor,
    wiring::{capture_dir, capture_filename, window_label},
};

/// Failure writing a captured frame or record.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CaptureTapError {
    #[error("capture tap: {0}")]
    Capture(#[from] md_capture::CaptureError),
    #[error("capture tap io: {0}")]
    Io(#[from] std::io::Error),
    #[error("capture tap manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("schema {schema} window regressed: open {open}, saw {saw}")]
    WindowRegression {
        schema: &'static str,
        open: u64,
        saw: u64,
    },
    #[error("capture date regressed: open {open}, saw {saw}")]
    DayRegression { open: u32, saw: u32 },
    #[error("venue {venue} session does not resolve for ts_recv {ts_recv}")]
    SessionUnresolved { venue: &'static str, ts_recv: u64 },
    #[error("manifest in {} lists partition {found}, tap writes {want}", .dir.display())]
    ManifestPartition {
        dir: PathBuf,
        found: String,
        want: String,
    },
}

/// The one capture point a session drives: raw pre-adapter frames.
pub trait CaptureTap {
    fn raw(
        &mut self,
        stream: StreamId,
        ts_recv: u64,
        payload: &[u8],
    ) -> Result<(), CaptureTapError>;
}

/// Capture disabled. Default when `TapConfig` has no dir set.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTap;

impl CaptureTap for NoopTap {
    fn raw(
        &mut self,
        _stream: StreamId,
        _ts_recv: u64,
        _payload: &[u8],
    ) -> Result<(), CaptureTapError> {
        Ok(())
    }
}

const NS_PER_DAY: u64 = 86_400_000_000_000;

/// One open raw writer: the current window (no schema split).
struct RawSlot {
    window: u64,
    /// The window's offset from its session's open, which its label counts.
    offset: u64,
    path: PathBuf,
    records: u64,
    writer: md_capture::RawWriter<fs::File>,
}

/// Coverage record for one closed output file, listed in its dir's manifest.
#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    window: String,
    /// Leaf filename, resolved against the manifest's own directory.
    path: String,
    records: u64,
    sha256: String,
}

/// Per-directory manifest: a reader learns coverage without stat-ing files.
#[derive(Serialize, Deserialize)]
struct Manifest {
    venue: String,
    version: String,
    partition: String,
    files: Vec<ManifestEntry>,
}

impl Manifest {
    /// Manifest listing no files yet for `venue` under the `interval_ns` partition.
    fn new(venue: &VenueDescriptor, interval_ns: u64) -> Self {
        Self {
            venue: venue.tag.to_owned(),
            version: venue.version.to_owned(),
            partition: interval_token(interval_ns),
            files: Vec::new(),
        }
    }
}

/// Rewrite `dir/manifest.json`, entries sorted by leaf name so it is
/// byte-stable across replays. A synced sibling renamed over the old file
/// means a kill, full disk, or power cut mid-write leaves the old one whole.
fn write_manifest(dir: &Path, manifest: &mut Manifest) -> Result<(), CaptureTapError> {
    manifest.files.sort_by(|a, b| a.path.cmp(&b.path));
    let staging = dir.join("manifest.json.tmp");
    let mut file = fs::File::create(&staging)?;
    file.write_all(&serde_json::to_vec_pretty(manifest)?)?;
    file.sync_all()?;
    fs::rename(&staging, dir.join("manifest.json"))?;
    Ok(())
}

/// Canonical duration token for `interval_ns` (inverse of `parse_resolution`):
/// the largest unit that divides it evenly. Partition tokens are integer
/// multiples of one second, so the `s` branch always terminates.
fn interval_token(interval_ns: u64) -> String {
    const S: u64 = 1_000_000_000;
    for (unit, suffix) in [(86_400 * S, 'd'), (3_600 * S, 'h'), (60 * S, 'm'), (S, 's')] {
        if interval_ns.is_multiple_of(unit) {
            return format!("{}{}", interval_ns / unit, suffix);
        }
    }
    format!("{interval_ns}ns")
}

/// Lowercase hex of a digest. `sha2` 0.11 returns a `hybrid_array::Array` with no
/// `LowerHex`, so format the bytes directly; any `&[u8]`-deref digest works.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Real md-capture tap: one raw writer per window. Writers open lazily and roll
/// on window advance, bounding open handles to one; a frame regressing to a
/// finished window errors rather than reopening. Each close rewrites its dir's
/// manifest. Windows and date dirs follow the venue's
/// [`VenueDescriptor::session`]: a frame past the open session's end rolls the
/// whole date dir, and `meta.date` is the open trade date, advancing per roll.
pub struct MdCaptureTap {
    root: PathBuf,
    venue: &'static VenueDescriptor,
    meta: md_capture::CaptureMeta,
    interval_ns: u64,
    /// Resolves zoned session anchors, consulted only when a session opens.
    tzdb: BundledTzdb,
    raw: Option<RawSlot>,
    /// Session of the open date dir, seeded from the first frame's `ts_recv`.
    /// A frame past its end rolls the dir; one before its start errors.
    session: Option<OpenSession>,
    /// Manifest of each dir this tap writes, keyed by dir and rewritten on
    /// every file close there. Cleared at a day roll.
    manifest: HashMap<PathBuf, Manifest>,
}

impl MdCaptureTap {
    /// Configure the tap. `root` is the capture bucket root. Files open lazily.
    /// A zero `meta.date` takes the first frame's session label (live, where
    /// `ts_recv` is the wall clock); a nonzero one names the first date dir.
    pub fn new(
        root: PathBuf,
        venue: &'static VenueDescriptor,
        meta: md_capture::CaptureMeta,
        interval_ns: u64,
    ) -> Self {
        Self {
            root,
            venue,
            meta,
            interval_ns,
            tzdb: BundledTzdb::new(),
            raw: None,
            session: None,
            manifest: HashMap::new(),
        }
    }

    /// Open the session holding `ts_recv` and return its start anchor. The
    /// first frame seeds it; the dir keeps a nonzero `meta.date` name, and a
    /// zero one takes the session label. A later session rolls the date dir:
    /// finish the writer, drop the completed day's manifest stage, advance
    /// `meta.date` by the label day delta. A frame before the open session's
    /// start is a `DayRegression`, checked before any zone lookup so a clock
    /// regression is never reported as a session that does not resolve.
    fn enter_session(&mut self, ts_recv: u64) -> Result<u64, CaptureTapError> {
        let unresolved = CaptureTapError::SessionUnresolved {
            venue: self.venue.name,
            ts_recv,
        };
        if let Some(open) = &self.session
            && ts_recv < open.start
        {
            let saw = self
                .venue
                .session
                .session_label(ts_recv, &self.tzdb)
                .map(|label| {
                    add_days(
                        self.meta.date,
                        days_from_civil(label) - days_from_civil(open.label),
                    )
                })
                .ok_or(unresolved)?;
            return Err(CaptureTapError::DayRegression {
                open: self.meta.date,
                saw,
            });
        }
        let next = resolve_session(&self.venue.session, &self.tzdb, ts_recv).ok_or(unresolved)?;
        if let Some(open) = &self.session {
            let delta = days_from_civil(next.label) - days_from_civil(open.label);
            let date = add_days(self.meta.date, delta);
            self.finish_open_writers()?;
            // The close above wrote the completed day's last manifest.
            self.manifest.clear();
            tracing::info!(from = self.meta.date, to = date, "capture date roll");
            self.meta.date = date;
        } else if self.meta.date == 0 {
            self.meta.date = next.label;
        }
        let start = next.start;
        self.session = Some(next);
        Ok(start)
    }

    /// Ensure the raw writer for the window `offset` past the session open at
    /// `session_start` is open, finishing the prior window on advance and
    /// erroring on regression.
    fn ensure_raw(&mut self, session_start: u64, offset: u64) -> Result<(), CaptureTapError> {
        let window = session_start + offset;
        match &self.raw {
            Some(slot) if slot.window == window => return Ok(()),
            Some(slot) if window < slot.window => {
                return Err(CaptureTapError::WindowRegression {
                    schema: "raw",
                    open: slot.window,
                    saw: window,
                });
            }
            Some(_) => self.finish_open_writers()?,
            None => {}
        }
        let dir = capture_dir(&self.root, self.venue, self.meta.date);
        self.stage_existing_manifest(&dir)?;
        fs::create_dir_all(&dir)?;
        // Never truncate: an earlier run (a restart inside this window) keeps
        // its file, and this run takes the next free `_rN` part.
        let mut part = 0;
        let (path, file) = loop {
            let path = dir.join(capture_filename(
                self.venue,
                self.meta.date,
                offset,
                self.interval_ns,
                part,
            ));
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => break (path, file),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => part += 1,
                Err(err) => return Err(err.into()),
            }
        };
        let writer = md_capture::RawWriter::new(file, self.meta)?;
        self.raw = Some(RawSlot {
            window,
            offset,
            path,
            records: 0,
            writer,
        });
        Ok(())
    }

    /// The first time this tap writes into `dir`, stage the entries of the
    /// `manifest.json` an earlier run left there, so the next close lists the
    /// earlier files beside this run's. A manifest under another partition is
    /// refused before any byte is written: one manifest names one partition.
    /// One that does not parse (torn by a crash) is renamed to
    /// `manifest.json.corrupt` and logged, so it costs the earlier entries,
    /// never the session.
    fn stage_existing_manifest(&mut self, dir: &Path) -> Result<(), CaptureTapError> {
        if self.manifest.contains_key(dir) {
            return Ok(());
        }
        let path = dir.join("manifest.json");
        let mut staged = Manifest::new(self.venue, self.interval_ns);
        match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Manifest>(&bytes) {
                Ok(existing) if existing.partition != staged.partition => {
                    return Err(CaptureTapError::ManifestPartition {
                        dir: dir.to_path_buf(),
                        found: existing.partition,
                        want: staged.partition,
                    });
                }
                Ok(existing) => staged.files = existing.files,
                Err(err) => {
                    let aside = dir.join("manifest.json.corrupt");
                    fs::rename(&path, &aside)?;
                    tracing::warn!(
                        path = %path.display(),
                        aside = %aside.display(),
                        %err,
                        "unreadable capture manifest set aside"
                    );
                }
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        self.manifest.insert(dir.to_path_buf(), staged);
        Ok(())
    }

    /// List a just-closed file in its dir's manifest: hash the finished bytes
    /// (reopened, since the write handle was create-only) and rewrite the
    /// manifest, so a run killed later still leaves this file listed for the
    /// restart's merge.
    fn record_closed(
        &mut self,
        path: &Path,
        offset: u64,
        records: u64,
    ) -> Result<(), CaptureTapError> {
        // Stream the closed file through the hasher; a 1d window can be many GB,
        // so never slurp it whole. Cold path (close-time, one file at a time).
        use std::io::Read as _;
        let mut hasher = Sha256::new();
        let mut reader = std::io::BufReader::new(fs::File::open(path)?);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let entry = ManifestEntry {
            window: window_label(self.meta.date, offset, self.interval_ns),
            path: path
                .file_name()
                .expect("output file has a name")
                .to_string_lossy()
                .into_owned(),
            records,
            sha256: hex(&hasher.finalize()),
        };
        let dir = path.parent().expect("output file has a parent dir");
        let (venue, interval_ns) = (self.venue, self.interval_ns);
        let manifest = self
            .manifest
            .entry(dir.to_path_buf())
            .or_insert_with(|| Manifest::new(venue, interval_ns));
        manifest.files.push(entry);
        write_manifest(dir, manifest)
    }

    /// Finish the open writer and list it in its dir's manifest.
    fn finish_open_writers(&mut self) -> Result<(), CaptureTapError> {
        if let Some(slot) = self.raw.take() {
            slot.writer.finish()?;
            self.record_closed(&slot.path, slot.offset, slot.records)?;
        }
        Ok(())
    }

    /// Flush and close the open writer, whose close writes the final
    /// manifest; every earlier file was listed as it closed. Zstd frames are
    /// only valid once finished, so a session must call this before dropping
    /// the tap.
    pub fn finish(mut self) -> Result<(), CaptureTapError> {
        self.finish_open_writers()?;
        tracing::info!(venue = self.venue.name, "capture finished");
        Ok(())
    }
}

impl CaptureTap for MdCaptureTap {
    fn raw(
        &mut self,
        stream: StreamId,
        ts_recv: u64,
        payload: &[u8],
    ) -> Result<(), CaptureTapError> {
        let start = match &self.session {
            Some(open) if (open.start..open.end).contains(&ts_recv) => open.start,
            _ => self.enter_session(ts_recv)?,
        };
        // `start` opens the session holding `ts_recv`, so it is never later.
        let since_open = ts_recv - start;
        // A day partition is the session itself, so a 23h or 25h DST session
        // stays one window instead of spilling a sliver into a phantom one.
        let offset = if self.interval_ns >= NS_PER_DAY {
            0
        } else {
            since_open - since_open % self.interval_ns
        };
        self.ensure_raw(start, offset)?;
        let slot = self.raw.as_mut().expect("ensured present");
        slot.writer.write_row(stream.0, ts_recv, payload)?;
        slot.records += 1;
        Ok(())
    }
}

/// One concrete tap type selected at wiring time. Enum dispatch, not `dyn`:
/// the trait is object-safe now that the generic record method is gone, but a
/// vtable hop per frame buys nothing over a two-variant match.
// Boxing Md to shrink the enum would add a deref on every write to save 248
// bytes once. Wrong trade here.
#[allow(clippy::large_enum_variant)]
pub enum AnyTap {
    Noop(NoopTap),
    Md(MdCaptureTap),
}

impl AnyTap {
    /// Flush and close writers at session end (no-op for the noop tap).
    pub fn finish(self) -> Result<(), CaptureTapError> {
        match self {
            AnyTap::Noop(_) => Ok(()),
            AnyTap::Md(tap) => tap.finish(),
        }
    }
}

/// Finish the capture once a runner's session ends, on success (`Ok`) or a
/// fault (`Err`) alike, so a truncated tail never escapes. The session fault
/// propagates ahead of any finish error; a finish error on the fault path is
/// logged, not dropped silently.
pub(crate) fn finish_capture<T>(
    venue: &str,
    tap: AnyTap,
    outcome: Result<T, FeedError>,
) -> Result<T, FeedError> {
    let finished = tap.finish();
    match outcome {
        Err(session_err) => {
            if let Err(finish_err) = finished {
                tracing::error!(venue, %finish_err, "capture finish failed after session fault");
            }
            Err(session_err)
        }
        Ok(value) => {
            finished?;
            Ok(value)
        }
    }
}

impl CaptureTap for AnyTap {
    fn raw(
        &mut self,
        stream: StreamId,
        ts_recv: u64,
        payload: &[u8],
    ) -> Result<(), CaptureTapError> {
        match self {
            AnyTap::Noop(t) => t.raw(stream, ts_recv, payload),
            AnyTap::Md(t) => t.raw(stream, ts_recv, payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use md_capture::CaptureMeta;
    use md_pipeline::{ClosureModel, RollModel, SessionModel};

    use super::*;
    use crate::venue::NASDAQ;

    // New York midnight of 2026-05-21, the trade date `meta()` names: epoch day
    // 20594, and 00:00 EDT is 04:00 UTC.
    const OPEN: u64 = (20_594 * 24 + 4) * 3_600_000_000_000;

    fn meta() -> CaptureMeta {
        CaptureMeta {
            venue: 1,
            date: 20260521,
            schema: 1,
            feed: md_capture::FEED_UNSPECIFIED,
        }
    }

    // A later window finishes the prior file and opens a new one; a regression
    // to an earlier window errors instead of corrupting a closed file. Hours
    // count from the New York session open.
    #[test]
    fn window_rolls_and_rejects_regression() {
        let dir = std::env::temp_dir().join("datafeed_tap_roll");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        tap.raw(StreamId(0), OPEN + 13 * hour + 1, b"payload")
            .expect("hour 13");
        tap.raw(StreamId(0), OPEN + 14 * hour + 1, b"payload")
            .expect("hour 14 rolls");
        let regressed = tap.raw(StreamId(0), OPEN + 13 * hour + 2, b"payload");
        assert!(
            matches!(regressed, Err(CaptureTapError::WindowRegression { .. })),
            "regression rejected"
        );
        drop(tap);

        assert!(
            dir.join("nasdaq/v5/2026-05-21/nasdaq_v5_2026-05-21T13.mdc1.zst")
                .exists()
        );
        assert!(
            dir.join("nasdaq/v5/2026-05-21/nasdaq_v5_2026-05-21T14.mdc1.zst")
                .exists()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Sub-hour partition: two records in adjacent 30m windows produce two
    // distinct files (minute precision), not one truncating the other. Both
    // sit 13h past the New York session open.
    #[test]
    fn sub_hour_windows_do_not_collide() {
        let dir = std::env::temp_dir().join("datafeed_tap_subhour");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let half = 1_800_000_000_000u64;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), half);
        tap.raw(StreamId(0), OPEN + 13 * hour + 1, b"payload")
            .expect("13:00 window");
        tap.raw(StreamId(0), OPEN + 13 * hour + half + 1, b"payload")
            .expect("13:30 window rolls");
        tap.finish().expect("finish");

        let day_dir = dir.join("nasdaq/v5/2026-05-21");
        assert!(day_dir.join("nasdaq_v5_2026-05-21T1300.mdc1.zst").exists());
        assert!(day_dir.join("nasdaq_v5_2026-05-21T1330.mdc1.zst").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Live-shaped tap writes a raw frame at <root>/<venue>/<version>/<date>/
    // <venue>_<version>_<window>.mdc1.zst, no schema segment. The window comes
    // from ts_recv, not wall-clock: 13h past the New York open lands the T13
    // leaf. Readback recovers the stream tag, ts_recv, and payload verbatim.
    #[test]
    fn raw_writes_into_contract_path() {
        let dir = std::env::temp_dir().join("datafeed_tap_rawpath");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let ts_recv = OPEN + 13 * hour + 42;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        tap.raw(StreamId(1), ts_recv, b"venue-bytes")
            .expect("write raw");
        tap.finish().expect("finish");

        let raw_path = dir
            .join("nasdaq/v5/2026-05-21")
            .join("nasdaq_v5_2026-05-21T13.mdc1.zst");
        assert!(raw_path.exists(), "raw file at contract path");

        let (mut reader, _) =
            md_capture::RawReader::new(std::io::Cursor::new(std::fs::read(&raw_path).unwrap()))
                .expect("open raw reader");
        let row = reader.read_row().expect("read").expect("one row");
        assert_eq!(row.stream, 1);
        assert_eq!(row.ts_recv, ts_recv);
        assert_eq!(row.payload, b"venue-bytes");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // finish() writes one manifest.json per dir listing every closed raw file
    // with its record count and a sha256 that matches the bytes on disk. Rows
    // sit 13h and 14h past the New York session open.
    #[test]
    fn manifest_covers_written_files_with_counts_and_hashes() {
        let dir = std::env::temp_dir().join("datafeed_tap_manifest");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        tap.raw(StreamId(0), OPEN + 13 * hour + 10, b"payload")
            .expect("window 13, row 1");
        tap.raw(StreamId(0), OPEN + 13 * hour + 20, b"payload")
            .expect("window 13, row 2");
        tap.raw(StreamId(0), OPEN + 14 * hour + 30, b"payload")
            .expect("window 14, row 1");
        tap.finish().expect("finish");

        let day_dir = dir.join("nasdaq/v5/2026-05-21");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(day_dir.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["venue"], "nasdaq");
        assert_eq!(manifest["version"], "v5");
        assert_eq!(manifest["partition"], "1h");

        let files = manifest["files"].as_array().expect("files array");
        assert_eq!(files.len(), 2, "one entry per window");

        let by_path = |leaf: &str| {
            files
                .iter()
                .find(|f| f["path"] == leaf)
                .unwrap_or_else(|| panic!("no {leaf} entry"))
        };
        assert_eq!(by_path("nasdaq_v5_2026-05-21T13.mdc1.zst")["records"], 2);
        assert_eq!(by_path("nasdaq_v5_2026-05-21T14.mdc1.zst")["records"], 1);

        // Every listed file exists in the dir and its sha256 matches its bytes.
        for f in files {
            let leaf = f["path"].as_str().unwrap();
            let bytes = std::fs::read(day_dir.join(leaf)).expect("listed file exists");
            assert_eq!(
                f["sha256"],
                hex(&Sha256::digest(&bytes)),
                "manifest hash matches {leaf}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Frame whose New York day advances rolls the whole date dir: old day's
    // writer finishes and its manifest.json lands at roll time, before finish.
    // ts_recv drives the day; trade-date name advances by the day delta, so the
    // next day opens a distinct dir. Fails if roll skips flush or reuses the dir.
    #[test]
    fn session_day_advance_rolls_dir_and_flushes_manifest() {
        let dir = std::env::temp_dir().join("datafeed_tap_dayroll");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let day = 24 * hour;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        // first frame seeds the open session at 21:00 in New York, 01:00 UTC on
        // 2026-05-22; dir named by meta.date.
        tap.raw(StreamId(0), OPEN + 21 * hour, b"payload")
            .expect("day one");
        // New York midnight is 04:00 UTC, so a frame 24h past the open is in the
        // next session on the same UTC day.
        tap.raw(StreamId(0), OPEN + day + 10, b"payload")
            .expect("next session day rolls");

        // The roll flushed day one's manifest already, without waiting on finish.
        let day1 = dir.join("nasdaq/v5/2026-05-21");
        let m1: serde_json::Value =
            serde_json::from_slice(&std::fs::read(day1.join("manifest.json")).unwrap())
                .expect("day-one manifest present at roll");
        let files = m1["files"].as_array().expect("files array");
        assert_eq!(files.len(), 1, "day one indexes its single raw file");
        assert_eq!(files[0]["records"], 1);
        let leaf = files[0]["path"].as_str().unwrap();
        let bytes = std::fs::read(day1.join(leaf)).expect("day-one file exists");
        assert_eq!(files[0]["sha256"], hex(&Sha256::digest(&bytes)));

        // The second session opened its own distinct trade-date dir, not flushed
        // until finish closes it.
        let day2 = dir.join("nasdaq/v5/2026-05-22");
        assert!(day2.exists(), "second session opened a distinct date dir");
        assert!(
            !day2.join("manifest.json").exists(),
            "second day not flushed before finish"
        );

        tap.finish().expect("finish");
        assert!(
            day2.join("manifest.json").exists(),
            "finish flushes the final open day"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A frame whose New York day is earlier than the open day errors instead of
    // reopening a finished date dir, the day-level analogue of the window guard.
    #[test]
    fn earlier_session_day_rejected() {
        let dir = std::env::temp_dir().join("datafeed_tap_dayregress");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let day = 24 * hour;

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        tap.raw(StreamId(0), OPEN + day + 10, b"payload")
            .expect("later session day seeds");
        // three hours earlier on the same UTC day, in the session before
        let regressed = tap.raw(StreamId(0), OPEN + 21 * hour, b"payload");
        assert!(
            matches!(regressed, Err(CaptureTapError::DayRegression { .. })),
            "earlier session day rejected"
        );
        drop(tap);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Records either side of UTC midnight in one New York session, 1d partition:
    // one window, one file, one manifest entry holding both. A UTC floor splits
    // them into two same-named windows, truncating the first. The config offset
    // stays unset: the tap anchors on the venue session itself.
    #[test]
    fn straddle_writes_one_file_no_dup_manifest() {
        use crate::config::{DatafeedConfig, SinkKind, SourceConfig, TapConfig};

        let dir = std::env::temp_dir().join("datafeed_tap_straddle");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;

        let config = DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 1,
            session_midnight_ns: 0,
            source: SourceConfig::Live {
                endpoint: "127.0.0.1:1".parse().unwrap(),
                soupbin: Default::default(),
            },
            sink: SinkKind::Null,
            taps: TapConfig {
                normalized_dir: None,
                raw_dir: Some(dir.clone()),
                capture_date: 0,
                partition: "1d".to_string(),
            },
            ohlcv_resolutions: crate::config::default_resolution_tokens(),
            observability: None,
        };
        let mut tap = crate::wiring::build_tap(&config).expect("md tap builds");

        // 20:00 UTC (16:00 in New York) then 02:00 UTC of the next UTC day (22:00
        // in New York): one session day straddling UTC midnight. Live names the
        // dir after that session, 2026-05-21.
        let pre = OPEN + 16 * hour;
        let post = OPEN + 22 * hour;
        tap.raw(StreamId(0), pre, b"pre")
            .expect("pre-UTC-midnight record");
        tap.raw(StreamId(0), post, b"post")
            .expect("post-UTC-midnight record");
        tap.finish().expect("finish");

        let day_dir = dir.join("nasdaq/v5/2026-05-21");
        let mut leaves: Vec<String> = std::fs::read_dir(&day_dir)
            .expect("day dir exists")
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".mdc1.zst"))
            .collect();
        leaves.sort();
        assert_eq!(
            leaves,
            vec!["nasdaq_v5_2026-05-21.mdc1.zst".to_string()],
            "exactly one output file for the session day"
        );

        // Both records survive in the one file: the first window was never
        // truncated by a reopened writer.
        let raw_path = day_dir.join("nasdaq_v5_2026-05-21.mdc1.zst");
        let (mut reader, _) =
            md_capture::RawReader::new(std::io::Cursor::new(std::fs::read(&raw_path).unwrap()))
                .expect("open raw reader");
        let first = reader.read_row().expect("read").expect("first record");
        let second = reader.read_row().expect("read").expect("second record");
        assert_eq!(first.ts_recv, pre);
        assert_eq!(second.ts_recv, post);

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(day_dir.join("manifest.json")).unwrap()).unwrap();
        let files = manifest["files"].as_array().expect("files array");
        assert_eq!(files.len(), 1, "one manifest entry, no phantom duplicate");
        assert_eq!(files[0]["path"], "nasdaq_v5_2026-05-21.mdc1.zst");
        assert_eq!(files[0]["records"], 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A restart inside the window keeps the first run's file byte-for-byte and
    // writes an `_r1` sibling; the manifest lists both runs' files.
    #[test]
    fn restart_in_window_keeps_earlier_file_and_manifest_entry() {
        let dir = std::env::temp_dir().join("datafeed_tap_restart");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;
        let day_dir = dir.join("nasdaq/v5/2026-05-21");
        let (base, part) = (
            "nasdaq_v5_2026-05-21.mdc1.zst",
            "nasdaq_v5_2026-05-21_r1.mdc1.zst",
        );

        let mut first = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        first
            .raw(StreamId(0), OPEN + 10, b"one")
            .expect("first run");
        first
            .raw(StreamId(0), OPEN + 20, b"two")
            .expect("first run");
        first.finish().expect("first run finishes");
        let first_bytes = std::fs::read(day_dir.join(base)).expect("first run file");

        let mut second = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        second
            .raw(StreamId(0), OPEN + 30, b"three")
            .expect("restarted run");
        second.finish().expect("restarted run finishes");

        assert_eq!(
            std::fs::read(day_dir.join(base)).expect("first run file kept"),
            first_bytes,
            "restart must not truncate the first run's file"
        );
        let (mut reader, _) = md_capture::RawReader::new(std::io::Cursor::new(
            std::fs::read(day_dir.join(part)).expect("restart part file"),
        ))
        .expect("open raw reader");
        let row = reader.read_row().expect("read").expect("restart row");
        assert_eq!(
            (row.ts_recv, row.payload.as_slice()),
            (OPEN + 30, &b"three"[..])
        );
        assert!(reader.read_row().expect("read").is_none());

        let manifest = rolled_manifest(&day_dir);
        let files = manifest["files"].as_array().expect("files array");
        let listed: Vec<(&str, u64)> = files
            .iter()
            .map(|f| (f["path"].as_str().unwrap(), f["records"].as_u64().unwrap()))
            .collect();
        assert_eq!(listed, [(base, 2), (part, 1)]);
        for f in files {
            assert_eq!(f["window"], "2026-05-21", "a restart part keeps its window");
            let bytes = std::fs::read(day_dir.join(f["path"].as_str().unwrap())).unwrap();
            assert_eq!(f["sha256"], hex(&Sha256::digest(&bytes)));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A run killed without `finish` leaves its closed windows listed for the
    // restart's merge; only its in-progress file goes unlisted.
    #[test]
    fn killed_run_closed_windows_survive_restart() {
        let dir = std::env::temp_dir().join("datafeed_tap_killed_run");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let day_dir = dir.join("nasdaq/v5/2026-05-21");

        let mut killed = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        killed
            .raw(StreamId(0), OPEN + 13 * hour + 1, b"t13")
            .expect("hour 13");
        killed
            .raw(StreamId(0), OPEN + 14 * hour + 1, b"t14")
            .expect("hour 14 closes hour 13");
        drop(killed);

        let mut restarted = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        restarted
            .raw(StreamId(0), OPEN + 14 * hour + 2, b"t14 again")
            .expect("restart inside hour 14");
        restarted.finish().expect("restarted run finishes");

        let files = rolled_manifest(&day_dir)["files"].clone();
        let listed: Vec<(&str, u64)> = files
            .as_array()
            .expect("files array")
            .iter()
            .map(|f| (f["path"].as_str().unwrap(), f["records"].as_u64().unwrap()))
            .collect();
        assert_eq!(
            listed,
            [
                ("nasdaq_v5_2026-05-21T13.mdc1.zst", 1),
                ("nasdaq_v5_2026-05-21T14_r1.mdc1.zst", 1)
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A zero `meta.date` (live) takes the first frame's session label, so a
    // restart on 2026-05-22 writes the 2026-05-22 dir, leaf, and header.
    #[test]
    fn zero_date_adopts_first_frame_session() {
        let dir = std::env::temp_dir().join("datafeed_tap_adopt_date");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        // 2026-05-23 is epoch day 20596; 02:00 UTC is 22:00 in New York on
        // 2026-05-22, so the session label trails the UTC date.
        let ts_recv = (20_596 * 24 + 2) * hour;
        let meta = CaptureMeta { date: 0, ..meta() };

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta, 24 * hour);
        tap.raw(StreamId(0), ts_recv, b"payload")
            .expect("first frame");
        tap.finish().expect("finish");

        let leaf = dir.join("nasdaq/v5/2026-05-22/nasdaq_v5_2026-05-22.mdc1.zst");
        let (_, header) = md_capture::RawReader::new(std::io::Cursor::new(
            std::fs::read(&leaf).expect("leaf named after the first frame's session"),
        ))
        .expect("open raw reader");
        assert_eq!(header.date, 20260522);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // One manifest describes one partition, so a run under another partition
    // errors before writing a byte into a dir an earlier run indexed.
    #[test]
    fn restart_under_another_partition_is_refused() {
        let dir = std::env::temp_dir().join("datafeed_tap_restart_partition");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;
        let hour = 3_600_000_000_000u64;
        let day_dir = dir.join("nasdaq/v5/2026-05-21");
        let listing = || {
            let mut names: Vec<_> = std::fs::read_dir(&day_dir)
                .expect("day dir")
                .map(|e| e.unwrap().file_name())
                .collect();
            names.sort();
            names
        };

        let mut first = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        first
            .raw(StreamId(0), OPEN + 10, b"one")
            .expect("first run");
        first.finish().expect("first run finishes");
        let manifest = || std::fs::read(day_dir.join("manifest.json")).expect("manifest");
        let (files_before, manifest_before) = (listing(), manifest());

        let mut hourly = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), hour);
        let err = hourly.raw(StreamId(0), OPEN + 20, b"two");
        assert!(
            matches!(err, Err(CaptureTapError::ManifestPartition { .. })),
            "got {err:?}"
        );
        hourly.finish().expect("nothing staged to flush");
        assert_eq!(listing(), files_before, "no file opened");
        assert_eq!(manifest(), manifest_before, "manifest untouched");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A manifest torn by a kill or a full disk must not end the restarted
    // session: the tap sets it aside and indexes its own files.
    #[test]
    fn torn_manifest_is_set_aside_and_capture_continues() {
        let dir = std::env::temp_dir().join("datafeed_tap_torn_manifest");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;
        let day_dir = dir.join("nasdaq/v5/2026-05-21");

        let mut first = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        first
            .raw(StreamId(0), OPEN + 10, b"one")
            .expect("first run");
        first.finish().expect("first run finishes");
        let whole = std::fs::read(day_dir.join("manifest.json")).expect("manifest");
        let torn = &whole[..whole.len() / 2];
        std::fs::write(day_dir.join("manifest.json"), torn).expect("tear manifest");

        let mut second = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        second
            .raw(StreamId(0), OPEN + 20, b"two")
            .expect("a torn manifest must not fail the run");
        second.finish().expect("restarted run finishes");

        assert_eq!(
            std::fs::read(day_dir.join("manifest.json.corrupt")).expect("torn manifest kept"),
            torn
        );
        let files = rolled_manifest(&day_dir)["files"].clone();
        let listed: Vec<_> = files
            .as_array()
            .expect("files array")
            .iter()
            .map(|f| f["path"].as_str().expect("leaf").to_owned())
            .collect();
        assert_eq!(listed, ["nasdaq_v5_2026-05-21_r1.mdc1.zst"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A flush that fails before it completes (a full disk) leaves the earlier
    // manifest whole: the new one replaces it only once fully written.
    #[test]
    fn failed_manifest_flush_keeps_earlier_manifest() {
        let dir = std::env::temp_dir().join("datafeed_tap_failed_flush");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;
        let day_dir = dir.join("nasdaq/v5/2026-05-21");

        let mut first = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        first
            .raw(StreamId(0), OPEN + 10, b"one")
            .expect("first run");
        first.finish().expect("first run finishes");
        let before = std::fs::read(day_dir.join("manifest.json")).expect("manifest");

        // A dir at the staging path makes the flush fail before any rename.
        std::fs::create_dir(day_dir.join("manifest.json.tmp")).expect("block staging");
        let mut second = MdCaptureTap::new(dir.clone(), &NASDAQ, meta(), day);
        second
            .raw(StreamId(0), OPEN + 20, b"two")
            .expect("second run");
        assert!(
            matches!(second.finish(), Err(CaptureTapError::Io(_))),
            "flush reports the failed write"
        );
        assert_eq!(
            std::fs::read(day_dir.join("manifest.json")).expect("manifest kept"),
            before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Manifest of a date dir, read at a roll before `finish` runs.
    fn rolled_manifest(day_dir: &Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(day_dir.join("manifest.json")).unwrap())
            .expect("manifest flushed at roll")
    }

    // A continuous session rolls on UTC midnight only: frames past New York
    // midnight (05:00 UTC in January) and up to the last nanosecond of the UTC
    // day stay in the open date dir, and the first frame of the next UTC day
    // rolls it.
    #[test]
    fn continuous_session_rolls_only_at_utc_midnight() {
        static CONTINUOUS: VenueDescriptor = VenueDescriptor {
            name: "test-continuous",
            tag: "cont",
            version: "v1",
            feed_id: 200,
            session: SessionModel {
                roll: RollModel::Continuous,
                closure: ClosureModel::None,
            },
            ..NASDAQ
        };
        let dir = std::env::temp_dir().join("datafeed_tap_continuous");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;
        let hour = 3_600_000_000_000u64;

        let mut tap = MdCaptureTap::new(dir.clone(), &CONTINUOUS, meta(), day);
        for ts in [day + hour, day + 5 * hour + 1, 2 * day - 1] {
            tap.raw(StreamId(0), ts, b"payload").expect("same UTC day");
        }
        tap.raw(StreamId(0), 2 * day, b"payload")
            .expect("UTC midnight rolls");

        let files = rolled_manifest(&dir.join("cont/v1/2026-05-21"))["files"].clone();
        assert_eq!(files.as_array().map(Vec::len), Some(1));
        assert_eq!(files[0]["records"], 3, "no roll at a local midnight");
        assert!(
            dir.join("cont/v1/2026-05-22").exists(),
            "UTC day two opened"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // 2026-11-01 ends EDT, so its New York session runs 25h, 04:00 UTC to
    // 05:00 UTC the next day. Under a day partition the 25th hour stays in the
    // session's one file with no extra window or manifest entry; a 24h step
    // from the anchor would open a phantom window there and truncate the file.
    #[test]
    fn nasdaq_fall_back_session_is_one_day_window() {
        let dir = std::env::temp_dir().join("datafeed_tap_fallback");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        let day = 24 * hour;
        // 2026-11-01 is epoch day 20758; 00:00 EDT is 04:00 UTC.
        let open = (20_758 * 24 + 4) * hour;
        let meta = CaptureMeta {
            date: 20261101,
            ..meta()
        };

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta, day);
        tap.raw(StreamId(0), open + hour, b"payload")
            .expect("first session hour");
        tap.raw(StreamId(0), open + 24 * hour + hour / 2, b"payload")
            .expect("25th session hour");
        tap.raw(StreamId(0), open + 25 * hour, b"payload")
            .expect("next session rolls");

        let files = rolled_manifest(&dir.join("nasdaq/v5/2026-11-01"))["files"].clone();
        assert_eq!(files.as_array().map(Vec::len), Some(1), "one window");
        assert_eq!(files[0]["path"], "nasdaq_v5_2026-11-01.mdc1.zst");
        assert_eq!(files[0]["records"], 2);
        assert!(
            dir.join("nasdaq/v5/2026-11-02").exists(),
            "next session dir"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // 2026-03-08 starts EDT, so its New York session runs 23h, 05:00 UTC to
    // 04:00 UTC the next day: the session's last nanosecond stays, and a frame
    // 23h after the open belongs to the next session and rolls the dir.
    #[test]
    fn nasdaq_spring_forward_session_rolls_after_23h() {
        let dir = std::env::temp_dir().join("datafeed_tap_springforward");
        let _ = std::fs::remove_dir_all(&dir);
        let hour = 3_600_000_000_000u64;
        // 2026-03-08 is epoch day 20520; 00:00 EST is 05:00 UTC.
        let open = (20_520 * 24 + 5) * hour;
        let meta = CaptureMeta {
            date: 20260308,
            ..meta()
        };

        let mut tap = MdCaptureTap::new(dir.clone(), &NASDAQ, meta, 24 * hour);
        tap.raw(StreamId(0), open + hour, b"payload")
            .expect("first session hour");
        tap.raw(StreamId(0), open + 23 * hour - 1, b"payload")
            .expect("last session nanosecond");
        tap.raw(StreamId(0), open + 23 * hour, b"payload")
            .expect("next session rolls");

        let files = rolled_manifest(&dir.join("nasdaq/v5/2026-03-08"))["files"].clone();
        assert_eq!(files.as_array().map(Vec::len), Some(1));
        assert_eq!(files[0]["records"], 2);
        assert!(
            dir.join("nasdaq/v5/2026-03-09").exists(),
            "next session dir"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A zone the bundled database does not know fails the frame with an error
    // naming the venue, never a panic and never a silent UTC anchor.
    #[test]
    fn unresolvable_session_is_an_error() {
        static UNKNOWN_ZONE: VenueDescriptor = VenueDescriptor {
            name: "test-unknown-zone",
            tag: "unknown",
            version: "v1",
            feed_id: 200,
            session: SessionModel {
                roll: RollModel::DailyZoned {
                    tz: "Nowhere/Unknown",
                    local_ns: 0,
                },
                closure: ClosureModel::None,
            },
            ..NASDAQ
        };
        let dir = std::env::temp_dir().join("datafeed_tap_unknown_zone");
        let _ = std::fs::remove_dir_all(&dir);
        let day = 86_400_000_000_000u64;

        let mut tap = MdCaptureTap::new(dir.clone(), &UNKNOWN_ZONE, meta(), day);
        let err = tap.raw(StreamId(0), day, b"payload");
        assert!(
            matches!(
                err,
                Err(CaptureTapError::SessionUnresolved {
                    venue: "test-unknown-zone",
                    ts_recv,
                }) if ts_recv == day
            ),
            "got {err:?}"
        );
        assert!(!dir.exists(), "nothing written for an unplaced frame");
    }

    #[test]
    fn noop_tap_never_errors() {
        let mut tap = NoopTap;
        tap.raw(StreamId(0), 0, &[]).expect("noop raw");
    }
}
