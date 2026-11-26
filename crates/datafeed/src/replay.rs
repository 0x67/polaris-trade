//! Replay container selection, pacing assembly, and the replay feed.

use std::{fs, io::Read, path::Path, sync::atomic::AtomicBool};

use md_pipeline::{EgressSequencer, IngestError, IngestFrame, IngestSource, OutputSink, StreamId};

use crate::{
    ConfigError, FeedError,
    config::{DatafeedConfig, LoadMode, PacingConfig, PacingModeConfig, ReplayFormat},
    feed::{Engine, Progress, run_batch},
    session::StepBuffers,
    source::SourceError,
    venue::VenueDescriptor,
};

const BATCH_SIZE: usize = 64;

#[derive(Clone, Copy)]
pub(crate) struct ReplayInput<'a> {
    pub path: &'a Path,
    pub format: ReplayFormat,
    pub base_ts: u64,
    pub step_ns: u64,
    pub load: LoadMode,
    pub pacing: &'a PacingConfig,
}

type Paced<S> = md_replay::PacedSource<S, md_replay::ThreadSleepPacer>;

/// An opened replay: its source and step buffers, and the sequencer stamping
/// egress from the replayed clock.
pub(crate) struct ReplayFeed<S: OutputSink> {
    venue: &'static VenueDescriptor,
    seq: EgressSequencer<S, md_replay::SharedClock>,
    source: ReplaySource,
}

enum ReplaySource {
    Binary(
        Paced<md_replay::BinaryFileReplay>,
        StepBuffers<md_replay::BinaryFileFrame>,
    ),
    // The only container that tags rows with a stream; binary and pcap
    // frames are all stream 0, which every venue declares.
    Capture(
        DeclaredStreams<Paced<md_replay::CaptureReplay>>,
        StepBuffers<md_replay::CaptureFrame>,
    ),
    Pcap(
        Paced<md_replay::PcapReplay>,
        StepBuffers<md_replay::PcapFrame>,
    ),
}

impl<S: OutputSink> ReplayFeed<S> {
    /// Opens the capture `input` names for `config`'s venue, publishing into
    /// `sink`. A multi-stream venue needs the capture container, the only one
    /// that tags rows with their stream.
    pub(crate) fn open(
        config: &DatafeedConfig,
        input: ReplayInput<'_>,
        sink: S,
    ) -> Result<Self, FeedError> {
        let container = resolve_container(input.format, input.path)?;
        if config.venue.streams.len() > 1 && container != md_replay::ContainerKind::Capture {
            return Err(ConfigError::ReplayNeedsCapture {
                venue: config.venue.name,
                container,
            }
            .into());
        }
        reject_unsupported_eager(container, input.load)?;
        let pacing = build_pacing(input.pacing)?;
        let clock = md_replay::SharedClock::new();
        let source = match container {
            md_replay::ContainerKind::BinaryFramed | md_replay::ContainerKind::Gzip => {
                let source = open_binary(
                    container,
                    input.path,
                    input.base_ts,
                    input.step_ns,
                    input.load,
                )?;
                ReplaySource::Binary(paced(source, pacing, &clock), StepBuffers::new(BATCH_SIZE))
            }
            md_replay::ContainerKind::Capture => {
                let source = md_replay::CaptureReplay::from_reader(open_file(input.path)?)
                    .map_err(SourceError::from)?;
                let source =
                    DeclaredStreams::new(paced(source, pacing, &clock), config.venue.streams);
                ReplaySource::Capture(source, StepBuffers::new(BATCH_SIZE))
            }
            md_replay::ContainerKind::Pcap => {
                let source = md_replay::PcapReplay::from_reader(open_file(input.path)?)
                    .map_err(SourceError::from)?;
                ReplaySource::Pcap(paced(source, pacing, &clock), StepBuffers::new(BATCH_SIZE))
            }
        };
        Ok(ReplayFeed {
            venue: config.venue,
            seq: EgressSequencer::new(sink, clock),
            source,
        })
    }

    pub(crate) fn sink(&self) -> &S {
        self.seq.sink()
    }

    /// Replays one batch. Once the capture ends it fails on what the source
    /// held back: a retained io error, or a row on an undeclared stream.
    pub(crate) fn advance(
        &mut self,
        engine: &mut Engine,
        stop: &AtomicBool,
    ) -> Result<Progress, FeedError> {
        match &mut self.source {
            ReplaySource::Binary(source, buffers) => {
                if !run_batch(source, buffers, &mut self.seq, engine, stop)? {
                    return Ok(Progress::Running);
                }
                fail_on_retained_io_error(source.source())?;
            }
            ReplaySource::Capture(source, buffers) => {
                if !run_batch(source, buffers, &mut self.seq, engine, stop)? {
                    return Ok(Progress::Running);
                }
                if let Some(stream) = source.undeclared {
                    return Err(SourceError::UndeclaredStream {
                        venue: self.venue.name,
                        stream,
                        declared: self.venue.streams,
                    }
                    .into());
                }
            }
            ReplaySource::Pcap(source, buffers) => {
                if !run_batch(source, buffers, &mut self.seq, engine, stop)? {
                    return Ok(Progress::Running);
                }
            }
        }
        Ok(Progress::Ended)
    }
}

fn open_binary(
    container: md_replay::ContainerKind,
    path: &Path,
    base_ts: u64,
    step_ns: u64,
    load: LoadMode,
) -> Result<md_replay::BinaryFileReplay, SourceError> {
    match (container, load) {
        (md_replay::ContainerKind::BinaryFramed, LoadMode::Stream) => Ok(
            md_replay::BinaryFileReplay::from_path(path, base_ts, step_ns)?,
        ),
        (md_replay::ContainerKind::BinaryFramed, LoadMode::Eager) => Ok(
            md_replay::BinaryFileReplay::from_path_eager(path, base_ts, step_ns)?,
        ),
        (md_replay::ContainerKind::Gzip, LoadMode::Stream) => Ok(
            md_replay::BinaryFileReplay::from_gz(path, base_ts, step_ns)?,
        ),
        (md_replay::ContainerKind::Gzip, LoadMode::Eager) => Ok(
            md_replay::BinaryFileReplay::from_gz_eager(path, base_ts, step_ns)?,
        ),
        _ => Err(SourceError::NotBinaryContainer { container }),
    }
}

fn open_file(path: &Path) -> Result<fs::File, SourceError> {
    fs::File::open(path).map_err(|source| SourceError::Open {
        path: path.to_path_buf(),
        source,
    })
}

fn fail_on_retained_io_error(source: &md_replay::BinaryFileReplay) -> Result<(), SourceError> {
    match source.io_error() {
        Some(error) => Err(SourceError::RetainedIo {
            kind: error.kind(),
            message: error.to_string(),
        }),
        None => Ok(()),
    }
}

/// Read the venue, date, and feed from an MDC1 capture header without decoding
/// the zstd body, returning `None` when `path` is not a capture container (other
/// replay formats carry no such header). Mirrors the header layout that
/// `md_capture::read_header` parses (see polaris crates/formats/mdc).
pub(crate) fn peek_capture_meta(path: &Path) -> Option<(u16, u32, u8)> {
    // Best effort: an unreadable path yields no meta here, and the real
    // container open downstream reports the io error (or the eager/format
    // rejection) with its own message.
    match fs::File::open(path) {
        Ok(file) => peek_capture_header(file),
        Err(_) => None,
    }
}

// header = "MDC1" magic + venue:u16 LE + date:u32 LE + schema:u16 LE + kind:u8
// + feed:u8 + pad. A non-MDC1 or too-short file is not a capture, so returns
// None rather than erroring; the real reader later rejects a genuinely bad one.
fn peek_capture_header<R: Read>(mut reader: R) -> Option<(u16, u32, u8)> {
    let mut header = [0u8; 16];
    if reader.read_exact(&mut header).is_err() {
        return None;
    }
    if &header[0..4] != b"MDC1" {
        return None;
    }
    let venue = u16::from_le_bytes([header[4], header[5]]);
    let date = u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
    let feed = header[13];
    Some((venue, date, feed))
}

fn resolve_container(
    format: ReplayFormat,
    path: &Path,
) -> Result<md_replay::ContainerKind, SourceError> {
    Ok(match format {
        ReplayFormat::Binary => md_replay::ContainerKind::BinaryFramed,
        ReplayFormat::BinaryGz => md_replay::ContainerKind::Gzip,
        ReplayFormat::Capture => md_replay::ContainerKind::Capture,
        ReplayFormat::Pcap => md_replay::ContainerKind::Pcap,
        ReplayFormat::Auto => md_replay::detect_container(path)?,
    })
}

fn reject_unsupported_eager(
    container: md_replay::ContainerKind,
    load: LoadMode,
) -> Result<(), ConfigError> {
    if load == LoadMode::Eager
        && matches!(
            container,
            md_replay::ContainerKind::Capture | md_replay::ContainerKind::Pcap
        )
    {
        return Err(ConfigError::EagerLoadUnsupported { container });
    }
    Ok(())
}

fn pacing_mode(mode: PacingModeConfig) -> md_replay::PacingMode {
    match mode {
        PacingModeConfig::RealTime => md_replay::PacingMode::RealTime,
        PacingModeConfig::Scaled { scale } => md_replay::PacingMode::Scaled(scale),
        PacingModeConfig::Unthrottled => md_replay::PacingMode::Unthrottled,
    }
}

fn build_pacing(config: &PacingConfig) -> Result<md_replay::Pacing, ConfigError> {
    let default_mode = pacing_mode(config.default_mode);
    if config.windows.is_empty() {
        return Ok(md_replay::Pacing::Fixed(default_mode));
    }
    let segments = config
        .windows
        .iter()
        .map(|window| md_replay::PaceWindow {
            from_ts: window.from_ts,
            to_ts: window.to_ts,
            mode: pacing_mode(window.mode),
        })
        .collect();
    Ok(md_replay::Pacing::Schedule(md_replay::PacingSchedule::new(
        default_mode,
        segments,
    )?))
}

/// Ends a replay at the first frame on a stream the venue does not declare. An
/// adapter ignores such a frame without counting it, so the replay would
/// otherwise reach a clean end with those rows missing. Frames before it still
/// reach the session; `undeclared` keeps the stream for the caller to report.
struct DeclaredStreams<S> {
    inner: S,
    streams: &'static [StreamId],
    undeclared: Option<StreamId>,
}

impl<S> DeclaredStreams<S> {
    fn new(inner: S, streams: &'static [StreamId]) -> Self {
        DeclaredStreams {
            inner,
            streams,
            undeclared: None,
        }
    }
}

impl<S: IngestSource> IngestSource for DeclaredStreams<S> {
    type Frame = S::Frame;

    fn recv_batch(&mut self, out: &mut Vec<S::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.undeclared.is_some() {
            return Err(IngestError::Closed);
        }
        let start = out.len();
        let appended = self.inner.recv_batch(out, max)?;
        let Some(bad) = out[start..]
            .iter()
            .position(|frame| !self.streams.contains(&frame.stream()))
        else {
            return Ok(appended);
        };
        self.undeclared = Some(out[start + bad].stream());
        out.truncate(start + bad);
        Ok(bad)
    }
}

fn paced<S: IngestSource>(
    source: S,
    pacing: md_replay::Pacing,
    clock: &md_replay::SharedClock,
) -> Paced<S> {
    md_replay::PacedSource::new(
        md_replay::ReplayDriver::new(source, md_replay::ThreadSleepPacer, pacing),
        clock.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write as _,
        path::Path,
        sync::{Arc, atomic::AtomicBool},
    };

    use flate2::{Compression, write::GzEncoder};
    use md_pipeline::StreamId;

    use crate::{
        ConfigError, FeedError,
        app::{CliOverrides, load_config, run_config},
        config::{
            DatafeedConfig, LoadMode, PacingConfig, ReplayFormat, SinkKind, SourceConfig,
            TapConfig, default_resolution_tokens,
        },
        source::SourceError,
        venue::NASDAQ,
    };

    fn run(config_path: &Path) -> Result<(), FeedError> {
        let config = load_config(config_path, &CliOverrides::default())?;
        run_config(config, Arc::new(AtomicBool::new(false)))
    }

    fn write_json(path: &Path, config: &DatafeedConfig) {
        let json = serde_json::to_string(config).expect("serialize test config");
        fs::write(path, json).expect("write test config");
    }

    // `S` SystemEvent, start of messages: 12 bytes, with a zero locate,
    // tracking number, and timestamp.
    fn system_event_msg() -> Vec<u8> {
        let mut message = vec![0u8; 12];
        message[0] = b'S';
        message[11] = b'O';
        message
    }

    fn one_event_row() -> Vec<u8> {
        let message = system_event_msg();
        let mut row = Vec::new();
        row.extend_from_slice(&(message.len() as u16).to_be_bytes());
        row.extend_from_slice(&message);
        row
    }

    fn replay_config(path: &Path, format: ReplayFormat, load: LoadMode) -> DatafeedConfig {
        DatafeedConfig {
            venue: &NASDAQ,
            venue_id: 1,
            session_midnight_ns: 0,
            source: SourceConfig::Replay {
                path: path.to_path_buf(),
                format,
                base_ts: 0,
                step_ns: 1,
                load,
                pacing: PacingConfig::default(),
            },
            sink: SinkKind::Null,
            taps: TapConfig::default(),
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        }
    }

    // The header peek recovers the venue, date, and feed md_capture actually
    // wrote, and returns None for a buffer that is not an MDC1 capture. Building
    // the header through md_capture's own writer keeps the peek honest against
    // the real container layout: a format change breaks this test.
    #[test]
    fn peek_capture_meta_reads_header_venue_date_and_feed() {
        use super::peek_capture_header;

        let meta = md_capture::CaptureMeta {
            venue: 7,
            date: 20260521,
            schema: 1,
            feed: proto_nasdaq::FEED_NASDAQ,
        };
        let writer = md_capture::RawWriter::new(Vec::new(), meta).expect("open raw capture writer");
        let bytes = writer.finish().expect("finish capture writer");

        assert_eq!(
            peek_capture_header(std::io::Cursor::new(bytes)),
            Some((7, 20260521, proto_nasdaq::FEED_NASDAQ)),
        );
        assert_eq!(
            peek_capture_header(std::io::Cursor::new(b"not-a-capture".to_vec())),
            None,
        );
        assert_eq!(peek_capture_header(std::io::Cursor::new(Vec::new())), None,);
    }

    #[test]
    fn replay_io_error_fails() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&one_event_row()).expect("gzip write");
        let mut bytes = encoder.finish().expect("gzip finish");
        bytes.truncate(bytes.len() / 2);

        let replay_path = std::env::temp_dir().join("datafeed_replay_io_error_fails.gz");
        fs::write(&replay_path, bytes).expect("write corrupt gzip fixture");
        let config_path = std::env::temp_dir().join("datafeed_replay_io_error_fails.json");
        write_json(
            &config_path,
            &replay_config(&replay_path, ReplayFormat::BinaryGz, LoadMode::Stream),
        );

        let error = run(&config_path).expect_err("corrupt gzip tail must fail the run");
        assert!(
            matches!(error, FeedError::Source(SourceError::RetainedIo { .. })),
            "got: {error}"
        );

        let _ = fs::remove_file(replay_path);
        let _ = fs::remove_file(config_path);
    }

    // Frames ahead of the first undeclared one still reach the session; the
    // guard then closes the source and keeps the stream it refused.
    #[test]
    fn declared_streams_stops_at_first_undeclared_frame() {
        use md_pipeline::{
            IngestError, IngestSource, StreamId,
            ingest::testing::{MockFrame, MockSource, ScenarioSpec},
        };

        use super::DeclaredStreams;

        let frame = |stream: u16| MockFrame {
            payload: vec![b'T'],
            ts_recv: 1,
            stream: StreamId(stream),
        };
        let inner = MockSource::from_scenario(ScenarioSpec {
            frames: vec![frame(0), frame(0), frame(1), frame(0)],
            inject_exhaustion: false,
        });
        let mut source = DeclaredStreams::new(inner, &[StreamId(0)]);
        let mut out = Vec::new();

        assert_eq!(source.recv_batch(&mut out, 8), Ok(2));
        assert_eq!(out, [frame(0), frame(0)]);
        assert_eq!(source.undeclared, Some(StreamId(1)));
        assert_eq!(
            source.recv_batch(&mut out, 8),
            Err(IngestError::Closed),
            "no frame after the refused one is delivered"
        );
    }

    // A NASDAQ capture with a stream-1 row must fail the replay, not end clean,
    // and the rows tapped before it still land in a finished, listed raw file.
    #[test]
    fn capture_row_on_undeclared_stream_fails_replay() {
        let meta = md_capture::CaptureMeta {
            venue: 1,
            date: 20260521,
            schema: 1,
            feed: proto_nasdaq::FEED_NASDAQ,
        };
        // rows received just after the header date's New York midnight
        let open = NASDAQ
            .session
            .resolve_anchor(meta.date, &md_pipeline::BundledTzdb::new())
            .expect("New York anchor resolves");
        let mut writer =
            md_capture::RawWriter::new(Vec::new(), meta).expect("open raw capture writer");
        let event = system_event_msg();
        writer
            .write_row(0, open + 10, &event)
            .expect("write stream 0");
        writer
            .write_row(0, open + 15, &event)
            .expect("write stream 0");
        writer
            .write_row(1, open + 20, &event)
            .expect("write stream 1");
        let bytes = writer.finish().expect("finish capture writer");

        let replay_path = std::env::temp_dir().join("datafeed_undeclared_stream.mdc1");
        fs::write(&replay_path, bytes).expect("write capture fixture");
        let raw_dir = std::env::temp_dir().join("datafeed_undeclared_stream_raw");
        let _ = fs::remove_dir_all(&raw_dir);
        let config_path = std::env::temp_dir().join("datafeed_undeclared_stream.json");
        let mut config = replay_config(&replay_path, ReplayFormat::Capture, LoadMode::Stream);
        config.taps.raw_dir = Some(raw_dir.clone());
        write_json(&config_path, &config);

        let error =
            run(&config_path).expect_err("a row on an undeclared stream must fail the replay");
        assert!(
            matches!(
                error,
                FeedError::Source(SourceError::UndeclaredStream {
                    stream: StreamId(1),
                    ..
                })
            ),
            "got: {error}"
        );

        let day_dir = raw_dir.join("nasdaq/v5/2026-05-21");
        let leaf = "nasdaq_v5_2026-05-21.mdc1.zst";
        let (mut reader, _) = md_capture::RawReader::new(std::io::Cursor::new(
            fs::read(day_dir.join(leaf)).expect("read tapped raw file"),
        ))
        .expect("open raw reader");
        let mut tapped = Vec::new();
        while let Some(row) = reader.read_row().expect("finished zstd body decodes") {
            tapped.push((row.stream, row.ts_recv));
        }
        assert_eq!(tapped, [(0, open + 10), (0, open + 15)]);
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(day_dir.join("manifest.json")).expect("manifest written on failure"),
        )
        .expect("manifest parses");
        assert_eq!(manifest["files"][0]["path"], leaf);
        assert_eq!(manifest["files"][0]["records"], 2);

        let _ = fs::remove_file(replay_path);
        let _ = fs::remove_file(config_path);
        let _ = fs::remove_dir_all(raw_dir);
    }

    #[test]
    fn eager_capture_config_rejected() {
        let replay_path = Path::new("/nonexistent/replay.mdc");
        let config_path = std::env::temp_dir().join("datafeed_eager_capture_rejected.json");
        write_json(
            &config_path,
            &replay_config(replay_path, ReplayFormat::Capture, LoadMode::Eager),
        );

        let error = run(&config_path).expect_err("capture eager must be rejected");
        assert!(
            matches!(
                error,
                FeedError::Config(ConfigError::EagerLoadUnsupported {
                    container: md_replay::ContainerKind::Capture
                })
            ),
            "got: {error}"
        );
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn auto_format_detects_gzip_container() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&one_event_row()).expect("gzip write");
        let bytes = encoder.finish().expect("gzip finish");

        let replay_path = std::env::temp_dir().join("datafeed_auto_gzip.dat");
        fs::write(&replay_path, bytes).expect("write gzip fixture");
        let config_path = std::env::temp_dir().join("datafeed_auto_gzip.json");
        write_json(
            &config_path,
            &replay_config(&replay_path, ReplayFormat::Auto, LoadMode::Stream),
        );

        run(&config_path).expect("auto-detected gzip replay reaches clean end");

        let _ = fs::remove_file(replay_path);
        let _ = fs::remove_file(config_path);
    }
}
