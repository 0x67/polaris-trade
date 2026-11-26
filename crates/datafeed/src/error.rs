//! Typed failures returned by the library entry points.
//!
//! A host branches on the [`FeedError`] variant and reads the structured
//! fields of the inner error, never the message text.

use std::{io, path::PathBuf};

use md_replay::{ContainerKind, PacingScheduleError};

use crate::{
    config::{ResolutionParseError, SourceConfig},
    session::SessionError,
    source::SourceError,
    tap::CaptureTapError,
};

/// Failure of a feed run, from config load to session end.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FeedError {
    /// The deployment config is unreadable, invalid, or contradicts its capture.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A live or replay source could not be opened or failed while open.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The session loop stopped on a sink fault, a tap fault, or an unresolved anchor.
    #[error(transparent)]
    Session(#[from] SessionError),
    /// Closing the capture tap failed after the session ended.
    #[error(transparent)]
    Tap(#[from] CaptureTapError),
    /// The tokio runtime that hosts the exporters and the live connect did not start.
    #[error("tokio runtime: {0}")]
    Runtime(#[source] io::Error),
    /// The metrics and logging pipeline did not install.
    #[cfg(feature = "observability")]
    #[error("observability init: {0}")]
    Observability(#[from] observability::PipelineError),
}

impl From<ResolutionParseError> for FeedError {
    fn from(err: ResolutionParseError) -> Self {
        FeedError::Config(err.into())
    }
}

/// Why a deployment config was refused, before or while a session starts.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("read config {}: {source}", path.display())]
    Read { path: PathBuf, source: io::Error },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Yaml(#[from] serde_norway::Error),
    #[error(transparent)]
    Resolution(#[from] ResolutionParseError),
    #[error(transparent)]
    Pacing(#[from] PacingScheduleError),
    #[error("config taps.capture_date {config} disagrees with capture header {header}")]
    CaptureDateMismatch { config: u32, header: u32 },
    #[error("config venue_id {config} disagrees with capture header {header}")]
    VenueIdMismatch { config: u16, header: u16 },
    #[error(
        "capture feed id {header} disagrees with configured venue `{venue_tag}-{venue_version}` \
         (feed {expected}): wrong capture for this venue?"
    )]
    CaptureFeedMismatch {
        header: u8,
        expected: u8,
        venue_tag: &'static str,
        venue_version: &'static str,
    },
    #[error(
        "replay capture `{found_tag}_{found_version}` disagrees with configured venue \
         `{venue_tag}_{venue_version}`: wrong capture for this venue?"
    )]
    ReplayPathFeedMismatch {
        found_tag: String,
        found_version: String,
        venue_tag: &'static str,
        venue_version: &'static str,
    },
    #[error("{venue} replay needs the capture container, got {container:?}")]
    ReplayNeedsCapture {
        venue: &'static str,
        container: ContainerKind,
    },
    #[error("eager load unsupported for {container:?} replay")]
    EagerLoadUnsupported { container: ContainerKind },
    #[error(
        "session_midnight_ns {session_midnight_ns} is set on a live source; leave it unset, \
         live installs each trade date's anchor from frame receive time"
    )]
    LiveAnchorSet { session_midnight_ns: u64 },
    #[error("unsupported venue/source combination: {venue:?} / {config_source:?}")]
    UnsupportedSource {
        venue: &'static str,
        config_source: Box<SourceConfig>,
    },
    #[error("taps.normalized_dir was removed with normalized capture; use taps.raw_dir")]
    RetiredNormalizedDir,
    #[error("taps.capture_date must resolve to a nonzero YYYYMMDD when a replay tap is on")]
    ReplayTapDateUnset,
    #[error(
        "taps.capture_date {date} is set on a live source; leave it unset, live capture takes \
         the trade date from the first frame's session"
    )]
    LiveTapDateSet { date: u32 },
    #[error(
        "config requests observability but the crate was built without the `observability` feature"
    )]
    ObservabilityUnavailable,
}
