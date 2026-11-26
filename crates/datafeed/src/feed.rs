//! Pull-based feed session: a host builds a [`FeedSession`] in code and calls
//! [`FeedSession::poll`] from its own loop to take normalized records.
//!
//! Each poll advances the source by at most one batch through the same
//! [`crate::session`] step the batch runner loops over, then hands back what
//! the egress sequencer queued. The binary drives one through
//! [`crate::app::run_config`].

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use md_pipeline::{DynAdapter, EgressClock, EgressSequencer, IngestSource};

use crate::{
    FeedError, app,
    config::{DatafeedConfig, SinkKind, SourceConfig, TapConfig, default_resolution_tokens},
    live::LiveFeed,
    record::{OwnedRecord, RecordQueue},
    replay::{ReplayFeed, ReplayInput},
    restart::RestartState,
    session::{self, SessionError, SessionState, SessionStats, Step, StepBuffers},
    tap::AnyTap,
    venue::VenueDescriptor,
    wiring,
};

/// The source-independent half of a session: adapter, engine state, resume
/// points, and capture tap. A live reconnect keeps it; a replay from sequence
/// 1 rebuilds the adapter and state.
pub(crate) struct Engine {
    pub(crate) adapter: Box<dyn DynAdapter>,
    pub(crate) state: SessionState,
    pub(crate) restart: RestartState,
    pub(crate) tap: AnyTap,
}

/// Whether a source keeps feeding after one advance.
pub(crate) enum Progress {
    Running,
    Ended,
}

/// Runs one [`session::step`]. When the source closes, or `stop` is set after
/// the batch, it also flushes the open OHLCV bars, as [`session::run`] does on
/// return, and reports that this source's run ended.
pub(crate) fn run_batch<Src, C>(
    source: &mut Src,
    buffers: &mut StepBuffers<Src::Frame>,
    seq: &mut EgressSequencer<RecordQueue, C>,
    engine: &mut Engine,
    stop: &AtomicBool,
) -> Result<bool, SessionError>
where
    Src: IngestSource,
    C: EgressClock,
{
    let step = session::step(
        source,
        &mut *engine.adapter,
        seq,
        &mut engine.tap,
        &mut engine.state,
        &mut engine.restart,
        buffers,
    )?;
    let ended = match step {
        Step::Routed => stop.load(Ordering::Relaxed),
        Step::Retry => false,
        Step::Closed => true,
    };
    if ended {
        session::flush_bars(&mut engine.state, seq)?;
    }
    Ok(ended)
}

#[expect(
    clippy::large_enum_variant,
    reason = "one per session, built once; boxing only adds a deref per poll"
)]
enum Feed {
    Replay(ReplayFeed),
    Live(LiveFeed),
}

/// Once the source has ended, queued records drain first, then the fault that
/// ended it (if any) returns once, then [`FeedError::Closed`] on every poll.
enum Phase {
    Running,
    Ending(Option<FeedError>),
}

/// One venue feed, live or replay, driven by its host one [`poll`] at a time.
///
/// Build one with [`FeedSession::builder`], poll until [`FeedError::Closed`],
/// then [`close`] it to finish the capture tap; dropping it without `close`
/// leaves the open capture file unfinished. A poll can block: a paced replay
/// sleeps each frame's pacing delay, and a live dial waits for connect and
/// login. Not `Send`: the adapter is a `Box<dyn DynAdapter>`, and replay
/// shares its egress clock through an `Rc`.
///
/// [`poll`]: FeedSession::poll
/// [`close`]: FeedSession::close
pub struct FeedSession {
    config: DatafeedConfig,
    engine: Engine,
    feed: Feed,
    queue: RecordQueue,
    stop: Arc<AtomicBool>,
    phase: Phase,
}

impl FeedSession {
    /// Starts a session for `venue` reading from `source`. Every other
    /// setting takes the value a config file gets when it leaves the key out.
    pub fn builder(venue: &'static VenueDescriptor, source: SourceConfig) -> FeedSessionBuilder {
        FeedSessionBuilder::from(DatafeedConfig {
            venue,
            venue_id: 0,
            session_midnight_ns: 0,
            source,
            sink: SinkKind::Null,
            taps: TapConfig::default(),
            ohlcv_resolutions: default_resolution_tokens(),
            observability: None,
        })
    }

    /// Appends up to `max` records to `out` and returns how many; `Ok(0)`
    /// means nothing is ready yet. With nothing queued, it first takes one
    /// step: a frame batch, a live dial, or at most 50 ms of a reconnect wait.
    /// Records past `max` stay queued for the next call.
    ///
    /// # Errors
    ///
    /// A fault that ends the source (sink, tap, anchor, or source) returns
    /// once, after the records routed before it; then every call returns
    /// [`FeedError::Closed`].
    pub fn poll(&mut self, out: &mut Vec<OwnedRecord>, max: usize) -> Result<usize, FeedError> {
        if self.queue.is_empty() && matches!(self.phase, Phase::Running) {
            self.phase = match self.advance() {
                Ok(Progress::Running) => Phase::Running,
                Ok(Progress::Ended) => Phase::Ending(None),
                Err(err) => Phase::Ending(Some(err)),
            };
        }
        if self.queue.is_empty()
            && let Phase::Ending(fault) = &mut self.phase
        {
            return Err(fault.take().unwrap_or(FeedError::Closed));
        }
        Ok(self.queue.drain_into(out, max))
    }

    fn advance(&mut self) -> Result<Progress, FeedError> {
        match &mut self.feed {
            Feed::Replay(feed) => feed.advance(&mut self.engine, &self.stop),
            Feed::Live(feed) => feed.advance(&mut self.engine, &self.config, &self.stop),
        }
    }

    /// Counters since the engine state was last built: session start, or a
    /// live reconnect that replays from sequence 1.
    pub fn stats(&self) -> &SessionStats {
        self.engine.state.stats()
    }

    /// Finishes the capture tap: closes the open file and writes its manifest.
    ///
    /// # Errors
    ///
    /// [`FeedError::Tap`] when the capture cannot be finished.
    pub fn close(self) -> Result<(), FeedError> {
        tracing::info!(venue = self.config.venue.name, stats = ?self.stats(), "feed session closed");
        self.engine.tap.finish()?;
        Ok(())
    }
}

/// Assembles a [`FeedSession`] in code, with no config file. It covers the
/// session part of a [`DatafeedConfig`]: venue, source, venue id, session
/// anchor, capture taps, and OHLCV resolutions. The file's `sink` and
/// `observability` keys belong to the binary: a host takes records from
/// [`FeedSession::poll`], and the library installs no metrics recorder.
#[must_use]
pub struct FeedSessionBuilder {
    config: DatafeedConfig,
    stop: Arc<AtomicBool>,
}

impl From<DatafeedConfig> for FeedSessionBuilder {
    /// A builder holding a decoded config file's settings.
    fn from(config: DatafeedConfig) -> Self {
        FeedSessionBuilder {
            config,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl FeedSessionBuilder {
    /// Registry id stamped on every record header. On MDC1 replay `0` adopts
    /// the capture header's venue id.
    pub fn venue_id(mut self, venue_id: u16) -> Self {
        self.config.venue_id = venue_id;
        self
    }

    /// Epoch-ns UTC of the trade date's midnight in the venue's zone. `0`
    /// derives it: from the capture date on MDC1 replay, from each frame's
    /// receive time on a live source, which refuses any other value.
    pub fn session_midnight_ns(mut self, session_midnight_ns: u64) -> Self {
        self.config.session_midnight_ns = session_midnight_ns;
        self
    }

    /// Raw capture tap settings. The default writes no capture.
    pub fn taps(mut self, taps: TapConfig) -> Self {
        self.config.taps = taps;
        self
    }

    /// OHLCV bar widths as duration tokens (`"1s"`, `"5m"`, `"1h"`).
    pub fn ohlcv_resolutions(mut self, tokens: Vec<String>) -> Self {
        self.config.ohlcv_resolutions = tokens;
        self
    }

    /// A flag the host sets to end the session: polled after each frame
    /// batch, through a live reconnect wait, and while a live dial is open.
    /// The session then flushes its open bars and ends as if the source
    /// closed.
    pub fn stop(mut self, stop: Arc<AtomicBool>) -> Self {
        self.stop = stop;
        self
    }

    /// Validates the settings and opens the source: a replay opens its file
    /// now, a live source dials on the first [`FeedSession::poll`].
    ///
    /// # Errors
    ///
    /// [`FeedError::Config`] for settings that contradict each other or an
    /// MDC1 capture header, [`FeedError::Source`] for a replay that does not
    /// open, and [`FeedError::Runtime`] when a live source's tokio runtime
    /// does not start.
    pub fn build(self) -> Result<FeedSession, FeedError> {
        let FeedSessionBuilder { mut config, stop } = self;
        app::reconcile_replay_capture(&mut config)?;
        let tap = wiring::build_tap(&config)?;
        let queue = RecordQueue::default();
        let feed = match &config.source {
            SourceConfig::Live { .. } | SourceConfig::LiveDual { .. } => {
                Feed::Live(LiveFeed::new(&config, queue.clone())?)
            }
            SourceConfig::Replay {
                path,
                format,
                base_ts,
                step_ns,
                load,
                pacing,
            } => {
                let input = ReplayInput {
                    path,
                    format: *format,
                    base_ts: *base_ts,
                    step_ns: *step_ns,
                    load: *load,
                    pacing,
                };
                Feed::Replay(ReplayFeed::open(&config, input, queue.clone())?)
            }
        };
        let engine = Engine {
            adapter: (config.venue.make)(&config),
            state: SessionState::from_config(&config)?,
            restart: RestartState::default(),
            tap,
        };
        Ok(FeedSession {
            config,
            engine,
            feed,
            queue,
            stop,
            phase: Phase::Running,
        })
    }
}
