//! Live feed: dialing, the frame batches, and the reconnect wait.

use std::{
    io,
    mem::ManuallyDrop,
    net::SocketAddr,
    panic,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use client_soupbintcp::SoupBinClientConfig;
use md_pipeline::{EgressSequencer, StreamId, SystemClock};
use tokio::runtime::Runtime;
use transport_socket::tokio::TcpStream;

use crate::{
    ConfigError, FeedError,
    config::{DatafeedConfig, SourceConfig},
    feed::{Engine, Progress, run_batch},
    record::RecordQueue,
    session::{SessionState, StepBuffers},
    source::{self, DualLiveSource, LiveFrame, LiveSoupBinSource},
    venue::VenueDescriptor,
};

const BATCH_SIZE: usize = 64;

/// Bounded reconnect backoff: 100ms doubling per consecutive frameless
/// session, capped at 3.2s. A session that delivered frames resets it.
fn reconnect_delay(consecutive_empty: u32) -> Duration {
    Duration::from_millis(100u64 << consecutive_empty.min(5))
}

/// Longest one poll waits toward a reconnect, and so the shutdown latency
/// while a reconnect is pending.
const RECONNECT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Tracks consecutive sessions that closed without delivering frames, from
/// the cumulative `SessionStats::frames` counter.
struct ReconnectPacer {
    prev_frames: u64,
    consecutive_empty: u32,
}

impl ReconnectPacer {
    fn new() -> Self {
        ReconnectPacer {
            prev_frames: 0,
            consecutive_empty: 0,
        }
    }

    /// Delay before the next reconnect attempt; longer after each session
    /// that closed without progress (login loop, dead venue).
    fn next_delay(&mut self, total_frames: u64) -> Duration {
        if total_frames == self.prev_frames {
            self.consecutive_empty += 1;
        } else {
            self.consecutive_empty = 0;
        }
        self.prev_frames = total_frames;
        reconnect_delay(self.consecutive_empty)
    }
}

/// Waits toward `retry_at` for at most one `RECONNECT_POLL_INTERVAL`, so one
/// poll never sleeps out a whole backoff. True once `retry_at` has passed or
/// `stop` is set: the caller then dials, and a set `stop` aborts that dial.
fn backoff_over(retry_at: Instant, stop: &AtomicBool) -> bool {
    if stop.load(Ordering::Relaxed) {
        return true;
    }
    let now = Instant::now();
    if now < retry_at {
        thread::sleep((retry_at - now).min(RECONNECT_POLL_INTERVAL));
    }
    Instant::now() >= retry_at
}

/// What a live run dials, with the stream id each leg tags its frames with.
enum LiveDial {
    Single {
        endpoint: SocketAddr,
        soupbin: SoupBinClientConfig,
        stream: StreamId,
    },
    /// A two-stream venue's primary and secondary channel sessions; `streams` is primary,
    /// then secondary.
    Dual {
        primary_endpoint: SocketAddr,
        secondary_endpoint: SocketAddr,
        soupbin: SoupBinClientConfig,
        streams: [StreamId; 2],
    },
}

/// Pairs a live source with the venue's declared streams: `Live` needs a
/// one-stream SoupBinTCP venue and `LiveDual` a two-stream one, whose first
/// stream tags the primary leg and second the secondary leg (see
/// [`VenueDescriptor::streams`]). `None` for any other pairing.
fn live_dial(source: &SourceConfig, venue: &VenueDescriptor) -> Option<LiveDial> {
    if !venue.soupbin_live {
        return None;
    }
    match (source, venue.streams) {
        (SourceConfig::Live { endpoint, soupbin }, &[stream]) => Some(LiveDial::Single {
            endpoint: *endpoint,
            soupbin: soupbin.clone(),
            stream,
        }),
        (
            SourceConfig::LiveDual {
                primary_endpoint,
                secondary_endpoint,
                soupbin,
            },
            &[primary, secondary],
        ) => Some(LiveDial::Dual {
            primary_endpoint: *primary_endpoint,
            secondary_endpoint: *secondary_endpoint,
            soupbin: soupbin.clone(),
            streams: [primary, secondary],
        }),
        _ => None,
    }
}

/// The runtime that drives a live feed's connect and login. A host may poll
/// and drop the session on a thread inside its own async runtime, where
/// `Runtime::block_on` and a blocking runtime drop both panic, so a dial runs
/// on a thread of its own and drop shuts the runtime down in the background.
struct DialRuntime(ManuallyDrop<Runtime>);

impl DialRuntime {
    // The session reads the socket without yielding to the runtime once
    // connected, so one worker driving the reactor is enough.
    fn new() -> io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        Ok(DialRuntime(ManuallyDrop::new(runtime)))
    }

    /// Runs `connect` to completion on a scoped thread outside any runtime
    /// the caller is in. A panic inside `connect` resumes on the caller.
    fn block_on<T: Send>(&self, connect: impl Future<Output = T> + Send) -> Result<T, FeedError> {
        thread::scope(|scope| {
            let dial = thread::Builder::new()
                .name("datafeed-dial".into())
                .spawn_scoped(scope, || self.0.block_on(connect))
                .map_err(FeedError::Runtime)?;
            Ok(dial
                .join()
                .unwrap_or_else(|payload| panic::resume_unwind(payload)))
        })
    }
}

impl Drop for DialRuntime {
    fn drop(&mut self) {
        // SAFETY: `drop` runs once, and nothing reads the field after it.
        let runtime = unsafe { ManuallyDrop::take(&mut self.0) };
        runtime.shutdown_background();
    }
}

/// The connection a live feed reads, or the wait before its next dial.
#[expect(
    clippy::large_enum_variant,
    reason = "one per live feed; boxing would allocate on every reconnect"
)]
enum Link {
    /// Not connected: dial now, or once `retry_at` passes.
    Down {
        retry_at: Option<Instant>,
    },
    Single(LiveSoupBinSource<TcpStream>),
    Dual(DualLiveSource<TcpStream>),
}

/// A live run: one SoupBinTCP session, or a primary and secondary pair,
/// redialed at its resume point each time it closes. The adapter, engine
/// state, tap, and `EgressSequencer` outlive every connection, so
/// `egress_seq` stays one monotonic sequence across them.
pub(crate) struct LiveFeed {
    dial: LiveDial,
    runtime: DialRuntime,
    seq: EgressSequencer<RecordQueue, SystemClock>,
    buffers: StepBuffers<LiveFrame>,
    link: Link,
    pacer: ReconnectPacer,
}

impl LiveFeed {
    /// Checks that `config` is a live run the venue can dial. [`live_dial`]
    /// refuses any source and venue pairing with no dial. A set
    /// `session_midnight_ns` is refused too: a live run has no end date, and
    /// the session installs each trade date's anchor from an unset one.
    pub(crate) fn new(config: &DatafeedConfig, queue: RecordQueue) -> Result<Self, FeedError> {
        if config.session_midnight_ns != 0 {
            return Err(ConfigError::LiveAnchorSet {
                session_midnight_ns: config.session_midnight_ns,
            }
            .into());
        }
        let Some(dial) = live_dial(&config.source, config.venue) else {
            return Err(ConfigError::UnsupportedSource {
                venue: config.venue.name,
                source_kind: config.source.kind(),
            }
            .into());
        };
        let runtime = DialRuntime::new().map_err(FeedError::Runtime)?;
        Ok(LiveFeed {
            dial,
            runtime,
            seq: EgressSequencer::new(queue, SystemClock),
            buffers: StepBuffers::new(BATCH_SIZE),
            link: Link::Down { retry_at: None },
            pacer: ReconnectPacer::new(),
        })
    }

    /// Takes one step: a dial, one bounded slice of the reconnect wait, or one
    /// frame batch. A source that closes records its resume point and
    /// schedules the redial; `stop` ends the run instead.
    pub(crate) fn advance(
        &mut self,
        engine: &mut Engine,
        config: &DatafeedConfig,
        stop: &AtomicBool,
    ) -> Result<Progress, FeedError> {
        let ended = match &mut self.link {
            Link::Down { retry_at } => {
                let retry_at = *retry_at;
                if retry_at.is_some_and(|at| !backoff_over(at, stop)) {
                    return Ok(Progress::Running);
                }
                return self.dial(engine, config, stop);
            }
            Link::Single(source) => {
                let ended = run_batch(source, &mut self.buffers, &mut self.seq, engine, stop)?;
                if ended {
                    let next = source.next_expected_sequence();
                    engine.restart.capture_soupbin_resume(next);
                }
                ended
            }
            Link::Dual(source) => {
                let ended = run_batch(source, &mut self.buffers, &mut self.seq, engine, stop)?;
                if ended {
                    let (primary_next, secondary_next) = source.next_expected_sequences();
                    engine
                        .restart
                        .capture_dual_resume(primary_next, secondary_next);
                }
                ended
            }
        };
        if !ended {
            return Ok(Progress::Running);
        }
        if stop.load(Ordering::Relaxed) {
            return Ok(Progress::Ended);
        }
        let stats = engine.state.stats();
        tracing::warn!(
            venue = config.venue.name,
            ?stats,
            "source closed; reconnecting"
        );
        let retry_at = Instant::now() + self.pacer.next_delay(stats.frames);
        self.link = Link::Down {
            retry_at: Some(retry_at),
        };
        Ok(Progress::Running)
    }

    /// Dials at the resume point `engine.restart` holds. A full replay from
    /// sequence 1 rebuilds the adapter and engine state; a dial `stop`
    /// aborted ends the run.
    fn dial(
        &mut self,
        engine: &mut Engine,
        config: &DatafeedConfig,
        stop: &AtomicBool,
    ) -> Result<Progress, FeedError> {
        let restart = &mut engine.restart;
        let connected = match &self.dial {
            LiveDial::Single {
                endpoint,
                soupbin,
                stream,
            } => self
                .runtime
                .block_on(source::connect_tcp_resuming(
                    *endpoint, soupbin, *stream, restart, stop,
                ))??
                .map(|(source, replay)| (Link::Single(source), replay)),
            LiveDial::Dual {
                primary_endpoint,
                secondary_endpoint,
                soupbin,
                streams,
            } => self
                .runtime
                .block_on(source::connect_dual_resuming(
                    *primary_endpoint,
                    *secondary_endpoint,
                    *streams,
                    soupbin,
                    restart,
                    stop,
                ))??
                .map(|(source, replay)| (Link::Dual(source), replay)),
        };
        let Some((link, replay)) = connected else {
            return Ok(Progress::Ended);
        };
        if replay {
            engine.adapter = (config.venue.make)(config);
            engine.state = SessionState::from_config(config)?;
        }
        self.link = link;
        Ok(Progress::Running)
    }
}

#[cfg(test)]
mod tests {
    use md_pipeline::BundledTzdb;

    use super::*;
    use crate::venue::NASDAQ;

    // NASDAQ over SoupBinTCP: the built-in descriptor has no live source, and
    // the live path needs a venue that dials.
    static NASDAQ_SOUPBIN: VenueDescriptor = VenueDescriptor {
        name: "test-nasdaq-soupbin",
        soupbin_live: true,
        ..NASDAQ
    };

    // Each leg must dial its own endpoint and tag the id its descriptor position
    // names: a swap sends primary frames to the secondary decoder with no error.
    // Distinct endpoints and ids other than 0 and 1 expose a swap or a literal.
    #[test]
    fn live_dial_tags_each_leg_with_its_descriptor_stream() {
        let primary_endpoint = "127.0.0.1:1".parse().expect("loopback address");
        let secondary_endpoint = "127.0.0.1:2".parse().expect("loopback address");
        let one_stream = VenueDescriptor {
            streams: &[StreamId(3)],
            ..NASDAQ_SOUPBIN
        };
        let live = SourceConfig::Live {
            endpoint: primary_endpoint,
            soupbin: Default::default(),
        };
        assert!(matches!(
            live_dial(&live, &one_stream),
            Some(LiveDial::Single {
                stream: StreamId(3),
                ..
            })
        ));

        let two_streams = VenueDescriptor {
            streams: &[StreamId(5), StreamId(9)],
            ..NASDAQ_SOUPBIN
        };
        let live_dual = SourceConfig::LiveDual {
            primary_endpoint,
            secondary_endpoint,
            soupbin: Default::default(),
        };
        let Some(LiveDial::Dual {
            primary_endpoint: primary,
            secondary_endpoint: secondary,
            streams,
            ..
        }) = live_dial(&live_dual, &two_streams)
        else {
            panic!("a two-stream SoupBinTCP venue dials LiveDual");
        };
        assert_eq!(
            (primary, secondary, streams),
            (
                primary_endpoint,
                secondary_endpoint,
                [StreamId(5), StreamId(9)]
            )
        );
    }

    // A live run has no end date, so a set anchor goes stale at the first roll.
    // Stop is pre-set, so the unset control ends at its first poll, before dialing.
    #[test]
    fn live_session_refuses_a_set_session_anchor() {
        use std::sync::Arc;

        use crate::FeedSession;

        let build_with = |session_midnight_ns: u64| {
            let source = SourceConfig::Live {
                endpoint: "127.0.0.1:1".parse().expect("loopback address"),
                soupbin: Default::default(),
            };
            FeedSession::builder(&NASDAQ_SOUPBIN, source)
                .venue_id(1)
                .session_midnight_ns(session_midnight_ns)
                .stop(Arc::new(AtomicBool::new(true)))
                .build()
        };
        let mut unset = build_with(0).expect("an unset anchor builds");
        assert!(
            matches!(unset.poll(&mut Vec::new(), 1), Err(FeedError::Closed)),
            "the set stop ends the run before it dials"
        );
        let anchor = NASDAQ
            .session
            .resolve_anchor(20260424, &BundledTzdb::new())
            .expect("New York anchor resolves");
        let err = build_with(anchor).err().expect("a set anchor is refused");
        assert!(
            matches!(
                err,
                FeedError::Config(ConfigError::LiveAnchorSet { session_midnight_ns }) if session_midnight_ns == anchor
            ),
            "got: {err}"
        );
    }

    // A host on `#[tokio::main]` polls and closes inside its own runtime, where
    // `Runtime::block_on` and a blocking runtime drop both panic. The pre-set
    // stop ends the dial once it runs on the session's runtime.
    #[tokio::test]
    async fn live_session_polls_and_closes_inside_a_host_runtime() {
        use std::sync::Arc;

        use crate::FeedSession;

        let source = SourceConfig::Live {
            endpoint: "127.0.0.1:1".parse().expect("loopback address"),
            soupbin: Default::default(),
        };
        let mut session = FeedSession::builder(&NASDAQ_SOUPBIN, source)
            .venue_id(1)
            .stop(Arc::new(AtomicBool::new(true)))
            .build()
            .expect("a live session builds");
        assert!(matches!(
            session.poll(&mut Vec::new(), 1),
            Err(FeedError::Closed)
        ));
        session.close().expect("close with no capture tap");
    }

    // Delay must stay bounded however long the venue is down: an uncapped
    // shift overflows the u64 and panics the reconnect loop.
    #[test]
    fn reconnect_delay_is_bounded() {
        assert_eq!(reconnect_delay(0), Duration::from_millis(100));
        assert_eq!(reconnect_delay(3), Duration::from_millis(800));
        assert_eq!(reconnect_delay(5), Duration::from_millis(3_200));
        assert_eq!(reconnect_delay(64), Duration::from_millis(3_200));
    }

    // One poll must not sleep out a whole backoff: a wait toward a 3.2s retry
    // sleeps at most one poll interval and reports the backoff still running,
    // and a set stop ends the wait at once.
    #[test]
    fn backoff_wait_is_bounded_per_poll() {
        let stop = AtomicBool::new(false);
        let retry_at = Instant::now() + reconnect_delay(5);
        let start = Instant::now();
        assert!(
            !backoff_over(retry_at, &stop),
            "the backoff is still running"
        );
        let waited = start.elapsed();
        assert!(
            waited < reconnect_delay(1),
            "one wait took {waited:?}, expected about one poll interval"
        );

        stop.store(true, Ordering::Relaxed);
        let start = Instant::now();
        assert!(backoff_over(retry_at, &stop), "a set stop ends the wait");
        assert!(start.elapsed() < RECONNECT_POLL_INTERVAL);
    }
}
