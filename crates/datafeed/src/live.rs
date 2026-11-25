//! Live session assembly and the reconnect loop.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use client_soupbintcp::SoupBinClientConfig;
use md_pipeline::{EgressSequencer, IngestSource, StreamId, SystemClock};

use crate::{
    AppResult,
    config::{DatafeedConfig, SourceConfig},
    restart::RestartState,
    session::{self, SessionState},
    sink::AnySink,
    source::{self, SourceError},
    tap::{AnyTap, finish_capture},
    venue::VenueDescriptor,
};

const BATCH_SIZE: usize = 64;

/// Bounded reconnect backoff: 100ms doubling per consecutive frameless
/// session, capped at 3.2s. A session that delivered frames resets it.
fn reconnect_delay(consecutive_empty: u32) -> Duration {
    Duration::from_millis(100u64 << consecutive_empty.min(5))
}

/// Shutdown-latency bound for the reconnect wait: `pace` polls `stop` at this
/// cadence, so a stop mid-backoff is observed within one increment.
const RECONNECT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Tracks consecutive sessions that closed without delivering frames, from
/// the cumulative `SessionStats::frames` counter `session::run` returns.
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

    /// Sleeps before the next reconnect attempt; longer after each session
    /// that closed without progress (login loop, dead venue). Wakes every
    /// `RECONNECT_POLL_INTERVAL` to observe `stop`, so shutdown never waits the
    /// full backoff cap.
    fn pace(&mut self, total_frames: u64, stop: &AtomicBool) {
        if total_frames == self.prev_frames {
            self.consecutive_empty += 1;
        } else {
            self.consecutive_empty = 0;
        }
        self.prev_frames = total_frames;
        sleep_interruptible(reconnect_delay(self.consecutive_empty), stop);
    }
}

/// Sleeps up to `total`, returning early the moment `stop` is set. Checks the
/// flag before each `RECONNECT_POLL_INTERVAL` step, so a pre-set flag returns
/// without sleeping and worst-case latency is one increment.
fn sleep_interruptible(total: Duration, stop: &AtomicBool) {
    let mut remaining = total;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let step = remaining.min(RECONNECT_POLL_INTERVAL);
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}

/// What a live run dials, with the stream id each leg tags its frames with.
enum LiveDial<'a> {
    Single {
        endpoint: SocketAddr,
        soupbin: &'a SoupBinClientConfig,
        stream: StreamId,
    },
    /// A two-stream venue's primary and secondary channel sessions; `streams` is primary,
    /// then secondary.
    Dual {
        primary_endpoint: SocketAddr,
        secondary_endpoint: SocketAddr,
        soupbin: &'a SoupBinClientConfig,
        streams: [StreamId; 2],
    },
}

/// Pairs a live source with the venue's declared streams: `Live` needs a
/// one-stream SoupBinTCP venue and `LiveDual` a two-stream one, whose first
/// stream tags the primary leg and second the secondary leg (see
/// [`VenueDescriptor::streams`]). `None` for any other pairing.
fn live_dial<'a>(source: &'a SourceConfig, venue: &VenueDescriptor) -> Option<LiveDial<'a>> {
    if !venue.soupbin_live {
        return None;
    }
    match (source, venue.streams) {
        (SourceConfig::Live { endpoint, soupbin }, &[stream]) => Some(LiveDial::Single {
            endpoint: *endpoint,
            soupbin,
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
            soupbin,
            streams: [primary, secondary],
        }),
        _ => None,
    }
}

/// Runs a live venue session, reconnecting until `stop` is set or a fault ends
/// it. [`live_dial`] picks one SoupBinTCP session or a primary plus secondary channel
/// pair from the source and the venue's declared streams, and rejects
/// any other pairing before dialing. A set `session_midnight_ns` is rejected
/// too: a live run has no end date, and [`session::run`] installs each trade
/// date's anchor from an unset one instead.
pub(crate) fn run(
    runtime: &tokio::runtime::Runtime,
    config: &DatafeedConfig,
    sink: AnySink,
    tap: AnyTap,
    stop: &AtomicBool,
) -> AppResult<()> {
    if config.session_midnight_ns != 0 {
        return Err(format!(
            "session_midnight_ns {} is set on a live source; leave it unset, live installs \
             each trade date's anchor from frame receive time",
            config.session_midnight_ns
        )
        .into());
    }
    match live_dial(&config.source, config.venue) {
        Some(LiveDial::Single {
            endpoint,
            soupbin,
            stream,
        }) => reconnect(
            config,
            sink,
            tap,
            stop,
            |restart| {
                runtime.block_on(source::connect_tcp_resuming(
                    endpoint, soupbin, stream, restart, stop,
                ))
            },
            |source, restart| restart.capture_soupbin_resume(source.next_expected_sequence()),
        ),
        Some(LiveDial::Dual {
            primary_endpoint,
            secondary_endpoint,
            soupbin,
            streams,
        }) => reconnect(
            config,
            sink,
            tap,
            stop,
            |restart| {
                runtime.block_on(source::connect_dual_resuming(
                    primary_endpoint,
                    secondary_endpoint,
                    streams,
                    soupbin,
                    restart,
                    stop,
                ))
            },
            |source, restart| {
                let (primary_next, secondary_next) = source.next_expected_sequences();
                restart.capture_dual_resume(primary_next, secondary_next);
            },
        ),
        None => Err(format!(
            "unsupported venue/source combination: {:?} / {:?}",
            config.venue.name, config.source
        )
        .into()),
    }
}

/// Reconnect loop over one live source shape: `connect` dials at the resume
/// point `RestartState` holds, and `capture_resume` records where the source
/// stopped once its session returns. A full replay rebuilds adapter and
/// engine state.
fn reconnect<Src: IngestSource>(
    config: &DatafeedConfig,
    sink: AnySink,
    mut tap: AnyTap,
    stop: &AtomicBool,
    mut connect: impl FnMut(&mut RestartState) -> Result<Option<(Src, bool)>, SourceError>,
    capture_resume: impl Fn(&Src, &mut RestartState),
) -> AppResult<()> {
    let venue = config.venue.name;
    let mut adapter = (config.venue.make)(config);
    let mut sequence = EgressSequencer::new(sink, SystemClock);
    let mut restart = RestartState::default();
    let mut pacer = ReconnectPacer::new();

    // Capture the loop outcome instead of `?`-returning, so `finish_capture`
    // runs on every exit: a mid-session connect or session fault breaks with
    // the error, writers still close and manifests still write, then the fault
    // propagates. `replay::run` routes its outcome through the same helper.
    let outcome: AppResult<()> = 'session: {
        let mut state = match SessionState::from_config(config) {
            Ok(state) => state,
            Err(err) => break 'session Err(err.into()),
        };
        loop {
            let connected = match connect(&mut restart) {
                Ok(connected) => connected,
                Err(err) => break 'session Err(err.into()),
            };
            let Some((source, replay)) = connected else {
                break 'session Ok(());
            };
            if replay {
                adapter = (config.venue.make)(config);
                state = match SessionState::from_config(config) {
                    Ok(state) => state,
                    Err(err) => break 'session Err(err.into()),
                };
            }
            let (stats, source) = match session::run(
                source,
                &mut *adapter,
                &mut sequence,
                &mut tap,
                &mut state,
                &mut restart,
                session::RunControl {
                    batch: BATCH_SIZE,
                    stop,
                },
            ) {
                Ok(session_outcome) => session_outcome,
                Err(err) => break 'session Err(err.into()),
            };
            capture_resume(&source, &mut restart);
            if stop.load(Ordering::Relaxed) {
                break 'session Ok(());
            }
            tracing::warn!(venue, ?stats, "source closed; reconnecting");
            pacer.pace(stats.frames, stop);
        }
    };

    finish_capture(venue, tap, outcome)
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
    // Stop is pre-set, so the unset control returns before dialing.
    #[test]
    fn live_run_refuses_a_set_session_anchor() {
        use crate::{sink::NullSink, tap::NoopTap};

        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let stop = AtomicBool::new(true);
        let run_with = |session_midnight_ns: u64| {
            let mut config: DatafeedConfig = serde_json::from_str(&format!(
                r#"{{"venue":"nasdaq","venue_id":1,"session_midnight_ns":{session_midnight_ns},
                    "source":{{"kind":"live","endpoint":"127.0.0.1:1"}}}}"#
            ))
            .expect("config parses");
            config.venue = &NASDAQ_SOUPBIN;
            let (sink, tap) = (AnySink::Null(NullSink), AnyTap::Noop(NoopTap));
            run(&runtime, &config, sink, tap, &stop)
        };
        run_with(0).expect("an unset anchor runs until the set stop ends it");
        let anchor = NASDAQ
            .session
            .resolve_anchor(20260424, &BundledTzdb::new())
            .expect("New York anchor resolves");
        let err = run_with(anchor).expect_err("a set anchor is refused");
        assert!(
            err.to_string().contains("session_midnight_ns"),
            "got: {err}"
        );
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

    // A stop flag already set must abort the backoff at once, well under
    // reconnect_delay(1) = 200ms, so shutdown is never held by a pending
    // reconnect. Bounds the wait to one poll increment.
    #[test]
    fn pace_returns_promptly_when_stopped() {
        use std::time::Instant;

        let stop = AtomicBool::new(true);
        let mut pacer = ReconnectPacer::new();
        let start = Instant::now();
        pacer.pace(0, &stop);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "stopped pace returned in {elapsed:?}, expected under one poll increment"
        );
    }
}
