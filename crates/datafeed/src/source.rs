//! Live `IngestSource` implementations over `client_soupbintcp`. Wraps
//! `SoupBinClient` behind `md_pipeline::IngestSource` so the same session
//! hot loop drives live and replay sources interchangeably.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use client_soupbintcp::{
    SoupBinClient, SoupBinClientConfig, SoupBinError, SoupBinEvent, SoupBinMessage,
};
use md_pipeline::{IngestError, IngestFrame, IngestSource, StreamId};
use transport_core::{AsyncReady, StreamRecv, StreamSend, StreamTrySend, TransportError};
use transport_socket::{TcpConfig, tokio::TcpStream};

use crate::restart::RestartState;

/// Failure standing up a live source.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    #[error("transport connect: {0}")]
    Transport(#[from] TransportError),
    #[error("soupbin session: {0}")]
    SoupBin(#[from] SoupBinError),
}

/// One frame pulled off a live SoupBinTCP session: owned payload bytes, the
/// receive timestamp stamped at this ingest boundary, the stream id the venue
/// descriptor declares for this leg, and the exact SoupBinTCP session sequence
/// this frame carried on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveFrame {
    payload: Vec<u8>,
    ts_recv: u64,
    stream: StreamId,
    venue_seq: u64,
}

impl IngestFrame for LiveFrame {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn ts_recv(&self) -> u64 {
        self.ts_recv
    }
    fn stream(&self) -> StreamId {
        self.stream
    }
    fn venue_seq(&self) -> u64 {
        self.venue_seq
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Wraps one `SoupBinClient` session as an `IngestSource`. Drives the
/// client's non-blocking `poll` in a loop (the sync busy-poll core the
/// design targets), never `.await`s on the hot path.
pub struct LiveSoupBinSource<T> {
    client: SoupBinClient<T>,
    stream: StreamId,
    closed: bool,
}

impl<T: StreamRecv + StreamSend + AsyncReady> LiveSoupBinSource<T> {
    /// Runs the SoupBinTCP login handshake over an already-connected
    /// transport, tagging every yielded frame with `stream`.
    pub async fn connect(
        transport: T,
        cfg: SoupBinClientConfig,
        stream: StreamId,
    ) -> Result<Self, SoupBinError> {
        let client = SoupBinClient::connect(transport, cfg).await?;
        Ok(LiveSoupBinSource {
            client,
            stream,
            closed: false,
        })
    }
}

impl<T> LiveSoupBinSource<T> {
    /// Sequence the server will assign next. Capture this before a dropped
    /// socket so a reconnect can resume mid-session instead of a full
    /// sequence-1 replay; see `crate::restart::RestartState`.
    pub fn next_expected_sequence(&self) -> u64 {
        self.client.next_expected_sequence()
    }
}

impl<T: StreamRecv + StreamTrySend> IngestSource for LiveSoupBinSource<T> {
    type Frame = LiveFrame;

    // Drains up to `max` ready data frames. Heartbeats are consumed and
    // skipped (not data); EndOfSession ends the batch immediately: if frames
    // were already collected this call they are returned first (Ok(n)), the
    // NEXT call then surfaces Closed (poll errors EndOfSession once
    // state is Closed), so end-of-source still never surfaces as a bare
    // Ok(0), matching the IngestSource contract.
    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let mut n = 0;
        while n < max {
            match self.client.poll(Instant::now()) {
                Ok(Some(SoupBinMessage::Data(frame))) => {
                    out.push(LiveFrame {
                        payload: frame.as_ref().to_vec(),
                        ts_recv: now_ns(),
                        stream: self.stream,
                        venue_seq: frame.sequence(),
                    });
                    n += 1;
                }
                Ok(Some(SoupBinMessage::Event(SoupBinEvent::EndOfSession))) => {
                    self.closed = true;
                    return if n > 0 {
                        Ok(n)
                    } else {
                        Err(IngestError::Closed)
                    };
                }
                Ok(Some(SoupBinMessage::Event(_))) => {
                    // heartbeat sent, received or timed out: not a data frame. A
                    // timeout closed the client, so the next poll errors EndOfSession
                }
                Ok(None) => break,
                Err(SoupBinError::EndOfSession) => {
                    self.closed = true;
                    return if n > 0 {
                        Ok(n)
                    } else {
                        Err(IngestError::Closed)
                    };
                }
                // Every other SoupBinError (protocol violation, frame too
                // large, transport failure) ends this session the same way:
                // IngestSource has no general error variant, only
                // PoolExhausted (backpressure, does not apply here) and
                // Closed. The caller reconnects via RestartState.
                Err(err) => {
                    tracing::error!(
                        stream = self.stream.0,
                        %err,
                        "soupbin session error; closing source"
                    );
                    self.closed = true;
                    return if n > 0 {
                        Ok(n)
                    } else {
                        Err(IngestError::Closed)
                    };
                }
            }
        }
        Ok(n)
    }
}

/// Merges two single-channel live sources into one, each leg keeping its own
/// stream id. Backs dual-channel live wiring: two physically separate
/// SoupBinTCP sessions feeding one adapter, each frame on its leg's stream.
pub struct DualLiveSource<T> {
    primary: LiveSoupBinSource<T>,
    secondary: LiveSoupBinSource<T>,
    closed: bool,
}

impl<T> DualLiveSource<T> {
    /// `primary`/`secondary` must already be tagged with the venue's primary and secondary
    /// stream ids respectively (see `connect_dual`).
    pub fn new(primary: LiveSoupBinSource<T>, secondary: LiveSoupBinSource<T>) -> Self {
        DualLiveSource {
            primary,
            secondary,
            closed: false,
        }
    }

    /// Next sequence advertised by each connected SoupBinTCP leg.
    pub fn next_expected_sequences(&self) -> (u64, u64) {
        (
            self.primary.next_expected_sequence(),
            self.secondary.next_expected_sequence(),
        )
    }
}

impl<T: StreamRecv + StreamTrySend> IngestSource for DualLiveSource<T> {
    type Frame = LiveFrame;

    // Either channel closing ends the merged source: a dual-channel adapter
    // needs both feeds (primary book + secondary stats), so there is no useful
    // partial-source state to keep running.
    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let before = out.len();
        if let Err(err) = self.primary.recv_batch(out, max) {
            self.closed = true;
            return Err(err);
        }
        let got = out.len() - before;
        let remaining = max.saturating_sub(got);
        if remaining > 0
            && let Err(err) = self.secondary.recv_batch(out, remaining)
        {
            self.closed = true;
            return if got > 0 { Ok(got) } else { Err(err) };
        }
        Ok(out.len() - before)
    }
}

/// Connects a single-channel live source over TCP.
pub async fn connect_tcp(
    endpoint: SocketAddr,
    soupbin_cfg: SoupBinClientConfig,
    stream: StreamId,
) -> Result<LiveSoupBinSource<TcpStream>, SourceError> {
    let transport = connect_transport(endpoint).await?;
    let source = LiveSoupBinSource::connect(transport, soupbin_cfg, stream).await?;
    Ok(source)
}

/// Connects both legs of a dual-channel live source, each with its own
/// login config so legs resume at independent sequences. The stream id pair
/// tags the primary leg, then the secondary leg.
pub async fn connect_dual(
    primary_endpoint: SocketAddr,
    secondary_endpoint: SocketAddr,
    [primary_stream, secondary_stream]: [StreamId; 2],
    primary_cfg: SoupBinClientConfig,
    secondary_cfg: SoupBinClientConfig,
) -> Result<DualLiveSource<TcpStream>, SourceError> {
    let primary = connect_tcp(primary_endpoint, primary_cfg, primary_stream).await?;
    let secondary = connect_tcp(secondary_endpoint, secondary_cfg, secondary_stream).await?;
    Ok(DualLiveSource::new(primary, secondary))
}

/// Poll cadence for `stop` while a connect attempt is outstanding: a dial to a
/// down venue aborts within this bound on shutdown.
const CONNECT_STOP_POLL: Duration = Duration::from_millis(50);

/// Completes once `stop` is set, polling at `CONNECT_STOP_POLL`. Raced against a
/// connect (see `connect_tcp_resuming`) so a hung dial drops promptly on stop.
async fn wait_for_stop(stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(CONNECT_STOP_POLL).await;
    }
}

/// Connect a single leg at its resume point, abortable by `stop`: a dial to a
/// down venue drops on shutdown, yielding `None`. Wraps
/// `connect_tcp_resuming_inner`.
pub async fn connect_tcp_resuming(
    endpoint: SocketAddr,
    base: &SoupBinClientConfig,
    stream: StreamId,
    restart: &mut RestartState,
    stop: &AtomicBool,
) -> Result<Option<(LiveSoupBinSource<TcpStream>, bool)>, SourceError> {
    tokio::select! {
        biased;
        _ = wait_for_stop(stop) => Ok(None),
        result = connect_tcp_resuming_inner(endpoint, base, stream, restart) => result.map(Some),
    }
}

/// Connect a single leg at its captured resume point. A rejected resume is
/// retried from sequence 1; an accepted sequence-1 response also reports a
/// full replay so the caller can reset adapter and engine state.
async fn connect_tcp_resuming_inner(
    endpoint: SocketAddr,
    base: &SoupBinClientConfig,
    stream: StreamId,
    restart: &mut RestartState,
) -> Result<(LiveSoupBinSource<TcpStream>, bool), SourceError> {
    let login = restart.resume_login_config(base);
    let requested = login.requested_sequence_number;
    match connect_tcp(endpoint, login, stream).await {
        Ok(source) => {
            let replay = requested > 1 && source.next_expected_sequence() == 1;
            if replay {
                restart.reset_for_replay();
            }
            Ok((source, replay))
        }
        Err(SourceError::SoupBin(err @ SoupBinError::LoginRejected { .. })) if requested > 1 => {
            tracing::warn!(requested, %err, "resume login rejected; replaying from 1");
            restart.reset_for_replay();
            let source = connect_tcp(endpoint, restart.resume_login_config(base), stream).await?;
            Ok((source, true))
        }
        Err(err) => Err(err),
    }
}

/// Outcome of a connected dual-leg resume attempt; see `classify_dual_resume`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DualResumeOutcome {
    /// Both legs resumed mid-session; engine state stays.
    Resume,
    /// Full replay: engines reset, both legs stream from a common start.
    Replay,
    /// Legs disagree (one resumed, one restarted at 1): keeping this source
    /// would lose the resumed leg's prefix, reconnect both legs at 1.
    ReconnectAtOne,
}

/// Replay decision for a connected dual source. Requesting sequence 1 on
/// either leg means a fresh session feed; a leg answering with sequence 1
/// means the venue restarted that leg. Either forces an engine reset, and a
/// reset with one leg still resumed mid-session must reconnect both legs.
fn classify_dual_resume(
    primary_requested: u64,
    secondary_requested: u64,
    primary_next: u64,
    secondary_next: u64,
) -> DualResumeOutcome {
    let fresh = primary_requested == 1 || secondary_requested == 1;
    let primary_at_one = primary_next == 1;
    let secondary_at_one = secondary_next == 1;
    if !fresh && !primary_at_one && !secondary_at_one {
        return DualResumeOutcome::Resume;
    }
    if primary_at_one != secondary_at_one {
        return DualResumeOutcome::ReconnectAtOne;
    }
    DualResumeOutcome::Replay
}

/// Connect both legs of a dual source at their resume points, abortable by
/// `stop`: a dial to a down venue drops on shutdown, yielding `None`. Wraps
/// `connect_dual_resuming_inner`.
pub async fn connect_dual_resuming(
    primary_endpoint: SocketAddr,
    secondary_endpoint: SocketAddr,
    streams: [StreamId; 2],
    base: &SoupBinClientConfig,
    restart: &mut RestartState,
    stop: &AtomicBool,
) -> Result<Option<(DualLiveSource<TcpStream>, bool)>, SourceError> {
    tokio::select! {
        biased;
        _ = wait_for_stop(stop) => Ok(None),
        result = connect_dual_resuming_inner(
            primary_endpoint,
            secondary_endpoint,
            streams,
            base,
            restart,
        ) => result.map(Some),
    }
}

/// Connect both legs of a dual source at their captured resume points. Any leg
/// restarting at sequence 1 (or a fresh requested session) reports a full
/// replay so the caller resets adapter and engine state; a mixed acceptance
/// drops the source and reconnects both legs at sequence 1 so no leg prefix
/// is lost.
async fn connect_dual_resuming_inner(
    primary_endpoint: SocketAddr,
    secondary_endpoint: SocketAddr,
    streams: [StreamId; 2],
    base: &SoupBinClientConfig,
    restart: &mut RestartState,
) -> Result<(DualLiveSource<TcpStream>, bool), SourceError> {
    let (primary_login, secondary_login) = restart.resume_dual_login_configs(base);
    let primary_requested = primary_login.requested_sequence_number;
    let secondary_requested = secondary_login.requested_sequence_number;
    match connect_dual(
        primary_endpoint,
        secondary_endpoint,
        streams,
        primary_login,
        secondary_login,
    )
    .await
    {
        Ok(source) => {
            let (primary_next, secondary_next) = source.next_expected_sequences();
            match classify_dual_resume(
                primary_requested,
                secondary_requested,
                primary_next,
                secondary_next,
            ) {
                DualResumeOutcome::Resume => Ok((source, false)),
                DualResumeOutcome::Replay => {
                    restart.reset_for_replay();
                    Ok((source, true))
                }
                DualResumeOutcome::ReconnectAtOne => {
                    drop(source);
                    tracing::warn!(
                        primary_next,
                        secondary_next,
                        "dual resume mismatch; replaying both legs from sequence 1"
                    );
                    restart.reset_for_replay();
                    let (primary_login, secondary_login) = restart.resume_dual_login_configs(base);
                    let source = connect_dual(
                        primary_endpoint,
                        secondary_endpoint,
                        streams,
                        primary_login,
                        secondary_login,
                    )
                    .await?;
                    Ok((source, true))
                }
            }
        }
        Err(SourceError::SoupBin(err @ SoupBinError::LoginRejected { .. }))
            if primary_requested > 1 || secondary_requested > 1 =>
        {
            tracing::warn!(
                primary_requested,
                secondary_requested,
                %err,
                "dual resume login rejected; replaying both legs from sequence 1"
            );
            restart.reset_for_replay();
            let (primary_login, secondary_login) = restart.resume_dual_login_configs(base);
            let source = connect_dual(
                primary_endpoint,
                secondary_endpoint,
                streams,
                primary_login,
                secondary_login,
            )
            .await?;
            Ok((source, true))
        }
        Err(err) => Err(err),
    }
}

async fn connect_transport(endpoint: SocketAddr) -> Result<TcpStream, TransportError> {
    TcpStream::connect(&TcpConfig::new(endpoint)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // Full truth table of the dual replay decision, including the fresh
    // requested-sequence-1 session: skipping the engine reset there feeds a
    // full re-stream into engines still holding prior-session state.
    #[test]
    fn dual_resume_decision_truth_table() {
        use DualResumeOutcome::*;
        let cases = [
            // (primary_requested, secondary_requested, primary_next, secondary_next, expected)
            (1, 1, 1, 1, Replay),         // fresh session, both legs from start
            (1, 1, 5, 5, Replay),         // fresh request, venue starts later
            (500, 700, 500, 700, Resume), // both legs resumed mid-session
            (500, 700, 1, 1, Replay),     // venue restarted both legs
            (500, 700, 500, 1, ReconnectAtOne), // secondary restarted, primary resumed
            (500, 700, 1, 700, ReconnectAtOne), // primary restarted, secondary resumed
            (1, 700, 1, 700, ReconnectAtOne), // fresh primary beside resumed secondary
            (500, 1, 500, 1, ReconnectAtOne), // fresh secondary beside resumed primary
        ];
        for (primary_requested, secondary_requested, primary_next, secondary_next, expected) in
            cases
        {
            assert_eq!(
                classify_dual_resume(
                    primary_requested,
                    secondary_requested,
                    primary_next,
                    secondary_next
                ),
                expected,
                "requested ({primary_requested}, {secondary_requested}), next ({primary_next}, {secondary_next})"
            );
        }
    }
}
