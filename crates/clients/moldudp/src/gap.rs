//! Gap tracking and rate-limited unicast re-request emission.
//!
//! `GapRequestHandler` records missing sequence ranges as they're detected and
//! clears them as messages arrive. `GapRequestEmitter` turns pending gaps into
//! `MoldUDP64` Request Packets, rate-limited by coverage so stuck gap can't
//! flood re-request server.

use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use transport_core::{DatagramSend, TransportError};

use crate::error::MoldUdpError;

/// One contiguous run of missing sequence numbers, ready to re-request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapRequest {
    /// First missing sequence.
    pub start_seq: u64,
    /// Missing messages from `start_seq` on.
    pub count: u16,
}

/// Tracks missing sequence ranges as `start -> end (exclusive)`. Coalesces
/// adjacent/overlapping ranges on record; splits range on partial fill.
#[derive(Debug, Default)]
pub struct GapRequestHandler {
    gaps: BTreeMap<u64, u64>,
}

impl GapRequestHandler {
    /// Handler with no gap recorded.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `[start, end)` as missing. `start >= end` is no-op.
    pub fn record_missing_range(&mut self, start: u64, end_exclusive: u64) {
        if start >= end_exclusive {
            return;
        }
        let merged_end = self
            .gaps
            .get(&start)
            .copied()
            .unwrap_or(start)
            .max(end_exclusive);
        self.gaps.insert(start, merged_end);
    }

    /// Record single missing sequence number.
    pub fn record_gap(&mut self, seq: u64) {
        self.record_missing_range(seq, seq + 1);
    }

    /// Clear `seq` from whichever pending range covers it, splitting that
    /// range if `seq` falls in its interior.
    pub fn mark_received(&mut self, seq: u64) {
        let Some((&start, &end)) = self.gaps.range(..=seq).next_back() else {
            return;
        };
        if seq >= end {
            return;
        }
        self.gaps.remove(&start);
        if start < seq {
            self.gaps.insert(start, seq);
        }
        if seq + 1 < end {
            self.gaps.insert(seq + 1, end);
        }
    }

    // allocation-free check for hot path
    pub(crate) fn has_pending(&self) -> bool {
        !self.gaps.is_empty()
    }

    /// Expand tracked ranges into re-request-sized chunks (`count` fits `u16`).
    pub fn pending_gaps(&self) -> Vec<GapRequest> {
        let mut out = Vec::new();
        for (&start, &end) in &self.gaps {
            let mut cur = start;
            while cur < end {
                let count = u16::try_from(end - cur).unwrap_or(u16::MAX);
                out.push(GapRequest {
                    start_seq: cur,
                    count,
                });
                cur += u64::from(count);
            }
        }
        out
    }
}

/// Sends `MoldUDP64` Request Packets for pending gaps, rate-limited by
/// coverage: gap inside range requested within last interval is skipped, so
/// gap that never fills can't flood re-request server, and gap shrinking from
/// its head as retransmissions land is not re-requested.
#[derive(Debug)]
pub struct GapRequestEmitter {
    /// Re-request server every packet goes to.
    pub server_addr: SocketAddr,
    // `[start, end)` requested within last interval, with send time
    requested: Vec<(u64, u64, Instant)>,
    max_per_gap_per_sec: u32,
}

impl GapRequestEmitter {
    /// Emitter sending to `server_addr`, requesting any one missing range at
    /// most `max_per_gap_per_sec` (min 1) times per second.
    pub fn new(server_addr: SocketAddr, max_per_gap_per_sec: u32) -> Self {
        Self {
            server_addr,
            requested: Vec::new(),
            max_per_gap_per_sec: max_per_gap_per_sec.max(1),
        }
    }

    /// Send Request Packet from `sock` for each gap not covered by range
    /// requested within last `1 / max_per_gap_per_sec` s, returning how many
    /// were sent. Full socket buffer stops this round without marking rest
    /// requested, so next call retries them. Failed send is marked like sent
    /// one, so broken socket costs one attempt per interval, not one per call.
    ///
    /// # Errors
    ///
    /// [`MoldUdpError::Transport`] when send fails for reason other than full buffer.
    pub fn emit<Q: DatagramSend>(
        &mut self,
        gaps: &[GapRequest],
        session: [u8; 10],
        sock: &mut Q,
    ) -> Result<usize, MoldUdpError> {
        let interval = Duration::from_secs(1) / self.max_per_gap_per_sec;
        let now = Instant::now();
        // expired ranges limit nothing; pruning keeps list bounded
        self.requested
            .retain(|&(_, _, at)| now.duration_since(at) < interval);
        let mut sent = 0usize;
        for gap in gaps {
            let end = gap.start_seq + u64::from(gap.count);
            let covered = self
                .requested
                .iter()
                .any(|&(start, stop, _)| start <= gap.start_seq && end <= stop);
            if covered {
                continue;
            }
            let packet = encode_request_packet(session, gap.start_seq, gap.count);
            let result = sock.send_to(&packet, self.server_addr);
            if let Err(TransportError::Io { error, .. }) = &result
                && error.kind() == io::ErrorKind::WouldBlock
            {
                break;
            }
            self.requested.push((gap.start_seq, end, now));
            result?;
            sent += 1;
        }
        Ok(sent)
    }
}

/// Request Packet: `Session[10]`, `Sequence[8 BE]`, `RequestedMessageCount[2 BE]`.
fn encode_request_packet(session: [u8; 10], start_seq: u64, count: u16) -> [u8; 20] {
    let mut packet = [0u8; 20];
    packet[0..10].copy_from_slice(&session);
    packet[10..18].copy_from_slice(&start_seq.to_be_bytes());
    packet[18..20].copy_from_slice(&count.to_be_bytes());
    packet
}
