//! Decode path: burst reap, session and sequence checks, reassembly, gap
//! detection, and turning ready items into outcomes.

use std::{cmp::Ordering, mem, sync::Arc, time::Instant};

use transport_core::DatagramRecv;

use super::{Backing, Inner, MoldUdpOutcome, ReadyItem, record_gap, record_message};
use crate::{
    ab::ArbiterVerdict,
    error::MoldUdpError,
    event::MoldUdpEvent,
    frame::{Frame, Held, MessageView, OwnedFrame},
    wire::{self, DownstreamHeader, PacketKind},
};

impl<T: DatagramRecv> Inner<T> {
    /// Release item handed out by previous call. Its borrow is dead once
    /// caller can call again; must run before decode reuses `backing`.
    pub(super) fn retire_current(&mut self) {
        if let Some(ReadyItem::Inline { .. }) = self.current.take() {
            self.release_inline();
        }
    }

    fn release_inline(&mut self) {
        self.inline_pending -= 1;
        if self.inline_pending == 0 {
            // drops datagram: slab back to its pool
            self.backing = Backing::empty();
        }
    }

    /// Hold `item` as current and borrow outcome from it.
    pub(super) fn yield_item(
        &mut self,
        item: ReadyItem<T::Frame>,
    ) -> Result<MoldUdpOutcome<'_, T::Frame>, MoldUdpError> {
        let item: &ReadyItem<T::Frame> = self.current.insert(item);
        let outcome = match item {
            ReadyItem::Inline {
                sequence,
                stream_id,
                offset,
                len,
            } => Frame {
                payload: &self.backing.bytes()[*offset..*offset + *len],
                sequence: *sequence,
                stream_id: *stream_id,
            },
            ReadyItem::View {
                view,
                sequence,
                stream_id,
            } => Frame {
                payload: view.as_ref(),
                sequence: *sequence,
                stream_id: *stream_id,
            },
            ReadyItem::Event(ev) => return Ok(MoldUdpOutcome::Event(*ev)),
            ReadyItem::Gap => return Err(MoldUdpError::GapDetected),
        };
        record_message();
        Ok(MoldUdpOutcome::Frame(outcome))
    }

    /// Owned outcome of `item`: `Inline` shares its datagram through `Arc`.
    pub(super) fn yield_owned(
        &mut self,
        item: ReadyItem<T::Frame>,
    ) -> Result<MoldUdpOutcome<'static, T::Frame>, MoldUdpError> {
        let (view, sequence, stream_id) = match item {
            ReadyItem::Inline {
                sequence,
                stream_id,
                offset,
                len,
            } => {
                let arc = self.share_backing();
                self.release_inline();
                (MessageView::new(arc, offset, len), sequence, stream_id)
            }
            ReadyItem::View {
                view,
                sequence,
                stream_id,
            } => (view, sequence, stream_id),
            ReadyItem::Event(ev) => return Ok(MoldUdpOutcome::Event(ev)),
            ReadyItem::Gap => return Err(MoldUdpError::GapDetected),
        };
        record_message();
        Ok(MoldUdpOutcome::Owned(OwnedFrame {
            view,
            sequence,
            stream_id,
        }))
    }

    fn share_backing(&mut self) -> Arc<Held<T::Frame>> {
        let arc;
        (self.backing, arc) = mem::replace(&mut self.backing, Backing::empty()).share();
        arc
    }

    /// Reap one burst from every leg into `pending_datagrams`, tagged with
    /// leg index. Returns whether anything landed.
    pub(super) fn reap_legs(&mut self) -> Result<bool, MoldUdpError> {
        let mut reaped = false;
        for (stream, leg) in (0..=u8::MAX).zip(&mut self.legs) {
            if leg.recv_burst(&mut self.recv_batch)? == 0 {
                continue;
            }
            reaped = true;
            self.pending_datagrams.extend(
                self.recv_batch
                    .drain()
                    .map(|frame| (stream, Held::Frame(frame))),
            );
        }
        Ok(reaped)
    }

    /// Record tail gap from heartbeat or end-of-session next-expected: anything
    /// between highest sequence seen and server's `next_expected` was lost
    /// during quiet traffic.
    fn note_tail_gap(&mut self, next_expected: u64) {
        let expected = self.next_unseen;
        if next_expected > expected {
            self.next_unseen = next_expected;
            self.gap_handler
                .record_missing_range(expected, next_expected);
            tracing::warn!(
                expected,
                next_expected,
                "sequence gap detected; queueing re-request"
            );
            self.ready.push_back(ReadyItem::Gap);
            record_gap();
        }
    }

    /// Promote arbiter gap candidates whose confirm window elapsed into gap
    /// handler. Only multi-leg path stages candidates; no-op on one leg.
    pub(super) fn drain_confirmed_gaps(&mut self) {
        let Some(arbiter) = self.arbiter.as_mut() else {
            return;
        };
        for seq in arbiter.confirmed_gaps(Instant::now()) {
            self.gap_handler.record_gap(seq);
            self.ready.push_back(ReadyItem::Gap);
            record_gap();
        }
    }

    /// Lock session on first datagram, reject later mismatch; anchor expected
    /// sequence on first packet unless config anchored it.
    fn accept_header(&mut self, header: &DownstreamHeader) -> Result<(), MoldUdpError> {
        match self.session {
            None => self.session = Some(header.session),
            Some(expected) if expected != header.session => {
                return Err(MoldUdpError::SessionMismatch {
                    expected,
                    got: header.session,
                });
            }
            Some(_) => {}
        }
        // anchor on first packet, not seq 1, so live join sees no giant gap;
        // heartbeat and end of session carry next-expected, so any packet anchors
        if !self.seq_anchored {
            self.reassembler.reset_expected(header.sequence);
            self.next_unseen = header.sequence;
            if let Some(arbiter) = self.arbiter.as_mut() {
                arbiter.rebase(header.sequence);
            }
            self.seq_anchored = true;
        }
        Ok(())
    }

    /// Decode next pending datagram into `ready` items. In-order messages drain
    /// inline (no `Arc`); message ahead of `expected_next` promotes datagram
    /// to shared `Arc` (at most once per datagram) and buffers view in
    /// reassembler. Runs only with `ready` empty and `current` retired, so no
    /// `Inline` item still references `backing`.
    pub(super) fn process_next_pending(&mut self) -> Result<(), MoldUdpError> {
        let Some((stream_id, datagram)) = self.pending_datagrams.pop_front() else {
            return Ok(());
        };
        let header = wire::parse_header(datagram.as_ref())?;
        self.accept_header(&header)?;

        match header.kind() {
            PacketKind::Heartbeat => {
                let next_expected = header.sequence;
                self.note_tail_gap(next_expected);
                self.ready
                    .push_back(ReadyItem::Event(MoldUdpEvent::Heartbeat { next_expected }));
                return Ok(());
            }
            PacketKind::EndOfSession => {
                let next_expected = header.sequence;
                self.note_tail_gap(next_expected);
                self.ready
                    .push_back(ReadyItem::Event(MoldUdpEvent::EndOfSession {
                        next_expected,
                    }));
                return Ok(());
            }
            PacketKind::Data => {}
        }

        // collect (seq, offset, len) first: iterator borrows datagram, which moves below
        self.blocks.clear();
        for (seq, block) in (header.sequence..).zip(header.blocks(datagram.as_ref())) {
            let (offset, bytes) = block?;
            self.blocks.push((seq, offset, bytes.len()));
        }

        // blocks are contiguous: only gap is jump past highest sequence seen,
        // recorded once; A/B stages it so lagging leg gets its confirm window
        let unseen = self.next_unseen;
        if header.sequence > unseen {
            if let Some(arbiter) = self.arbiter.as_mut() {
                arbiter.note_missing_range(unseen, header.sequence, Instant::now());
            } else {
                self.gap_handler
                    .record_missing_range(unseen, header.sequence);
                self.ready.push_back(ReadyItem::Gap);
                record_gap();
            }
        }
        self.next_unseen = unseen.max(
            header
                .sequence
                .saturating_add(u64::from(header.message_count)),
        );

        let mut backing = Backing::Owned(datagram);
        let mut inline = 0;
        for &(seq, offset, len) in &self.blocks {
            if let Some(arbiter) = self.arbiter.as_mut() {
                match arbiter.observe(stream_id, seq, Instant::now()) {
                    ArbiterVerdict::Duplicate | ArbiterVerdict::OutOfWindow => continue,
                    ArbiterVerdict::Forward => {}
                }
            }

            self.gap_handler.mark_received(seq);

            match seq.cmp(&self.reassembler.expected_next()) {
                Ordering::Equal => {
                    self.reassembler.advance_expected(1);
                    self.ready.push_back(ReadyItem::Inline {
                        sequence: seq,
                        stream_id,
                        offset,
                        len,
                    });
                    inline += 1;
                    for (next_seq, view) in (seq + 1..).zip(self.reassembler.drain_ready()) {
                        self.ready.push_back(ReadyItem::View {
                            view,
                            sequence: next_seq,
                            stream_id,
                        });
                    }
                }
                Ordering::Greater => {
                    let arc;
                    (backing, arc) = backing.share();
                    let view = MessageView::new(arc, offset, len);
                    if let Some(cursor) = self.reassembler.insert(seq, view)? {
                        for (next_seq, view) in (seq..).zip(cursor) {
                            self.ready.push_back(ReadyItem::View {
                                view,
                                sequence: next_seq,
                                stream_id,
                            });
                        }
                    }
                }
                Ordering::Less => {} // stale duplicate
            }
        }

        if inline > 0 {
            self.backing = backing;
            self.inline_pending = inline;
        }
        Ok(())
    }
}
