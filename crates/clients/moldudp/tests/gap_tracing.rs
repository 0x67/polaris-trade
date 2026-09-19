//! Gap events go through `tracing`: one warn per discontinuity at detection,
//! never per packet, and nothing for in-order feed.

pub mod support;

use std::sync::{Arc, Mutex};

use client_moldudp::{MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use support::{mold_heartbeat, mold_packet};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span,
};

const SESSION: [u8; 10] = *b"SESSIONID1";

/// One dispatched event: level plus its `message` field text.
#[derive(Debug, Clone)]
struct CapturedEvent {
    level: Level,
    message: String,
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

/// Hand-rolled subscriber recording events; path under test opens no span.
#[derive(Clone, Default)]
struct CapturingSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            message: visitor.message,
        });
    }

    fn enter(&self, _span: &span::Id) {}
    fn exit(&self, _span: &span::Id) {}
}

/// Drain `packets` through one mock leg, recording events emitted meanwhile.
fn captured(packets: &[Vec<u8>], frames: usize) -> Vec<CapturedEvent> {
    let subscriber = CapturingSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    tracing::subscriber::with_default(subscriber, || {
        let mut leg = support::mock_leg();
        for p in packets {
            leg.driver_mut().inject(p);
        }
        let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])
            .expect("receiver");
        support::drain(&mut rx, frames);
        while rx.poll().is_ok_and(|outcome| outcome.is_some()) {}
    });
    events.lock().unwrap().clone()
}

#[test]
fn in_order_sequence_emits_no_gap_event() {
    let packets: Vec<_> = (1u64..=3)
        .map(|seq| mold_packet(&SESSION, seq, format!("m{seq}").as_bytes()))
        .collect();
    let events = captured(&packets, 3);
    assert!(events.is_empty(), "in-order feed emitted {events:?}");
}

#[test]
fn tail_gap_discontinuity_emits_exactly_one_warn() {
    // heartbeat saying next is 4 after seq 1 is detection transition; fires once
    let events = captured(
        &[
            mold_packet(&SESSION, 1, b"one"),
            mold_heartbeat(&SESSION, 4),
        ],
        1,
    );
    assert_eq!(events.len(), 1, "exactly one gap event, got {events:?}");
    assert_eq!(events[0].level, Level::WARN);
    assert!(
        events[0].message.contains("sequence gap detected"),
        "unexpected message: {}",
        events[0].message
    );
}
