//! Gap events go through `tracing`: one warn per discontinuity at detection
//! (data packet, heartbeat tail, or A/B confirmation), never per packet, and
//! nothing for in-order feed.

pub mod support;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use client_moldudp::{MoldUdpError, MoldUdpReceiver, MoldUdpReceiverConfig};
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

/// Poll mock legs (one per packet list) until idle, recording events emitted
/// meanwhile.
fn captured(cfg: &MoldUdpReceiverConfig, legs: &[&[Vec<u8>]]) -> Vec<CapturedEvent> {
    let subscriber = CapturingSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    tracing::subscriber::with_default(subscriber, || {
        let legs = legs
            .iter()
            .map(|packets| {
                let mut leg = support::mock_leg();
                for p in *packets {
                    leg.driver_mut().inject(p);
                }
                leg
            })
            .collect();
        let mut rx = MoldUdpReceiver::from_legs(cfg, legs).expect("receiver");
        loop {
            match rx.poll() {
                Ok(None) => break,
                Ok(Some(_)) | Err(MoldUdpError::GapDetected) => {}
                Err(e) => panic!("poll: {e}"),
            }
        }
    });
    events.lock().unwrap().clone()
}

fn assert_one_warn(events: &[CapturedEvent], message: &str) {
    assert_eq!(events.len(), 1, "exactly one gap event, got {events:?}");
    assert_eq!(events[0].level, Level::WARN);
    assert!(
        events[0].message.contains(message),
        "unexpected message: {}",
        events[0].message
    );
}

#[test]
fn in_order_sequence_emits_no_gap_event() {
    let packets: Vec<_> = (1u64..=3)
        .map(|seq| mold_packet(&SESSION, seq, format!("m{seq}").as_bytes()))
        .collect();
    let events = captured(&MoldUdpReceiverConfig::default(), &[&packets]);
    assert!(events.is_empty(), "in-order feed emitted {events:?}");
}

#[test]
fn tail_gap_discontinuity_emits_exactly_one_warn() {
    // heartbeat saying next is 4 after seq 1 is detection transition; fires once
    let packets = [
        mold_packet(&SESSION, 1, b"one"),
        mold_heartbeat(&SESSION, 4),
    ];
    let events = captured(&MoldUdpReceiverConfig::default(), &[&packets]);
    assert_one_warn(&events, "sequence gap detected");
}

#[test]
fn data_packet_gap_emits_exactly_one_warn() {
    // 3 ahead of expected 2 opens gap; late 2 fills it quietly
    let packets = [1, 3, 2].map(|seq| mold_packet(&SESSION, seq, b"m"));
    let events = captured(&MoldUdpReceiverConfig::default(), &[&packets]);
    assert_one_warn(&events, "sequence gap detected");
}

#[test]
fn multi_leg_confirmed_gap_emits_exactly_one_warn() {
    // both legs lose 2; zero confirm window confirms it on leg A's 3
    let cfg = MoldUdpReceiverConfig {
        gap_confirm_window: Duration::ZERO,
        ..MoldUdpReceiverConfig::default()
    };
    let leg_a = [1, 3].map(|seq| mold_packet(&SESSION, seq, b"m"));
    let leg_b = [mold_packet(&SESSION, 1, b"m")];
    let events = captured(&cfg, &[&leg_a, &leg_b]);
    assert_one_warn(&events, "sequence gap confirmed");
}
