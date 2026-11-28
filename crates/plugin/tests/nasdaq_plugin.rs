//! The NASDAQ adapter exported through `export_adapter!` and driven through
//! the host's `PluginAdapter`, in process, against the same adapter as a
//! native `DynAdapter`, over the committed NASDAQ fixtures.

mod support;

use md_pipeline::{DynAdapter, MarketEvent, StreamId};
use polaris_plugin::PluginVenue;

// 2026-05-21 00:00 New York, in UTC nanoseconds
const NEW_YORK_MIDNIGHT: u64 = 1_779_336_000_000_000_000;

#[test]
fn the_plugin_declares_the_nasdaq_venue() {
    assert_eq!(
        *support::plugin().venue(),
        PluginVenue {
            name: "nasdaq",
            tag: "nasdaq",
            version: "v5",
            feed_id: proto_nasdaq::FEED_NASDAQ,
            soupbin_live: false,
            streams: &[StreamId(0)],
            session: support::SESSION,
        }
    );
}

// @lat: [[plugin#Tests#NASDAQ parity]]
#[test]
fn nasdaq_through_the_plugin_matches_the_native_adapter() {
    let (json, native) = support::configs();
    let mut plugin = support::plugin().create(&json).expect("the adapter builds");
    let mut native: Box<dyn DynAdapter> = Box::new(native);
    assert_eq!(plugin.session(), native.session());
    assert_eq!(plugin.record_types(), native.record_types());

    let prefix = support::prefix_frames();
    let book = support::book_frames();
    let frames: Vec<&[u8]> = prefix.iter().chain(&book).map(Vec::as_slice).collect();
    let mut events: (Vec<MarketEvent>, Vec<MarketEvent>) = (Vec::new(), Vec::new());
    for (seq, payload) in (1..).zip(&frames) {
        if seq == prefix.len() as u64 {
            plugin.set_session_anchor(NEW_YORK_MIDNIGHT);
            native.set_session_anchor(NEW_YORK_MIDNIGHT);
        }
        plugin.feed(StreamId(0), payload, seq * 10, seq);
        native.feed(StreamId(0), payload, seq * 10, seq);
        plugin.drain_into(&mut events.0);
        native.drain_into(&mut events.1);
    }

    assert_eq!(plugin.fault(), None);
    assert_eq!(events.0.len(), events.1.len(), "event count");
    let first_diff = events.0.iter().zip(&events.1).position(|(a, b)| a != b);
    assert_eq!(first_diff, None, "first event that differs");
    assert_eq!(plugin.stats(), native.stats());
    assert_eq!(
        plugin.stats().decoded,
        frames.len() as u64,
        "every frame decoded"
    );
    let mbo = events
        .0
        .iter()
        .filter(|e| matches!(e, MarketEvent::Mbo(_)))
        .count();
    assert!(mbo > 0, "the book frames emit order events");
}
