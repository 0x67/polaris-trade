//! Mapping rules the recorded session does not exercise.

use md_events::{LevelAction, Side};
use md_pipeline::{MarketEvent, VenueAdapter, VenueId};
use venue_hyperliquid::{Config, HyperliquidAdapter, STREAM};

fn adapter() -> HyperliquidAdapter {
    HyperliquidAdapter::new(Config { venue: VenueId(3) })
}

fn feed(a: &mut HyperliquidAdapter, json: &str, seq: u64) -> Vec<MarketEvent> {
    a.feed(STREAM, json.as_bytes(), 10, seq);
    a.drain().collect()
}

fn trade(coin: &str, px: &str) -> String {
    format!(
        r#"{{"coin":"{coin}","side":"B","px":"{px}","sz":"1.0","time":1,"hash":"0x0","tid":9}}"#
    )
}

// @lat: [[hyperliquid#Tests#Out of range values drop the whole frame]]
#[test]
fn out_of_range_price_drops_the_frame_and_keeps_the_definition_pending() {
    let mut a = adapter();
    let bad = format!(
        r#"{{"channel":"trades","data":[{},{}]}}"#,
        trade("BTC", "100.0"),
        trade("BTC", "9300000000.0")
    );
    let dropped = feed(&mut a, &bad, 1);
    assert_eq!(dropped.len(), 0, "{dropped:?}");
    assert_eq!(a.stats().malformed, 1);

    // the dropped frame must not have minted BTC without emitting its Definition
    let good = format!(
        r#"{{"channel":"trades","data":[{}]}}"#,
        trade("BTC", "100.0")
    );
    let events = feed(&mut a, &good, 2);
    assert!(
        matches!(events[0], MarketEvent::Definition(_)),
        "{events:?}"
    );
    assert!(matches!(events[1], MarketEvent::Trade(_)));
}

// @lat: [[hyperliquid#Tests#Book push replaces the book]]
#[test]
fn book_push_clears_then_lists_levels_and_skips_zero_sizes() {
    let mut a = adapter();
    let book = r#"{"channel":"l2Book","data":{"coin":"ETH","time":5,"levels":[[{"px":"10.5","sz":"2","n":1},{"px":"10.4","sz":"0","n":0},{"px":"10.3","sz":"1","n":1}],[{"px":"10.6","sz":"3","n":1}]]}}"#;
    let events = feed(&mut a, book, 1);
    let levels: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Level(l) => Some((l.action, l.side, l.level, l.price, l.qty)),
            _ => None,
        })
        .collect();
    assert_eq!(
        levels,
        [
            (LevelAction::Clear, Side::None, 0, i64::MAX, 0),
            (LevelAction::New, Side::Bid, 0, 10_500_000_000, 200_000_000),
            (LevelAction::New, Side::Bid, 1, 10_300_000_000, 100_000_000),
            (LevelAction::New, Side::Ask, 0, 10_600_000_000, 300_000_000),
        ]
    );
}

// @lat: [[hyperliquid#Tests#Spot coins drop the frame]]
#[test]
fn spot_coin_names_drop_the_frame_without_minting() {
    let mut a = adapter();
    for coin in ["@107", "PURR/USDC"] {
        let frame = format!(
            r#"{{"channel":"trades","data":[{}]}}"#,
            trade(coin, "100.0")
        );
        assert_eq!(feed(&mut a, &frame, 1).len(), 0);
    }
    assert_eq!(a.stats().malformed, 2);
}
