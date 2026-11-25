//! Fixture parity: frames recorded from the public feed against the committed
//! normalized output.

use std::path::Path;

use md_pipeline::{VenueId, adapter::testing::assert_fixture_parity};
use venue_binance::{BinanceAdapter, Config, Symbol};

// @lat: [[binance#Tests#Recorded session parity]]
#[test]
fn recorded_session_matches_expected_records() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/session");
    let cfg = Config {
        venue: VenueId(5),
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
    };
    assert_fixture_parity::<BinanceAdapter>(cfg, &dir);
}
