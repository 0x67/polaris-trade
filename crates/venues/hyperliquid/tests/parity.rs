//! Fixture parity: frames recorded from the public feed against the committed
//! normalized output.

use std::path::Path;

use md_pipeline::{VenueId, adapter::testing::assert_fixture_parity};
use venue_hyperliquid::{Config, HyperliquidAdapter};

// @lat: [[hyperliquid#Tests#Recorded session parity]]
#[test]
fn recorded_session_matches_expected_records() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/session");
    assert_fixture_parity::<HyperliquidAdapter>(Config { venue: VenueId(3) }, &dir);
}
