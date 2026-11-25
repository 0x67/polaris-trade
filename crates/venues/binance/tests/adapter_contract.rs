//! `BinanceAdapter` against the md-pipeline venue-adapter conformance suite.

use md_pipeline::{
    StreamId, VenueId,
    adapter::testing::{ContractFrames, SampleFrame, assert_adapter_contract},
};
use venue_binance::{
    BinanceAdapter, Config, KIND_DEPTH_UPDATE, KIND_SNAPSHOT, KIND_TRADE, MARKET_STREAM,
    SNAPSHOT_STREAM, Symbol,
};

fn sample(stream: StreamId, kind: u8, json: &str) -> SampleFrame {
    SampleFrame {
        stream,
        kind,
        payload: json.as_bytes().to_vec(),
    }
}

// @lat: [[binance#Tests#Adapter contract]]
#[test]
fn binance_adapter_meets_seam_contract() {
    // The diff buffers and the snapshot covers it, so every record carries
    // the arrival data of its own frame. Fed again, the snapshot equals the
    // live book's update id and applies once more.
    let decodable = vec![
        sample(
            MARKET_STREAM,
            KIND_DEPTH_UPDATE,
            r#"{"e":"depthUpdate","E":1795537595913,"s":"BTCUSDT","U":90,"u":100,"b":[["10.00000000","1.00000000"]],"a":[]}"#,
        ),
        sample(
            SNAPSHOT_STREAM,
            KIND_SNAPSHOT,
            r#"{"lastUpdateId":100,"bids":[["10.00000000","1.00000000"],["9.50000000","2.00000000"]],"asks":[["10.50000000","3.00000000"]]}"#,
        ),
        sample(
            MARKET_STREAM,
            KIND_TRADE,
            r#"{"e":"trade","E":1795537595915,"s":"BTCUSDT","t":7,"p":"10.50000000","q":"0.25000000","T":1795537595915,"m":false,"M":true}"#,
        ),
    ];
    let unknown = vec![
        sample(MARKET_STREAM, b'z', r#"{"result":null,"id":1}"#),
        sample(
            SNAPSHOT_STREAM,
            b'z',
            r#"{"code":-1121,"msg":"Invalid symbol."}"#,
        ),
    ];
    let cfg = Config {
        venue: VenueId(5),
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
    };
    assert_adapter_contract::<BinanceAdapter>(&cfg, &ContractFrames { decodable, unknown });
}
