//! `HyperliquidAdapter` against the md-pipeline venue-adapter conformance suite.

use md_pipeline::{
    VenueId,
    adapter::testing::{ContractFrames, SampleFrame, assert_adapter_contract},
};
use venue_hyperliquid::{
    Config, HyperliquidAdapter, KIND_ACTIVE_ASSET_CTX, KIND_L2_BOOK, KIND_TRADES, STREAM,
};

fn sample(kind: u8, json: &str) -> SampleFrame {
    SampleFrame {
        stream: STREAM,
        kind,
        payload: json.as_bytes().to_vec(),
    }
}

// @lat: [[hyperliquid#Tests#Adapter contract]]
#[test]
fn hyperliquid_adapter_meets_seam_contract() {
    // The trades frame leads with two prints on a new coin, so the gap case
    // sees one frame that emits several records.
    let decodable = vec![
        sample(
            KIND_TRADES,
            r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"59775.0","sz":"0.5","time":1795534853628,"hash":"0x0","tid":1},{"coin":"BTC","side":"A","px":"59774.0","sz":"0.25","time":1795534853700,"hash":"0x0","tid":2}]}"#,
        ),
        sample(
            KIND_L2_BOOK,
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1795534853800,"levels":[[{"px":"59774.0","sz":"1.5","n":2}],[{"px":"59775.0","sz":"2.5","n":3}]]}}"#,
        ),
        sample(
            KIND_ACTIVE_ASSET_CTX,
            r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"-0.0000110511","openInterest":"38158.9","oraclePx":"59817.0","markPx":"59776.0"}}}"#,
        ),
    ];
    let unknown = vec![sample(
        b'z',
        r#"{"channel":"subscriptionResponse","data":{"method":"subscribe"}}"#,
    )];
    let cfg = Config { venue: VenueId(3) };
    assert_adapter_contract::<HyperliquidAdapter>(&cfg, &ContractFrames { decodable, unknown });
}
