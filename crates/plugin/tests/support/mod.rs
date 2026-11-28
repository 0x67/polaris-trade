//! The NASDAQ adapter exported as a plugin of this binary, its native twin,
//! and the committed NASDAQ fixtures as wire frames.

use std::ffi::CString;

use md_pipeline::{
    ClosureModel, IngestError, IngestFrame, IngestSource, RollModel, SessionModel, VenueAdapter,
    VenueId,
};
use md_replay::BinaryFileReplay;
use polaris_plugin::{Plugin, export::Venue};
use proto_nasdaq::adapter::{Config, NasdaqAdapter};

/// Venue id every NASDAQ test uses.
pub const VENUE_ID: u16 = 2;

/// The session model NASDAQ declares, New York midnight.
pub const SESSION: SessionModel = SessionModel {
    roll: RollModel::DailyZoned {
        tz: "America/New_York",
        local_ns: 0,
    },
    closure: ClosureModel::None,
};

const PREFIX: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin"
);
const BOOK_HEX: &str =
    include_str!("../../../datafeed/tests/fixtures/orderbook-reference/stream.hex");

polaris_plugin::export_adapter!(
    adapter: NasdaqAdapter,
    venue: Venue {
        name: c"nasdaq",
        tag: c"nasdaq",
        version: c"v5",
        feed_id: proto_nasdaq::FEED_NASDAQ,
        soupbin_live: false,
        session: SESSION,
    },
    config: config,
);

/// The adapter config from the JSON the host hands `create`.
fn config(json: &str) -> Option<Config> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    Some(Config {
        venue: VenueId(u16::try_from(value["venue_id"].as_u64()?).ok()?),
        session_midnight_ns: value["session_midnight_ns"].as_u64()?,
    })
}

/// The exported plugin, loaded by the host.
pub fn plugin() -> Plugin {
    // SAFETY: the entry returns this binary's own static vtable.
    unsafe { Plugin::new(&*polaris_adapter_entry()) }.expect("the NASDAQ plugin loads")
}

/// The JSON a host hands `create`, and the native adapter of the same config.
pub fn configs() -> (CString, NasdaqAdapter) {
    let json = format!(r#"{{"venue_id": {VENUE_ID}, "session_midnight_ns": 0}}"#);
    let native = NasdaqAdapter::new(Config {
        venue: VenueId(VENUE_ID),
        session_midnight_ns: 0,
    });
    (CString::new(json).expect("no NUL in JSON"), native)
}

/// The ITCH prefix fixture's messages: stock directories, trading actions and
/// a system event.
pub fn prefix_frames() -> Vec<Vec<u8>> {
    let mut replay = BinaryFileReplay::from_path(PREFIX, 0, 1).expect("the prefix fixture opens");
    let mut frames = Vec::new();
    let mut batch = Vec::new();
    loop {
        batch.clear();
        match replay.recv_batch(&mut batch, 256) {
            Ok(0) | Err(IngestError::Closed) => break,
            Ok(_) => {}
            Err(err) => panic!("the prefix fixture fails to read: {err}"),
        }
        frames.extend(batch.iter().map(|f| f.payload().to_vec()));
    }
    frames
}

/// The order-book reference fixture's ITCH messages: a stock directory, then
/// adds, executions, cancels, deletes and replaces.
pub fn book_frames() -> Vec<Vec<u8>> {
    BOOK_HEX
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            (0..line.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&line[at..at + 2], 16).expect("fixture hex"))
                .collect()
        })
        .collect()
}
