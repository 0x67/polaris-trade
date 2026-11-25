//! `NasdaqAdapter` against the md-pipeline venue-adapter conformance suite.
//! Feature-gated with `adapter` since the adapter only exists behind that
//! feature.
#![cfg(feature = "adapter")]

use md_pipeline::{
    StreamId, VenueId,
    adapter::testing::{ContractFrames, SampleFrame, assert_adapter_contract},
};
use proto_nasdaq::{
    adapter::{Config, NasdaqAdapter},
    itch_v5::BYTE_LEN,
};

const ITCH: StreamId = StreamId(0);

/// Zero-filled `kind` frame of `len` bytes on the ITCH stream.
fn blank(kind: u8, len: usize) -> SampleFrame {
    let mut payload = vec![0u8; len];
    payload[0] = kind;
    SampleFrame {
        stream: ITCH,
        kind,
        payload,
    }
}

/// Every kind with a `BYTE_LEN` entry reaches a dispatch arm, so this samples
/// all 23. `E`, `C` and `U` each emit two events, which pins `decoded` as a
/// frame count; `E` leads, so the frame the gap case expects flagged has two.
#[test]
fn nasdaq_adapter_meets_seam_contract() {
    let decodable = std::iter::once(b'E')
        .chain((0..=u8::MAX).filter(|&kind| kind != b'E'))
        .filter_map(|kind| {
            BYTE_LEN[usize::from(kind)].map(|len| blank(kind, usize::from(len.get())))
        })
        .collect();
    let frames = ContractFrames {
        decodable,
        unknown: vec![blank(b'z', 12)],
    };
    let cfg = Config {
        venue: VenueId(1),
        session_midnight_ns: 0,
    };
    assert_adapter_contract::<NasdaqAdapter>(&cfg, &frames);
}
