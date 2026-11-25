//! Arbitrary bytes through the frame walk and every checked decode path.
//! `decode_checked` and `decode_any` must never panic; plain `decode` runs
//! only on frames `decode_checked` accepted, then must agree with it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use pbn::md_events::{
    Bbo, Control, Definition, Fill, Imbalance, Level, Mbo, Mbp10, Ohlcv, Stats, Status, Trade,
    TradeBreak,
};
use polaris_fuzz::assert_decode_agreement;

fuzz_target!(|data: &[u8]| {
    for frame in pbn::frames(data) {
        // fused walk: first error ends it; error itself is the oracle pass
        let Ok(frame) = frame else { break };
        // u8 word count caps every accepted frame at 1020 bytes
        assert!(frame.bytes.len() <= pbn::MAX_FRAME_LEN);
        // typed dispatch across all known rtypes must never panic
        let _ = pbn::decode_any(&frame);
        assert_decode_agreement::<Control>(&frame);
        assert_decode_agreement::<Mbo>(&frame);
        assert_decode_agreement::<Level>(&frame);
        assert_decode_agreement::<Mbp10>(&frame);
        assert_decode_agreement::<Bbo>(&frame);
        assert_decode_agreement::<Trade>(&frame);
        assert_decode_agreement::<TradeBreak>(&frame);
        assert_decode_agreement::<Ohlcv>(&frame);
        assert_decode_agreement::<Imbalance>(&frame);
        assert_decode_agreement::<Fill>(&frame);
        assert_decode_agreement::<Stats>(&frame);
        assert_decode_agreement::<Status>(&frame);
        assert_decode_agreement::<Definition>(&frame);
    }
});
