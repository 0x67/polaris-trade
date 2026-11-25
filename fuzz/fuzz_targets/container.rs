//! Arbitrary container bytes through `read_stream`. Truncated, overlong, or
//! corrupt input (including hostile zstd payloads) must yield a structured
//! `PbnError`, never a panic; the body walk must terminate.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    match pbn::read_stream(data, 1) {
        Ok((_header, body)) => {
            // walk is fused and finite; every accepted frame respects the cap
            for frame in pbn::frames(&body) {
                let Ok(frame) = frame else { break };
                assert!(frame.bytes.len() <= pbn::MAX_FRAME_LEN);
                let _ = pbn::decode_any(&frame);
            }
        }
        // structured error is the pass; PbnError is the whole failure surface
        Err(_e) => {}
    }
});
