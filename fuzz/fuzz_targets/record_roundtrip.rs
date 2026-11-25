//! Arbitrary field values into well-formed records, then `write_record`,
//! frame walk, `decode_checked`: bytes and fields must round-trip exactly.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use polaris_fuzz::{assert_roundtrip, mbo, trade};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    if let Ok(t) = trade(&mut u) {
        assert_roundtrip(&t);
    }
    if let Ok(m) = mbo(&mut u) {
        assert_roundtrip(&m);
    }
});
