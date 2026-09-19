#![no_main]
//! Compressed-variant fuzz: hostile bytes through `CompressedReader::feed`.
//! Corrupt input must return structured `SoupBinError`, never panic. Every
//! accepted feed's inflated output must respect the reader's hard cap: a zlib
//! bomb hits `FrameTooLarge` (a structured error) rather than growing without
//! bound. The cap is the capacity passed to `CompressedReader::new`.

use client_soupbintcp::compressed::CompressedReader;
use libfuzzer_sys::fuzz_target;

const CAP: usize = 4096;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // first byte picks split point; two feeds exercise persistent inflate state
    let rest = &data[1..];
    let cut = usize::from(data[0]) % (rest.len() + 1);
    let mut reader = CompressedReader::new(CAP);
    for chunk in [&rest[..cut], &rest[cut..]] {
        if chunk.is_empty() {
            continue;
        }
        match reader.feed(chunk) {
            // one feed's output never exceeds the cap; the bomb path is Err
            Ok(out) => assert!(
                out.len() <= CAP,
                "inflated {} bytes, past cap {CAP}",
                out.len()
            ),
            // structured reject (corrupt stream or cap hit); later feeds pointless
            Err(_) => return,
        }
    }
});
