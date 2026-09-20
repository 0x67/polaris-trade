#![no_main]
//! Compressed-variant fuzz: hostile bytes through `CompressedReader::feed`.
//! Corrupt input must return structured `SoupBinError`, never panic. Every
//! feed's inflated output must respect the reader's hard cap, so a zlib bomb
//! drains in bounded steps rather than growing without bound. Each chunk is
//! fed until drained, as the session does. The cap is the capacity passed to
//! `CompressedReader::new`.

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
        let mut input = chunk;
        while !input.is_empty() || reader.capped() {
            match reader.feed(input) {
                // one feed's output never exceeds the cap, bomb included
                Ok((consumed, out)) => {
                    assert!(
                        out.len() <= CAP,
                        "inflated {} bytes, past cap {CAP}",
                        out.len()
                    );
                    input = &input[consumed..];
                }
                // structured reject (corrupt or stalled stream); later feeds pointless
                Err(_) => return,
            }
        }
    }
});
