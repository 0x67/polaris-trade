//! `compressed` feature: inflate cap. Inflate itself and plain upstream run
//! through protocol table under this feature.
#![cfg(feature = "compressed")]

use std::io::Write;

use client_soupbintcp::CompressedReader;
use flate2::{Compression, write::ZlibEncoder};

// zlib bomb: few compressed bytes inflate far past reader's cap; each feed
// stops at cap with input left over, and repeat feeds allocate nothing
#[test]
fn inflate_bomb_bounded_at_cap() {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&vec![0u8; 1 << 20]).expect("compress");
    let bomb = encoder.finish().expect("finish");
    assert!(bomb.len() < 4096, "highly compressible input stays small");

    let mut reader = CompressedReader::new(256);
    let (consumed, out) = reader.feed(&bomb).expect("feed");
    assert_eq!(out.len(), 256, "one feed stops at cap");
    assert!(consumed < bomb.len(), "input left for later feeds");

    let mut rest = &bomb[consumed..];
    let info = allocation_counter::measure(|| {
        for _ in 0..64 {
            let (consumed, out) = reader.feed(rest).expect("feed");
            assert_eq!(out.len(), 256, "every feed stops at cap");
            rest = &rest[consumed..];
        }
    });
    assert_eq!(info.count_total, 0, "bomb must not grow memory");
}
