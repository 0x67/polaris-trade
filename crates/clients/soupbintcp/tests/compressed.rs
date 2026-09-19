//! `compressed` feature: inflate cap. Inflate itself and plain upstream run
//! through protocol table under this feature.
#![cfg(feature = "compressed")]

use std::io::Write;

use client_soupbintcp::{CompressedReader, SoupBinError};
use flate2::{Compression, write::ZlibEncoder};

// zlib bomb: few compressed bytes inflate past reader's ceiling; feed must
// reject, not allocate full expansion
#[test]
fn inflate_bomb_rejected_at_cap() {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&vec![0u8; 1 << 20]).expect("compress");
    let bomb = encoder.finish().expect("finish");
    assert!(bomb.len() < 4096, "highly compressible input stays small");

    let mut reader = CompressedReader::new(256);
    match reader.feed(&bomb) {
        Err(SoupBinError::FrameTooLarge { max, .. }) => assert_eq!(max, 256),
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }
}
