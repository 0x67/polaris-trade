#![no_main]
//! MoldUDP64 datagram fuzz: `parse_header` plus block walk on arbitrary bytes.
//! Reject path must return structured [`client_moldudp::MoldUdpError`], never
//! panic. Accept path: walk terminates, every block stays inside datagram.

use client_moldudp::wire::{self, HEADER_LEN};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(header) = wire::parse_header(data) else {
        // structured reject; panic would abort fuzzer here
        return;
    };
    let mut yielded: u32 = 0;
    for item in header.blocks(data) {
        match item {
            Ok((offset, block)) => {
                // offset points past header + 2-byte length prefix
                assert!(offset >= HEADER_LEN + 2, "block offset inside header");
                assert!(
                    offset + block.len() <= data.len(),
                    "block overruns datagram: offset {offset} len {} datagram {}",
                    block.len(),
                    data.len()
                );
                yielded += 1;
            }
            // iterator fuses after truncation error; walk ends
            Err(_) => break,
        }
    }
    assert!(
        yielded <= u32::from(header.message_count),
        "walk yielded {yielded} blocks, header declared {}",
        header.message_count
    );
});
