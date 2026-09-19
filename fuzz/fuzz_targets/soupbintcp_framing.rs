#![no_main]
//! SoupBinTCP framing fuzz: same byte stream fed contiguous vs chunked through
//! `wire::parse_packet` accumulation must yield identical frames, identical
//! consumed totals, identical terminal error. Split points derive from input
//! bytes, no RNG. Mirrors `client.rs::take_one_packet` accumulation contract.

use client_soupbintcp::wire::{PacketType, parse_packet};
use libfuzzer_sys::fuzz_target;

/// One pass outcome: decoded frames, total bytes consumed, terminal error text.
#[derive(Debug, PartialEq)]
struct Outcome {
    frames: Vec<(PacketType, Vec<u8>)>,
    consumed: usize,
    // `SoupBinError` is non-exhaustive, no `PartialEq`; rendered text compares fine
    error: Option<String>,
}

/// Parse whole stream in one buffer, front-consuming loop.
fn parse_contiguous(stream: &[u8]) -> Outcome {
    let mut out = Outcome {
        frames: Vec::new(),
        consumed: 0,
        error: None,
    };
    loop {
        match parse_packet(&stream[out.consumed..]) {
            Ok(None) => break,
            Ok(Some((frame, consumed))) => {
                out.frames.push((frame.ty, frame.payload.to_vec()));
                out.consumed += consumed;
            }
            Err(e) => {
                out.error = Some(e.to_string());
                break;
            }
        }
    }
    out
}

/// Feed stream chunk by chunk into accumulation buffer, drain full packets
/// after each chunk. Split positions from `cuts`, sorted, in-bounds.
fn parse_chunked(stream: &[u8], cuts: &[usize]) -> Outcome {
    let mut out = Outcome {
        frames: Vec::new(),
        consumed: 0,
        error: None,
    };
    let mut acc: Vec<u8> = Vec::new();
    let mut prev = 0usize;
    let mut bounds: Vec<usize> = cuts.to_vec();
    bounds.push(stream.len());
    bounds.sort_unstable();
    'feed: for cut in bounds {
        acc.extend_from_slice(&stream[prev..cut]);
        prev = cut;
        loop {
            match parse_packet(&acc) {
                Ok(None) => break,
                Ok(Some((frame, consumed))) => {
                    out.frames.push((frame.ty, frame.payload.to_vec()));
                    acc.drain(..consumed);
                    out.consumed += consumed;
                }
                Err(e) => {
                    out.error = Some(e.to_string());
                    break 'feed;
                }
            }
        }
    }
    out
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // first byte = split count (max 15), next n bytes = raw split positions
    let n_cuts = usize::from(data[0] % 16).min(data.len() - 1);
    let raw = &data[1..1 + n_cuts];
    let stream = &data[1 + n_cuts..];
    let cuts: Vec<usize> = raw
        .iter()
        .map(|&b| usize::from(b) % (stream.len() + 1))
        .collect();

    let contiguous = parse_contiguous(stream);
    let chunked = parse_chunked(stream, &cuts);
    assert_eq!(
        contiguous, chunked,
        "chunked parse diverged from contiguous at cuts {cuts:?}"
    );
});
