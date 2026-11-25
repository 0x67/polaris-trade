//! Wire parser: fragmentation, unknown types, zero-length guard.

pub mod common;

use client_soupbintcp::{PacketType, SoupBinError, parse_packet};
use common::packet;

#[test]
fn partial_packet_held() {
    let full = packet(b'S', b"hello");

    // under 3 bytes: no full length + type prefix yet
    assert!(parse_packet(&full[..2]).unwrap().is_none());

    // length + type known, body short one byte
    assert!(parse_packet(&full[..full.len() - 1]).unwrap().is_none());

    // full packet parses once all bytes present
    let (frame, consumed) = parse_packet(&full).unwrap().unwrap();
    assert_eq!(frame.ty, PacketType::SequencedData);
    assert_eq!(frame.payload, b"hello");
    assert_eq!(consumed, full.len());
}

#[test]
fn unknown_type_rejected() {
    let buf = packet(b'?', b"x");
    match parse_packet(&buf).unwrap_err() {
        SoupBinError::UnknownPacketType(b) => assert_eq!(b, b'?'),
        other => panic!("expected UnknownPacketType, got {other:?}"),
    }
}

#[test]
fn zero_length_rejected() {
    // 3 bytes (minimum to read length), length claims 0: rejected, not held as partial
    let buf = vec![0u8, 0u8, 0u8];
    match parse_packet(&buf).unwrap_err() {
        SoupBinError::ProtocolViolation(_) => {}
        other => panic!("expected ProtocolViolation, got {other:?}"),
    }
}
