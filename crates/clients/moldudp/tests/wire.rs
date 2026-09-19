//! Wire codec: header parse, packet classification, message block iteration,
//! and zero-alloc guarantee on iterator hot path.

use client_moldudp::{MessageBlockIter, MoldUdpError, PacketKind, parse_header, wire};

fn header_bytes(session: &[u8; 10], sequence: u64, message_count: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(session);
    buf.extend_from_slice(&sequence.to_be_bytes());
    buf.extend_from_slice(&message_count.to_be_bytes());
    buf
}

#[test]
fn header_rejects_short() {
    let buf = vec![0u8; 19];
    let err = parse_header(&buf).unwrap_err();
    assert!(matches!(err, MoldUdpError::PacketTooShort));
}

#[test]
fn header_parses_be_seq() {
    let session = *b"SESSIONID1";
    let buf = header_bytes(&session, 0x0102_0304_0506_0708, 3);
    let header = parse_header(&buf).unwrap();
    assert_eq!(header.session, session);
    assert_eq!(header.sequence, 0x0102_0304_0506_0708);
    assert_eq!(header.message_count, 3);
}

#[test]
fn heartbeat_and_eos_classified() {
    let session = *b"SESSIONID1";
    let heartbeat = header_bytes(&session, 42, 0);
    assert_eq!(
        parse_header(&heartbeat).unwrap().kind(),
        PacketKind::Heartbeat
    );

    let eos = header_bytes(&session, 42, 0xFFFF);
    assert_eq!(parse_header(&eos).unwrap().kind(), PacketKind::EndOfSession);

    let data = header_bytes(&session, 42, 5);
    assert_eq!(parse_header(&data).unwrap().kind(), PacketKind::Data);
}

#[test]
fn block_iter_borrows_slice() {
    // two blocks: b"hi" and b"bye"
    let mut body = Vec::new();
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(b"hi");
    body.extend_from_slice(&3u16.to_be_bytes());
    body.extend_from_slice(b"bye");

    let info = allocation_counter::measure(|| {
        let mut iter = MessageBlockIter::new(&body, 2);
        let (offset1, first) = iter.next().unwrap().unwrap();
        assert_eq!(first, b"hi");
        assert_eq!(offset1, wire::HEADER_LEN + 2); // header + this block's prefix
        assert_eq!(first.as_ptr(), body[2..].as_ptr()); // borrowed, not copied
        let (offset2, second) = iter.next().unwrap().unwrap();
        assert_eq!(second, b"bye");
        assert_eq!(offset2, wire::HEADER_LEN + 2 + 2 + 2); // + first prefix, payload, this prefix
        assert!(iter.next().is_none());
    });
    assert_eq!(info.count_total, 0, "block iteration must not allocate");
}

#[test]
fn block_iter_stops_on_truncation() {
    // declares 10-byte block, supplies 3 bytes
    let mut body = Vec::new();
    body.extend_from_slice(&10u16.to_be_bytes());
    body.extend_from_slice(b"abc");

    let mut iter = MessageBlockIter::new(&body, 1);
    let err = iter.next().unwrap().unwrap_err();
    assert!(matches!(err, MoldUdpError::PacketTooShort));
    assert!(iter.next().is_none(), "iterator must stop after truncation");
}
