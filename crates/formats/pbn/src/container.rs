//! Full stream container: the fixed header plus an optionally zstd-compressed
//! record body. `read_stream` version-gates on the header before touching the
//! body; `write_stream` compresses per the header's `FLAG_ZSTD_BODY` bit. The
//! returned body is walked with `stream::frames`.

use crate::{
    error::PbnError,
    header::{FLAG_ZSTD_BODY, HEADER_LEN, StreamHeader},
    version::check_compat,
};

/// zstd compression level for bodies written with `FLAG_ZSTD_BODY`.
const ZSTD_LEVEL: i32 = 3;

/// Worst-case decompressed size a body may reach, as a multiple of its
/// compressed length. A zstd bomb (tiny input, vast output) aborts at this
/// bound before exhausting memory; real capture bodies compress far under it.
const MAX_DECOMPRESS_RATIO: u64 = 100;

/// Absolute floor on the decompress cap so a small but legitimately repetitive
/// body is not rejected by the ratio alone.
const MIN_DECOMPRESS_CAP: u64 = 4 * 1024 * 1024;

/// Compress a record body with zstd.
pub fn compress(body: &[u8]) -> Result<Vec<u8>, PbnError> {
    zstd::encode_all(body, ZSTD_LEVEL).map_err(|_| PbnError::CorruptBody)
}

/// Decompress a zstd record body, bounded by a ratio cap so untrusted input
/// cannot drive unbounded allocation. Streams the frame and aborts the moment
/// output passes the cap, never trusting a declared frame content-size.
pub fn decompress(compressed: &[u8]) -> Result<Vec<u8>, PbnError> {
    let cap = (compressed.len() as u64)
        .saturating_mul(MAX_DECOMPRESS_RATIO)
        .max(MIN_DECOMPRESS_CAP);
    let mut decoder =
        zstd::stream::read::Decoder::new(compressed).map_err(|_| PbnError::CorruptBody)?;
    let mut out = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = std::io::Read::read(&mut decoder, &mut buf).map_err(|_| PbnError::CorruptBody)?;
        if n == 0 {
            break;
        }
        if out.len() as u64 + n as u64 > cap {
            return Err(PbnError::BodyTooLarge { max: cap });
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

/// Assemble a full stream: the 32-byte header image followed by the body,
/// zstd-compressed when the header sets `FLAG_ZSTD_BODY`.
pub fn write_stream(header: &StreamHeader, body: &[u8]) -> Result<Vec<u8>, PbnError> {
    let mut out = header.to_bytes().to_vec();
    if header.flags & FLAG_ZSTD_BODY != 0 {
        out.extend_from_slice(&compress(body)?);
    } else {
        out.extend_from_slice(body);
    }
    Ok(out)
}

/// Parse and version-gate a full stream, returning the header and the
/// decompressed record body (ready for `stream::frames`). Fails closed on bad
/// magic, unknown proto, or a schema-major mismatch before decompressing.
pub fn read_stream(buf: &[u8], reader_major: u8) -> Result<(StreamHeader, Vec<u8>), PbnError> {
    let header = StreamHeader::from_bytes(buf)?;
    check_compat(reader_major, &header)?;
    let raw = &buf[HEADER_LEN..];
    let body = if header.flags & FLAG_ZSTD_BODY != 0 {
        decompress(raw)?
    } else {
        raw.to_vec()
    };
    Ok((header, body))
}

#[cfg(test)]
mod tests {
    use md_events::{RecordFlags, RecordHeader, Side, Trade, rtype};

    use super::*;
    use crate::{
        frames,
        header::{FLAG_SYMBOLOGY, KIND_NORMALIZED, PROTO_VERSION},
        stream::write_record,
    };

    fn header(flags: u16) -> StreamHeader {
        StreamHeader {
            proto_version: PROTO_VERSION,
            body_kind: KIND_NORMALIZED,
            schema_major: 1,
            schema_minor: 0,
            venue: 1,
            flags,
            dataset: 0,
            start_ts: 0,
            end_ts: 0,
        }
    }

    fn body(n: u64) -> Vec<u8> {
        let mut b = Vec::new();
        for i in 0..n {
            let hdr = RecordHeader {
                length: (core::mem::size_of::<Trade>() / 4) as u8,
                rtype: rtype::TRADE,
                venue: 1,
                instrument_id: 5,
                ts_event: i,
                ts_recv: i,
                ts_egress: 0,
                venue_seq: i,
                egress_seq: 0,
                flags: RecordFlags::NONE,
                _pad: [0u8; 6],
            };
            let t = Trade {
                hdr,
                price: 1_000_000_000,
                qty: 50,
                trade_ref: i,
                aggressor: Side::Bid,
                _pad: [0u8; 7],
            };
            write_record(&mut b, &t).unwrap();
        }
        b
    }

    #[test]
    fn uncompressed_round_trip() {
        let b = body(4);
        let stream = write_stream(&header(0), &b).unwrap();
        let (h, out) = read_stream(&stream, 1).unwrap();
        assert_eq!(h.schema_major, 1);
        assert_eq!(out, b);
        assert_eq!(frames(&out).count(), 4);
    }

    #[test]
    // zstd C FFI: Miri cannot call foreign functions
    #[cfg_attr(miri, ignore)]
    fn compressed_round_trip() {
        let b = body(64); // repetitive: compresses well below raw
        let stream = write_stream(&header(FLAG_ZSTD_BODY | FLAG_SYMBOLOGY), &b).unwrap();
        assert!(stream.len() < HEADER_LEN + b.len());
        let (h, out) = read_stream(&stream, 1).unwrap();
        assert_eq!(h.flags & FLAG_ZSTD_BODY, FLAG_ZSTD_BODY);
        assert_eq!(out, b);
        assert_eq!(frames(&out).count(), 64);
    }

    #[test]
    fn version_gate_before_body() {
        let stream = write_stream(&header(0), &body(1)).unwrap();
        assert_eq!(
            read_stream(&stream, 2),
            Err(PbnError::VersionMismatch {
                reader: 2,
                stream: 1
            })
        );
    }

    #[test]
    // zstd C FFI: Miri cannot call foreign functions
    #[cfg_attr(miri, ignore)]
    fn corrupt_zstd_body_errors() {
        let mut stream = write_stream(&header(FLAG_ZSTD_BODY), &body(8)).unwrap();
        // clobber the compressed payload
        for byte in stream.iter_mut().skip(HEADER_LEN) {
            *byte = 0xFF;
        }
        assert_eq!(read_stream(&stream, 1), Err(PbnError::CorruptBody));
    }

    #[test]
    // zstd C FFI: Miri cannot call foreign functions
    #[cfg_attr(miri, ignore)]
    fn decompress_bomb_rejected_at_cap() {
        // 8 MiB of zeros compresses to a few KB: past the 4 MiB floor and far
        // past compressed-len x ratio, so decompress must abort with the cap.
        let bomb = compress(&vec![0u8; 8 * 1024 * 1024]).unwrap();
        assert!(bomb.len() < MIN_DECOMPRESS_CAP as usize);
        match decompress(&bomb) {
            Err(PbnError::BodyTooLarge { max }) => assert_eq!(max, MIN_DECOMPRESS_CAP),
            other => panic!("expected BodyTooLarge, got {other:?}"),
        }
    }
}
