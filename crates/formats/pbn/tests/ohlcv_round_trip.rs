use md_events::{Ohlcv, RecordFlags, RecordHeader, rtype};
use pbn::{StreamHeader, check_compat, frames, header, write_record};

fn ohlcv(idx: u64, resolution: u64) -> Ohlcv {
    let bucket_start = resolution * idx;
    let hdr = RecordHeader {
        length: (core::mem::size_of::<Ohlcv>() / 4) as u8,
        rtype: rtype::OHLCV,
        venue: 1,
        instrument_id: 5,
        ts_event: bucket_start,
        ts_recv: bucket_start + resolution,
        ts_egress: 0,
        venue_seq: idx,
        egress_seq: 0,
        flags: RecordFlags::NONE,
        _pad: [0u8; 6],
    };
    Ohlcv {
        hdr,
        open: 1_000 + idx as i64,
        high: 1_050 + idx as i64,
        low: 990 + idx as i64,
        close: 1_010 + idx as i64,
        volume: 500 + idx,
        value: (1_010 + idx as i64) * (500 + idx as i64),
        resolution,
        trades: 7,
        _pad: [0u8; 4],
    }
}

// A 120-byte Ohlcv (rtype 0x16) with distinct resolutions round-trips through a
// pbn stream: each frame the self-delimiting walk yields is exactly size_of::
// <Ohlcv> bytes, validates, and decodes back to the identical record image,
// resolution included. Guards the record's grown size against the frame walk.
#[test]
fn ohlcv_round_trips_through_pbn_stream() {
    let bars: Vec<Ohlcv> = [1_000_000_000u64, 60_000_000_000, 3_600_000_000_000]
        .into_iter()
        .enumerate()
        .map(|(i, res)| ohlcv(i as u64, res))
        .collect();

    let sh = StreamHeader {
        proto_version: header::PROTO_VERSION,
        body_kind: header::KIND_NORMALIZED,
        schema_major: 1,
        schema_minor: 0,
        venue: 1,
        flags: 0,
        dataset: 0,
        start_ts: 0,
        end_ts: 0,
    };
    let mut stream = sh.to_bytes().to_vec();
    let body_start = stream.len();
    for b in &bars {
        write_record(&mut stream, b).unwrap();
    }

    let parsed = StreamHeader::from_bytes(&stream).unwrap();
    check_compat(1, &parsed).unwrap();
    let decoded: Vec<Ohlcv> = frames(&stream[body_start..])
        .map(|f| {
            let f = f.unwrap();
            assert_eq!(
                f.bytes.len(),
                core::mem::size_of::<Ohlcv>(),
                "frame spans the full 120-byte record"
            );
            f.decode_checked::<Ohlcv>().unwrap().unwrap()
        })
        .collect();

    assert_eq!(decoded.len(), bars.len());
    for (d, src) in decoded.iter().zip(&bars) {
        assert_eq!(
            d.resolution, src.resolution,
            "resolution survives the round-trip"
        );
        assert_eq!(d.hdr.ts_event, src.hdr.ts_event);
        assert_eq!(
            (d.open, d.high, d.low, d.close),
            (src.open, src.high, src.low, src.close)
        );
        assert_eq!(d.volume, src.volume);
        assert_eq!(d.value, src.value);
        assert_eq!(d.trades, src.trades);
    }
}
