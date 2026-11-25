//! Support for the pbn targets (`record_roundtrip`, `stream_walk`) and the
//! seed generator in `tests/gen_seeds.rs`: record builders drawing field values
//! from [`arbitrary::Unstructured`], plus the round-trip and decode-agreement
//! oracles.

#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]

use arbitrary::{Result, Unstructured};
use pbn::md_events::{Action, Mbo, Record, RecordFlags, RecordHeader, Side, Trade};

/// Header for `T` with correct `length`/`rtype`; remaining fields drawn.
/// Correct framing fields are the round-trip precondition: the walk trusts
/// `length`, so a wrong word count is a framing bug, not a codec bug.
fn header<T: Record>(u: &mut Unstructured) -> Result<RecordHeader> {
    Ok(RecordHeader {
        length: (core::mem::size_of::<T>() / 4) as u8,
        rtype: T::RTYPE,
        venue: u.arbitrary()?,
        instrument_id: u.arbitrary()?,
        ts_event: u.arbitrary()?,
        ts_recv: u.arbitrary()?,
        ts_egress: u.arbitrary()?,
        venue_seq: u.arbitrary()?,
        egress_seq: u.arbitrary()?,
        flags: RecordFlags(u.arbitrary()?),
        _pad: [0u8; 6],
    })
}

fn side(u: &mut Unstructured) -> Result<Side> {
    Ok(*u.choose(&[Side::Bid, Side::Ask, Side::None])?)
}

fn action(u: &mut Unstructured) -> Result<Action> {
    Ok(*u.choose(&[
        Action::Add,
        Action::Cancel,
        Action::Modify,
        Action::Fill,
        Action::FillWithPrice,
        Action::Trade,
        Action::Reset,
    ])?)
}

/// `Trade` with drawn fields and valid discriminants.
pub fn trade(u: &mut Unstructured) -> Result<Trade> {
    Ok(Trade {
        hdr: header::<Trade>(u)?,
        price: u.arbitrary()?,
        qty: u.arbitrary()?,
        trade_ref: u.arbitrary()?,
        aggressor: side(u)?,
        _pad: [0u8; 7],
    })
}

/// `Mbo` with drawn fields and valid discriminants.
pub fn mbo(u: &mut Unstructured) -> Result<Mbo> {
    Ok(Mbo {
        hdr: header::<Mbo>(u)?,
        order_id: u.arbitrary()?,
        price: u.arbitrary()?,
        qty: u.arbitrary()?,
        action: action(u)?,
        side: side(u)?,
        participant: u.arbitrary()?,
        _pad: [0u8; 7],
    })
}

/// Round-trip oracle: `write_record`, walk one frame, `decode_checked` back,
/// assert byte and field equality against the source record.
pub fn assert_roundtrip<T: Record + PartialEq + core::fmt::Debug>(rec: &T) {
    let mut buf = Vec::new();
    pbn::write_record(&mut buf, rec).expect("record under frame ceiling");
    assert_eq!(buf.len(), core::mem::size_of::<T>(), "verbatim byte image");
    let frame = pbn::frames(&buf)
        .next()
        .expect("one frame written")
        .expect("well-formed frame");
    assert_eq!(
        frame.bytes,
        buf.as_slice(),
        "frame bytes match written image"
    );
    let got: T = frame
        .decode_checked()
        .expect("valid record validates")
        .expect("rtype matches");
    assert_eq!(&got, rec, "decoded fields match source");
}

/// Agreement oracle: plain `decode` runs ONLY on frames `decode_checked`
/// accepted, then both must agree. `decode` skips discriminant validation
/// (see pbn::stream), so calling it on unvalidated bytes is UB in the harness.
pub fn assert_decode_agreement<T: Record + PartialEq + core::fmt::Debug>(frame: &pbn::Frame) {
    if let Ok(Some(checked)) = frame.decode_checked::<T>() {
        let plain: T = frame.decode().expect("accepted frame decodes plain");
        assert_eq!(plain, checked, "decode disagrees with decode_checked");
    }
}
