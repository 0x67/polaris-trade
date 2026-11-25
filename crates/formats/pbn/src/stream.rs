//! Record framing: the single self-delimiting rule shared by wire, file, and
//! replay. Each record is `RecordHeader.length` (32-bit words at byte 0) long;
//! the walk validates every boundary, so a hostile or truncated buffer errors
//! instead of reading out of bounds. Each record keeps its verbatim `md_events`
//! byte image; pbn adds the bounds-checked walk and the writer frame-ceiling
//! guard.

use md_events::{Definition, Record, RecordHeader, decode_prefix, rtype};

use crate::error::PbnError;

/// Max record body. `RecordHeader.length` is one u8 counting 32-bit words, so
/// 255 * 4 = 1020 bytes. A writer must reject anything larger; letting `length`
/// wrap to a smaller value would desync every downstream reader silently.
pub const MAX_FRAME_LEN: usize = 255 * 4;

const REC_HEADER_LEN: usize = core::mem::size_of::<RecordHeader>();

/// Append one record's verbatim byte image to `out`, self-delimited by its own
/// header length. Rejects a record past the 1020-byte frame ceiling.
pub fn write_record<T: Record>(out: &mut Vec<u8>, rec: &T) -> Result<(), PbnError> {
    let size = core::mem::size_of::<T>();
    if size > MAX_FRAME_LEN {
        return Err(PbnError::FrameTooLong {
            size,
            max: MAX_FRAME_LEN,
        });
    }
    // SAFETY: T: Record is Copy + repr(C) (md-events contract); viewing it as its
    // own byte pattern for size_of::<T>() bytes is the inverse of decode_prefix.
    let bytes = unsafe { core::slice::from_raw_parts(rec as *const T as *const u8, size) };
    out.extend_from_slice(bytes);
    Ok(())
}

/// One validated record frame: its rtype and the exact on-wire bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    /// Record type byte, from header offset 1.
    pub rtype: u8,
    /// The record's exact on-wire bytes, `length` words long.
    pub bytes: &'a [u8],
}

impl Frame<'_> {
    /// Decode this frame into a concrete record if its rtype matches `T`. Returns
    /// `None` on rtype mismatch or a length shorter than `T`, other than the
    /// short `Definition` that `decode_prefix` reads. Does not validate enum
    /// discriminants; use `decode_checked` for untrusted bytes.
    pub fn decode<T: Record>(&self) -> Option<T> {
        if self.rtype != T::RTYPE {
            return None;
        }
        decode_prefix::<T>(self.bytes).map(|(rec, _)| rec)
    }

    /// Decode into `T` with full validation for untrusted bytes: `Ok(None)` on
    /// rtype mismatch, `Err` if the frame is too short for `T` or carries an
    /// out-of-range enum discriminant, else `Ok(Some(rec))`. It accepts exactly
    /// the frames [`Frame::decode`] reads, a `Definition` written before its
    /// tail included. The length check runs first, so any other short frame
    /// fails with `RecordTooShort` naming `size_of::<T>()`. The discriminant
    /// checks run on the raw bytes before the byte-copy, so an invalid enum is
    /// never materialized (which would be undefined behavior).
    pub fn decode_checked<T: Record>(&self) -> Result<Option<T>, PbnError> {
        if self.rtype != T::RTYPE {
            return Ok(None);
        }
        let need = core::mem::size_of::<T>();
        let got = self.bytes.len();
        let too_short = || PbnError::RecordTooShort {
            rtype: self.rtype,
            need,
            got,
        };
        let pre_tail = T::RTYPE == rtype::DEFINITION && got == Definition::PRE_TAIL_LEN;
        if got < need && !pre_tail {
            return Err(too_short());
        }
        crate::validate::validate(self.rtype, self.bytes)?;
        decode_prefix::<T>(self.bytes)
            .map(|(rec, _)| Some(rec))
            .ok_or_else(too_short)
    }
}

/// Walk a record body, yielding one validated `Frame` per record. Every boundary
/// is bounds-checked: a length word smaller than the record header, or a record
/// running past the buffer, ends the walk with an error rather than an
/// out-of-bounds read. On any error the walk stops (fused).
pub fn frames(body: &[u8]) -> Frames<'_> {
    Frames {
        buf: body,
        pos: 0,
        done: false,
    }
}

/// Iterator over [`frames`].
pub struct Frames<'a> {
    buf: &'a [u8],
    pos: usize,
    done: bool,
}

impl<'a> Iterator for Frames<'a> {
    type Item = Result<Frame<'a>, PbnError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.pos >= self.buf.len() {
            return None;
        }
        let rest = &self.buf[self.pos..];
        if rest.len() < REC_HEADER_LEN {
            self.done = true;
            return Some(Err(PbnError::Truncated));
        }
        let len_words = rest[0];
        let rtype = rest[1];
        let total = len_words as usize * 4;
        if total < REC_HEADER_LEN {
            self.done = true;
            return Some(Err(PbnError::BadLength { rtype, len_words }));
        }
        if total > rest.len() {
            self.done = true;
            return Some(Err(PbnError::Truncated));
        }
        self.pos += total;
        Some(Ok(Frame {
            rtype,
            bytes: &rest[..total],
        }))
    }
}

impl core::iter::FusedIterator for Frames<'_> {}

#[cfg(test)]
mod tests {
    use md_events::{
        Action, AssetClass, Definition, DefinitionTail, RecordFlags, Side, Trade, TradingState,
        rtype,
    };

    use super::*;

    fn trade(seq: u64) -> Trade {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Trade>() / 4) as u8,
            rtype: rtype::TRADE,
            venue: 1,
            instrument_id: 5,
            ts_event: 100,
            ts_recv: 110,
            ts_egress: 0,
            venue_seq: seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        Trade {
            hdr,
            price: 1_000_000_000,
            qty: 50,
            aggressor: Side::Bid,
            trade_ref: seq,
            _pad: [0u8; 7],
        }
    }

    #[test]
    fn round_trips_boundaries() {
        let mut buf = Vec::new();
        for i in 0..3 {
            write_record(&mut buf, &trade(i)).unwrap();
        }
        let got: Vec<_> = frames(&buf).map(|f| f.unwrap()).collect();
        assert_eq!(got.len(), 3);
        for (i, f) in got.iter().enumerate() {
            assert_eq!(f.rtype, rtype::TRADE);
            let t = f.decode::<Trade>().unwrap();
            assert_eq!(t.trade_ref, i as u64);
        }
    }

    #[test]
    fn decode_wrong_type_is_none() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(0)).unwrap();
        let f = frames(&buf).next().unwrap().unwrap();
        // asking for a different record type than the frame carries
        assert!(f.decode::<md_events::Mbo>().is_none());
    }

    #[test]
    fn decode_checked_valid() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(7)).unwrap();
        let f = frames(&buf).next().unwrap().unwrap();
        let t = f.decode_checked::<Trade>().unwrap().unwrap();
        assert_eq!(t.trade_ref, 7);
        // wrong type is a clean None, not an error
        assert!(f.decode_checked::<md_events::Mbo>().unwrap().is_none());
    }

    // A corrupt enum byte must error before the record is materialized, never
    // read as an invalid Rust enum (undefined behavior).
    #[test]
    fn decode_checked_rejects_bad_discriminant() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(0)).unwrap();
        let off = core::mem::offset_of!(Trade, aggressor);
        buf[off] = 0x7F; // not a valid Side
        let f = frames(&buf).next().unwrap().unwrap();
        assert!(matches!(
            f.decode_checked::<Trade>(),
            Err(PbnError::BadDiscriminant { .. })
        ));
    }

    // A 104-byte definition written before the tail passes the checked decode,
    // plain decode agrees, and the value writes back as one 176-byte frame.
    #[test]
    fn pre_tail_definition_frame_decodes() {
        let mut def = Definition::new(5, 1, 0, 9, [b'X'; 20], *b"MAIN", AssetClass::Etf, 0, 100);
        def.tail.price_scale = 4;
        let mut buf = Vec::new();
        write_record(&mut buf, &def).unwrap();
        buf.truncate(Definition::PRE_TAIL_LEN);
        buf[0] = (Definition::PRE_TAIL_LEN / 4) as u8;

        let frame = frames(&buf).next().unwrap().unwrap();
        let checked = frame.decode_checked::<Definition>().unwrap().unwrap();
        assert_eq!(frame.decode::<Definition>(), Some(checked));
        assert_eq!(checked.tail, DefinitionTail::ABSENT);
        assert_eq!(checked.raw_symbol, [b'X'; 20]);

        let mut again = Vec::new();
        write_record(&mut again, &checked).unwrap();
        let walked: Vec<_> = frames(&again).map(|f| f.unwrap()).collect();
        assert_eq!(walked.len(), 1);
        assert_eq!(walked[0].decode_checked::<Definition>(), Ok(Some(checked)));
    }

    // A definition cut inside its tail reports the full record size, not the
    // offset of the first enum byte validation would read past the frame.
    #[test]
    fn short_definition_frame_reports_record_size() {
        let def = Definition::new(5, 1, 0, 9, [b'X'; 20], *b"MAIN", AssetClass::Etf, 0, 100);
        let mut buf = Vec::new();
        write_record(&mut buf, &def).unwrap();
        buf.truncate(120);
        buf[0] = 30;

        let frame = frames(&buf).next().unwrap().unwrap();
        assert_eq!(
            frame.decode_checked::<Definition>(),
            Err(PbnError::RecordTooShort {
                rtype: rtype::DEFINITION,
                need: core::mem::size_of::<Definition>(),
                got: 120,
            })
        );
    }

    // A trailing partial record must error, not silently look like clean EOF.
    #[test]
    fn truncated_tail_errors() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(0)).unwrap();
        write_record(&mut buf, &trade(1)).unwrap();
        buf.truncate(buf.len() - 4); // chop the last record mid-body
        let results: Vec<_> = frames(&buf).collect();
        assert!(results[0].is_ok());
        assert_eq!(results[1], Err(PbnError::Truncated));
        // fused: nothing after the error
        assert_eq!(results.len(), 2);
    }

    // A zero length word cannot advance the walk; it must error, not loop.
    #[test]
    fn zero_length_is_bad_length() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(0)).unwrap();
        buf[0] = 0;
        let first = frames(&buf).next().unwrap();
        assert_eq!(
            first,
            Err(PbnError::BadLength {
                rtype: rtype::TRADE,
                len_words: 0
            })
        );
    }

    // A length word pointing past the buffer must fail closed, never read OOB.
    #[test]
    fn overlong_length_is_truncated() {
        let mut buf = Vec::new();
        write_record(&mut buf, &trade(0)).unwrap();
        buf[0] = 255; // claims 1020 bytes; buffer is one Trade
        assert_eq!(frames(&buf).next().unwrap(), Err(PbnError::Truncated));
    }

    // Hostile fuzz: random bytes never panic and never read out of bounds.
    #[test]
    fn hostile_bytes_never_panic() {
        for seed in 0u32..2000 {
            // cheap deterministic pseudo-bytes, no rng dependency
            let n = (seed % 97) as usize;
            let buf: Vec<u8> = (0..n)
                .map(|i| (seed.wrapping_mul(2654435761).wrapping_add(i as u32) & 0xff) as u8)
                .collect();
            // touching bytes must stay in-bounds by construction
            for frame in frames(&buf).flatten() {
                assert!(frame.bytes.len() >= REC_HEADER_LEN);
            }
        }
    }

    // The enum path stays silent about invalid discriminants for now: this
    // documents that the framing walk is OOB-safe, while a hostile enum byte is
    // a separate hardening step (per-record discriminant validation).
    #[test]
    fn tradingstate_enum_reserved_note() {
        // sanity anchor: a valid state constructs; discriminant validation of
        // arbitrary bytes is not yet enforced at decode.
        assert_eq!(TradingState::Continuous as u8, 3);
        assert_eq!(Action::Add as u8, 0);
    }
}
