//! Self-describing stream header. A fixed 32-byte little-endian prefix carried
//! uncompressed ahead of the (optionally zstd) record body, so a reader routes
//! and version-checks a file without decompressing it. Magic `PBN1`, distinct
//! from the internal capture container `MDC1`.
//!
//! The byte layout is the contract, not the Rust struct: the header is
//! serialized by hand to fixed offsets, so `schema_major` always sits at byte 6
//! and the version gate reads one byte without a full parse.

use crate::error::PbnError;

/// Header magic, first 4 bytes of every pbn stream.
pub const MAGIC: [u8; 4] = *b"PBN1";
/// Fixed header length in bytes.
pub const HEADER_LEN: usize = 32;
/// Wire-envelope version this build writes and understands.
pub const PROTO_VERSION: u8 = 1;

/// Body kind: normalized md-events records.
pub const KIND_NORMALIZED: u8 = 0;

/// `flags` bit: the body is zstd-compressed.
pub const FLAG_ZSTD_BODY: u16 = 1 << 0;
/// `flags` bit: the body opens with a Definition symbology preamble.
pub const FLAG_SYMBOLOGY: u16 = 1 << 1;

/// Pinned byte offsets. `SCHEMA_MAJOR` at a fixed offset lets the version gate
/// read one byte without decompressing the body.
mod off {
    pub const MAGIC: usize = 0;
    pub const PROTO_VERSION: usize = 4;
    pub const BODY_KIND: usize = 5;
    pub const SCHEMA_MAJOR: usize = 6;
    pub const SCHEMA_MINOR: usize = 7;
    pub const VENUE: usize = 8;
    pub const FLAGS: usize = 10;
    pub const DATASET: usize = 12;
    pub const START_TS: usize = 16;
    pub const END_TS: usize = 24;
}

/// Fixed-layout stream header. Serialized by hand to a 32-byte little-endian
/// image; deliberately not `#[repr(C)]`, since the byte layout, not the struct
/// memory layout, is the wire contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamHeader {
    /// Wire-envelope version.
    pub proto_version: u8,
    /// Body kind (`KIND_NORMALIZED`).
    pub body_kind: u8,
    /// Record-schema major. A mismatch is rejected fail-closed.
    pub schema_major: u8,
    /// Record-schema minor. Additive growth; absorbed by skip-unknown-tail.
    pub schema_minor: u8,
    /// Venue id, or a sentinel for a mixed-venue stream.
    pub venue: u16,
    /// Header flag bits (`FLAG_ZSTD_BODY`, `FLAG_SYMBOLOGY`).
    pub flags: u16,
    /// Dataset selector.
    pub dataset: u32,
    /// Inclusive start, ns since epoch. 0 = open / live.
    pub start_ts: u64,
    /// Exclusive end, ns since epoch. 0 = open / live.
    pub end_ts: u64,
}

impl StreamHeader {
    /// Serialize to the fixed 32-byte little-endian image.
    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[off::MAGIC..off::MAGIC + 4].copy_from_slice(&MAGIC);
        b[off::PROTO_VERSION] = self.proto_version;
        b[off::BODY_KIND] = self.body_kind;
        b[off::SCHEMA_MAJOR] = self.schema_major;
        b[off::SCHEMA_MINOR] = self.schema_minor;
        b[off::VENUE..off::VENUE + 2].copy_from_slice(&self.venue.to_le_bytes());
        b[off::FLAGS..off::FLAGS + 2].copy_from_slice(&self.flags.to_le_bytes());
        b[off::DATASET..off::DATASET + 4].copy_from_slice(&self.dataset.to_le_bytes());
        b[off::START_TS..off::START_TS + 8].copy_from_slice(&self.start_ts.to_le_bytes());
        b[off::END_TS..off::END_TS + 8].copy_from_slice(&self.end_ts.to_le_bytes());
        b
    }

    /// Parse and validate the fixed header. Fails closed on bad magic or an
    /// unknown proto_version before exposing any field.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, PbnError> {
        if buf.len() < HEADER_LEN {
            return Err(PbnError::Truncated);
        }
        let magic: [u8; 4] = buf[off::MAGIC..off::MAGIC + 4].try_into().expect("4 bytes");
        if magic != MAGIC {
            return Err(PbnError::BadMagic { found: magic });
        }
        let proto_version = buf[off::PROTO_VERSION];
        if proto_version != PROTO_VERSION {
            return Err(PbnError::UnknownProtoVersion {
                found: proto_version,
            });
        }
        Ok(Self {
            proto_version,
            body_kind: buf[off::BODY_KIND],
            schema_major: buf[off::SCHEMA_MAJOR],
            schema_minor: buf[off::SCHEMA_MINOR],
            venue: u16::from_le_bytes(buf[off::VENUE..off::VENUE + 2].try_into().expect("2 bytes")),
            flags: u16::from_le_bytes(buf[off::FLAGS..off::FLAGS + 2].try_into().expect("2 bytes")),
            dataset: u32::from_le_bytes(
                buf[off::DATASET..off::DATASET + 4]
                    .try_into()
                    .expect("4 bytes"),
            ),
            start_ts: u64::from_le_bytes(
                buf[off::START_TS..off::START_TS + 8]
                    .try_into()
                    .expect("8 bytes"),
            ),
            end_ts: u64::from_le_bytes(
                buf[off::END_TS..off::END_TS + 8]
                    .try_into()
                    .expect("8 bytes"),
            ),
        })
    }

    /// Read only the schema major from a header image, without a full parse.
    /// Backs the cheap fail-closed version gate. Still validates magic first.
    pub fn peek_schema_major(buf: &[u8]) -> Result<u8, PbnError> {
        if buf.len() < HEADER_LEN {
            return Err(PbnError::Truncated);
        }
        let magic: [u8; 4] = buf[off::MAGIC..off::MAGIC + 4].try_into().expect("4 bytes");
        if magic != MAGIC {
            return Err(PbnError::BadMagic { found: magic });
        }
        Ok(buf[off::SCHEMA_MAJOR])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StreamHeader {
        StreamHeader {
            proto_version: PROTO_VERSION,
            body_kind: KIND_NORMALIZED,
            schema_major: 1,
            schema_minor: 0,
            venue: 7,
            flags: FLAG_SYMBOLOGY,
            dataset: 42,
            start_ts: 1_779_296_400_000_000_000,
            end_ts: 1_779_300_000_000_000_000,
        }
    }

    #[test]
    fn round_trips() {
        let h = sample();
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), HEADER_LEN);
        assert_eq!(StreamHeader::from_bytes(&bytes).unwrap(), h);
    }

    // schema_major must be recoverable at a fixed offset without a full parse,
    // so the version gate is a single-byte read before any body decompression.
    #[test]
    fn schema_major_at_offset_6() {
        let bytes = sample().to_bytes();
        assert_eq!(bytes[6], 1);
        assert_eq!(StreamHeader::peek_schema_major(&bytes).unwrap(), 1);
    }

    #[test]
    fn magic_at_front() {
        let bytes = sample().to_bytes();
        assert_eq!(&bytes[0..4], b"PBN1");
    }

    #[test]
    fn bad_magic_fails_closed() {
        let mut bytes = sample().to_bytes();
        bytes[0] = b'X';
        assert!(matches!(
            StreamHeader::from_bytes(&bytes),
            Err(PbnError::BadMagic { .. })
        ));
        assert!(matches!(
            StreamHeader::peek_schema_major(&bytes),
            Err(PbnError::BadMagic { .. })
        ));
    }

    #[test]
    fn unknown_proto_fails_closed() {
        let mut bytes = sample().to_bytes();
        bytes[4] = 99;
        assert!(matches!(
            StreamHeader::from_bytes(&bytes),
            Err(PbnError::UnknownProtoVersion { found: 99 })
        ));
    }

    #[test]
    fn short_buffer_truncated() {
        let bytes = sample().to_bytes();
        assert!(matches!(
            StreamHeader::from_bytes(&bytes[..16]),
            Err(PbnError::Truncated)
        ));
    }
}
