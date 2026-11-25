//! Format read/write errors. Fail-closed: a malformed prefix or a version
//! mismatch errors before any record is materialized, never a silent misread.

/// A pbn stream read or write failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PbnError {
    /// First 4 header bytes are not `PBN1`.
    #[error("bad pbn magic: {found:?}")]
    BadMagic { found: [u8; 4] },
    /// Header proto_version the reader does not understand.
    #[error("unknown pbn proto version: {found}")]
    UnknownProtoVersion { found: u8 },
    /// Stream schema major differs from the reader's own.
    #[error("schema major mismatch: reader {reader}, stream {stream}")]
    VersionMismatch { reader: u8, stream: u8 },
    /// Buffer ended mid-header or mid-record.
    #[error("truncated pbn stream")]
    Truncated,
    /// Record length word is zero or smaller than the record header.
    #[error("bad record length: rtype {rtype:#04x}, {len_words} words")]
    BadLength { rtype: u8, len_words: u8 },
    /// Record body exceeds the 1020-byte frame ceiling (u8 length in words).
    #[error("frame too long: {size} bytes exceeds {max}")]
    FrameTooLong { size: usize, max: usize },
    /// Record claims an rtype but its bytes are shorter than that record type.
    #[error("record too short for rtype {rtype:#04x}: need {need}, got {got}")]
    RecordTooShort { rtype: u8, need: usize, got: usize },
    /// An enum-typed field holds an out-of-range discriminant. Caught before a
    /// typed decode so reading the invalid enum is never reached.
    #[error("bad discriminant in rtype {rtype:#04x} at offset {offset}")]
    BadDiscriminant { rtype: u8, offset: usize },
    /// A zstd body failed to compress or decompress.
    #[error("corrupt or undecodable zstd body")]
    CorruptBody,
    /// Decompressed body exceeded the ratio cap: a zstd bomb (small input,
    /// huge output) is rejected before it can exhaust memory.
    #[error("decompressed body exceeds cap of {max} bytes")]
    BodyTooLarge { max: u64 },
}
