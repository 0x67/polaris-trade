//! Parse error types for wire decoding.

use thiserror::Error;

/// fixed-size mismatch: slice length did not match the expected wire size.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("wrong length: expected {expected} bytes, got {got}")]
pub struct WrongLength {
    pub expected: usize,
    pub got: usize,
}

/// stream walk stopped with bytes left over after the last full message.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("trailing bytes at offset {offset}: {remaining} bytes remain")]
pub struct TrailingBytes {
    pub offset: usize,
    pub remaining: usize,
}

/// wire decode failure. Grows as new protocols add failure modes.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseError {
    #[error(transparent)]
    WrongLength(#[from] WrongLength),
    #[error("unsupported message kind: 0x{msg_type:02x}")]
    UnsupportedKind { msg_type: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_length_display() {
        let err = WrongLength {
            expected: 31,
            got: 30,
        };
        assert_eq!(err.to_string(), "wrong length: expected 31 bytes, got 30");
    }

    #[test]
    fn parse_error_from_wrong_length() {
        let wrong_len = WrongLength {
            expected: 20,
            got: 15,
        };
        let parse_err: ParseError = wrong_len.into();
        assert_eq!(parse_err, ParseError::WrongLength(wrong_len));
    }

    #[test]
    fn trailing_bytes_display() {
        let err = TrailingBytes {
            offset: 100,
            remaining: 5,
        };
        assert_eq!(
            err.to_string(),
            "trailing bytes at offset 100: 5 bytes remain"
        );
    }
}
