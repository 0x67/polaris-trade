//! Shared oracle macros for ITCH and OUCH fuzz targets.
//!
//! Each target declares `mod owned` (path include of its venue crate's
//! `tests/support/owned_*.rs`) and a root `fn decode_owned(row)` that runs
//! the owned decode for the direction it fuzzes, so macros here resolve
//! `crate::owned` and `crate::decode_owned` per target. Every target uses
//! both macros.

/// Decode row through owned baseline, unwrap expected variant.
/// Mismatch means dispatch routed byte to wrong kind or baseline rejects
/// input generated views accept.
macro_rules! owned_kind {
    ($variant:ident, $row:expr) => {
        match crate::decode_owned($row) {
            Some(crate::owned::OwnedMessage::$variant(m)) => m,
            other => panic!(
                "owned baseline disagrees with dispatch: expected {}, got {other:?}",
                stringify!($variant)
            ),
        }
    };
}

/// Wrong-length row: parse must return structured `WrongLength` carrying
/// accurate sizes. `Ok` here means view `SIZE` drifted from `BYTE_LEN`.
macro_rules! assert_rejects {
    ($view:ident, $data:expr) => {
        match $view::parse($data) {
            Err(e) => {
                assert_eq!(e.expected, $view::SIZE, "WrongLength::expected");
                assert_eq!(e.got, $data.len(), "WrongLength::got");
            }
            Ok(_) => panic!(concat!(stringify!($view), " accepted wrong-length row")),
        }
    };
}

pub(crate) use assert_rejects;
pub(crate) use owned_kind;
