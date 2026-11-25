//! Typed error for open, index, and query failures.

use pbn::PbnError;

/// Every failure the reader, index, and query engine surface to the caller.
///
/// A format fault (bad magic, truncation, bad discriminant, schema-major
/// mismatch) arrives already typed from `pbn` and is carried through `Pbn`, so
/// a version mismatch fails closed at open. The remaining variants are the
/// engine's own not-found and stale-cache conditions.
#[derive(thiserror::Error, Debug)]
pub enum MdexError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    // see pbn::error::PbnError: magic, truncation, discriminant, version
    #[error("format: {0}")]
    Pbn(#[from] PbnError),
    #[error("stale or unreadable index cache")]
    StaleCache,
    #[error("symbol not found: {0}")]
    SymbolNotFound(String),
    #[error("sequence not found: {0}")]
    SequenceNotFound(u64),
    #[error("instrument not found: {0}")]
    InstrumentNotFound(u32),
    #[error("no state before {0}")]
    NoStateBefore(u64),
}

#[cfg(test)]
mod tests {
    use pbn::PbnError;

    use super::*;

    #[test]
    fn version_mismatch_maps_to_pbn() {
        let err: MdexError = PbnError::VersionMismatch {
            reader: 1,
            stream: 2,
        }
        .into();
        assert!(matches!(
            err,
            MdexError::Pbn(PbnError::VersionMismatch {
                reader: 1,
                stream: 2
            })
        ));
    }
}
