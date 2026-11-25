//! Schema-version compatibility gate.
//!
//! Pre-v1.0 policy: no backward-compatibility guarantee. Any breaking
//! record-layout change bumps the schema major, and a reader rejects a
//! mismatched major fail-closed rather than misreading bytes. Additive growth (a
//! new trailing field, a new rtype) stays a minor and is absorbed by the
//! skip-unknown-tail walk. Cross-version upgrade materialization is deferred
//! until real old-version data exists; `UpgradePolicy` reserves the seam.

use crate::{error::PbnError, header::StreamHeader};

/// How a reader treats a compatible-major stream whose minor differs. Only
/// `AsIs` exists pre-v1.0: known fields decode, unknown tails skip. No upgrade
/// materialization is built yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum UpgradePolicy {
    /// Decode known fields as-is; skip unknown tails by record length.
    #[default]
    AsIs,
}

/// Reject a stream whose schema major differs from the reader's. Runs before any
/// record is decoded, so an incompatible layout can never be misread as valid.
pub fn check_compat(reader_major: u8, hdr: &StreamHeader) -> Result<(), PbnError> {
    if hdr.schema_major != reader_major {
        return Err(PbnError::VersionMismatch {
            reader: reader_major,
            stream: hdr.schema_major,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{KIND_NORMALIZED, PROTO_VERSION};

    fn hdr(major: u8, minor: u8) -> StreamHeader {
        StreamHeader {
            proto_version: PROTO_VERSION,
            body_kind: KIND_NORMALIZED,
            schema_major: major,
            schema_minor: minor,
            venue: 1,
            flags: 0,
            dataset: 0,
            start_ts: 0,
            end_ts: 0,
        }
    }

    #[test]
    fn same_major_ok() {
        assert!(check_compat(1, &hdr(1, 0)).is_ok());
        // minor difference is compatible
        assert!(check_compat(1, &hdr(1, 5)).is_ok());
    }

    #[test]
    fn major_mismatch_rejects() {
        assert_eq!(
            check_compat(1, &hdr(2, 0)),
            Err(PbnError::VersionMismatch {
                reader: 1,
                stream: 2
            })
        );
    }

    #[test]
    fn upgrade_policy_default_as_is() {
        assert_eq!(UpgradePolicy::default(), UpgradePolicy::AsIs);
    }
}
