//! Coverage matrix. Parser-level coverage (decoded) and book/derive-level
//! handling are tracked as two separate axes, with a test asserting the decoded
//! set matches the adapter's dispatch arms, so "decoded" is never silently
//! conflated with "handled" (the failure mode of every surveyed reference
//! engine).

use std::collections::BTreeMap;

/// How far one wire kind is carried.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Coverage {
    /// The parser decodes this wire kind.
    pub decoded: bool,
    /// The engine applies it to the book.
    pub book_applied: bool,
    /// It feeds a derived view.
    pub derived: bool,
}

impl Coverage {
    /// Decoded with no book or derived-view impact.
    pub const REFERENCE: Self = Self {
        decoded: true,
        book_applied: false,
        derived: false,
    };
    /// Decoded, applied to the book, and fed to derived views.
    pub const BOOK_AND_DERIVED: Self = Self {
        decoded: true,
        book_applied: true,
        derived: true,
    };
    /// Decoded and fed to derived views without touching the book.
    pub const TAPE_ONLY: Self = Self {
        decoded: true,
        book_applied: false,
        derived: true,
    };

    /// Applied to the book or fed to a derived view.
    pub fn handled(self) -> bool {
        self.book_applied || self.derived
    }
}

/// Per-adapter coverage table keyed by wire kind.
#[derive(Clone, Debug, Default)]
pub struct CoverageMatrix {
    rows: BTreeMap<u8, Coverage>,
}

impl CoverageMatrix {
    /// Empty matrix.
    pub fn new() -> Self {
        CoverageMatrix {
            rows: BTreeMap::new(),
        }
    }

    /// Record coverage for a wire kind.
    pub fn set(&mut self, wire_kind: u8, cov: Coverage) -> &mut Self {
        self.rows.insert(wire_kind, cov);
        self
    }

    /// Coverage for a wire kind.
    pub fn get(&self, wire_kind: u8) -> Option<Coverage> {
        self.rows.get(&wire_kind).copied()
    }

    /// Wire kinds the parser decodes.
    pub fn decoded_kinds(&self) -> Vec<u8> {
        self.rows
            .iter()
            .filter(|(_, c)| c.decoded)
            .map(|(&k, _)| k)
            .collect()
    }

    /// Wire kinds carried to the book or a derived view.
    pub fn handled_kinds(&self) -> Vec<u8> {
        self.rows
            .iter()
            .filter(|(_, c)| c.handled())
            .map(|(&k, _)| k)
            .collect()
    }

    /// Assert the decoded set equals the adapter's actual dispatch arms. Any
    /// divergence means the matrix has rotted against the code.
    pub fn matches_dispatch(&self, dispatched: &[u8]) -> Result<(), String> {
        let mut want = dispatched.to_vec();
        want.sort_unstable();
        want.dedup();
        let have = self.decoded_kinds();
        if have == want {
            Ok(())
        } else {
            Err(format!(
                "coverage decoded {have:?} != dispatch arms {want:?}"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Decoded and handled are separate axes: a kind can be decoded yet
    // unhandled, and the matrix must equal the adapter's dispatch arms.
    #[test]
    fn matrix_matches_dispatch() {
        let mut m = CoverageMatrix::new();
        m.set(
            1,
            Coverage {
                decoded: true,
                book_applied: true,
                derived: false,
            },
        )
        .set(
            2,
            Coverage {
                decoded: true,
                book_applied: false,
                derived: false,
            },
        )
        .set(
            3,
            Coverage {
                decoded: true,
                book_applied: false,
                derived: true,
            },
        );

        assert_eq!(m.decoded_kinds(), vec![1, 2, 3]);
        assert_eq!(
            m.handled_kinds(),
            vec![1, 3],
            "kind 2 decoded but not handled"
        );

        assert!(m.matches_dispatch(&[3, 2, 1]).is_ok());
        assert!(m.matches_dispatch(&[1, 2]).is_err(), "missing arm 3 is rot");
    }
}
