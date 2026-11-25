//! Fixed-width alpha field trimming utilities.

/// trims trailing ascii-space padding from a fixed-width alpha field.
///
/// Returns a borrowed sub-slice; no copy. Trailing spaces are removed,
/// but interior spaces are preserved.
pub fn trim_alpha(bytes: &[u8]) -> &[u8] {
    let end = bytes.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    &bytes[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_all_spaces() {
        assert_eq!(trim_alpha(b"   "), b"");
    }

    #[test]
    fn no_padding_unchanged() {
        assert_eq!(trim_alpha(b"ABC"), b"ABC");
    }

    #[test]
    fn empty_input() {
        assert_eq!(trim_alpha(b""), b"");
    }

    #[test]
    fn single_trailing_space_trimmed() {
        assert_eq!(trim_alpha(b"AB "), b"AB");
    }

    #[test]
    fn interior_spaces_preserved() {
        assert_eq!(trim_alpha(b"AB CD  "), b"AB CD");
    }
}
