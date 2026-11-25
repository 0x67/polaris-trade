//! Generic TLV appendage walker, shared by any venue with optional trailing fields.

/// Borrowed iterator over an appendage tail: elements of len(1) + tag(1) + value(len-1).
///
/// Venue-neutral wire shape: each element starts with a length byte `L` counting
/// the tag byte plus the value bytes that follow it. Stops permanently (`None`)
/// on `L == 0` or a truncated element (fewer than `L` bytes left after the length
/// byte). Never panics, never allocates.
pub struct AppendageIter<'a> {
    rest: &'a [u8],
}

impl<'a> AppendageIter<'a> {
    /// wraps the appendage tail bytes for walking.
    pub fn new(tail: &'a [u8]) -> Self {
        Self { rest: tail }
    }
}

impl<'a> Iterator for AppendageIter<'a> {
    type Item = (u8, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let &len = self.rest.first()?;
        if len == 0 {
            self.rest = &[];
            return None;
        }
        // element body = tag(1) + value(len-1), all after the length byte
        let elem_len = len as usize;
        let after_len = &self.rest[1..];
        if after_len.len() < elem_len {
            self.rest = &[];
            return None;
        }
        let tag = after_len[0];
        let value = &after_len[1..elem_len];
        self.rest = &after_len[elem_len..];
        Some((tag, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tail_yields_none() {
        let mut it = AppendageIter::new(b"");
        assert_eq!(it.next(), None);
    }

    #[test]
    fn single_element() {
        let tail = [3u8, 0x10, 0xAA, 0xBB];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), Some((0x10, &tail[2..4])));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn multiple_elements() {
        // elem1: len=2, tag=0x01, value=[0xFF]; elem2: len=3, tag=0x02, value=[0x01,0x02]
        let tail = [2u8, 0x01, 0xFF, 3u8, 0x02, 0x01, 0x02];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), Some((0x01, &tail[2..3])));
        assert_eq!(it.next(), Some((0x02, &tail[5..7])));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn truncated_header_only_length_byte() {
        let tail = [5u8];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn truncated_value_shorter_than_declared() {
        // len=5 claims tag+4 value bytes, only tag+1 byte present
        let tail = [5u8, 0x01, 0xAA];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn zero_length_stops() {
        let tail = [0u8, 0x01, 0x02];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn length_one_yields_tag_with_empty_value() {
        let tail = [1u8, 0x07];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), Some((0x07, &tail[2..2])));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn garbage_after_valid_elements_stops_cleanly() {
        // one valid element (len=2, tag=0x01, value=[0x99]) then a truncated header
        let tail = [2u8, 0x01, 0x99, 9u8, 0x02];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), Some((0x01, &tail[2..3])));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn stays_none_after_first_stop() {
        let tail = [0u8];
        let mut it = AppendageIter::new(&tail);
        assert_eq!(it.next(), None);
        assert_eq!(it.next(), None);
        assert_eq!(it.next(), None);
    }
}
