//! [`Frame`]: sequenced message `SoupBinClient` hands back, borrowed from its
//! decode buffer.

/// One sequenced data message.
#[derive(Debug)]
pub struct Frame<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) sequence: u64,
}

impl Frame<'_> {
    /// Sequence number server assigned.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl AsRef<[u8]> for Frame<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.payload
    }
}
