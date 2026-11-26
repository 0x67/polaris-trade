//! Byte view of a published record's header, shared by the test binaries that
//! hash or compare records as a sink stores them: header bytes, then body.

use md_events::RecordHeader;

/// The `repr(C)` bytes of `header`.
pub fn header_bytes(header: &RecordHeader) -> &[u8] {
    // SAFETY: RecordHeader is repr(C) with no implicit padding (explicit `_pad`,
    // 56-byte size const-asserted in md-events), so all its bytes are initialized.
    unsafe {
        core::slice::from_raw_parts(
            (header as *const RecordHeader).cast::<u8>(),
            size_of::<RecordHeader>(),
        )
    }
}
