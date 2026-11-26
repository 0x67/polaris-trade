#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
#![warn(clippy::undocumented_unsafe_blocks)]
//! C ABI over [`datafeed::FeedSession`]. A host opens a session from a JSON
//! config, polls stamped records into an array it owns, and closes the
//! session. cbindgen generates `include/polaris_abi.h` from this file with
//! `cbindgen.toml`, and `tests/header.rs` fails when the committed header
//! differs from its output.
//!
//! Every entry point catches a Rust panic and returns [`POLARIS_ERR_PANIC`],
//! which needs a library built with `panic = "unwind"`. The workspace
//! `release` profile aborts on panic, so the C library is built with the
//! `ffi` profile: release codegen, unwinding panics.

use std::{
    ffi::{CStr, c_char},
    panic::{self, AssertUnwindSafe},
    ptr,
};

use datafeed::{FeedError, FeedSession, FeedSessionBuilder, OwnedRecord, config::DatafeedConfig};

/// Version of this ABI. It changes when an exported signature, a struct
/// layout or the meaning of a status changes.
pub const POLARIS_ABI_VERSION: u32 = 1;

/// Success.
pub const POLARIS_OK: i32 = 0;

/// `polaris_feed_poll` only, and not an error: the source has ended and every
/// record was handed out. Every later poll returns it. When a fault ends the
/// source, one poll returns the fault's negative status first, after the
/// records routed before it.
pub const POLARIS_FEED_END: i32 = 1;

/// A required pointer argument is NULL.
pub const POLARIS_ERR_NULL: i32 = -1;

/// The config is not UTF-8, does not decode as a config, or contradicts
/// itself or the header of the capture it replays.
pub const POLARIS_ERR_CONFIG: i32 = -2;

/// The replay file or the live connection did not open, or failed while open.
pub const POLARIS_ERR_SOURCE: i32 = -3;

/// The session stopped on a capture tap fault or a session anchor it could
/// not resolve.
pub const POLARIS_ERR_SESSION: i32 = -4;

/// `polaris_feed_close` could not finish the capture: close the open file or
/// write its manifest.
pub const POLARIS_ERR_TAP: i32 = -5;

/// A thread or async runtime the session needs did not start.
pub const POLARIS_ERR_RUNTIME: i32 = -6;

/// A Rust panic was caught at the entry point. A handle it happened on is
/// poisoned: every later poll returns this status, and `polaris_feed_close`
/// releases it without finishing the capture. Only a library built with
/// `panic = "unwind"` (the workspace `ffi` profile) returns it; a build with
/// `panic = "abort"` aborts the process instead.
pub const POLARIS_ERR_PANIC: i32 = -7;

/// A failure this ABI version has no status for.
pub const POLARIS_ERR_OTHER: i32 = -8;

/// One record handed out by `polaris_feed_poll`: the egress stream it was
/// published on, and its bytes as published, the 56-byte `RecordHeader` then
/// the body (layouts in the events crate's `md_events.h`).
///
/// `bytes` points into a buffer the handle owns and reuses. It stays valid
/// until the next call on the same handle, so copy what you keep first.
#[repr(C)]
pub struct PolarisRecord {
    /// Egress stream the record was published on.
    pub stream: u16,
    /// Number of bytes at `bytes`, header included.
    pub len: u32,
    /// First byte of the record header.
    pub bytes: *const u8,
}

/// An open feed session, opaque to C. `polaris_feed_open` creates it and
/// `polaris_feed_close` releases it. Calls on one handle must not overlap,
/// and a handle stays on the thread that opened it.
pub struct PolarisFeed {
    session: FeedSession,
    /// Reused by every poll, so a record costs no allocation; the records a
    /// poll hands out point into it.
    records: Vec<OwnedRecord>,
    poisoned: bool,
}

impl PolarisFeed {
    fn open(config_json: &CStr) -> Result<Self, i32> {
        let config: DatafeedConfig = config_json
            .to_str()
            .ok()
            .and_then(|json| serde_json::from_str(json).ok())
            .ok_or(POLARIS_ERR_CONFIG)?;
        let session = FeedSessionBuilder::from(config)
            .build()
            .map_err(|err| status(&err))?;
        Ok(PolarisFeed {
            session,
            records: Vec::new(),
            poisoned: false,
        })
    }

    /// Replaces the last poll's records with at most `cap` new ones.
    fn poll(&mut self, cap: usize) -> Result<&[OwnedRecord], i32> {
        self.records.clear();
        self.session
            .poll(&mut self.records, cap)
            .map_err(|err| status(&err))?;
        Ok(&self.records)
    }
}

fn status(err: &FeedError) -> i32 {
    match err {
        FeedError::Closed => POLARIS_FEED_END,
        FeedError::Config(_) => POLARIS_ERR_CONFIG,
        FeedError::Source(_) => POLARIS_ERR_SOURCE,
        FeedError::Session(_) => POLARIS_ERR_SESSION,
        FeedError::Tap(_) => POLARIS_ERR_TAP,
        FeedError::Runtime(_) => POLARIS_ERR_RUNTIME,
        _ => POLARIS_ERR_OTHER,
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "a record is at most MAX_RECORD_LEN bytes, asserted below to fit u32"
)]
fn c_record(rec: &OwnedRecord) -> PolarisRecord {
    let bytes = rec.bytes();
    PolarisRecord {
        stream: rec.stream().0,
        len: bytes.len() as u32,
        bytes: bytes.as_ptr(),
    }
}

const _: () = assert!(datafeed::MAX_RECORD_LEN <= u32::MAX as usize);

/// Runs an entry point's body and turns a panic into [`POLARIS_ERR_PANIC`]:
/// a panic that unwinds out of an `extern "C"` function aborts the process.
/// `AssertUnwindSafe` holds because the one state a panic can leave half
/// updated is a handle's, and [`with_feed`] poisons it.
fn guard(body: impl FnOnce() -> i32) -> i32 {
    panic::catch_unwind(AssertUnwindSafe(body)).unwrap_or(POLARIS_ERR_PANIC)
}

/// Runs `body` on an open handle under [`guard`]. A panic poisons the handle,
/// since its engine state may be half updated.
///
/// # Safety
///
/// `feed` is null or a handle from [`polaris_feed_open`], not yet closed and
/// not in use by another call.
unsafe fn with_feed(feed: *mut PolarisFeed, body: impl FnOnce(&mut PolarisFeed) -> i32) -> i32 {
    // SAFETY: the caller passes null or a live handle no other call holds.
    let Some(feed) = (unsafe { feed.as_mut() }) else {
        return POLARIS_ERR_NULL;
    };
    if feed.poisoned {
        return POLARIS_ERR_PANIC;
    }
    let status = guard(|| body(&mut *feed));
    if status == POLARIS_ERR_PANIC {
        feed.poisoned = true;
    }
    status
}

/// Opens a session from `config_json`: NUL-terminated UTF-8 JSON shaped like
/// the datafeed config file, whose `sink` and `observability` keys are
/// ignored. A replay opens its file here, a live source dials on the first
/// poll. Writes the handle to `*out` and returns `POLARIS_OK`, or writes NULL
/// there and returns a negative status.
///
/// # Safety
///
/// `config_json` is NULL or a NUL-terminated string valid for the call; `out`
/// is NULL or valid for one pointer write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn polaris_feed_open(
    config_json: *const c_char,
    out: *mut *mut PolarisFeed,
) -> i32 {
    if out.is_null() {
        return POLARIS_ERR_NULL;
    }
    // SAFETY: non-null, and the caller makes `out` valid for one write.
    unsafe { out.write(ptr::null_mut()) };
    if config_json.is_null() {
        return POLARIS_ERR_NULL;
    }
    // SAFETY: non-null, and the caller passes a NUL-terminated string that
    // outlives the call.
    let config_json = unsafe { CStr::from_ptr(config_json) };
    guard(|| match PolarisFeed::open(config_json) {
        Ok(feed) => {
            // SAFETY: as for the NULL written above.
            unsafe { out.write(Box::into_raw(Box::new(feed))) };
            POLARIS_OK
        }
        Err(status) => status,
    })
}

/// Writes up to `cap` records to `out` and their count to `*written`. With
/// nothing queued it first takes one step of the source (a frame batch, a
/// live dial, or at most 50 ms of a reconnect wait), which blocks through a
/// paced replay's delays or a live dial. `POLARIS_OK` with a count of 0 means
/// nothing is ready yet. Each record's `bytes` is valid until the next call on `feed`.
///
/// # Safety
///
/// `feed` is NULL or an open handle; `out` is NULL or valid for `cap` writes
/// of `polaris_record`, and `written` is NULL or valid for one write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn polaris_feed_poll(
    feed: *mut PolarisFeed,
    out: *mut PolarisRecord,
    cap: usize,
    written: *mut usize,
) -> i32 {
    if written.is_null() {
        return POLARIS_ERR_NULL;
    }
    // SAFETY: non-null, and the caller makes `written` valid for one write.
    unsafe { written.write(0) };
    if out.is_null() {
        return POLARIS_ERR_NULL;
    }
    let body = |feed: &mut PolarisFeed| match feed.poll(cap) {
        Ok(records) => {
            for (i, rec) in records.iter().enumerate() {
                // SAFETY: a poll hands out at most `cap` records, and the
                // caller makes `out` valid for `cap` writes.
                unsafe { out.add(i).write(c_record(rec)) };
            }
            // SAFETY: as for the 0 written above.
            unsafe { written.write(records.len()) };
            POLARIS_OK
        }
        Err(status) => status,
    };
    // SAFETY: the caller passes NULL or an open handle no other call holds.
    unsafe { with_feed(feed, body) }
}

/// Finishes the capture tap (closes the open file, writes its manifest) and
/// frees the handle either way, returning `POLARIS_OK` or `POLARIS_ERR_TAP`.
/// A handle poisoned by a caught panic is freed without finishing the capture
/// and returns `POLARIS_ERR_PANIC`. NULL is a no-op that returns `POLARIS_OK`.
///
/// # Safety
///
/// `feed` is NULL or an open handle. After the call it is invalid, and so is
/// every record a poll handed out from it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn polaris_feed_close(feed: *mut PolarisFeed) -> i32 {
    if feed.is_null() {
        return POLARIS_OK;
    }
    // SAFETY: non-null, and the caller passes a handle `polaris_feed_open`
    // made with `Box::into_raw` that no call has released yet.
    let feed = unsafe { Box::from_raw(feed) };
    guard(move || {
        let PolarisFeed {
            session, poisoned, ..
        } = *feed;
        if poisoned {
            return POLARIS_ERR_PANIC;
        }
        match session.close() {
            Ok(()) => POLARIS_OK,
            Err(err) => status(&err),
        }
    })
}

#[cfg(test)]
mod tests;
