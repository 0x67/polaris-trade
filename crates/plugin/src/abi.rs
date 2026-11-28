//! The `repr(C)` types and constants of the plugin ABI. cbindgen reads this
//! file together with `polaris-datafeed-ffi`'s `src/lib.rs` into
//! `polaris_abi.h`, so every doc comment here is also C header text.

use core::{
    ffi::c_char,
    mem::{offset_of, size_of},
    ptr,
};

use md_events::RecordHeader;

/// Version of the plugin ABI, separate from the embedding API's
/// `POLARIS_ABI_VERSION`, so a change to one does not refuse builds of the
/// other. It changes when the vtable, a struct it reaches, the meaning of a
/// status or the layout of a record in `md_events.h` changes. `polaris_record`
/// and `polaris_record_header` belong to both ABIs, so a change to either
/// moves both versions.
pub const POLARIS_PLUGIN_ABI_VERSION: u32 = 1;

/// Success.
pub const POLARIS_PLUGIN_OK: i32 = 0;

/// A required pointer argument is NULL.
pub const POLARIS_PLUGIN_ERR_NULL: i32 = -1;

/// `create` only: the config is not UTF-8, or the plugin does not accept it.
pub const POLARIS_PLUGIN_ERR_CONFIG: i32 = -2;

/// A Rust panic was caught at the entry point. A handle it happened on is
/// poisoned: every later call on it returns this status, and `destroy` still
/// frees it. Only a plugin built with `panic = "unwind"` (the workspace `ffi`
/// profile) returns it; a plugin built with `panic = "abort"` aborts the host
/// process instead.
pub const POLARIS_PLUGIN_ERR_PANIC: i32 = -3;

/// `venue` and `session` only: the session model names a zone other than the
/// one the plugin declared, so the plugin has no static string to hand out
/// for it.
pub const POLARIS_PLUGIN_ERR_SESSION: i32 = -4;

/// `polaris_session_model.roll`: sessions roll on UTC midnight.
pub const POLARIS_ROLL_CONTINUOUS: u8 = 0;

/// `polaris_session_model.roll`: sessions roll at local midnight, local time
/// being UTC plus `offset_ns`.
pub const POLARIS_ROLL_DAILY_FIXED: u8 = 1;

/// `polaris_session_model.roll`: sessions roll `local_ns` after local midnight
/// in the IANA zone `tz`, whose offset the host resolves per date.
pub const POLARIS_ROLL_DAILY_ZONED: u8 = 2;

/// `polaris_session_model.closure`: every session trades.
pub const POLARIS_CLOSURE_NONE: u8 = 0;

/// `polaris_session_model.closure`: closed from the anchor of the `close_dow`
/// session until the anchor of the `open_dow` session, days counted from
/// Sunday (0) to Saturday (6).
pub const POLARIS_CLOSURE_WEEKEND: u8 = 1;

/// The 56-byte header every record starts with, the C view of md-events
/// `RecordHeader`. `length` is the whole record in 32-bit words, and `rtype`
/// names the record type that follows (`md_events.h` holds the layouts).
#[repr(C)]
pub struct PolarisRecordHeader {
    /// Record length in 32-bit words, header included.
    pub length: u8,
    /// Record type.
    pub rtype: u8,
    /// Venue id.
    pub venue: u16,
    /// Instrument id.
    pub instrument_id: u32,
    /// Event time, epoch nanoseconds UTC.
    pub ts_event: u64,
    /// Receive time, epoch nanoseconds UTC.
    pub ts_recv: u64,
    /// Egress time, 0 until the host's egress sequencer stamps it.
    pub ts_egress: u64,
    /// Wire sequence of the frame the record came from.
    pub venue_seq: u64,
    /// Egress sequence, 0 until the host's egress sequencer stamps it.
    pub egress_seq: u64,
    /// Record flag bits.
    pub flags: u16,
    /// Zero.
    pub pad: [u8; 6],
}

// The header's C view must lay out as md-events does; polaris_abi.h checks the
// size again on the C side.
const _: () = {
    assert!(size_of::<PolarisRecordHeader>() == 56);
    assert!(size_of::<RecordHeader>() == size_of::<PolarisRecordHeader>());
    assert!(offset_of!(RecordHeader, length) == offset_of!(PolarisRecordHeader, length));
    assert!(offset_of!(RecordHeader, rtype) == offset_of!(PolarisRecordHeader, rtype));
    assert!(offset_of!(RecordHeader, venue) == offset_of!(PolarisRecordHeader, venue));
    assert!(
        offset_of!(RecordHeader, instrument_id) == offset_of!(PolarisRecordHeader, instrument_id)
    );
    assert!(offset_of!(RecordHeader, ts_event) == offset_of!(PolarisRecordHeader, ts_event));
    assert!(offset_of!(RecordHeader, ts_recv) == offset_of!(PolarisRecordHeader, ts_recv));
    assert!(offset_of!(RecordHeader, ts_egress) == offset_of!(PolarisRecordHeader, ts_egress));
    assert!(offset_of!(RecordHeader, venue_seq) == offset_of!(PolarisRecordHeader, venue_seq));
    assert!(offset_of!(RecordHeader, egress_seq) == offset_of!(PolarisRecordHeader, egress_seq));
    assert!(offset_of!(RecordHeader, flags) == offset_of!(PolarisRecordHeader, flags));
};

/// One record handed across the C ABI: by `polaris_feed_poll` to an embedding
/// host, and by a plugin's `drain` to its host. `bytes` holds the record as
/// published, the 56-byte `polaris_record_header` then the body (layouts in
/// the events crate's `md_events.h`). The call that hands a record out states
/// how long `bytes` stays valid.
#[repr(C)]
pub struct PolarisRecord {
    /// Stream the record belongs to: the egress stream it was published on,
    /// or for a plugin the stream of the `feed` call that emitted it.
    pub stream: u16,
    /// Number of bytes at `bytes`, header included.
    pub len: u32,
    /// First byte of the record header.
    pub bytes: *const u8,
}

/// One adapter inside a plugin, opaque to the host. `create` makes it and
/// `destroy` frees it. Calls on one handle must not overlap, and a handle
/// stays on the thread that created it.
pub struct PolarisAdapter {
    _opaque: [u8; 0],
}

/// A venue's trading-day model: how its session rolls (`POLARIS_ROLL_*`) and
/// which sessions it does not trade (`POLARIS_CLOSURE_*`). Fields a rule does
/// not use are 0, and `tz` is NULL.
#[repr(C)]
pub struct PolarisSessionModel {
    /// One of the `POLARIS_ROLL_*` values.
    pub roll: u8,
    /// One of the `POLARIS_CLOSURE_*` values.
    pub closure: u8,
    /// `POLARIS_CLOSURE_WEEKEND`: weekday of the first closed session.
    pub close_dow: u8,
    /// `POLARIS_CLOSURE_WEEKEND`: weekday of the first session after it.
    pub open_dow: u8,
    /// `POLARIS_ROLL_DAILY_FIXED`: local time minus UTC, in nanoseconds.
    pub offset_ns: i64,
    /// `POLARIS_ROLL_DAILY_ZONED`: roll time relative to local midnight of the
    /// labelled date, in nanoseconds; negative rolls the evening before.
    pub local_ns: i64,
    /// `POLARIS_ROLL_DAILY_ZONED`: IANA zone name, NUL-terminated UTF-8 in
    /// static storage of the plugin, valid while its library stays loaded.
    pub tz: *const c_char,
}

impl Default for PolarisSessionModel {
    fn default() -> Self {
        PolarisSessionModel {
            roll: POLARIS_ROLL_CONTINUOUS,
            closure: POLARIS_CLOSURE_NONE,
            close_dow: 0,
            open_dow: 0,
            offset_ns: 0,
            local_ns: 0,
            tz: ptr::null(),
        }
    }
}

/// What a plugin declares about its venue, before any adapter exists: the
/// host's venue descriptor takes these fields from it. Every string is
/// NUL-terminated UTF-8 and every array lives in static storage of the
/// plugin, valid while its library stays loaded.
#[repr(C)]
pub struct PolarisVenue {
    /// The name a deployment config selects the venue by.
    pub name: *const c_char,
    /// Lowercase venue tag for the storage tree.
    pub tag: *const c_char,
    /// Feed version token for the storage tree.
    pub version: *const c_char,
    /// Feed id the capture header carries.
    pub feed_id: u8,
    /// 1 when a live run dials the venue over `SoupBinTCP`, one session per
    /// stream; 0 otherwise.
    pub soupbin_live: u8,
    /// Ingest streams the adapter consumes, the first one the stream whose
    /// trades feed the session's bars.
    pub streams: *const u16,
    /// Number of entries at `streams`, at least 1.
    pub stream_count: usize,
    /// The session model every adapter of this plugin reports.
    pub session: PolarisSessionModel,
}

impl Default for PolarisVenue {
    fn default() -> Self {
        PolarisVenue {
            name: ptr::null(),
            tag: ptr::null(),
            version: ptr::null(),
            feed_id: 0,
            soupbin_live: 0,
            streams: ptr::null(),
            stream_count: 0,
            session: PolarisSessionModel::default(),
        }
    }
}

/// An adapter's decode counters since `create`, summed over every stream, each
/// counting whole wire frames.
#[repr(C)]
#[derive(Default)]
pub struct PolarisAdapterStats {
    /// Frames dispatched to a decoder.
    pub decoded: u64,
    /// Frames whose message type the adapter does not know.
    pub unknown_kind: u64,
    /// Frames of a known type with an invalid length or body.
    pub malformed: u64,
}

/// Everything a host calls in a plugin. `abi_version` comes first: a host
/// reads it before any other field and stops when it differs from its own
/// `POLARIS_PLUGIN_ABI_VERSION`, since the rest of the layout may differ. No
/// entry is NULL.
///
/// Every entry returns `POLARIS_PLUGIN_OK` or a negative `POLARIS_PLUGIN_ERR_*`
/// status: `POLARIS_PLUGIN_ERR_NULL` for a NULL pointer argument and
/// `POLARIS_PLUGIN_ERR_PANIC` for a caught panic, plus the statuses an entry
/// names. Handle entries take a handle from `create` that `destroy` has not
/// freed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PolarisAdapterVtable {
    /// The `POLARIS_PLUGIN_ABI_VERSION` the plugin was built with.
    pub abi_version: u32,
    /// Writes the venue metadata to `*out`. Needs no handle.
    pub venue: Option<unsafe extern "C" fn(out: *mut PolarisVenue) -> i32>,
    /// Builds an adapter from `config_json`, NUL-terminated UTF-8 JSON
    /// borrowed for the call: an object whose `venue_id` and
    /// `session_midnight_ns` keys the host sets from its deployment config,
    /// plus any key the plugin documents. Writes the handle to `*out` and
    /// returns `POLARIS_PLUGIN_OK`, or writes NULL there and returns a
    /// negative status, `POLARIS_PLUGIN_ERR_CONFIG` when the plugin refuses
    /// the config.
    pub create: Option<
        unsafe extern "C" fn(config_json: *const c_char, out: *mut *mut PolarisAdapter) -> i32,
    >,
    /// Writes the md-events rtypes the adapter builds from the wire to
    /// `*out`, an array in static storage of the plugin, and their count to
    /// `*len`.
    pub record_types: Option<
        unsafe extern "C" fn(
            adapter: *mut PolarisAdapter,
            out: *mut *const u8,
            len: *mut usize,
        ) -> i32,
    >,
    /// Writes the adapter's session model to `*out`. It equals the one
    /// `venue` declares, or the call returns `POLARIS_PLUGIN_ERR_SESSION`.
    pub session: Option<
        unsafe extern "C" fn(adapter: *mut PolarisAdapter, out: *mut PolarisSessionModel) -> i32,
    >,
    /// Decodes one wire frame of `len` bytes received on `stream` at
    /// `ts_recv` with wire sequence `venue_seq`. `payload` may be NULL when
    /// `len` is 0. A malformed or unknown frame moves a counter and is not an
    /// error.
    pub feed: Option<
        unsafe extern "C" fn(
            adapter: *mut PolarisAdapter,
            stream: u16,
            payload: *const u8,
            len: usize,
            ts_recv: u64,
            venue_seq: u64,
        ) -> i32,
    >,
    /// Hands out every record emitted since the last drain, in emission
    /// order: writes a pointer to the first `polaris_record` to `*out` and the
    /// count to `*written`. The array and the record bytes belong to the
    /// handle and stay valid until the next `feed`, `drain` or `destroy` on
    /// it, so a host copies what it keeps first.
    pub drain: Option<
        unsafe extern "C" fn(
            adapter: *mut PolarisAdapter,
            out: *mut *const PolarisRecord,
            written: *mut usize,
        ) -> i32,
    >,
    /// Writes the adapter's counters to `*out`.
    pub stats: Option<
        unsafe extern "C" fn(adapter: *mut PolarisAdapter, out: *mut PolarisAdapterStats) -> i32,
    >,
    /// Replaces the session anchor, epoch nanoseconds UTC, that wire
    /// time-of-day offsets count from for every frame fed after the call.
    pub set_session_anchor:
        Option<unsafe extern "C" fn(adapter: *mut PolarisAdapter, anchor_ns: u64) -> i32>,
    /// Frees the handle, poisoned or not. NULL is a no-op that returns
    /// `POLARIS_PLUGIN_OK`.
    pub destroy: Option<unsafe extern "C" fn(adapter: *mut PolarisAdapter) -> i32>,
}

/// Type of `polaris_adapter_entry`, the one symbol a plugin exports. It
/// returns the plugin's vtable, in static storage valid while the library
/// stays loaded.
pub type PolarisAdapterEntry = unsafe extern "C" fn() -> *const PolarisAdapterVtable;
