//! The plugin side. [`export_adapter!`](crate::export_adapter) implements
//! [`Export`] for a type of its own and exports the vtable [`vtable`] builds
//! from it. Every entry runs under `catch_unwind`, and a panic poisons the
//! handle it happened on, as `polaris-datafeed-ffi` does for a feed handle.

use core::{
    ffi::{CStr, c_char},
    mem::{self, size_of},
    ptr,
};
use std::panic::{self, AssertUnwindSafe};

pub use md_pipeline::VenueAdapter;
use md_pipeline::{ClosureModel, MarketEvent, RollModel, SessionModel, StreamId};

use crate::abi::{
    POLARIS_CLOSURE_NONE, POLARIS_CLOSURE_WEEKEND, POLARIS_PLUGIN_ABI_VERSION,
    POLARIS_PLUGIN_ERR_CONFIG, POLARIS_PLUGIN_ERR_NULL, POLARIS_PLUGIN_ERR_PANIC,
    POLARIS_PLUGIN_ERR_SESSION, POLARIS_PLUGIN_OK, POLARIS_ROLL_CONTINUOUS,
    POLARIS_ROLL_DAILY_FIXED, POLARIS_ROLL_DAILY_ZONED, PolarisAdapter, PolarisAdapterStats,
    PolarisAdapterVtable, PolarisRecord, PolarisSessionModel, PolarisVenue,
};

/// Venue metadata a plugin declares: every field of the host's venue
/// descriptor that does not need an adapter, except the streams, which the
/// `venue` entry takes from [`VenueAdapter::streams`] in its order. The host
/// keeps that order, so the first stream is the one whose trades feed the
/// session's bars.
#[derive(Clone, Copy, Debug)]
pub struct Venue {
    /// The name a deployment config selects the venue by.
    pub name: &'static CStr,
    /// Lowercase venue tag for the storage tree.
    pub tag: &'static CStr,
    /// Feed version token for the storage tree.
    pub version: &'static CStr,
    /// Feed id the capture header carries.
    pub feed_id: u8,
    /// Whether a live run dials the venue over `SoupBinTCP`.
    pub soupbin_live: bool,
    /// The session model every adapter reports from [`VenueAdapter::session`].
    pub session: SessionModel,
}

/// One plugin: its adapter type, the venue it declares, and how `create`'s
/// JSON becomes the adapter's config. [`export_adapter!`](crate::export_adapter)
/// implements it; [`vtable`] builds the entries from it.
pub trait Export {
    /// The adapter every handle owns.
    type Adapter: VenueAdapter;

    /// The venue metadata `venue` hands out.
    const VENUE: Venue;

    /// The zone of a [`RollModel::DailyZoned`] session as a C string in
    /// static storage, `None` for any other roll.
    const ZONE: Option<&'static CStr>;

    /// Decodes `create`'s JSON into the adapter's config. `None` refuses it
    /// with `POLARIS_PLUGIN_ERR_CONFIG`.
    fn config(json: &str) -> Option<<Self::Adapter as VenueAdapter>::Config>;
}

/// The vtable of plugin `E`.
pub const fn vtable<E: Export>() -> PolarisAdapterVtable {
    PolarisAdapterVtable {
        abi_version: POLARIS_PLUGIN_ABI_VERSION,
        venue: Some(venue::<E>),
        create: Some(create::<E>),
        record_types: Some(record_types::<E>),
        session: Some(session::<E>),
        feed: Some(feed::<E>),
        drain: Some(drain::<E>),
        stats: Some(stats::<E>),
        set_session_anchor: Some(set_session_anchor::<E>),
        destroy: Some(destroy::<E>),
    }
}

/// Exports a [`VenueAdapter`] as a plugin: defines `polaris_adapter_entry`,
/// which returns the plugin's vtable. One invocation per shared library, in
/// a crate built as a `cdylib` with `panic = "unwind"`.
///
/// `adapter` is the [`VenueAdapter`](crate::export::VenueAdapter) type,
/// `venue` an [`export::Venue`](crate::export::Venue) const expression, and
/// `config` a `fn(&str) -> Option<Config>` that decodes `create`'s JSON into
/// the adapter's config. The crate README shows a full invocation.
#[macro_export]
macro_rules! export_adapter {
    (adapter: $adapter:ty, venue: $venue:expr, config: $config:expr $(,)?) => {
        /// The plugin's vtable, the symbol a host looks up after loading the
        /// library.
        #[unsafe(no_mangle)]
        pub extern "C" fn polaris_adapter_entry() -> *const $crate::PolarisAdapterVtable {
            const VENUE: $crate::export::Venue = $venue;
            const ZONE_BUF: [u8; $crate::export::zone_buf_len(&VENUE.session)] =
                $crate::export::zone_buf(&VENUE.session);
            struct Plugin;
            impl $crate::export::Export for Plugin {
                type Adapter = $adapter;
                const VENUE: $crate::export::Venue = VENUE;
                const ZONE: ::core::option::Option<&'static ::core::ffi::CStr> =
                    $crate::export::zone_cstr(&ZONE_BUF);
                fn config(
                    json: &str,
                ) -> ::core::option::Option<<$adapter as $crate::export::VenueAdapter>::Config>
                {
                    ($config)(json)
                }
            }
            static VTABLE: $crate::PolarisAdapterVtable = $crate::export::vtable::<Plugin>();
            const _: $crate::PolarisAdapterEntry = polaris_adapter_entry;
            &VTABLE
        }
    };
}

/// Bytes the zone name of `session` takes with its NUL, 1 for a roll without
/// a zone. [`export_adapter!`](crate::export_adapter) sizes the zone buffer
/// with it.
#[doc(hidden)]
pub const fn zone_buf_len(session: &SessionModel) -> usize {
    zone_name(session).len() + 1
}

/// The zone name of `session` followed by NUL, in a buffer of
/// [`zone_buf_len`] bytes.
#[doc(hidden)]
pub const fn zone_buf<const N: usize>(session: &SessionModel) -> [u8; N] {
    let name = zone_name(session).as_bytes();
    let mut buf = [0; N];
    let mut i = 0;
    while i < name.len() {
        buf[i] = name[i];
        i += 1;
    }
    buf
}

/// The C string in a [`zone_buf`], `None` for the empty one a roll without a
/// zone has.
///
/// # Panics
///
/// At compile time, when the zone name holds a NUL byte.
#[doc(hidden)]
pub const fn zone_cstr(buf: &'static [u8]) -> Option<&'static CStr> {
    match CStr::from_bytes_with_nul(buf) {
        Ok(_) if buf.len() == 1 => None,
        Ok(zone) => Some(zone),
        Err(_) => panic!("the session zone name holds a NUL byte"),
    }
}

const fn zone_name(session: &SessionModel) -> &'static str {
    match session.roll {
        RollModel::DailyZoned { tz, .. } => tz,
        RollModel::Continuous | RollModel::DailyFixed { .. } => "",
    }
}

/// One adapter and the buffers its drains hand out.
struct Handle<A> {
    adapter: A,
    /// Events emitted since the last drain. `feed` drains the adapter at once,
    /// so each event keeps the stream of the frame that emitted it.
    events: Vec<MarketEvent>,
    /// Stream and end index into `events` of each frame fed since the last drain.
    spans: Vec<(StreamId, usize)>,
    /// What the last drain handed out, pointing into `events`.
    records: Vec<PolarisRecord>,
    /// The last call was a drain, so `records` is out with the host and the
    /// next feed or drain clears it before touching `events`.
    handed: bool,
    poisoned: bool,
}

// a record is at most `size_of::<MarketEvent>()` bytes, so its length fits the u32 field
const _: () = assert!(size_of::<MarketEvent>() <= u32::MAX as usize);

impl<A: VenueAdapter> Handle<A> {
    fn new(config: A::Config) -> Self {
        Handle {
            adapter: A::new(config),
            events: Vec::new(),
            spans: Vec::new(),
            records: Vec::new(),
            handed: false,
            poisoned: false,
        }
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        self.clear_handed();
        self.adapter.feed(stream, payload, ts_recv, venue_seq);
        self.events.extend(self.adapter.drain());
        self.spans.push((stream, self.events.len()));
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "a record's length fits u32, asserted above"
    )]
    fn drain(&mut self) -> &[PolarisRecord] {
        self.clear_handed();
        let mut start = 0;
        for &(stream, end) in &self.spans {
            for event in &self.events[start..end] {
                let bytes = event.as_bytes();
                self.records.push(PolarisRecord {
                    stream: stream.0,
                    len: bytes.len() as u32,
                    bytes: bytes.as_ptr(),
                });
            }
            start = end;
        }
        self.handed = true;
        &self.records
    }

    fn clear_handed(&mut self) {
        if mem::take(&mut self.handed) {
            self.events.clear();
            self.spans.clear();
            self.records.clear();
        }
    }
}

/// Runs an entry's body and turns a panic into `POLARIS_PLUGIN_ERR_PANIC`: a
/// panic that unwinds out of an `extern "C"` function aborts the process.
/// `AssertUnwindSafe` holds because the one state a panic can leave half
/// updated is a handle's, and [`with_handle`] poisons it.
fn guard(body: impl FnOnce() -> i32) -> i32 {
    panic::catch_unwind(AssertUnwindSafe(body)).unwrap_or(POLARIS_PLUGIN_ERR_PANIC)
}

/// Runs `body` on a live handle under [`guard`], and poisons the handle when
/// it panics.
///
/// # Safety
///
/// `adapter` is NULL or a handle `create::<E>` made that no call has freed and
/// no other call holds.
unsafe fn with_handle<E: Export>(
    adapter: *mut PolarisAdapter,
    body: impl FnOnce(&mut Handle<E::Adapter>) -> i32,
) -> i32 {
    // SAFETY: the caller passes NULL or a live `Handle<E::Adapter>` no other call holds.
    let Some(handle) = (unsafe { adapter.cast::<Handle<E::Adapter>>().as_mut() }) else {
        return POLARIS_PLUGIN_ERR_NULL;
    };
    if handle.poisoned {
        return POLARIS_PLUGIN_ERR_PANIC;
    }
    let status = guard(|| body(&mut *handle));
    if status == POLARIS_PLUGIN_ERR_PANIC {
        handle.poisoned = true;
    }
    status
}

/// `session` in C form, or `None` when it names a zone other than `zone`.
pub(crate) fn c_session(
    session: SessionModel,
    zone: Option<&'static CStr>,
) -> Option<PolarisSessionModel> {
    let mut out = PolarisSessionModel::default();
    match session.roll {
        RollModel::Continuous => out.roll = POLARIS_ROLL_CONTINUOUS,
        RollModel::DailyFixed { offset_ns } => {
            out.roll = POLARIS_ROLL_DAILY_FIXED;
            out.offset_ns = offset_ns;
        }
        RollModel::DailyZoned { tz, local_ns } => {
            let zone = zone.filter(|zone| zone.to_bytes() == tz.as_bytes())?;
            out.roll = POLARIS_ROLL_DAILY_ZONED;
            out.local_ns = local_ns;
            out.tz = zone.as_ptr();
        }
    }
    match session.closure {
        ClosureModel::None => out.closure = POLARIS_CLOSURE_NONE,
        ClosureModel::Weekend {
            close_dow,
            open_dow,
        } => {
            out.closure = POLARIS_CLOSURE_WEEKEND;
            out.close_dow = close_dow;
            out.open_dow = open_dow;
        }
    }
    Some(out)
}

/// # Safety
///
/// `out` is NULL or valid for one `PolarisVenue` write.
unsafe extern "C" fn venue<E: Export>(out: *mut PolarisVenue) -> i32 {
    if out.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    guard(|| {
        let Some(session) = c_session(E::VENUE.session, E::ZONE) else {
            return POLARIS_PLUGIN_ERR_SESSION;
        };
        let streams = <E::Adapter as VenueAdapter>::streams();
        let venue = PolarisVenue {
            name: E::VENUE.name.as_ptr(),
            tag: E::VENUE.tag.as_ptr(),
            version: E::VENUE.version.as_ptr(),
            feed_id: E::VENUE.feed_id,
            soupbin_live: u8::from(E::VENUE.soupbin_live),
            // `StreamId` is transparent over u16
            streams: streams.as_ptr().cast::<u16>(),
            stream_count: streams.len(),
            session,
        };
        // SAFETY: non-null, and the caller makes `out` valid for one write.
        unsafe { out.write(venue) };
        POLARIS_PLUGIN_OK
    })
}

/// # Safety
///
/// `config_json` is NULL or a NUL-terminated string valid for the call; `out`
/// is NULL or valid for one pointer write.
unsafe extern "C" fn create<E: Export>(
    config_json: *const c_char,
    out: *mut *mut PolarisAdapter,
) -> i32 {
    if out.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    // SAFETY: non-null, and the caller makes `out` valid for one write.
    unsafe { out.write(ptr::null_mut()) };
    if config_json.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    // SAFETY: non-null, and the caller passes a NUL-terminated string that
    // outlives the call.
    let json = unsafe { CStr::from_ptr(config_json) };
    guard(|| {
        let Some(config) = json.to_str().ok().and_then(E::config) else {
            return POLARIS_PLUGIN_ERR_CONFIG;
        };
        let handle = Box::new(Handle::<E::Adapter>::new(config));
        // SAFETY: as for the NULL written above.
        unsafe { out.write(Box::into_raw(handle).cast()) };
        POLARIS_PLUGIN_OK
    })
}

/// # Safety
///
/// `adapter` as [`with_handle`]; `out` and `len` are NULL or valid for one
/// write each.
unsafe extern "C" fn record_types<E: Export>(
    adapter: *mut PolarisAdapter,
    out: *mut *const u8,
    len: *mut usize,
) -> i32 {
    if out.is_null() || len.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    let body = |_: &mut Handle<E::Adapter>| {
        let types = <E::Adapter as VenueAdapter>::record_types();
        // SAFETY: both non-null, and the caller makes each valid for one write.
        unsafe {
            out.write(types.as_ptr());
            len.write(types.len());
        }
        POLARIS_PLUGIN_OK
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` as [`with_handle`]; `out` is NULL or valid for one write.
unsafe extern "C" fn session<E: Export>(
    adapter: *mut PolarisAdapter,
    out: *mut PolarisSessionModel,
) -> i32 {
    if out.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    let body = |handle: &mut Handle<E::Adapter>| match c_session(handle.adapter.session(), E::ZONE)
    {
        Some(session) => {
            // SAFETY: non-null, and the caller makes `out` valid for one write.
            unsafe { out.write(session) };
            POLARIS_PLUGIN_OK
        }
        None => POLARIS_PLUGIN_ERR_SESSION,
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` as [`with_handle`]; `payload` is valid for `len` reads, or NULL
/// with `len` 0.
unsafe extern "C" fn feed<E: Export>(
    adapter: *mut PolarisAdapter,
    stream: u16,
    payload: *const u8,
    len: usize,
    ts_recv: u64,
    venue_seq: u64,
) -> i32 {
    let payload = if len == 0 {
        &[][..]
    } else if payload.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    } else {
        // SAFETY: non-null, and the caller makes `payload` valid for `len` reads
        // for the call.
        unsafe { core::slice::from_raw_parts(payload, len) }
    };
    let body = |handle: &mut Handle<E::Adapter>| {
        handle.feed(StreamId(stream), payload, ts_recv, venue_seq);
        POLARIS_PLUGIN_OK
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` as [`with_handle`]; `out` and `written` are NULL or valid for one
/// write each.
unsafe extern "C" fn drain<E: Export>(
    adapter: *mut PolarisAdapter,
    out: *mut *const PolarisRecord,
    written: *mut usize,
) -> i32 {
    if out.is_null() || written.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    let body = |handle: &mut Handle<E::Adapter>| {
        let records = handle.drain();
        // SAFETY: both non-null, and the caller makes each valid for one write.
        unsafe {
            out.write(records.as_ptr());
            written.write(records.len());
        }
        POLARIS_PLUGIN_OK
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` as [`with_handle`]; `out` is NULL or valid for one write.
unsafe extern "C" fn stats<E: Export>(
    adapter: *mut PolarisAdapter,
    out: *mut PolarisAdapterStats,
) -> i32 {
    if out.is_null() {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    let body = |handle: &mut Handle<E::Adapter>| {
        let stats = handle.adapter.stats();
        let stats = PolarisAdapterStats {
            decoded: stats.decoded,
            unknown_kind: stats.unknown_kind,
            malformed: stats.malformed,
        };
        // SAFETY: non-null, and the caller makes `out` valid for one write.
        unsafe { out.write(stats) };
        POLARIS_PLUGIN_OK
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` as [`with_handle`].
unsafe extern "C" fn set_session_anchor<E: Export>(
    adapter: *mut PolarisAdapter,
    anchor_ns: u64,
) -> i32 {
    let body = |handle: &mut Handle<E::Adapter>| {
        handle.adapter.set_session_anchor(anchor_ns);
        POLARIS_PLUGIN_OK
    };
    // SAFETY: the caller passes NULL or a live handle no other call holds.
    unsafe { with_handle::<E>(adapter, body) }
}

/// # Safety
///
/// `adapter` is NULL or a handle `create::<E>` made that no call has freed or
/// holds. It is invalid after the call, and so is every record it handed out.
unsafe extern "C" fn destroy<E: Export>(adapter: *mut PolarisAdapter) -> i32 {
    if adapter.is_null() {
        return POLARIS_PLUGIN_OK;
    }
    // SAFETY: non-null, and the caller passes a handle `create::<E>` made with
    // `Box::into_raw` that no call has freed.
    let handle = unsafe { Box::from_raw(adapter.cast::<Handle<E::Adapter>>()) };
    guard(move || {
        drop(handle);
        POLARIS_PLUGIN_OK
    })
}
