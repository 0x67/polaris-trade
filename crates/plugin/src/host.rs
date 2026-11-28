//! The host side. [`Plugin`] checks a plugin's vtable and reads the venue it
//! declares; [`PluginAdapter`] drives one adapter of it as a [`DynAdapter`].
//! Every plugin status other than `POLARIS_PLUGIN_OK` becomes a
//! [`PluginError`] at load and create, or an [`AdapterFault`] while the
//! adapter runs, which the session runner checks once per frame batch.

mod decode;

use core::{
    cell::Cell,
    ffi::{CStr, c_char},
    ptr::{self, NonNull},
    slice,
};

use md_pipeline::{
    AdapterFault, AdapterStats, ClosureModel, DynAdapter, MarketEvent, RollModel, SessionModel,
    StreamId,
};

use crate::abi::{
    POLARIS_CLOSURE_NONE, POLARIS_CLOSURE_WEEKEND, POLARIS_PLUGIN_ABI_VERSION,
    POLARIS_PLUGIN_ERR_NULL, POLARIS_PLUGIN_ERR_PANIC, POLARIS_PLUGIN_OK, POLARIS_ROLL_CONTINUOUS,
    POLARIS_ROLL_DAILY_FIXED, POLARIS_ROLL_DAILY_ZONED, PolarisAdapter, PolarisAdapterStats,
    PolarisAdapterVtable, PolarisRecord, PolarisSessionModel, PolarisVenue,
};

/// Why the host refused a plugin or one of its adapters.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PluginError {
    /// The plugin was built against another plugin ABI version. No other
    /// field of its vtable was read.
    #[error("plugin ABI version {found}, host plugin ABI version {expected}")]
    AbiVersion {
        /// The plugin's `abi_version`.
        found: u32,
        /// The host's [`POLARIS_PLUGIN_ABI_VERSION`].
        expected: u32,
    },
    /// A vtable entry is NULL.
    #[error("plugin vtable entry {entry} is NULL")]
    MissingEntry {
        /// The entry's field name.
        entry: &'static str,
    },
    /// An entry returned a status other than `POLARIS_PLUGIN_OK`, such as
    /// `POLARIS_PLUGIN_ERR_CONFIG` from a `create` that refused its config.
    #[error("plugin entry {entry} failed with status {status}")]
    Failed {
        /// The entry's field name.
        entry: &'static str,
        /// The status it returned.
        status: i32,
    },
    /// An entry returned `POLARIS_PLUGIN_OK` but handed out data the host
    /// cannot use.
    #[error("plugin entry {entry} handed out {what}")]
    Invalid {
        /// The entry's field name.
        entry: &'static str,
        /// What was wrong with it.
        what: &'static str,
    },
}

/// The venue a plugin declares: every field of a host venue descriptor except
/// how to build an adapter, which [`Plugin::create`] does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PluginVenue {
    /// The name a deployment config selects the venue by.
    pub name: &'static str,
    /// Lowercase venue tag for the storage tree.
    pub tag: &'static str,
    /// Feed version token for the storage tree.
    pub version: &'static str,
    /// Feed id the capture header carries.
    pub feed_id: u8,
    /// Whether a live run dials the venue over `SoupBinTCP`, one session per
    /// stream.
    pub soupbin_live: bool,
    /// Ingest streams, the first one the stream whose trades feed the
    /// session's bars. Never empty.
    pub streams: &'static [StreamId],
    /// The session model every adapter of the plugin reports.
    pub session: SessionModel,
}

/// A plugin whose vtable passed the version check, with the venue it declares.
#[derive(Clone, Copy)]
pub struct Plugin {
    vtable: &'static PolarisAdapterVtable,
    venue: PluginVenue,
}

impl Plugin {
    /// Checks `abi_version` first, then that no entry is NULL, then reads the venue.
    ///
    /// # Safety
    /// `vtable` behaves as `polaris_abi.h` documents, and its library stays
    /// loaded for the rest of the process: its strings, arrays and functions
    /// are used as `'static`.
    ///
    /// # Errors
    /// [`PluginError::AbiVersion`] naming both versions, a `MissingEntry`, or a
    /// `Failed` or `Invalid` venue entry.
    pub unsafe fn new(vtable: &'static PolarisAdapterVtable) -> Result<Self, PluginError> {
        if vtable.abi_version != POLARIS_PLUGIN_ABI_VERSION {
            return Err(PluginError::AbiVersion {
                found: vtable.abi_version,
                expected: POLARIS_PLUGIN_ABI_VERSION,
            });
        }
        let entries = [
            ("venue", vtable.venue.is_some()),
            ("create", vtable.create.is_some()),
            ("record_types", vtable.record_types.is_some()),
            ("session", vtable.session.is_some()),
            ("feed", vtable.feed.is_some()),
            ("drain", vtable.drain.is_some()),
            ("stats", vtable.stats.is_some()),
            ("set_session_anchor", vtable.set_session_anchor.is_some()),
            ("destroy", vtable.destroy.is_some()),
        ];
        if let Some(&(entry, _)) = entries.iter().find(|(_, set)| !set) {
            return Err(PluginError::MissingEntry { entry });
        }
        // SAFETY: the caller vouches for the vtable and for the library staying loaded.
        let venue = unsafe { read_venue(vtable) }?;
        Ok(Plugin { vtable, venue })
    }

    /// The venue the plugin declared.
    pub fn venue(&self) -> &PluginVenue {
        &self.venue
    }

    /// Builds one adapter from `config_json`, the JSON object the vtable's
    /// `create` documents, and reads its record types and session model.
    ///
    /// # Errors
    ///
    /// [`PluginError::Failed`] when an entry returns a status, with
    /// `POLARIS_PLUGIN_ERR_CONFIG` from a refused config, and
    /// [`PluginError::Invalid`] for a NULL handle, a NULL record type array or
    /// a session model other than the venue's.
    pub fn create(&self, config_json: &CStr) -> Result<PluginAdapter, PluginError> {
        let mut handle = ptr::null_mut();
        let status = match self.vtable.create {
            // SAFETY: `config_json` is NUL-terminated and outlives the call, and `handle` is a
            // local the entry writes once.
            Some(create) => unsafe { create(config_json.as_ptr(), &raw mut handle) },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        succeeded("create", status)?;
        let handle = NonNull::new(handle).ok_or(PluginError::Invalid {
            entry: "create",
            what: "a NULL handle",
        })?;
        // dropping the adapter on an error below destroys the handle
        let mut adapter = PluginAdapter {
            vtable: self.vtable,
            handle,
            record_types: &[],
            session: self.venue.session,
            fault: Cell::new(None),
        };
        adapter.record_types = adapter.read_record_types()?;
        if adapter.read_session()? != self.venue.session {
            return Err(PluginError::Invalid {
                entry: "session",
                what: "a session model other than its venue's",
            });
        }
        Ok(adapter)
    }
}

/// One plugin adapter as a [`DynAdapter`]. Its record types and session model
/// are read once at [`Plugin::create`]. A call the plugin fails, and a drained
/// record the host cannot rebuild, set [`DynAdapter::fault`]; the adapter then
/// ignores every later feed, drain and anchor. Dropping it destroys the
/// plugin's handle. It stays on the thread that created it, as the plugin ABI
/// requires of a handle.
pub struct PluginAdapter {
    vtable: &'static PolarisAdapterVtable,
    handle: NonNull<PolarisAdapter>,
    record_types: &'static [u8],
    session: SessionModel,
    /// The first fault, in a `Cell` so a failed `stats(&self)` can set it.
    fault: Cell<Option<AdapterFault>>,
}

impl PluginAdapter {
    fn read_record_types(&self) -> Result<&'static [u8], PluginError> {
        let mut types = ptr::null();
        let mut len = 0;
        let status = match self.vtable.record_types {
            // SAFETY: the handle is live and no other call holds it; `types` and `len` are locals.
            Some(record_types) => unsafe {
                record_types(self.handle.as_ptr(), &raw mut types, &raw mut len)
            },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        succeeded("record_types", status)?;
        if len == 0 {
            return Ok(&[]);
        }
        if types.is_null() {
            return Err(PluginError::Invalid {
                entry: "record_types",
                what: "a NULL array",
            });
        }
        // SAFETY: the plugin hands out `len` bytes in static storage, and the library stays
        // loaded (`Plugin::new`).
        Ok(unsafe { slice::from_raw_parts(types, len) })
    }

    fn read_session(&self) -> Result<SessionModel, PluginError> {
        let mut out = PolarisSessionModel::default();
        let status = match self.vtable.session {
            // SAFETY: the handle is live and no other call holds it; `out` is a local.
            Some(session) => unsafe { session(self.handle.as_ptr(), &raw mut out) },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        succeeded("session", status)?;
        // SAFETY: the zone, if any, is a static string of the loaded library.
        unsafe { rust_session(&out) }.ok_or(PluginError::Invalid {
            entry: "session",
            what: "an unknown session model",
        })
    }

    /// Keeps the first fault: a later one is a consequence of it.
    fn fail(&self, fault: AdapterFault) {
        if self.fault.get().is_none() {
            self.fault.set(Some(fault));
        }
    }

    /// Faults on any status but `POLARIS_PLUGIN_OK` and says whether it did.
    fn faulted_by(&self, status: i32) -> bool {
        match status {
            POLARIS_PLUGIN_OK => false,
            POLARIS_PLUGIN_ERR_PANIC => {
                self.fail(AdapterFault::Panicked);
                true
            }
            status => {
                self.fail(AdapterFault::Status(status));
                true
            }
        }
    }
}

impl DynAdapter for PluginAdapter {
    fn record_types(&self) -> &'static [u8] {
        self.record_types
    }

    fn session(&self) -> SessionModel {
        self.session
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if self.fault.get().is_some() {
            return;
        }
        let status = match self.vtable.feed {
            // SAFETY: the handle is live and no other call holds it; `payload` is valid for its
            // length for the call.
            Some(feed) => unsafe {
                feed(
                    self.handle.as_ptr(),
                    stream.0,
                    payload.as_ptr(),
                    payload.len(),
                    ts_recv,
                    venue_seq,
                )
            },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        self.faulted_by(status);
    }

    fn drain_into(&mut self, out: &mut Vec<MarketEvent>) {
        if self.fault.get().is_some() {
            return;
        }
        let mut records = ptr::null();
        let mut written = 0;
        let status = match self.vtable.drain {
            // SAFETY: the handle is live and no other call holds it; `records` and `written` are
            // locals.
            Some(drain) => unsafe {
                drain(self.handle.as_ptr(), &raw mut records, &raw mut written)
            },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        if self.faulted_by(status) {
            return;
        }
        // SAFETY: the plugin hands out `written` records that stay valid until the next call on
        // the handle, and this loop copies each one out before then.
        let Some(records) = (unsafe { record_array(records, written) }) else {
            self.fail(AdapterFault::MalformedRecord {
                rtype: None,
                len: 0,
            });
            return;
        };
        for record in records {
            // SAFETY: as for the array above, each record's bytes.
            match unsafe { rebuild(record) } {
                Ok(event) => out.push(event),
                Err(fault) => {
                    self.fail(fault);
                    return;
                }
            }
        }
    }

    /// Zero counters when the plugin fails the call, which also sets the fault.
    fn stats(&self) -> AdapterStats {
        let mut counters = PolarisAdapterStats::default();
        let status = match self.vtable.stats {
            // SAFETY: the handle is live and no other call holds it; `counters` is a local.
            Some(stats) => unsafe { stats(self.handle.as_ptr(), &raw mut counters) },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        let mut out = AdapterStats::default();
        if !self.faulted_by(status) {
            out.decoded = counters.decoded;
            out.unknown_kind = counters.unknown_kind;
            out.malformed = counters.malformed;
        }
        out
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        if self.fault.get().is_some() {
            return;
        }
        let status = match self.vtable.set_session_anchor {
            // SAFETY: the handle is live and no other call holds it.
            Some(set_session_anchor) => unsafe {
                set_session_anchor(self.handle.as_ptr(), anchor_ns)
            },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        self.faulted_by(status);
    }

    fn fault(&self) -> Option<AdapterFault> {
        self.fault.get()
    }
}

impl Drop for PluginAdapter {
    fn drop(&mut self) {
        let status = match self.vtable.destroy {
            // SAFETY: the handle is live, and nothing uses it after this call.
            Some(destroy) => unsafe { destroy(self.handle.as_ptr()) },
            None => POLARIS_PLUGIN_ERR_NULL,
        };
        if status != POLARIS_PLUGIN_OK {
            tracing::warn!(status, "plugin adapter destroy failed");
        }
    }
}

fn succeeded(entry: &'static str, status: i32) -> Result<(), PluginError> {
    match status {
        POLARIS_PLUGIN_OK => Ok(()),
        status => Err(PluginError::Failed { entry, status }),
    }
}

/// # Safety
///
/// As [`Plugin::new`].
unsafe fn read_venue(vtable: &PolarisAdapterVtable) -> Result<PluginVenue, PluginError> {
    let mut out = PolarisVenue::default();
    let status = match vtable.venue {
        // SAFETY: `out` is a local the entry writes once.
        Some(venue) => unsafe { venue(&raw mut out) },
        None => POLARIS_PLUGIN_ERR_NULL,
    };
    succeeded("venue", status)?;
    let invalid = |what| PluginError::Invalid {
        entry: "venue",
        what,
    };
    // SAFETY: the strings are static data of the loaded library, as the caller vouches.
    let text = |ptr, what| unsafe { static_str(ptr) }.ok_or(invalid(what));
    let soupbin_live = match out.soupbin_live {
        0 => false,
        1 => true,
        _ => return Err(invalid("a soupbin_live other than 0 or 1")),
    };
    // SAFETY: the stream array is static data of the loaded library, as the caller vouches.
    let streams = unsafe { static_streams(out.streams, out.stream_count) }
        .ok_or(invalid("no streams, or a NULL or misaligned stream array"))?;
    // SAFETY: the zone, if any, is a static string of the loaded library.
    let session =
        unsafe { rust_session(&out.session) }.ok_or(invalid("an unknown session model"))?;
    Ok(PluginVenue {
        name: text(out.name, "a name that is NULL or not UTF-8")?,
        tag: text(out.tag, "a tag that is NULL or not UTF-8")?,
        version: text(out.version, "a version that is NULL or not UTF-8")?,
        feed_id: out.feed_id,
        soupbin_live,
        streams,
        session,
    })
}

/// The UTF-8 string at `ptr`, `None` when it is NULL or not UTF-8.
///
/// # Safety
///
/// `ptr` is NULL or a NUL-terminated string valid for the rest of the process.
unsafe fn static_str(ptr: *const c_char) -> Option<&'static str> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: non-null, and the caller vouches for the NUL and the lifetime.
    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}

/// The `len` stream ids at `ptr`, `None` when there are none or `ptr` is NULL
/// or misaligned.
///
/// # Safety
///
/// `ptr` is NULL or valid for `len` reads for the rest of the process.
unsafe fn static_streams(ptr: *const u16, len: usize) -> Option<&'static [StreamId]> {
    if len == 0 || ptr.is_null() || !ptr.is_aligned() {
        return None;
    }
    // SAFETY: non-null and aligned, valid for `len` reads as the caller vouches, and `StreamId`
    // is transparent over `u16`.
    Some(unsafe { slice::from_raw_parts(ptr.cast::<StreamId>(), len) })
}

/// The session model `c` describes, `None` for an unknown roll or closure or a
/// zoned roll whose zone is NULL or not UTF-8.
///
/// # Safety
///
/// `c.tz` is NULL or a NUL-terminated string valid for the rest of the process.
pub(crate) unsafe fn rust_session(c: &PolarisSessionModel) -> Option<SessionModel> {
    let roll = match c.roll {
        POLARIS_ROLL_CONTINUOUS => RollModel::Continuous,
        POLARIS_ROLL_DAILY_FIXED => RollModel::DailyFixed {
            offset_ns: c.offset_ns,
        },
        POLARIS_ROLL_DAILY_ZONED => RollModel::DailyZoned {
            // SAFETY: as the caller vouches.
            tz: unsafe { static_str(c.tz) }?,
            local_ns: c.local_ns,
        },
        _ => return None,
    };
    let closure = match c.closure {
        POLARIS_CLOSURE_NONE => ClosureModel::None,
        POLARIS_CLOSURE_WEEKEND => ClosureModel::Weekend {
            close_dow: c.close_dow,
            open_dow: c.open_dow,
        },
        _ => return None,
    };
    Some(SessionModel { roll, closure })
}

/// The `len` records at `ptr`, `None` when `ptr` is NULL or misaligned and
/// `len` is not 0.
///
/// # Safety
///
/// `ptr` is NULL or valid for `len` reads while the slice is used.
unsafe fn record_array<'a>(ptr: *const PolarisRecord, len: usize) -> Option<&'a [PolarisRecord]> {
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() || !ptr.is_aligned() {
        return None;
    }
    // SAFETY: non-null and aligned, and valid for `len` reads as the caller vouches.
    Some(unsafe { slice::from_raw_parts(ptr, len) })
}

/// The event `record` holds, or the fault that names why it is not one.
///
/// # Safety
///
/// `record.bytes` is NULL or valid for `record.len` reads for the call.
unsafe fn rebuild(record: &PolarisRecord) -> Result<MarketEvent, AdapterFault> {
    let len = record.len as usize;
    let bytes = if record.bytes.is_null() {
        &[][..]
    } else {
        // SAFETY: non-null, and valid for `len` reads as the caller vouches.
        unsafe { slice::from_raw_parts(record.bytes, len) }
    };
    decode::event(bytes).ok_or(AdapterFault::MalformedRecord {
        rtype: bytes.get(1).copied(),
        len,
    })
}
