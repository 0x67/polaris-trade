//! The export and the host wrapper over one test plugin in this test binary.
//! Its adapter emits one `Control` per frame whose `aux` is the payload length,
//! counts an empty frame as malformed, panics on the payload `panic`, and
//! emits a record with a zero header length on the payload `bad`.

use core::{
    ffi::{CStr, c_char},
    mem::size_of,
    ptr,
};
use std::sync::OnceLock;

use md_events::{Control, ControlKind, RecordFlags, RecordHeader, rtype};
use md_pipeline::{
    AdapterFault, AdapterStats, ClosureModel, CoverageMatrix, DynAdapter, MarketEvent, RollModel,
    SessionModel, StreamId, VenueAdapter,
};

use crate::{
    POLARIS_PLUGIN_ABI_VERSION, POLARIS_PLUGIN_ERR_CONFIG, POLARIS_PLUGIN_ERR_NULL,
    POLARIS_PLUGIN_ERR_PANIC, POLARIS_PLUGIN_OK, Plugin, PluginError, PluginVenue, PolarisAdapter,
    PolarisAdapterStats, PolarisAdapterVtable, PolarisRecord, PolarisSessionModel, PolarisVenue,
    export::{Venue, c_session},
    export_adapter,
    host::rust_session,
};

const STREAMS: [StreamId; 2] = [StreamId(0), StreamId(1)];

// A zoned roll the evening before and a weekend closure, so every field of the
// C session model carries a value.
const SESSION: SessionModel = SessionModel {
    roll: RollModel::DailyZoned {
        tz: "Asia/Singapore",
        local_ns: -7 * 3_600_000_000_000,
    },
    closure: ClosureModel::Weekend {
        close_dow: 5,
        open_dow: 0,
    },
};

struct TestAdapter {
    venue: u16,
    anchor_ns: u64,
    out: Vec<MarketEvent>,
    stats: AdapterStats,
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "a Control is 72 bytes, 18 words"
)]
fn control(venue: u16, ts_event: u64, ts_recv: u64, venue_seq: u64, aux: u64) -> MarketEvent {
    MarketEvent::Control(Control {
        hdr: RecordHeader {
            length: (size_of::<Control>() / 4) as u8,
            rtype: rtype::CONTROL,
            venue,
            instrument_id: 0,
            ts_event,
            ts_recv,
            ts_egress: 0,
            venue_seq,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0; 6],
        },
        aux,
        kind: ControlKind::Heartbeat,
        _pad: [0; 7],
    })
}

impl VenueAdapter for TestAdapter {
    type Config = u16;

    fn new(venue: u16) -> Self {
        TestAdapter {
            venue,
            anchor_ns: 0,
            out: Vec::new(),
            stats: AdapterStats::default(),
        }
    }

    fn streams() -> &'static [StreamId] {
        &STREAMS
    }

    fn coverage(_stream: StreamId) -> CoverageMatrix {
        CoverageMatrix::new()
    }

    fn record_types() -> &'static [u8] {
        &[rtype::CONTROL]
    }

    fn session(&self) -> SessionModel {
        SESSION
    }

    fn feed(&mut self, stream: StreamId, payload: &[u8], ts_recv: u64, venue_seq: u64) {
        if !STREAMS.contains(&stream) {
            return;
        }
        match payload {
            [] => self.stats.malformed += 1,
            b"panic" => panic!("the test adapter panics on request"),
            payload => {
                self.stats.decoded += 1;
                let mut event = control(
                    self.venue,
                    self.anchor_ns + ts_recv,
                    ts_recv,
                    venue_seq,
                    payload.len() as u64,
                );
                if payload == b"bad"
                    && let MarketEvent::Control(rec) = &mut event
                {
                    rec.hdr.length = 0;
                }
                self.out.push(event);
            }
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = MarketEvent> + '_ {
        self.out.drain(..)
    }

    fn stats(&self) -> &AdapterStats {
        &self.stats
    }

    fn set_session_anchor(&mut self, anchor_ns: u64) {
        self.anchor_ns = anchor_ns;
    }
}

export_adapter!(
    adapter: TestAdapter,
    venue: Venue {
        name: c"test",
        tag: c"testvenue",
        version: c"v1",
        feed_id: 9,
        soupbin_live: true,
        session: SESSION,
    },
    config: |json: &str| json.trim().parse::<u16>().ok(),
);

fn vtable() -> &'static PolarisAdapterVtable {
    // SAFETY: the entry returns the address of a static vtable.
    unsafe { &*polaris_adapter_entry() }
}

fn plugin() -> Plugin {
    // SAFETY: the vtable comes from this binary's own export.
    unsafe { Plugin::new(vtable()) }.expect("the test plugin loads")
}

/// A handle from the raw `create` entry.
fn raw_create(config: &CStr) -> (i32, *mut PolarisAdapter) {
    let mut handle = ptr::dangling_mut();
    // SAFETY: `config` is NUL-terminated and `handle` is a local.
    let status = unsafe { (vtable().create.unwrap())(config.as_ptr(), &raw mut handle) };
    (status, handle)
}

fn raw_feed(handle: *mut PolarisAdapter, stream: u16, payload: &[u8]) -> i32 {
    // SAFETY: `handle` is NULL or live, and `payload` is valid for its length.
    unsafe { (vtable().feed.unwrap())(handle, stream, payload.as_ptr(), payload.len(), 7, 1) }
}

/// The stream and `aux` of every record one raw drain hands out.
fn raw_drain(handle: *mut PolarisAdapter) -> (i32, Vec<(u16, u64)>) {
    let mut records = ptr::null();
    let mut written = usize::MAX;
    // SAFETY: `handle` is live, and `records` and `written` are locals.
    let status = unsafe { (vtable().drain.unwrap())(handle, &raw mut records, &raw mut written) };
    if status != POLARIS_PLUGIN_OK {
        return (status, Vec::new());
    }
    // SAFETY: the drain hands out `written` records, valid until the next call on `handle`.
    let records = unsafe { core::slice::from_raw_parts(records, written) };
    let decoded = records
        .iter()
        .map(|rec| {
            assert_eq!(rec.len as usize, size_of::<Control>());
            // SAFETY: each record points at `len` bytes of a `Control`.
            let bytes = unsafe { core::slice::from_raw_parts(rec.bytes, rec.len as usize) };
            let aux = u64::from_ne_bytes(bytes[56..64].try_into().unwrap());
            (rec.stream, aux)
        })
        .collect();
    (status, decoded)
}

fn raw_destroy(handle: *mut PolarisAdapter) -> i32 {
    // SAFETY: `handle` is NULL or live, and nothing uses it afterwards.
    unsafe { (vtable().destroy.unwrap())(handle) }
}

#[test]
fn the_venue_and_session_cross_as_declared() {
    let plugin = plugin();
    assert_eq!(
        *plugin.venue(),
        PluginVenue {
            name: "test",
            tag: "testvenue",
            version: "v1",
            feed_id: 9,
            soupbin_live: true,
            streams: &STREAMS,
            session: SESSION,
        }
    );
    let adapter = plugin.create(c"3").expect("the adapter builds");
    assert_eq!(adapter.session(), SESSION);
    assert_eq!(adapter.record_types(), [rtype::CONTROL]);
}

// An unknown roll or closure code reads as no model.
#[test]
fn every_session_model_crosses_unchanged() {
    let zone = Some(c"Asia/Singapore");
    let models = [
        SESSION,
        SessionModel {
            roll: RollModel::Continuous,
            closure: ClosureModel::None,
        },
        SessionModel {
            roll: RollModel::DailyFixed {
                offset_ns: 9 * 3_600_000_000_000,
            },
            closure: ClosureModel::Weekend {
                close_dow: 6,
                open_dow: 1,
            },
        },
    ];
    for model in models {
        let c = c_session(model, zone).expect("the zone is the declared one");
        // SAFETY: the zone is a static C string.
        assert_eq!(unsafe { rust_session(&c) }, Some(model));
    }
    let other_zone = SessionModel {
        roll: RollModel::DailyZoned {
            tz: "Europe/London",
            local_ns: 0,
        },
        closure: ClosureModel::None,
    };
    assert!(
        c_session(other_zone, zone).is_none(),
        "no static C string for it"
    );
    for unknown in [
        PolarisSessionModel {
            roll: 3,
            ..PolarisSessionModel::default()
        },
        PolarisSessionModel {
            closure: 2,
            ..PolarisSessionModel::default()
        },
    ] {
        // SAFETY: `tz` is NULL.
        assert_eq!(unsafe { rust_session(&unknown) }, None);
    }
}

// @lat: [[plugin#Tests#Drain buffer]]
#[test]
fn a_drain_hands_out_the_frames_fed_since_the_last_one() {
    let (status, handle) = raw_create(c"3");
    assert_eq!(status, POLARIS_PLUGIN_OK);

    assert_eq!(raw_feed(handle, 0, b"a"), POLARIS_PLUGIN_OK);
    assert_eq!(raw_feed(handle, 1, b"bb"), POLARIS_PLUGIN_OK);
    assert_eq!(raw_feed(handle, 2, b"ignored"), POLARIS_PLUGIN_OK);
    assert_eq!(raw_drain(handle), (POLARIS_PLUGIN_OK, vec![(0, 1), (1, 2)]));
    assert_eq!(raw_drain(handle), (POLARIS_PLUGIN_OK, vec![]));
    assert_eq!(raw_feed(handle, 1, b"ccc"), POLARIS_PLUGIN_OK);
    assert_eq!(raw_drain(handle), (POLARIS_PLUGIN_OK, vec![(1, 3)]));

    assert_eq!(raw_destroy(handle), POLARIS_PLUGIN_OK);
}

// Events, counters and the anchor match the same adapter run natively.
#[test]
fn the_host_adapter_runs_the_plugin_adapter() {
    let mut adapter = plugin().create(c"3").expect("the adapter builds");
    let mut native = TestAdapter::new(3);
    let mut out = (Vec::new(), Vec::new());

    let frames: [(u16, &[u8], u64); 4] = [
        (0, b"a", 0),
        (1, b"bb", 0),
        (0, b"", 1_000),
        (1, b"d", 1_000),
    ];
    for (stream, payload, anchor) in frames {
        DynAdapter::set_session_anchor(&mut adapter, anchor);
        DynAdapter::set_session_anchor(&mut native, anchor);
        DynAdapter::feed(&mut adapter, StreamId(stream), payload, 5, 6);
        DynAdapter::feed(&mut native, StreamId(stream), payload, 5, 6);
        adapter.drain_into(&mut out.0);
        native.drain_into(&mut out.1);
    }

    assert_eq!(out.0, out.1);
    assert_eq!(out.0.len(), 3);
    assert_eq!(DynAdapter::stats(&adapter), DynAdapter::stats(&native));
    assert_eq!(DynAdapter::stats(&adapter).malformed, 1);
    assert_eq!(adapter.fault(), None);
}

// @lat: [[plugin#Tests#Panic poisons the handle]]
#[test]
fn a_panic_poisons_the_handle() {
    let (_, handle) = raw_create(c"3");
    assert_eq!(raw_feed(handle, 0, b"panic"), POLARIS_PLUGIN_ERR_PANIC);
    assert_eq!(raw_feed(handle, 0, b"a"), POLARIS_PLUGIN_ERR_PANIC);
    assert_eq!(raw_drain(handle).0, POLARIS_PLUGIN_ERR_PANIC);
    let mut counters = PolarisAdapterStats::default();
    // SAFETY: `handle` is live and `counters` is a local.
    let status = unsafe { (vtable().stats.unwrap())(handle, &raw mut counters) };
    assert_eq!(status, POLARIS_PLUGIN_ERR_PANIC);
    assert_eq!(raw_destroy(handle), POLARIS_PLUGIN_OK);
}

// @lat: [[plugin#Tests#Faults reach the host adapter]]
#[test]
fn a_panic_faults_the_host_adapter() {
    let mut adapter = plugin().create(c"3").expect("the adapter builds");
    let mut out = Vec::new();
    DynAdapter::feed(&mut adapter, StreamId(0), b"a", 1, 1);
    adapter.drain_into(&mut out);
    DynAdapter::feed(&mut adapter, StreamId(0), b"panic", 2, 2);
    adapter.drain_into(&mut out);
    DynAdapter::feed(&mut adapter, StreamId(0), b"c", 3, 3);
    adapter.drain_into(&mut out);

    assert_eq!(out.len(), 1);
    assert_eq!(adapter.fault(), Some(AdapterFault::Panicked));
    assert_eq!(DynAdapter::stats(&adapter), AdapterStats::default());
    assert_eq!(
        adapter.fault(),
        Some(AdapterFault::Panicked),
        "the first fault stays"
    );
}

// A record the host cannot read ends the adapter after the records before it.
#[test]
fn a_malformed_record_faults_the_host_adapter() {
    let mut adapter = plugin().create(c"3").expect("the adapter builds");
    let mut out = Vec::new();
    DynAdapter::feed(&mut adapter, StreamId(0), b"a", 1, 1);
    DynAdapter::feed(&mut adapter, StreamId(0), b"bad", 2, 2);
    DynAdapter::feed(&mut adapter, StreamId(0), b"c", 3, 3);
    adapter.drain_into(&mut out);

    assert_eq!(out.len(), 1);
    assert_eq!(
        adapter.fault(),
        Some(AdapterFault::MalformedRecord {
            rtype: Some(rtype::CONTROL),
            len: size_of::<Control>(),
        })
    );
}

#[test]
fn null_pointers_return_the_null_status() {
    let vt = vtable();
    let (_, handle) = raw_create(c"3");
    let null = ptr::null_mut::<PolarisAdapter>();
    let mut created = ptr::null_mut::<PolarisAdapter>();
    let mut written = 0;
    let mut records = ptr::null::<PolarisRecord>();
    let mut types = ptr::null::<u8>();

    // SAFETY: every non-NULL pointer is a local or the live `handle`, and each
    // entry checks for NULL before it writes.
    let statuses = unsafe {
        [
            (vt.venue.unwrap())(ptr::null_mut::<PolarisVenue>()),
            (vt.create.unwrap())(ptr::null::<c_char>(), &raw mut created),
            (vt.create.unwrap())(c"3".as_ptr(), ptr::null_mut()),
            (vt.record_types.unwrap())(handle, ptr::null_mut(), &raw mut written),
            (vt.record_types.unwrap())(null, &raw mut types, &raw mut written),
            (vt.session.unwrap())(handle, ptr::null_mut()),
            (vt.feed.unwrap())(handle, 0, ptr::null(), 1, 0, 0),
            (vt.feed.unwrap())(null, 0, ptr::null(), 0, 0, 0),
            (vt.drain.unwrap())(handle, ptr::null_mut(), &raw mut written),
            (vt.drain.unwrap())(handle, &raw mut records, ptr::null_mut()),
            (vt.stats.unwrap())(handle, ptr::null_mut()),
            (vt.set_session_anchor.unwrap())(null, 0),
        ]
    };
    assert_eq!(statuses, [POLARIS_PLUGIN_ERR_NULL; 12]);
    assert_eq!(raw_destroy(null), POLARIS_PLUGIN_OK, "NULL is a no-op");
    assert_eq!(raw_destroy(handle), POLARIS_PLUGIN_OK);
}

#[test]
fn a_refused_config_returns_the_config_status() {
    for config in [c"not a venue id", c"\xff"] {
        let (status, handle) = raw_create(config);
        assert_eq!(status, POLARIS_PLUGIN_ERR_CONFIG);
        assert!(handle.is_null(), "a refused create writes NULL");
    }
    assert!(matches!(
        plugin().create(c"70000"),
        Err(PluginError::Failed {
            entry: "create",
            status: POLARIS_PLUGIN_ERR_CONFIG
        })
    ));
}

// Every other entry is NULL: a host reading past `abi_version` reports a missing entry.
// @lat: [[plugin#Tests#Version checked first]]
#[test]
fn another_abi_version_is_refused_before_any_entry_is_read() {
    static OTHER: PolarisAdapterVtable = PolarisAdapterVtable {
        abi_version: POLARIS_PLUGIN_ABI_VERSION + 1,
        venue: None,
        create: None,
        record_types: None,
        session: None,
        feed: None,
        drain: None,
        stats: None,
        set_session_anchor: None,
        destroy: None,
    };
    // SAFETY: the host reads only the version of a vtable of another version.
    let err = unsafe { Plugin::new(&OTHER) }.err();
    assert!(
        matches!(
            err,
            Some(PluginError::AbiVersion {
                found: 2,
                expected: 1
            })
        ),
        "{err:?}"
    );
}

#[test]
fn a_null_entry_is_refused_at_load() {
    static MISSING: OnceLock<PolarisAdapterVtable> = OnceLock::new();
    let missing = MISSING.get_or_init(|| PolarisAdapterVtable {
        destroy: None,
        ..*vtable()
    });
    // SAFETY: every other entry is this binary's own export.
    let err = unsafe { Plugin::new(missing) }.err();
    assert!(
        matches!(err, Some(PluginError::MissingEntry { entry: "destroy" })),
        "{err:?}"
    );
}
