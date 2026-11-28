# Plugin ABI

`polaris-plugin` under `crates/plugin/` is a versioned C ABI for venue adapters, so a closed adapter can be delivered as a shared library that an open host loads at run time.

A plugin crate turns its [[pipeline#Venue adapter seam]] adapter into a vtable with one macro. On the host side the crate checks the vtable and drives one adapter of it as a `DynAdapter`, which the session runner runs like any built-in venue. This crate has no loader: the datafeed's loader opens the shared library, finds the entry symbol and hands its vtable to [[crates/plugin/src/host.rs#Plugin#new]].

## Placement

The ABI types, the plugin-side export and the host wrapper share one member crate that depends on `polaris-events`, `polaris-pipeline`, `thiserror` and `tracing` only.

A third-party adapter crate pins `polaris-plugin` by git tag and links nothing of the datafeed runtime (tokio, taps, sinks), which it would if the types were defined in `polaris-datafeed-ffi`. Defining them in `polaris-pipeline` would add `unsafe` C entry points to the adapter trait crate that every adapter and consumer builds. The crate is `publish = false` with the workspace license and lints, like every member.

`polaris_record` belongs to both C ABIs, so it is defined here as [[crates/plugin/src/abi.rs#PolarisRecord]] and `polaris-datafeed-ffi` takes it from this crate ([[c-abi#Packaging and header]]).

## Versions

The plugin ABI has its own `POLARIS_PLUGIN_ABI_VERSION`, 1, apart from the embedding API's `POLARIS_ABI_VERSION`, so a change to one does not refuse every build of the other.

It moves when the vtable, a struct one of its entries takes, the meaning of a status, or the layout of a record in `md_events.h` changes. A change to `polaris_record` or `polaris_record_header` moves both versions. There is no negotiation: [[crates/plugin/src/host.rs#Plugin#new]] reads `abi_version`, the vtable's first field, before anything else and refuses a mismatch with `PluginError::AbiVersion`, which names both versions.

## Vtable

[[crates/plugin/src/abi.rs#PolarisAdapterVtable]] mirrors `DynAdapter` entry for entry, plus the venue declaration, `create` and `destroy`. A plugin exports it from `polaris_adapter_entry`.

The entries are `venue`, `create`, `record_types`, `session`, `feed`, `drain`, `stats`, `set_session_anchor` and `destroy`. Every entry returns an `int32_t`: `POLARIS_PLUGIN_OK` (0), `POLARIS_PLUGIN_ERR_NULL` (-1) for a NULL pointer argument, `POLARIS_PLUGIN_ERR_CONFIG` (-2) from `create` for a refused config, `POLARIS_PLUGIN_ERR_PANIC` (-3) for a caught panic, or `POLARIS_PLUGIN_ERR_SESSION` (-4) when a session model names a zone the plugin has no static string for. No Rust enum, `&str`, `Vec` or `Option` crosses by value: the session model is a struct of integers with `POLARIS_ROLL_*` and `POLARIS_CLOSURE_*` codes, strings are NUL-terminated, and arrays are a pointer with a count.

`polaris_adapter_stats` keeps the counters summed over every stream, as `AdapterStats` does: nothing outside tests reads them, and per-stream counters would cost an ABI version later.

### Venue and create config

The `venue` entry declares every field of a datafeed venue descriptor except how to build an adapter: name, tag, version, feed id, `soupbin_live`, streams and session model.

The host takes them once, before any adapter exists, as the tap and replay need them early ([[session#Config-driven venue selection]]). The streams are the adapter's `VenueAdapter::streams` in their order, so the first is the stream whose trades feed the session's bars. `create` takes NUL-terminated UTF-8 JSON: an object whose `venue_id` and `session_midnight_ns` the host sets from its deployment config, plus any key the plugin documents. [[crates/plugin/src/host.rs#Plugin#create]] refuses an adapter whose session model differs from the venue's.

## Strings and records

Strings and arrays a plugin hands out sit in its static storage while its library is loaded. The host does not unload a plugin library, so it reads them as `'static`.

`drain` hands out a pointer to an array of `polaris_record` and a count. The array and the record bytes belong to the handle and stay valid until the next `feed`, `drain` or `destroy` on it, which the header states. The export keeps three vectors per handle and reuses them: the events each `feed` drains from the adapter at once, the frame spans that give each event the stream of its frame, and the record array. No record costs an allocation once they have grown. The host copies each record out before its next call.

## Export

[[crates/plugin/src/export.rs#vtable]] builds the vtable from an `Export` impl, and the `export_adapter!` macro writes that impl and the `#[unsafe(no_mangle)]` entry from an adapter type, a `Venue` and a config decoder.

`Venue` holds the venue's strings as `&'static CStr`. The macro copies a zoned session's zone name into a NUL-terminated array at compile time, since `SessionModel` holds it as a `&'static str`. Each entry checks its pointers for NULL, runs its body under `catch_unwind`, and poisons the handle when the body panics: every later call on it returns `POLARIS_PLUGIN_ERR_PANIC`, and `destroy` still frees it, as `polaris-datafeed-ffi` does for a feed handle ([[c-abi#Panics and the ffi profile]]).

## Host wrapper

[[crates/plugin/src/host.rs#PluginAdapter]] is one plugin adapter as a `DynAdapter`. It reads the record types and session model once at create, calls the plugin per `feed` and per `drain`, and frees the handle on drop.

Every drained record goes through [[crates/plugin/src/host/decode.rs#event]] before it becomes a `MarketEvent`: the rtype must name a variant, the length must equal that record's size and its header's `length` words, and every enum field must hold one of its variants. A list of each enum's variants in an exhaustive match stops the build when md-events adds one. A record that fails, or a NULL or misaligned array, faults the adapter, so a bad record from a plugin is an error and never undefined behavior. The wrapper is `!Send`, as a handle stays on the thread that created it.

### Faults into the session

A plugin status other than success, or a record the host cannot rebuild, sets the adapter's fault, which `DynAdapter::fault` reports as a typed `AdapterFault`.

`Panicked` comes from `POLARIS_PLUGIN_ERR_PANIC`, `MalformedRecord` from a record that fails the checks, and `Status` from any other status. The first fault stays. After it the wrapper ignores every feed, drain and anchor, and `stats` returns zero counters once the plugin fails it. `DynAdapter::fault` defaults to `None`, so every in-tree adapter is unchanged. The session runner checks it once per frame batch, after feeding the batch and before routing it, and ends the session with `SessionError::Adapter`: the events drained before the fault route and publish first ([[tests#Two-stream test adapter#Adapter fault ends the session]]). `DynAdapter::stats` returns `AdapterStats` by value, because the wrapper copies the counters out of the plugin on each call and has none of its own to lend.

## Panics and the ffi profile

Catching a panic needs `panic = "unwind"`. A plugin's cdylib builds with the root `ffi` profile, which inherits `release` and unwinds; built with `abort`, a plugin panic ends the host process.

The header states this on `POLARIS_PLUGIN_ERR_PANIC` and the crate README gives the build command.

## Header

The plugin ABI is part of the generated `polaris_abi.h` of `polaris-datafeed-ffi`, under the same drift gate as the embedding API ([[tests#C ABI from a C host#Header drift]]).

cbindgen reads this crate's `src/abi.rs` beside `polaris-datafeed-ffi`'s `src/lib.rs`, and the config lists the vtable, the entry type and the record header, which no embedding function reaches. `polaris_record_header` is the C view of md-events `RecordHeader`. [[crates/plugin/src/abi.rs#PolarisRecordHeader]] const-asserts its size and every field offset against `RecordHeader` in Rust, and the header closes with a typedef whose array size is negative unless `sizeof(polaris_record_header)` is 56. C99 has no `_Static_assert`, and the typedef compiles under GCC, Clang and MSVC in the strict modes of [[tests#C ABI from a C host#C harness]]. cbindgen writes its trailer after its own include guard, and C99 refuses a typedef defined twice, so the config writes the guard by hand around the check.

## Tests

The library tests run the export and the host wrapper over a test plugin of the test binary itself, and an integration test runs NASDAQ through both against the native adapter.

The test plugin's adapter emits one `Control` per frame on two streams, declares a zoned session with a weekend closure, panics on the payload `panic`, and emits a record whose header length is 0 on the payload `bad`. Its entry symbol exists only in the test binary. CI runs the library tests under Miri in the `miri-datafeed-ffi` job: they call the entries from Rust, with no dlopen and no C.

### Record rebuild

Each of the ten `MarketEvent` record types, built as bytes with a pattern and valid enum values, rebuilds into an event whose own bytes equal the image.

[[crates/plugin/src/host/decode/tests.rs#each_record_type_rebuilds_byte_for_byte]] fails when an rtype maps to the wrong record or size. `a_record_of_another_length_or_rtype_is_refused` refuses a record one word short, one word long with its header agreeing, a header length one word short, a derived record type and slices too short for a header.

### Enum fields are checked

Every enum field of every record type, set to a value of no variant, makes the record refused.

[[crates/plugin/src/host/decode/tests.rs#an_enum_field_outside_its_variants_is_refused]] fails when a check is missing from the decoder, which would otherwise read an invalid enum into a typed record.

### Drain buffer

A drain hands out the records of every frame fed since the last drain, each with its frame's stream, and a second drain hands out none.

[[crates/plugin/src/tests.rs#a_drain_hands_out_the_frames_fed_since_the_last_one]] feeds streams 0, 1 and an undeclared 2, then drains, drains again, feeds once more and drains. The venue and session conversion cases sit beside it: `the_venue_and_session_cross_as_declared` and `every_session_model_crosses_unchanged`, which also refuses an unknown roll or closure code and a zone other than the declared one.

### Panic poisons the handle

A panic inside `feed` returns `POLARIS_PLUGIN_ERR_PANIC`, every later call on the handle returns it too, and `destroy` still frees it.

[[crates/plugin/src/tests.rs#a_panic_poisons_the_handle]] calls the raw entries. Without `catch_unwind` the test aborts, and without the poison flag the next feed returns success.

### Faults reach the host adapter

Through the host wrapper, a panic and a malformed record each set the typed fault after the events drained before them, and nothing is emitted after it.

[[crates/plugin/src/tests.rs#a_panic_faults_the_host_adapter]] expects `AdapterFault::Panicked` and zero counters from the poisoned handle, and `a_malformed_record_faults_the_host_adapter` expects `MalformedRecord` with the `Control` rtype and length after one good event. `the_host_adapter_runs_the_plugin_adapter` checks the events, counters and anchor against the same adapter run natively, `null_pointers_return_the_null_status` passes NULL to every pointer argument, and `a_refused_config_returns_the_config_status` refuses invalid and non-UTF-8 configs.

### Version checked first

A vtable of another ABI version is refused with both versions named, before any other field is read.

[[crates/plugin/src/tests.rs#another_abi_version_is_refused_before_any_entry_is_read]] leaves every entry NULL, so a host that read past `abi_version` would report a missing entry instead. `a_null_entry_is_refused_at_load` expects `MissingEntry` for a NULL `destroy`.

### NASDAQ parity

The NASDAQ adapter exported through `export_adapter!` and run through `PluginAdapter` emits the same events, counters, record types and session as the same adapter boxed as a native `DynAdapter`.

[[crates/plugin/tests/nasdaq_plugin.rs#nasdaq_through_the_plugin_matches_the_native_adapter]] feeds the venue crate's ITCH prefix fixture (14,348 frames of definitions, trading states and a system event), then the order-book reference fixture's 34 book messages, and sets a New York anchor on both adapters between them. It compares every event in order and requires every frame decoded and order events present. `the_plugin_declares_the_nasdaq_venue` checks the venue fields the plugin hands the host.

## Bench

`benches/plugin_path.rs` times NASDAQ through the export and the host wrapper against the native boxed adapter, in ns per record, over 33 rounds that alternate which pass goes first.

Each pass feeds one frame and drains it into a reused vector, as the session runner does. It runs the order-book reference fixture repeated 1,000 times and the ITCH prefix fixture apart, and prints each pass's median and the median paired difference. The plugin path adds an indirect call per feed and per drain, the plugin's copy of each event into its handle, and the host's checks and rebuild. No timing is recorded yet: the bench was built and not run, because the development host was on battery.
