use std::{ffi::CString, ptr};

use super::{
    POLARIS_ERR_CONFIG, POLARIS_ERR_NULL, POLARIS_ERR_PANIC, POLARIS_ERR_SOURCE, POLARIS_OK,
    PolarisFeed, PolarisRecord, polaris_feed_close, polaris_feed_open, polaris_feed_poll,
    with_feed,
};

const PREFIX: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../venues/nasdaq/tests/fixtures/nasdaq_itch_v5_prefix.bin"
);
const CAP: usize = 8;

fn replay_config(path: &str) -> CString {
    let json = serde_json::json!({
        "venue": "nasdaq",
        "venue_id": 2,
        "session_midnight_ns": 0,
        "source": {"kind": "replay", "path": path},
    });
    CString::new(json.to_string()).expect("a JSON document has no NUL")
}

fn open(config: &CString) -> (i32, *mut PolarisFeed) {
    let mut feed = ptr::dangling_mut();
    // SAFETY: `config` is NUL-terminated and `feed` is a local.
    let status = unsafe { polaris_feed_open(config.as_ptr(), &raw mut feed) };
    (status, feed)
}

/// Polls `feed` into `out`'s spare capacity and returns the status and count.
fn poll(feed: *mut PolarisFeed, out: &mut Vec<PolarisRecord>) -> (i32, usize) {
    let mut written = usize::MAX;
    // SAFETY: `out` has room for `CAP` records and `written` is a local.
    let status = unsafe { polaris_feed_poll(feed, out.as_mut_ptr(), CAP, &raw mut written) };
    (status, written)
}

#[test]
fn null_pointers_return_the_null_status() {
    let config = replay_config(PREFIX);
    let mut feed = ptr::dangling_mut::<PolarisFeed>();
    // SAFETY: a NULL config is the case under test; `feed` is a local.
    let status = unsafe { polaris_feed_open(ptr::null(), &raw mut feed) };
    assert_eq!(status, POLARIS_ERR_NULL);
    assert!(feed.is_null(), "a failed open leaves NULL in *out");
    // SAFETY: a NULL out is the case under test.
    let status = unsafe { polaris_feed_open(config.as_ptr(), ptr::null_mut()) };
    assert_eq!(status, POLARIS_ERR_NULL);

    let mut out = Vec::with_capacity(CAP);
    assert_eq!(poll(ptr::null_mut(), &mut out), (POLARIS_ERR_NULL, 0));
    let (status, feed) = open(&config);
    assert_eq!(status, POLARIS_OK);
    let mut written = usize::MAX;
    // SAFETY: `feed` is open; a NULL out is the case under test.
    let status = unsafe { polaris_feed_poll(feed, ptr::null_mut(), CAP, &raw mut written) };
    assert_eq!((status, written), (POLARIS_ERR_NULL, 0));
    // SAFETY: `feed` is open and `out` has room for `CAP`; a NULL written is
    // the case under test.
    let status = unsafe { polaris_feed_poll(feed, out.as_mut_ptr(), CAP, ptr::null_mut()) };
    assert_eq!(status, POLARIS_ERR_NULL);

    // SAFETY: `feed` is open and closed once.
    assert_eq!(unsafe { polaris_feed_close(feed) }, POLARIS_OK);
    // SAFETY: NULL is the case under test.
    assert_eq!(unsafe { polaris_feed_close(ptr::null_mut()) }, POLARIS_OK);
}

#[test]
fn open_returns_the_status_of_each_refusal() {
    let not_utf8 = CString::new(vec![0xff, 0xfe]).expect("no NUL");
    let not_json = CString::new("{").expect("no NUL");
    let unknown_venue = CString::new(
        r#"{"venue":"nowhere","venue_id":2,"session_midnight_ns":0,
            "source":{"kind":"replay","path":"x.bin"}}"#,
    )
    .expect("no NUL");
    for config in [&not_utf8, &not_json, &unknown_venue] {
        assert_eq!(open(config), (POLARIS_ERR_CONFIG, ptr::null_mut()));
    }
    let missing = replay_config(concat!(env!("CARGO_MANIFEST_DIR"), "/no-such-capture.bin"));
    assert_eq!(open(&missing), (POLARIS_ERR_SOURCE, ptr::null_mut()));
}

// The guard turns a panic in a handle call into a status, and the handle then
// hands out nothing, since its engine state may be half updated.
#[test]
fn a_caught_panic_poisons_the_handle() {
    let (status, feed) = open(&replay_config(PREFIX));
    assert_eq!(status, POLARIS_OK);
    let mut out = Vec::with_capacity(CAP);
    let (status, written) = poll(feed, &mut out);
    assert!(
        status == POLARIS_OK && written > 0,
        "the healthy handle polls"
    );

    // SAFETY: `feed` is open and nothing else holds it.
    let status = unsafe { with_feed(feed, |_| panic!("deliberate panic in a handle call")) };
    assert_eq!(status, POLARIS_ERR_PANIC);
    assert_eq!(poll(feed, &mut out), (POLARIS_ERR_PANIC, 0));
    // SAFETY: `feed` is open and closed once.
    let status = unsafe { polaris_feed_close(feed) };
    assert_eq!(
        status, POLARIS_ERR_PANIC,
        "a poisoned handle closes unfinished"
    );
}
