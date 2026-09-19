//! Conformance suite: one receive and send contract every backend proves once.
//!
//! [`run_datagram`] drives a [`DatagramRecv`](crate::DatagramRecv) through a
//! [`DatagramHarness`]; [`run_stream`] and [`run_stream_async`] drive a stream
//! against a plain blocking [`TcpStream`](std::net::TcpStream) peer. Each case
//! builds a fresh transport (reclaim continues on exhaustion's), so one failure
//! never leaks into an unrelated case.
//! Violations panic naming case and observation. Sync waits give up after 5 s;
//! async waits rely on caller timeout.
//!
//! # Datagram contract
//!
//! - payload bytes and order equal what was injected; frames stay unchanged
//!   while later bursts run; `Ok(0)` means idle;
//! - each `recv_burst` pushes at most `spare()` frames, returns exactly count
//!   pushed, and never fails after pushing;
//! - `build(2)` with both frames held: third datagram yields
//!   [`PoolExhausted`](crate::TransportError::PoolExhausted) (`in_use == capacity == 2`)
//!   and stays queued, or raises `drops`, as [`ExhaustionSignal`] declares;
//! - frames dropped on any thread free their buffers for new datagrams;
//! - `pool_stats`: `capacity` equals `build` argument; `in_use` never exceeds
//!   it and counts at least every frame caller holds; once caller drops them,
//!   `in_use` returns to its value before they were received, at once or
//!   after next `recv_burst` (drivers recycling on reap).
//!
//! # Stream contract
//!
//! `recv_into` returns `Ok(0)` for empty destination and idle open stream,
//! delivers bytes in order, and ends in `PeerClosed` once peer stops writing.
//! `try_send` resumed from its count delivers whole buffer; so does
//! `send_all`. After `ready()` resolves, next `recv_into` makes progress.

mod datagram;
mod stream;

use std::{
    thread,
    time::{Duration, Instant},
};

pub use datagram::{DatagramHarness, ExhaustionSignal, run_datagram};
pub use stream::{run_stream, run_stream_async};

// bound on every sync wait and peer-thread I/O; roomy for loaded CI hosts
const DEADLINE: Duration = Duration::from_secs(5);
const RETRY_PAUSE: Duration = Duration::from_micros(200);

// deterministic bytes, distinct per seed, so slot mix-up or stale buffer never matches
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    // xorshift64; odd start keeps state non-zero
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let take = (len - out.len()).min(8);
        out.extend_from_slice(&state.to_le_bytes()[..take]);
    }
    out
}

// retry until `attempt` yields or `DEADLINE` passes; pauses only after a miss
fn poll_until<R>(case: &str, what: &str, mut attempt: impl FnMut() -> Option<R>) -> R {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(r) = attempt() {
            return r;
        }
        assert!(
            Instant::now() < deadline,
            "{case}: no {what} within {DEADLINE:?}"
        );
        thread::sleep(RETRY_PAUSE);
    }
}

// reports lengths and first differing offset, never megabytes of bytes
fn assert_bytes(got: &[u8], want: &[u8], case: &str, what: &str) {
    if got != want {
        let at = got.iter().zip(want).position(|(g, w)| g != w);
        let at = at.unwrap_or(got.len().min(want.len()));
        panic!(
            "{case}: {what}: got {} bytes, want {}, first difference at byte {at}",
            got.len(),
            want.len()
        );
    }
}
