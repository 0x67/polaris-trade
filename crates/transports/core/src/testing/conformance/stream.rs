//! Stream cases: empty and idle reads, ordered bytes, peer close, partial and whole writes.

use std::{
    io::{self, Read, Write},
    mem::MaybeUninit,
    net::{Shutdown, TcpStream},
    panic,
    thread::{self, JoinHandle},
};

use super::{DEADLINE, assert_bytes, pattern, poll_until};
use crate::{AsyncReady, StreamRecv, StreamSend, StreamTrySend, TransportError};

// several reads per stream, so order across reads is checked
const READ_CHUNK: usize = 16 * 1024;
const ORDER_LEN: usize = 256 * 1024;
const ORDER_SEED: u64 = 401;
// fits kernel send buffer, so peer writes it without thread
const TAIL_LEN: usize = 1024;
const TRY_SEND_LEN: usize = 4 * 1024 * 1024;
const SEND_ALL_LEN: usize = 8 * 1024 * 1024;

/// Run every sync stream case, each on fresh `pair()`.
///
/// `pair` yields transport connected to blocking [`TcpStream`] peer. Cases:
/// empty and idle reads, ordered bytes, `PeerClosed` after peer shuts down
/// writing, `try_send` resumed until whole buffer arrives.
///
/// # Panics
///
/// On any contract violation, or when awaited progress does not show within 5 s.
pub fn run_stream<T, P>(mut pair: P)
where
    T: StreamRecv + StreamTrySend,
    P: FnMut() -> (T, TcpStream),
{
    stream_idle(pair());
    stream_order(pair());
    stream_closed(pair());
    stream_try_send(pair());
}

/// Run every async stream case, each on fresh `pair().await`.
///
/// `pair` as in [`run_stream`]. Awaits only transport futures, so caller
/// supplies runtime and overall timeout; peers run on threads. Cases: empty
/// and idle reads, ordered bytes with progress after each `ready()`,
/// `PeerClosed`, `send_all` of 8 MiB.
///
/// # Panics
///
/// On any contract violation, or when peer thread I/O stalls for 5 s.
pub async fn run_stream_async<T, P, F>(mut pair: P)
where
    T: StreamRecv + StreamSend + AsyncReady,
    P: FnMut() -> F,
    F: Future<Output = (T, TcpStream)>,
{
    stream_idle(pair().await);
    stream_order_async(pair().await).await;
    stream_closed_async(pair().await).await;
    stream_send_all(pair().await).await;
}

// open stream with nothing sent reads `Ok(0)`, never `PeerClosed`
fn stream_idle<T: StreamRecv>((mut t, _peer): (T, TcpStream)) {
    assert_idle(&mut t, "stream idle");
}

fn stream_order<T: StreamRecv>((mut t, peer): (T, TcpStream)) {
    const CASE: &str = "stream order";
    let writer = spawn_writer(peer, pattern(ORDER_SEED, ORDER_LEN));
    let mut got = Vec::with_capacity(ORDER_LEN);
    while got.len() < ORDER_LEN {
        let max = READ_CHUNK.min(ORDER_LEN - got.len());
        poll_until(CASE, "stream bytes", || {
            match recv_append(&mut t, &mut got, max, CASE) {
                Ok(0) => None,
                Ok(_) => Some(()),
                Err(e) => panic!("{CASE}: recv_into failed after {} bytes: {e}", got.len()),
            }
        });
    }
    assert_order_done(&mut t, &got, writer, CASE);
}

async fn stream_order_async<T: StreamRecv + AsyncReady>((mut t, peer): (T, TcpStream)) {
    const CASE: &str = "stream order async";
    let writer = spawn_writer(peer, pattern(ORDER_SEED, ORDER_LEN));
    let mut got = Vec::with_capacity(ORDER_LEN);
    while got.len() < ORDER_LEN {
        wait_ready(&mut t, CASE).await;
        let max = READ_CHUNK.min(ORDER_LEN - got.len());
        match recv_append(&mut t, &mut got, max, CASE) {
            Ok(0) => panic!("{CASE}: ready() resolved, then recv_into returned Ok(0)"),
            Ok(_) => {}
            Err(e) => panic!("{CASE}: recv_into failed after {} bytes: {e}", got.len()),
        }
    }
    assert_order_done(&mut t, &got, writer, CASE);
}

// data drains first, then `PeerClosed`; endless `Ok(0)` times out
fn stream_closed<T: StreamRecv>((mut t, mut peer): (T, TcpStream)) {
    const CASE: &str = "stream peer closed";
    let tail = pattern(402, TAIL_LEN);
    send_then_shutdown(&mut peer, &tail, CASE);
    let mut got = Vec::with_capacity(TAIL_LEN);
    poll_until(CASE, "PeerClosed", || {
        match recv_append(&mut t, &mut got, READ_CHUNK, CASE) {
            Ok(_) => {
                assert_within_tail(got.len(), CASE);
                None
            }
            Err(TransportError::PeerClosed) => Some(()),
            Err(e) => panic!("{CASE}: recv_into failed after {} bytes: {e}", got.len()),
        }
    });
    assert_bytes(&got, &tail, CASE, "bytes before close");
}

async fn stream_closed_async<T: StreamRecv + AsyncReady>((mut t, mut peer): (T, TcpStream)) {
    const CASE: &str = "stream peer closed async";
    let tail = pattern(402, TAIL_LEN);
    send_then_shutdown(&mut peer, &tail, CASE);
    let mut got = Vec::with_capacity(TAIL_LEN);
    // each pass reads at least one byte of bounded tail, or ends
    loop {
        wait_ready(&mut t, CASE).await;
        match recv_append(&mut t, &mut got, READ_CHUNK, CASE) {
            Ok(0) => panic!("{CASE}: ready() resolved, then recv_into returned Ok(0)"),
            Ok(_) => assert_within_tail(got.len(), CASE),
            Err(TransportError::PeerClosed) => break,
            Err(e) => panic!("{CASE}: recv_into failed after {} bytes: {e}", got.len()),
        }
    }
    assert_bytes(&got, &tail, CASE, "bytes before close");
}

fn stream_try_send<T: StreamTrySend>((mut t, peer): (T, TcpStream)) {
    const CASE: &str = "stream try_send";
    let reader = spawn_reader(peer, TRY_SEND_LEN);
    let data = pattern(403, TRY_SEND_LEN);
    let mut sent = 0;
    while sent < data.len() {
        let rest = &data[sent..];
        let n = poll_until(CASE, "try_send progress", || match t.try_send(rest) {
            Ok(0) => None,
            Ok(n) => Some(n),
            Err(e) => panic!("{CASE}: try_send failed after {sent} bytes: {e}"),
        });
        assert!(
            n <= rest.len(),
            "{CASE}: try_send reported {n} bytes of {} offered",
            rest.len()
        );
        sent += n;
    }
    assert_bytes(&join_peer(reader, CASE), &data, CASE, "peer received");
}

async fn stream_send_all<T: StreamSend>((mut t, peer): (T, TcpStream)) {
    const CASE: &str = "stream send_all";
    let reader = spawn_reader(peer, SEND_ALL_LEN);
    let data = pattern(404, SEND_ALL_LEN);
    if let Err(e) = t.send_all(&data).await {
        panic!("{CASE}: send_all failed: {e}");
    }
    // blocking join is safe: reader now waits on kernel buffers only
    assert_bytes(&join_peer(reader, CASE), &data, CASE, "peer received");
}

// one `recv_into` landing past `buf`'s end, at most `max` bytes
fn recv_append<T: StreamRecv>(
    t: &mut T,
    buf: &mut Vec<u8>,
    max: usize,
    case: &str,
) -> Result<usize, TransportError> {
    buf.reserve(max);
    let n = t.recv_into(&mut buf.spare_capacity_mut()[..max])?;
    assert!(
        n <= max,
        "{case}: recv_into reported {n} bytes into {max}-byte destination"
    );
    // SAFETY: `StreamRecv` contract: `Ok(n)` initialised first `n` spare slots;
    // `n <= max` checked and `reserve(max)` left capacity for `len + max`.
    unsafe { buf.set_len(buf.len() + n) };
    Ok(n)
}

fn assert_idle<T: StreamRecv>(t: &mut T, case: &str) {
    let empty = t.recv_into(&mut []);
    assert!(
        matches!(empty, Ok(0)),
        "{case}: recv_into on empty destination returned {empty:?}, want Ok(0)"
    );
    let mut probe = [MaybeUninit::<u8>::uninit(); 64];
    let idle = t.recv_into(&mut probe);
    assert!(
        matches!(idle, Ok(0)),
        "{case}: recv_into on idle open stream returned {idle:?}, want Ok(0)"
    );
}

fn assert_within_tail(read: usize, case: &str) {
    assert!(
        read <= TAIL_LEN,
        "{case}: read {read} bytes, peer sent {TAIL_LEN}"
    );
}

// every byte read: peer finished, bytes match, stream still open with nothing extra
fn assert_order_done<T: StreamRecv>(
    t: &mut T,
    got: &[u8],
    writer: JoinHandle<io::Result<TcpStream>>,
    case: &str,
) {
    let _peer = join_peer(writer, case);
    assert_bytes(got, &pattern(ORDER_SEED, ORDER_LEN), case, "stream bytes");
    assert_idle(t, case);
}

async fn wait_ready<T: AsyncReady>(t: &mut T, case: &str) {
    if let Err(e) = t.ready().await {
        panic!("{case}: ready() failed: {e}");
    }
}

// FIN follows data; peer stays open, so close is half-close, not reset
fn send_then_shutdown(peer: &mut TcpStream, bytes: &[u8], case: &str) {
    peer.write_all(bytes)
        .and_then(|()| peer.shutdown(Shutdown::Write))
        .unwrap_or_else(|e| panic!("{case}: peer write then shutdown failed: {e}"));
}

// peer writes on own thread, handing back its still-open stream
fn spawn_writer(mut peer: TcpStream, bytes: Vec<u8>) -> JoinHandle<io::Result<TcpStream>> {
    thread::spawn(move || {
        peer.set_write_timeout(Some(DEADLINE))?;
        peer.write_all(&bytes)?;
        Ok(peer)
    })
}

fn spawn_reader(mut peer: TcpStream, len: usize) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        peer.set_read_timeout(Some(DEADLINE))?;
        let mut got = vec![0; len];
        peer.read_exact(&mut got)?;
        Ok(got)
    })
}

fn join_peer<R>(peer: JoinHandle<io::Result<R>>, case: &str) -> R {
    match peer.join() {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => panic!("{case}: peer I/O failed: {e}"),
        Err(payload) => panic::resume_unwind(payload),
    }
}
