use std::{
    collections::VecDeque,
    io,
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
};

use socket2::SockAddr;
use transport_core::{FrameBatch, TransportError, pool::VecPool};

use super::{UdpFrame, burst};

const PEER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9);

// scripted socket: each recv call pops next outcome
fn script(
    steps: &mut VecDeque<io::Result<&'static [u8]>>,
) -> impl FnMut(&mut [MaybeUninit<u8>]) -> io::Result<(usize, SockAddr)> {
    |buf| {
        let payload = steps.pop_front().expect("script exhausted")?;
        for (slot, byte) in buf.iter_mut().zip(payload) {
            slot.write(*byte);
        }
        Ok((payload.len(), SockAddr::from(PEER)))
    }
}

fn run(
    pool: &VecPool,
    out: &mut FrameBatch<UdpFrame>,
    deferred: &mut Option<TransportError>,
    steps: &mut VecDeque<io::Result<&'static [u8]>>,
    peek: io::ErrorKind,
) -> Result<usize, TransportError> {
    burst("test", pool, out, deferred, script(steps), || {
        Err(peek.into())
    })
}

fn kind(result: &Result<usize, TransportError>) -> Option<io::ErrorKind> {
    match result {
        Err(TransportError::Io {
            stage: "recv_from",
            error,
        }) => Some(error.kind()),
        _ => None,
    }
}

#[test]
fn error_after_frames_waits_one_call_and_returns_once() {
    let pool = VecPool::new(
        NonZeroUsize::new(8).unwrap(),
        NonZeroUsize::new(64).unwrap(),
    )
    .unwrap();
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(8).unwrap());
    let mut deferred = None;
    let refused = io::ErrorKind::ConnectionRefused;
    let mut steps = VecDeque::from([
        Ok(&b"one"[..]),
        Ok(&b"two"[..]),
        Err(refused.into()),
        Err(io::ErrorKind::PermissionDenied.into()),
        Err(io::ErrorKind::WouldBlock.into()),
    ]);
    let block = io::ErrorKind::WouldBlock;

    let first = run(&pool, &mut out, &mut deferred, &mut steps, block);
    assert!(
        matches!(first, Ok(2)),
        "frames first, error held: {first:?}"
    );
    // held error returns before any syscall: script untouched
    let second = run(&pool, &mut out, &mut deferred, &mut steps, block);
    assert_eq!(kind(&second), Some(refused), "held error: {second:?}");
    assert_eq!(steps.len(), 2, "held error returned without recv call");
    // nothing pushed: error returns at once, not held
    let third = run(&pool, &mut out, &mut deferred, &mut steps, block);
    assert_eq!(
        kind(&third),
        Some(io::ErrorKind::PermissionDenied),
        "{third:?}"
    );
    let fourth = run(&pool, &mut out, &mut deferred, &mut steps, block);
    assert!(
        matches!(fourth, Ok(0)),
        "no error returned twice: {fourth:?}"
    );
    let got: Vec<_> = out.drain().map(|f| f.as_ref().to_vec()).collect();
    assert_eq!(got, [b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn empty_pool_on_idle_socket_is_idle_not_exhausted() {
    let pool = VecPool::new(NonZeroUsize::MIN, NonZeroUsize::new(64).unwrap()).unwrap();
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(4).unwrap());
    let mut deferred = None;
    let mut steps = VecDeque::from([Ok(&b"held"[..])]);
    let first = run(
        &pool,
        &mut out,
        &mut deferred,
        &mut steps,
        io::ErrorKind::WouldBlock,
    );
    assert!(matches!(first, Ok(1)), "{first:?}");

    // only slab held in `out`; peek says idle
    let idle = run(
        &pool,
        &mut out,
        &mut deferred,
        &mut steps,
        io::ErrorKind::WouldBlock,
    );
    assert!(matches!(idle, Ok(0)), "idle socket, empty pool: {idle:?}");
}

#[test]
fn pool_running_dry_after_push_keeps_frame_and_queued_datagram() {
    let pool = VecPool::new(NonZeroUsize::MIN, NonZeroUsize::new(64).unwrap()).unwrap();
    let mut out = FrameBatch::with_capacity(NonZeroUsize::new(2).unwrap());
    let mut deferred = None;
    let mut steps = VecDeque::from([Ok(&b"one"[..]), Ok(&b"two"[..])]);
    // second datagram stays queued throughout, so peek reports it
    let mut call = |out: &mut FrameBatch<UdpFrame>| {
        burst(
            "test",
            &pool,
            out,
            &mut deferred,
            script(&mut steps),
            || Ok(()),
        )
    };

    let first = call(&mut out);
    assert!(matches!(first, Ok(1)), "frame kept, no error: {first:?}");
    let got: Vec<_> = out.drain().map(|f| f.as_ref().to_vec()).collect();
    assert_eq!(got, [b"one".to_vec()]);

    // dropped frame freed only slab: queued datagram lands in it
    let second = call(&mut out);
    assert!(matches!(second, Ok(1)), "{second:?}");
    let got: Vec<_> = out.drain().map(|f| f.as_ref().to_vec()).collect();
    assert_eq!(got, [b"two".to_vec()]);
}
