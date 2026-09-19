//! Sync `UdpSocket` edges beyond conformance suite: bind failure keeps OS
//! error, `send_to` never blocks and fails only with kind `WouldBlock`, frames
//! carry real sender address, multicast join reaches kernel.

mod support;

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use transport_core::{
    DatagramRecv, DatagramSend, FrameBatch, Multicast, MulticastInterface, TransportError,
};
use transport_socket::{UdpConfig, UdpSocket};

#[test]
fn bind_of_port_in_use_is_bind_error_with_os_text() {
    let holder = support::receiver(NonZeroUsize::MIN);
    let addr = holder.local_addr().expect("local addr");

    let err = UdpSocket::bind(&UdpConfig::new(addr)).expect_err("second bind on held port");
    match &err {
        TransportError::Bind { addr: at, error } => {
            assert_eq!(*at, addr);
            assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "{error}");
            let text = err.to_string();
            assert!(
                text.contains(&error.to_string()),
                "OS error missing from {text:?}"
            );
        }
        other => panic!("got {other:?}, want Bind AddrInUse"),
    }
}

#[test]
fn send_to_flood_never_blocks_and_fails_only_with_would_block() {
    const FLOOD: usize = 50_000;
    const DEADLINE: Duration = Duration::from_secs(20);

    // receiver never reads, so queues fill
    let receiver = support::receiver(NonZeroUsize::MIN);
    let to = receiver.local_addr().expect("local addr");
    let mut cfg = UdpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    cfg.send_buf = NonZeroU32::new(4096);
    let mut tx = UdpSocket::bind(&cfg).expect("bind sender");

    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let payload = [0x5a; 1024];
        let mut would_block = 0_usize;
        for _ in 0..FLOOD {
            match tx.send_to(&payload, to) {
                Ok(_) => {}
                Err(TransportError::Io {
                    stage: "send_to",
                    error,
                }) if error.kind() == io::ErrorKind::WouldBlock => would_block += 1,
                Err(other) => {
                    let _ = done.send(Err(other));
                    return;
                }
            }
        }
        let _ = done.send(Ok(would_block));
    });
    match result.recv_timeout(DEADLINE) {
        Ok(Ok(_would_block)) => {}
        Ok(Err(err)) => panic!("send_to failed with non-WouldBlock error: {err}"),
        Err(e) => panic!("{FLOOD} sends not done within {DEADLINE:?} ({e}): send_to blocked"),
    }
    drop(receiver);
}

#[test]
fn frame_carries_sender_address() {
    let mut rx = support::receiver(NonZeroUsize::new(4).unwrap());
    let sender = support::sender();
    sender
        .send_to(b"who", rx.local_addr().expect("local addr"))
        .expect("send");
    let mut batch = FrameBatch::with_capacity(NonZeroUsize::MIN);
    let deadline = Instant::now() + Duration::from_secs(5);
    while rx.recv_burst(&mut batch).expect("recv") == 0 {
        assert!(Instant::now() < deadline, "datagram never arrived");
        thread::yield_now();
    }
    let frame = batch.drain().next().expect("one frame");
    assert_eq!(frame.as_ref(), b"who");
    assert_eq!(frame.peer(), sender.local_addr().expect("sender addr"));
}

// loopback delivers group datagrams even without membership, so receipt proves
// nothing; kernel refusing second join proves first one registered
#[cfg(unix)]
#[test]
fn second_join_of_same_group_is_refused_by_kernel() {
    let mut rx = UdpSocket::bind(&UdpConfig::new(SocketAddr::from((
        Ipv4Addr::UNSPECIFIED,
        0,
    ))))
    .expect("bind receiver");
    let group = Ipv4Addr::new(239, 255, 73, 91).into();
    let loopback = MulticastInterface {
        v4: Some(Ipv4Addr::LOCALHOST),
        ..MulticastInterface::default()
    };
    rx.join_multicast(group, loopback).expect("first join");
    let again = rx.join_multicast(group, loopback);
    assert!(
        matches!(
            again,
            Err(TransportError::Io {
                stage: "join_multicast",
                ..
            })
        ),
        "second join: {again:?}"
    );
}

#[test]
fn interface_of_other_family_is_invalid_config() {
    let mut rx = support::receiver(NonZeroUsize::MIN);
    let v6_scope = MulticastInterface {
        v6_scope_id: Some(1),
        ..MulticastInterface::default()
    };
    let v4 = rx.join_multicast(Ipv4Addr::new(239, 255, 73, 92).into(), v6_scope);
    assert!(
        matches!(
            v4,
            Err(TransportError::InvalidConfig {
                field: "iface.v6_scope_id",
                ..
            })
        ),
        "IPv4 group, IPv6 scope: {v4:?}"
    );
    let v4_iface = MulticastInterface {
        v4: Some(Ipv4Addr::LOCALHOST),
        ..MulticastInterface::default()
    };
    let v6 = rx.join_multicast("ff15::7391".parse().expect("group"), v4_iface);
    assert!(
        matches!(
            v6,
            Err(TransportError::InvalidConfig {
                field: "iface.v4",
                ..
            })
        ),
        "IPv6 group, IPv4 interface: {v6:?}"
    );
}
