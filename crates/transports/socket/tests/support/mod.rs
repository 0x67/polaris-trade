//! Loopback sockets shared by integration tests.

use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket},
    num::NonZeroUsize,
};

use transport_socket::{UdpConfig, UdpSocket};

/// Loopback receiver on ephemeral port with `slabs` receive slabs.
pub fn receiver(slabs: NonZeroUsize) -> UdpSocket {
    let mut cfg = UdpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    cfg.slab_count = slabs;
    UdpSocket::bind(&cfg).expect("bind loopback receiver")
}

/// Plain blocking std socket for sending at receivers.
pub fn sender() -> StdUdpSocket {
    StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind loopback sender")
}
