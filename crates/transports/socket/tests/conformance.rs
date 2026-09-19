//! Conformance suite on `UdpSocket`: datagram contract, exhaustion
//! signalled as `PoolExhausted`.

mod support;

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
};

use transport_core::{
    DatagramRecv,
    testing::conformance::{DatagramHarness, ExhaustionSignal, run_datagram},
};
use transport_socket::UdpSocket;

// fresh loopback receiver per case, wrapped by `wrap`; one std sender injects
fn datagram_contract<T: DatagramRecv>(
    mut wrap: impl FnMut(UdpSocket) -> T,
    addr: impl Fn(&T) -> SocketAddr,
) {
    let sender = support::sender();
    run_datagram(DatagramHarness {
        build: |slabs: NonZeroU32| {
            let slabs = NonZeroUsize::try_from(slabs).expect("slab count fits usize");
            wrap(support::receiver(slabs))
        },
        inject: |t: &mut T, bytes: &[u8]| {
            sender.send_to(bytes, addr(t)).expect("loopback send");
        },
        // sockets report exhaustion as error, count no drops
        drops: |_: &T| 0,
        exhaustion: ExhaustionSignal::PoolExhausted,
    });
}

#[test]
fn udp_socket_meets_datagram_contract() {
    datagram_contract(|s| s, |t| t.local_addr().expect("local addr"));
}
