# client_moldudp

MoldUDP64 client: wire codec, sequence reassembler, gap re-requests and A/B arbitration, over caller-built legs of any datagram transport.

## Legs and construction

`from_legs` on [[crates/clients/moldudp/src/receiver/mod.rs#MoldUdpReceiver]] takes legs the caller already bound and joined, so the receiver needs datagram receive only and runs on socket and kernel-bypass backends alike.

The config keeps session, sequence and gap-timing fields; socket options live on the legs. Each leg's pool must hold [[crates/clients/moldudp/src/receiver/mod.rs#MIN_LEG_POOL_CAPACITY]] buffers (reorder window plus one burst), read through `pool_stats`, else construction fails with `PoolTooSmall`. Two or more legs run the [[crates/clients/moldudp/src/ab.rs#AbArbiter]]: first arrival wins, and a gap is confirmed only after every leg missed it for the confirm window.

## Receive path

`poll` ([[crates/clients/moldudp/src/receiver/recv.rs]]) is synchronous: it returns `None` only after every leg, and the requester while a gap is pending, returned nothing in that call.

That contract lets a caller park on an edge-triggered `ReadySet` after `None`. In-order datagrams drain inline, borrowed from the still-owned frame with no allocation; a datagram ahead of the expected sequence becomes one shared `Arc`, and its messages wait in the [[crates/clients/moldudp/src/reassembly.rs#SequenceReassembler]]. Async `recv` and `recv_owned` exist only when legs (and the requester) implement `AsyncReady`; they spin the same path, then wait on every leg.

## Gap recovery

Recovery is type state: [[crates/clients/moldudp/src/receiver/recovery.rs#NoRecovery]] carries nothing, [[crates/clients/moldudp/src/receiver/recovery.rs#Requester]] owns a unicast socket attached by `with_requester`.

MoldUDP64 servers unicast a retransmission back to the request's source, so the socket that sends a re-request must also be read. `poll` sends due requests through [[crates/clients/moldudp/src/gap.rs#GapRequestEmitter#emit]] (rate-limited per gap, clock read only while a gap is pending; a full socket buffer retries next call) and reads the requester while gaps are pending. Each retransmitted datagram is copied into receiver memory and its slab returned at once, so the requester's frame type is unrelated to the legs'; public types stay parameterised by the leg frame alone. Retransmitted messages report stream id equal to the leg count, which the arbiter treats as window-only.

## Frames

[[crates/clients/moldudp/src/frame.rs#Frame]] and [[crates/clients/moldudp/src/frame.rs#OwnedFrame]] carry inherent `sequence()` and `stream_id()` and expose bytes through `AsRef<[u8]>`.

No transport trait carries protocol metadata. [[crates/clients/moldudp/src/frame.rs#MessageView]] is built only inside the crate, so its internal holder of either leg frame or copied retransmission never appears in a public signature.

## Tests

Tests run over loopback sockets and the `transport_core` mock driver; none needs privileges.

`gap_recovery.rs` answers re-requests from a real UDP server that sends missing packets to the request's source, once with socket legs and once with bypass mock legs, each with a one-slab socket requester (so a second retransmission lands only if the first slab went back). `receiver_alloc.rs` proves in-order `poll` allocates nothing; `e2e_ab.rs` parks two `MioUdp` legs on one `ReadySet`; `loopback.rs` and `receiver_owned.rs` cover async receive; the rest cover anchoring, tail gaps, session lock, arbiter, reassembler, emitter and wire codec.
