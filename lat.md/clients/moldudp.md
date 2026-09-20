# client_moldudp

MoldUDP64 client: wire codec, sequence reassembler, gap re-requests and A/B arbitration, over caller-built legs of any datagram transport.

Depends on [[core]] alone. A leg is any `DatagramRecv`: a [[socket]] type, [[io-uring]], or `UdpDecap` over [[afxdp]] or [[dpdk]].

## Legs and construction

`from_legs` on [[crates/clients/moldudp/src/receiver/mod.rs#MoldUdpReceiver]] takes legs the caller already bound and joined, so the receiver needs datagram receive only and runs on socket and kernel-bypass backends alike.

The config keeps session, sequence and gap-timing fields; socket options live on the legs. Each leg's pool must hold [[crates/clients/moldudp/src/receiver/mod.rs#MIN_LEG_POOL_CAPACITY]] buffers (reorder window plus one burst), read through `pool_stats`, else construction fails with `PoolTooSmall`. Two or more legs run the [[crates/clients/moldudp/src/ab.rs#AbArbiter]]: first arrival wins, and a gap is confirmed only after every leg missed it for the confirm window.

## Receive path

`poll` ([[crates/clients/moldudp/src/receiver/recv.rs]]) is synchronous: it returns `None` only after every leg, and the requester while a gap is pending, returned nothing in that call.

That contract lets a caller park on an edge-triggered `ReadySet` ([[socket#Readiness]]) after `None`. In-order datagrams drain inline, borrowed from the still-owned frame with no allocation, whatever their message count (block offsets go through one receiver-owned scratch sized for a full MTU datagram); a datagram ahead of the expected sequence becomes one shared `Arc`, and its messages wait in the [[crates/clients/moldudp/src/reassembly.rs#SequenceReassembler]]. Async `recv` and `recv_owned` exist only when legs (and the requester) implement `AsyncReady`; they spin the same path, then wait on every leg.

A gap opens only past the highest sequence seen on any source (data or heartbeat), so datagrams landing behind an open gap never re-report it or re-stage it. Detection on one leg, confirmation on several, and a heartbeat tail gap each log one `warn`.

## Gap recovery

Recovery is type state: [[crates/clients/moldudp/src/receiver/recovery.rs#NoRecovery]] carries nothing, [[crates/clients/moldudp/src/receiver/recovery.rs#Requester]] owns a unicast socket attached by `with_requester`.

MoldUDP64 servers unicast a retransmission back to the request's source, so the socket that sends a re-request must also be read. `poll` sends due requests through [[crates/clients/moldudp/src/gap.rs#GapRequestEmitter#emit]] and reads the requester while gaps are pending. The limit is by coverage: a gap inside a range requested within the last interval is skipped, so retransmissions filling a gap from its head trigger no further request, and expired ranges are pruned each call. The clock is read only while a gap is pending; a full socket buffer retries next call. Any other send failure never blocks receive: the range backs off one interval like a sent one, the failure is logged at `warn`, and the legs keep being read. Each retransmitted datagram is copied into receiver memory and its slab returned at once, so the requester's frame type is unrelated to the legs'; public types stay parameterised by the leg frame alone. Retransmitted messages report stream id equal to the leg count, which the arbiter treats as window-only.

On an L2 leg the requester is still a kernel UDP socket. A reply landing on a queue an AF_XDP program redirects reaches the XSK instead, so steer re-request replies off that queue with a flow rule on the requester's port, or bind the requester to the feed's destination port with the decap `dst_ip` filter unset, so the unicast reply passes through the leg. Neither path is covered by the kernel proof yet.

## Frames

[[crates/clients/moldudp/src/frame.rs#Frame]] and [[crates/clients/moldudp/src/frame.rs#OwnedFrame]] carry inherent `sequence()` and `stream_id()` and expose bytes through `AsRef<[u8]>`.

No transport trait carries protocol metadata. [[crates/clients/moldudp/src/frame.rs#MessageView]] is built only inside the crate, so its internal holder of either leg frame or copied retransmission never appears in a public signature.

## Decisions

What changed from the previous receiver, which built its own sockets and could not recover gaps on a real backend.

- The receiver bound and joined its own sockets through a combined bind trait with hard-coded options, so callers could not tune them and no kernel-bypass backend fit. Legs now arrive built and joined.
- Re-requests went through a base-trait `send` on an unconnected socket, which failed on every real backend, and nothing read the replies. The requester now sends with `send_to` and is read while gaps are pending.
- The multi-leg wait polled each leg's readiness future in turn, which starved the second leg on mio. `poll` is synchronous; parked callers wait on one `ReadySet`.
- A leg pool too small for the reorder window stalled a live feed; it now fails construction.
- Kept: A/B first-arrival arbitration, the 4096-slot reorder ring, inline borrowing for in-order datagrams, and `recv_owned` sharing a datagram through one `Arc` for cross-thread handoff.

## Tests

Tests run over loopback sockets and the `transport_core` mock driver; none needs privileges.

`gap_recovery.rs` answers re-requests from a real UDP server that sends missing packets to the request's source: `socket_leg_gap_filled_from_requester` and `bypass_leg_gap_filled_from_socket_requester`, each with a one-slab socket requester (so a second retransmission lands only if the first slab went back), assert the server saw exactly one request for the two-packet gap. `requester_send_failure.rs` breaks the requester's send and checks legs keep delivering with one attempt per interval. `receiver_alloc.rs` (`poll_in_order_burst_is_allocation_free`, `poll_many_message_datagrams_is_allocation_free`) proves in-order `poll` allocates nothing, including 20-message datagrams; `gap_tracing.rs` expects one `warn` per discontinuity at each detection site; `receiver_recovery.rs` also checks a gap is reported once while later packets land behind it; `e2e_ab.rs` parks two `MioUdp` legs on one `ReadySet`; `receiver_pool_config.rs` covers `PoolTooSmall` and zero legs; `loopback.rs` and `receiver_owned.rs` cover async receive; the rest cover anchoring, tail gaps, session lock, arbiter, reassembler, emitter and wire codec. Fuzz targets for the wire codec are under [[clients#Fuzzing]].
