# client_moldudp

MoldUDP64 market-data client: wire codec, sequence reassembler, gap re-request, and A/B line arbitration behind one receiver over any datagram transport.

## What it is

`MoldUdpReceiver<T, R>` runs the MoldUDP64 downstream receive path over legs of any `transport_core::DatagramRecv`. It parses the 20-byte downstream header, iterates message blocks straight out of the datagram, reorders by sequence, tracks gaps, and (for redundant A/B feeds) arbitrates first arrival. The same receiver runs on `transport_socket` sockets or on a kernel-bypass backend: the receiver never binds or joins anything itself.

## Legs

The caller builds each leg, joins its multicast group through `transport_core::Multicast::join_multicast`, and hands the legs to `MoldUdpReceiver::from_legs`. Two or more legs share one session and sequence space and run A/B arbitration. Each leg's receive pool must hold at least `MIN_LEG_POOL_CAPACITY` buffers (reorder window plus one burst); a smaller pool fails construction with `PoolTooSmall` instead of stalling a live feed.

## Receive

`poll()` is synchronous and never waits. It returns `Ok(None)` only after every leg (and, while a gap is pending, the requester) returned nothing in that call, so a caller parked on `transport_socket::mio::ReadySet` may wait after `None`. A datagram whose leading sequence is the next expected drains inline, borrowed from the still-owned frame with no allocation; a datagram ahead of it promotes its frame to one `Arc` and buffers message views until the gap fills. `Err(GapDetected)` reports a gap for that call only; keep polling.

When the legs implement `AsyncReady` (for example `transport_socket::tokio::AsyncUdp`), `recv().await` returns a borrowed outcome and `recv_owned().await` an owned one (`Send + 'static`) that moves to another thread without copying.

## Gap recovery

Recovery is opt-in type state. `with_requester(socket, server)` attaches a unicast socket (any `DatagramRecv + DatagramSend`, typically `transport_socket::UdpSocket`). `poll` then sends rate-limited Request Packets from it for pending gaps and reads it while any gap is pending, because a MoldUDP64 server unicasts the retransmission back to the request's source address and port. A retransmitted datagram is copied into receiver memory and its buffer returns to the requester's pool at once, so the requester's frame type need not match the legs'. Retransmitted messages report stream id equal to the leg count.

Re-requests go out only from `poll`: while gaps are pending, a parked caller waits with a timeout (and registers the requester in the same `ReadySet`). Any one missing range is requested at most `max_rerequests_per_gap_per_sec` times per second: a gap that lies inside a range requested within the last interval is skipped, so retransmissions filling a gap from its head trigger no further request. A send that meets a full socket buffer is retried on a later call. A send that fails for any other reason never blocks receive: it is logged at `warn`, the range backs off one interval like a sent one, and the legs keep being read.

On a kernel-bypass L2 leg the requester is still a kernel UDP socket. An AF_XDP program that redirects the feed's queue would capture a reply landing on that queue before the requester sees it: steer re-request replies to another queue (a flow rule on the requester's port), or bind the requester to the feed's destination port with the decap `dst_ip` filter unset, so the unicast reply passes through the leg.

## Usage

```rust,ignore
use client_moldudp::{MIN_LEG_POOL_CAPACITY, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::smallvec;
use transport_core::{Multicast, MulticastInterface};
use transport_socket::{UdpConfig, UdpSocket};

let mut cfg = UdpConfig::new("0.0.0.0:30001".parse()?);
cfg.slab_count = MIN_LEG_POOL_CAPACITY.try_into()?;
let mut leg = UdpSocket::bind(&cfg)?;
leg.join_multicast("233.54.12.1".parse()?, MulticastInterface::default())?;
let requester = UdpSocket::bind(&UdpConfig::new("0.0.0.0:0".parse()?))?;

let mut rx = MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), smallvec![leg])?
    .with_requester(requester, "10.0.0.2:40000".parse()?);
loop {
    match rx.poll() {
        Ok(Some(MoldUdpOutcome::Frame(frame))) => { let _ = (frame.sequence(), frame.as_ref()); }
        Ok(Some(_)) => {}                          // heartbeat / end of session
        Ok(None) => {}                             // idle: spin, or park on readiness
        Err(client_moldudp::MoldUdpError::GapDetected) => {}
        Err(e) => return Err(e.into()),
    }
}
```

The legs can be any `DatagramRecv`: `transport_socket` types on Linux, macOS and Windows, `transport_io_uring::IoUringUdp`, or `transport_core::decap::UdpDecap` over `transport_afxdp` or `transport_dpdk`. The receiver needs no privilege of its own; each leg brings its own platform and privilege requirements.

## Config

`MoldUdpReceiverConfig` is serde (JSON or TOML) and every field defaults: `max_rerequests_per_gap_per_sec` (4, requests per second for any one missing range), `gap_confirm_window` (5 ms, multi-leg only; humantime string such as `"5ms"`), and `start_sequence` (`None` adopts the first packet's sequence; set it to resume mid-stream without treating the backlog as one gap). Socket options live on the legs.

## Features

- `observability` (off by default): message and gap counters through `observability-core`, and `transport_core`'s receive metrics.

## Protocol specification

MoldUDP64 is a Nasdaq protocol. Obtain the specification from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Building and testing

```bash
cargo nextest run -p client_moldudp
cargo nextest run -p client_moldudp --features observability
```

Tests run over loopback sockets and the in-process `transport_core` mock driver; the gap test answers re-requests from a real UDP server.

## Logging

This crate emits [`tracing`](https://docs.rs/tracing) events at state transitions only (gap detected and re-request send failed at `warn`, re-requests sent at `debug`), never per message. Install any subscriber to see them; filter with `RUST_LOG=client_moldudp=debug`.

## License

MIT OR Apache-2.0, at your option.
