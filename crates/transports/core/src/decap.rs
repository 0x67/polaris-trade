//! L2-to-UDP decapsulation: [`UdpDecap`] turns any [`L2Recv`] into a [`DatagramRecv`].
//!
//! Parses Ethernet II with at most one 802.1Q tag, IPv4, then UDP. Keeps
//! datagrams sent to one destination port and, when set, one destination
//! address; drops and counts every other frame ([`DecapStats`]). Payload ends
//! where UDP length says, bounded by IPv4 total length, never by frame length,
//! so Ethernet padding on short frames never reaches caller.
//!
//! Limits: IPv4 only (IPv6 counts as `not_ipv4`). Fragments dropped, never
//! reassembled. IPv4 and UDP checksums not verified: corrupt frame NIC did not
//! drop reaches caller. Pure logic: no syscall, no allocation per frame.

use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    ops::Range,
};

use crate::{
    DatagramRecv, FrameBatch, L2Recv, Multicast, MulticastInterface, PoolStats, Transport,
    TransportError,
};

const ETH_HDR: usize = 14;
const VLAN_TAG: usize = 4;
// IPv4 header without options
const IPV4_HDR: usize = 20;
const UDP_HDR: usize = 8;
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_VLAN: u16 = 0x8100;
const IPPROTO_UDP: u8 = 17;
// more-fragments flag plus 13-bit fragment offset; don't-fragment excluded
const FRAGMENT_BITS: u16 = 0x3fff;

/// Datagram source over L2 source: each frame decapsulated to its UDP payload.
///
/// Each inner reap takes at most `burst` frames. Frames that do not fit
/// `out` wait in fixed queue and go out first on next call; `inner` is reaped
/// only once that queue is empty, so nothing drops for lack of room and
/// nothing grows. Reap whose frames all fail filter triggers another reap in
/// same call, so `Ok(0)` still means idle. Filtered frames drop at once,
/// returning their buffers.
#[derive(Debug)]
pub struct UdpDecap<S: L2Recv> {
    inner: S,
    dst_port: u16,
    dst_ip: Option<Ipv4Addr>,
    // lands each inner reap; capacity `burst`
    reaped: FrameBatch<S::Frame>,
    // reaped frames not yet delivered; refilled only when empty, so never above `burst`
    pending: VecDeque<S::Frame>,
    stats: DecapStats,
}

impl<S: L2Recv> UdpDecap<S> {
    /// Wrap `inner`, keeping UDP datagrams to `dst_port` and, when set, `dst_ip`.
    ///
    /// `burst` bounds every inner reap and sizes both internal queues,
    /// allocated here once.
    pub fn new(inner: S, dst_port: u16, dst_ip: Option<Ipv4Addr>, burst: NonZeroUsize) -> Self {
        Self {
            inner,
            dst_port,
            dst_ip,
            reaped: FrameBatch::with_capacity(burst),
            pending: VecDeque::with_capacity(burst.get()),
            stats: DecapStats::default(),
        }
    }

    /// Wrapped L2 source, e.g. for backend stats.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Wrapped L2 source, mutably. Reaping through it bypasses decap.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Frames dropped since construction, per reason.
    pub fn stats(&self) -> DecapStats {
        self.stats
    }

    // push pending frames while `out` has room; filtered ones drop here
    fn deliver(&mut self, out: &mut FrameBatch<DecapFrame<S::Frame>>) -> usize {
        let mut pushed = 0;
        while out.spare() > 0
            && let Some(frame) = self.pending.pop_front()
        {
            match parse_udp(frame.as_ref(), self.dst_port, self.dst_ip) {
                Ok((payload, peer)) => {
                    out.push(DecapFrame {
                        frame,
                        payload,
                        peer,
                    });
                    pushed += 1;
                }
                Err(reason) => self.stats.count(reason),
            }
        }
        pushed
    }

    // reaps only while nothing pushed yet, so inner error never follows pushed frames
    fn fill(
        &mut self,
        out: &mut FrameBatch<DecapFrame<S::Frame>>,
    ) -> Result<usize, TransportError> {
        loop {
            let pushed = self.deliver(out);
            if pushed > 0 || out.spare() == 0 {
                return Ok(pushed);
            }
            // `out` has room, so `deliver` emptied `pending`
            if self.inner.recv_burst(&mut self.reaped)? == 0 {
                return Ok(0);
            }
            self.pending.extend(self.reaped.drain());
        }
    }
}

impl<S: L2Recv> Transport for UdpDecap<S> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }
}

impl<S: L2Recv> DatagramRecv for UdpDecap<S> {
    type Frame = DecapFrame<S::Frame>;

    fn recv_burst(&mut self, out: &mut FrameBatch<Self::Frame>) -> Result<usize, TransportError> {
        debug_assert!(out.spare() > 0, "UdpDecap::recv_burst on full batch");
        #[cfg(feature = "observability")]
        let before = self.stats.total();
        let result = self.fill(out);
        #[cfg(feature = "observability")]
        crate::telemetry::record_drops(
            self.inner.name(),
            crate::telemetry::DropReason::DecapFiltered,
            self.stats.total() - before,
        );
        result
    }

    fn pool_stats(&self) -> PoolStats {
        self.inner.pool_stats()
    }
}

impl<S: L2Recv + Multicast> Multicast for UdpDecap<S> {
    fn join_multicast(
        &mut self,
        group: IpAddr,
        iface: MulticastInterface,
    ) -> Result<(), TransportError> {
        self.inner.join_multicast(group, iface)
    }
}

/// One UDP datagram inside its received L2 frame. Holds frame buffer until dropped.
///
/// [`AsRef<[u8]>`](AsRef) yields UDP payload only.
#[derive(Debug)]
pub struct DecapFrame<F> {
    frame: F,
    payload: Range<u32>,
    peer: SocketAddr,
}

impl<F> DecapFrame<F> {
    /// Sender: IPv4 source address and UDP source port.
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

impl<F: AsRef<[u8]>> AsRef<[u8]> for DecapFrame<F> {
    fn as_ref(&self) -> &[u8] {
        // `F::as_ref` is foreign: empty slice, never panic, if it no longer covers range
        self.frame
            .as_ref()
            .get(self.payload.start as usize..self.payload.end as usize)
            .unwrap_or_default()
    }
}

/// Frames [`UdpDecap`] dropped, per reason. Monotonic since construction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecapStats {
    /// Other ethertype, IP version not 4, or more than one VLAN tag.
    pub not_ipv4: u64,
    /// IPv4 fragment: more-fragments flag set or non-zero offset.
    pub fragment: u64,
    /// Not UDP, or UDP to other destination port or address.
    pub wrong_dst: u64,
    /// Header missing, or length field past frame or enclosing header.
    pub truncated: u64,
}

impl DecapStats {
    #[inline]
    fn count(&mut self, reason: DecapDrop) {
        let field = match reason {
            DecapDrop::NotIpv4 => &mut self.not_ipv4,
            DecapDrop::Fragment => &mut self.fragment,
            DecapDrop::WrongDst => &mut self.wrong_dst,
            DecapDrop::Truncated => &mut self.truncated,
        };
        *field += 1;
    }

    #[cfg(feature = "observability")]
    #[inline]
    fn total(self) -> u64 {
        self.not_ipv4 + self.fragment + self.wrong_dst + self.truncated
    }
}

/// Why [`parse_udp`] filtered frame out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecapDrop {
    NotIpv4,
    Fragment,
    WrongDst,
    Truncated,
}

/// Locate UDP payload of `frame` and its sender, or say why frame is filtered.
///
/// Range indexes `frame`. Non-UDP protocol counts as `WrongDst`: not addressed
/// to this UDP flow.
#[inline]
pub(crate) fn parse_udp(
    frame: &[u8],
    dst_port: u16,
    dst_ip: Option<Ipv4Addr>,
) -> Result<(Range<u32>, SocketAddr), DecapDrop> {
    let (eth, rest) = frame
        .split_first_chunk::<ETH_HDR>()
        .ok_or(DecapDrop::Truncated)?;
    let (l3, ip) = match u16::from_be_bytes([eth[12], eth[13]]) {
        ETHERTYPE_IPV4 => (ETH_HDR, rest),
        ETHERTYPE_VLAN => {
            let (tag, ip) = rest
                .split_first_chunk::<VLAN_TAG>()
                .ok_or(DecapDrop::Truncated)?;
            // second tag (802.1Q or 802.1ad) lands here as inner ethertype
            if u16::from_be_bytes([tag[2], tag[3]]) != ETHERTYPE_IPV4 {
                return Err(DecapDrop::NotIpv4);
            }
            (ETH_HDR + VLAN_TAG, ip)
        }
        _ => return Err(DecapDrop::NotIpv4),
    };

    let (hdr, _) = ip
        .split_first_chunk::<IPV4_HDR>()
        .ok_or(DecapDrop::Truncated)?;
    if hdr[0] >> 4 != 4 {
        return Err(DecapDrop::NotIpv4);
    }
    let ihl = usize::from(hdr[0] & 0x0f) * 4;
    if ihl < IPV4_HDR || ip.len() < ihl {
        return Err(DecapDrop::Truncated);
    }
    if u16::from_be_bytes([hdr[6], hdr[7]]) & FRAGMENT_BITS != 0 {
        return Err(DecapDrop::Fragment);
    }
    if hdr[9] != IPPROTO_UDP {
        return Err(DecapDrop::WrongDst);
    }
    // `None` when total length is below header end or past frame
    let segment = ip
        .get(ihl..usize::from(u16::from_be_bytes([hdr[2], hdr[3]])))
        .ok_or(DecapDrop::Truncated)?;
    let (udp, _) = segment
        .split_first_chunk::<UDP_HDR>()
        .ok_or(DecapDrop::Truncated)?;

    let dst = Ipv4Addr::new(hdr[16], hdr[17], hdr[18], hdr[19]);
    if u16::from_be_bytes([udp[2], udp[3]]) != dst_port || dst_ip.is_some_and(|want| want != dst) {
        return Err(DecapDrop::WrongDst);
    }
    let udp_len = usize::from(u16::from_be_bytes([udp[4], udp[5]]));
    if udp_len < UDP_HDR || udp_len > segment.len() {
        return Err(DecapDrop::Truncated);
    }

    let start = l3 + ihl + UDP_HDR;
    let end = l3 + ihl + udp_len;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "offsets at most 18 + 60 + 65_535, far below u32::MAX"
    )]
    let payload = start as u32..end as u32;
    let src = Ipv4Addr::new(hdr[12], hdr[13], hdr[14], hdr[15]);
    let peer = SocketAddr::from((src, u16::from_be_bytes([udp[0], udp[1]])));
    Ok((payload, peer))
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        ops::Range,
    };

    use super::{DecapDrop, parse_udp};

    const SRC: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
    const DST: Ipv4Addr = Ipv4Addr::new(239, 1, 1, 1);
    const SRC_PORT: u16 = 40_000;
    const DST_PORT: u16 = 30_001;
    // IPv4 header offset in untagged frame
    const IP: usize = 14;

    /// Well-formed frame: one tag header per TPID in `tpids`, IPv4 with `options`
    /// NOP bytes and don't-fragment set, UDP `SRC:SRC_PORT` to `DST:DST_PORT`.
    fn frame(tpids: &[u16], options: usize, payload: &[u8]) -> Vec<u8> {
        let ihl = 20 + options;
        let udp_len = u16::try_from(8 + payload.len()).unwrap();
        let total = u16::try_from(ihl).unwrap() + udp_len;
        let mut f = vec![0x02, 0, 0, 0, 0, 2, 0x02, 0, 0, 0, 0, 1];
        for tpid in tpids {
            f.extend(tpid.to_be_bytes());
            f.extend(100_u16.to_be_bytes());
        }
        f.extend(0x0800_u16.to_be_bytes());
        f.push(0x40 | u8::try_from(ihl / 4).unwrap());
        f.push(0);
        f.extend(total.to_be_bytes());
        f.extend([0, 0, 0x40, 0]);
        f.extend([64, 17, 0, 0]);
        f.extend(SRC.octets());
        f.extend(DST.octets());
        f.extend(std::iter::repeat_n(1, options));
        f.extend(SRC_PORT.to_be_bytes());
        f.extend(DST_PORT.to_be_bytes());
        f.extend(udp_len.to_be_bytes());
        f.extend([0, 0]);
        f.extend_from_slice(payload);
        f
    }

    fn parse(frame: &[u8]) -> Result<(Range<u32>, SocketAddr), DecapDrop> {
        parse_udp(frame, DST_PORT, None)
    }

    fn sender() -> SocketAddr {
        SocketAddr::from((SRC, SRC_PORT))
    }

    fn set_u16(frame: &mut [u8], at: usize, value: u16) {
        frame[at..at + 2].copy_from_slice(&value.to_be_bytes());
    }

    #[test]
    fn plain_frame_yields_payload_and_sender() {
        assert_eq!(parse(&frame(&[], 0, b"hello")), Ok((42..47, sender())));
    }

    #[test]
    fn one_vlan_tag_shifts_payload_by_four() {
        assert_eq!(
            parse(&frame(&[0x8100], 0, b"hello")),
            Ok((46..51, sender()))
        );
    }

    #[test]
    fn ipv4_options_are_skipped() {
        assert_eq!(parse(&frame(&[], 8, b"hello")), Ok((50..55, sender())));
    }

    #[test]
    fn ethernet_padding_stays_out_of_payload() {
        let mut f = frame(&[], 0, b"hi");
        f.resize(60, 0);
        assert_eq!(parse(&f), Ok((42..44, sender())));
    }

    #[test]
    fn fragments_are_dropped() {
        let mut first = frame(&[], 0, b"hello");
        first[IP + 6] |= 0x20;
        let mut last = frame(&[], 0, b"hello");
        last[IP + 7] = 1;
        assert_eq!(parse(&first), Err(DecapDrop::Fragment), "more-fragments");
        assert_eq!(parse(&last), Err(DecapDrop::Fragment), "non-zero offset");
    }

    #[test]
    fn other_destination_port_is_wrong_dst() {
        let f = frame(&[], 0, b"hello");
        assert_eq!(parse_udp(&f, DST_PORT + 1, None), Err(DecapDrop::WrongDst));
    }

    // unset address filter accepts same frame: plain_frame_yields_payload_and_sender
    #[test]
    fn destination_address_filter_keeps_only_its_address() {
        let f = frame(&[], 0, b"hello");
        let other = Ipv4Addr::new(239, 1, 1, 2);
        assert_eq!(
            parse_udp(&f, DST_PORT, Some(other)),
            Err(DecapDrop::WrongDst)
        );
        assert_eq!(parse_udp(&f, DST_PORT, Some(DST)), Ok((42..47, sender())));
    }

    #[test]
    fn non_udp_protocol_is_wrong_dst() {
        let mut f = frame(&[], 0, b"hello");
        f[IP + 9] = 6;
        assert_eq!(parse(&f), Err(DecapDrop::WrongDst));
    }

    #[test]
    fn non_ipv4_and_stacked_tags_are_not_ipv4() {
        // ethertype decides, never bytes after it: both keep IPv4 header behind
        let mut ipv6 = frame(&[], 0, b"hello");
        set_u16(&mut ipv6, 12, 0x86dd);
        let mut tagged_ipv6 = frame(&[0x8100], 0, b"hello");
        set_u16(&mut tagged_ipv6, 16, 0x86dd);
        let mut version6 = frame(&[], 0, b"hello");
        version6[IP] = 0x65;
        let cases = [
            ("ipv6 ethertype", ipv6),
            ("ipv6 ethertype behind 802.1Q tag", tagged_ipv6),
            ("ip version 6", version6),
            ("two 802.1Q tags", frame(&[0x8100, 0x8100], 0, b"hello")),
            ("802.1ad outer tag", frame(&[0x88a8], 0, b"hello")),
            ("802.1ad inner tag", frame(&[0x8100, 0x88a8], 0, b"hello")),
        ];
        for (case, f) in cases {
            assert_eq!(parse(&f), Err(DecapDrop::NotIpv4), "{case}");
        }
    }

    #[test]
    fn short_headers_and_overlong_lengths_are_truncated() {
        let plain = frame(&[], 0, b"hello");
        let with = |at: usize, value: u16| {
            let mut f = plain.clone();
            set_u16(&mut f, at, value);
            f
        };
        let mut ihl4 = plain.clone();
        ihl4[IP] = 0x44;
        let mut padded = frame(&[], 0, b"hi");
        padded.resize(60, 0);
        set_u16(&mut padded, IP + 24, 8 + 10);
        let cases = [
            ("ethernet header", plain[..13].to_vec()),
            ("vlan tag", frame(&[0x8100], 0, b"hello")[..17].to_vec()),
            ("ipv4 header", plain[..IP + 19].to_vec()),
            ("ipv4 options", frame(&[], 8, b"hello")[..IP + 27].to_vec()),
            ("ihl below 5", ihl4),
            ("udp header", plain[..IP + 27].to_vec()),
            ("payload cut short", plain[..plain.len() - 1].to_vec()),
            ("ipv4 total length below headers", with(IP + 2, 27)),
            ("udp length below header", with(IP + 24, 7)),
            ("udp length past frame", with(IP + 24, 8 + 6)),
            ("udp length past ipv4 total, inside padding", padded),
        ];
        for (case, f) in cases {
            assert_eq!(parse(&f), Err(DecapDrop::Truncated), "{case}");
        }
    }
}
