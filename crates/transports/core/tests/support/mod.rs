//! Frame builder shared by core tests and benches.

/// Untagged Ethernet II frame: IPv4 UDP from `10.0.0.1:40000` to
/// `239.1.1.1:dst_port` carrying `payload`.
///
/// Checksums left zero; decap never verifies them.
pub fn udp_frame(dst_port: u16, payload: &[u8]) -> Vec<u8> {
    let ip_len = u16::try_from(20 + 8 + payload.len()).expect("payload fits one IPv4 packet");
    let udp_len = ip_len - 20;
    let mut f = Vec::with_capacity(14 + usize::from(ip_len));
    // multicast destination MAC, local source MAC, ethertype IPv4
    f.extend_from_slice(&[
        0x01, 0x00, 0x5e, 0x01, 0x01, 0x01, 0x02, 0, 0, 0, 0, 1, 0x08, 0x00,
    ]);
    // version 4, IHL 5, total length, id 0, don't-fragment, TTL 64, UDP, checksum 0
    f.extend_from_slice(&[0x45, 0]);
    f.extend_from_slice(&ip_len.to_be_bytes());
    f.extend_from_slice(&[0, 0, 0x40, 0, 64, 17, 0, 0]);
    f.extend_from_slice(&[10, 0, 0, 1, 239, 1, 1, 1]);
    f.extend_from_slice(&40_000_u16.to_be_bytes());
    f.extend_from_slice(&dst_port.to_be_bytes());
    f.extend_from_slice(&udp_len.to_be_bytes());
    f.extend_from_slice(&[0, 0]);
    f.extend_from_slice(payload);
    f
}
