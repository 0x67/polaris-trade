//! SoupBinTCP packet builders for the in-process mock servers.

/// One logical packet: `Length[2 BE] + Type[1] + payload`.
pub fn build_packet(ty: u8, payload: &[u8]) -> Vec<u8> {
    let len = 1 + payload.len();
    let mut out = Vec::with_capacity(2 + len);
    out.extend_from_slice(&(len as u16).to_be_bytes());
    out.push(ty);
    out.extend_from_slice(payload);
    out
}

fn ascii_left_justify(s: &str, width: usize) -> Vec<u8> {
    let mut buf = vec![b' '; width];
    let bytes = s.as_bytes();
    let n = bytes.len().min(width);
    buf[..n].copy_from_slice(&bytes[..n]);
    buf
}

fn ascii_right_justify_numeric(n: u64, width: usize) -> Vec<u8> {
    let digits = n.to_string();
    let mut buf = vec![b' '; width];
    let bytes = digits.as_bytes();
    let take = bytes.len().min(width);
    let start = width - take;
    buf[start..start + take].copy_from_slice(&bytes[bytes.len() - take..]);
    buf
}

pub fn login_accepted_packet(session: &str, sequence: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(30);
    payload.extend_from_slice(&ascii_left_justify(session, 10));
    payload.extend_from_slice(&ascii_right_justify_numeric(sequence, 20));
    build_packet(b'A', &payload)
}
