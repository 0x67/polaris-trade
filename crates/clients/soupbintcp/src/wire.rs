//! Logical packet framing: `Length[2 BE u16]` + `Type[1]` + `Payload[Length-1]`.
//! Pure parser, no I/O, no config: caller holds partial packets in own buffer
//! and re-calls `parse_packet` once more bytes land.

use crate::error::SoupBinError;

/// One `SoupBinTCP` packet type byte. Fixed set per protocol v3.0.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    /// Client login.
    LoginRequest = b'L',
    /// Server accepted login.
    LoginAccepted = b'A',
    /// Server rejected login.
    LoginRejected = b'J',
    /// Server sequenced message.
    SequencedData = b'S',
    /// Server liveness.
    ServerHeartbeat = b'H',
    /// Server ended session.
    EndOfSession = b'Z',
    /// Free-form debug text, either direction.
    Debug = b'+',
    /// Client unsequenced message.
    UnsequencedData = b'U',
    /// Client liveness.
    ClientHeartbeat = b'R',
    /// Client ends session.
    LogoutRequest = b'O',
}

impl TryFrom<u8> for PacketType {
    type Error = SoupBinError;

    fn try_from(b: u8) -> Result<Self, SoupBinError> {
        match b {
            b'L' => Ok(Self::LoginRequest),
            b'A' => Ok(Self::LoginAccepted),
            b'J' => Ok(Self::LoginRejected),
            b'S' => Ok(Self::SequencedData),
            b'H' => Ok(Self::ServerHeartbeat),
            b'Z' => Ok(Self::EndOfSession),
            b'+' => Ok(Self::Debug),
            b'U' => Ok(Self::UnsequencedData),
            b'R' => Ok(Self::ClientHeartbeat),
            b'O' => Ok(Self::LogoutRequest),
            other => Err(SoupBinError::UnknownPacketType(other)),
        }
    }
}

/// One decoded packet: type plus payload borrowed from caller's buffer.
#[derive(Debug)]
pub struct PacketFrame<'a> {
    /// Packet type.
    pub ty: PacketType,
    /// Bytes after type byte.
    pub payload: &'a [u8],
}

/// Parse one logical packet from front of `buf`.
///
/// `Ok(None)`: `buf` holds only partial packet. `Ok(Some((frame, consumed)))`:
/// full packet decoded; caller drops `consumed` (`2 + Length`) bytes.
///
/// # Errors
///
/// [`SoupBinError::ProtocolViolation`] on zero length,
/// [`SoupBinError::UnknownPacketType`] on type byte outside protocol.
pub fn parse_packet(buf: &[u8]) -> Result<Option<(PacketFrame<'_>, usize)>, SoupBinError> {
    if buf.len() < 3 {
        return Ok(None);
    }
    let len = usize::from(u16::from_be_bytes([buf[0], buf[1]]));
    if len < 1 {
        return Err(SoupBinError::ProtocolViolation("zero-length packet".into()));
    }
    let total = 2 + len;
    if buf.len() < total {
        return Ok(None);
    }
    let ty = PacketType::try_from(buf[2])?;
    let payload = &buf[3..total];
    Ok(Some((PacketFrame { ty, payload }, total)))
}
