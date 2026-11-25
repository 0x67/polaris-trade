//! `io_uring` receive config. Bind address is `new` argument, rest public with
//! defaults. `validate` runs before first allocation or syscall.

use std::{net::SocketAddr, num::NonZeroU32};

use transport_core::{TransportError, config::validate};

use crate::probe::RecvPath;

/// Most slots one buffer group or buffer ring names: kernel ring limit, u16 buffer id.
pub(crate) const MAX_SLOTS: u32 = 32_768;
/// Deepest single-shot recv fleet; keeps submission queue within kernel limit.
const MAX_DEPTH: u32 = 4096;
// ProvideBuffers length and SO_RCVBUF travel as C `int`
const C_INT_MAX: u32 = i32::MAX.unsigned_abs();
const DEFAULT_SLOTS: NonZeroU32 = NonZeroU32::new(1024).unwrap();
const DEFAULT_SLOT_SIZE: NonZeroU32 = NonZeroU32::new(2048).unwrap();
const DEFAULT_DEPTH: NonZeroU32 = NonZeroU32::new(64).unwrap();

/// UDP receive over `io_uring`: bind address, receive pool shape, recv fleet, path.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct IoUringConfig {
    /// Local address. Port 0 picks ephemeral port; unspecified IP serves multicast.
    pub bind: SocketAddr,
    /// Receive slots, so most datagrams caller can hold at once. At most 32768.
    /// Default 1024.
    pub slots: NonZeroU32,
    /// Bytes per slot. Longer datagram counts `truncated` and is dropped.
    /// At most `i32::MAX`. Default 2048.
    pub slot_size: NonZeroU32,
    /// Single-shot recvs kept armed on [`RecvPath::Legacy`] and
    /// [`RecvPath::BufRing`]; [`RecvPath::Multishot`] arms one. At most `slots`
    /// and 4096. Default 64.
    pub depth: NonZeroU32,
    /// Receive path to force; kernel lacking it is `Unsupported`. `None` picks
    /// best path kernel supports. Default `None`.
    pub path: Option<RecvPath>,
    /// `SO_RCVBUF` bytes; `None` keeps OS default. Linux reports double.
    pub recv_buf: Option<NonZeroU32>,
}

impl IoUringConfig {
    /// Config binding `bind`, every option at default.
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            slots: DEFAULT_SLOTS,
            slot_size: DEFAULT_SLOT_SIZE,
            depth: DEFAULT_DEPTH,
            path: None,
            recv_buf: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        validate::at_most("slots", self.slots.get(), MAX_SLOTS)?;
        validate::at_most("slot_size", self.slot_size.get(), C_INT_MAX)?;
        validate::at_most("depth", self.depth.get(), MAX_DEPTH)?;
        // deeper fleet than pool only turns into ENOBUFS completions
        if self.depth > self.slots {
            return Err(TransportError::InvalidConfig {
                field: "depth",
                reason: "deeper than slots",
            });
        }
        if let Some(bytes) = self.recv_buf {
            validate::at_most("recv_buf", bytes.get(), C_INT_MAX)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    type Break = fn(&mut IoUringConfig);

    fn nz(v: u32) -> NonZeroU32 {
        NonZeroU32::new(v).unwrap()
    }

    #[test]
    fn validate_accepts_each_limit_and_names_field_past_it() {
        let mut cfg = IoUringConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
        (cfg.slots, cfg.slot_size, cfg.depth) = (nz(MAX_SLOTS), nz(C_INT_MAX), nz(MAX_DEPTH));
        cfg.recv_buf = Some(nz(C_INT_MAX));
        assert!(cfg.validate().is_ok(), "{cfg:?}");

        let cases: [(&str, Break); 5] = [
            ("slots", |c| c.slots = nz(MAX_SLOTS + 1)),
            ("slot_size", |c| c.slot_size = nz(C_INT_MAX + 1)),
            ("depth", |c| c.depth = nz(MAX_DEPTH + 1)),
            ("depth", |c| (c.slots, c.depth) = (nz(8), nz(9))),
            ("recv_buf", |c| c.recv_buf = Some(nz(C_INT_MAX + 1))),
        ];
        for (want, break_it) in cases {
            let mut bad = cfg.clone();
            break_it(&mut bad);
            assert!(
                matches!(bad.validate(), Err(TransportError::InvalidConfig { field, .. }) if field == want),
                "{bad:?}: want InvalidConfig naming {want}"
            );
        }
    }
}
