//! `AF_XDP` config. Required fields are `new` arguments, rest public with
//! defaults. [`AfxdpL2::bind`](crate::AfxdpL2::bind) validates before first
//! allocation or syscall.

use std::{num::NonZeroU32, os::unix::ffi::OsStrExt, path::PathBuf};

use transport_core::TransportError;

/// Bytes kernel reserves before packet data in every frame (`XDP_PACKET_HEADROOM`).
pub(crate) const XDP_PACKET_HEADROOM: u32 = 256;
// smallest aligned UMEM chunk (`XDP_UMEM_MIN_CHUNK_SIZE`)
const MIN_FRAME_SIZE: u32 = 2048;
const DEFAULT_FRAMES: NonZeroU32 = NonZeroU32::new(4096).unwrap();
const DEFAULT_FRAME_SIZE: NonZeroU32 = NonZeroU32::new(2048).unwrap();

/// How frames of bound queue reach socket.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum XdpRedirect {
    /// Load built-in program and attach it for transport's life; drop detaches.
    /// Program sends queue's frames to socket, rest to kernel stack.
    Builtin {
        /// Attach mode. No automatic fallback.
        mode: XdpMode,
    },
    /// Insert socket into XSKMAP external program pinned at `path`, keyed by
    /// queue. Program and map outlive transport.
    Pinned {
        /// Pinned map on bpffs, e.g. `/sys/fs/bpf/xsks_map`.
        path: PathBuf,
    },
}

/// XDP attach mode of built-in program.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XdpMode {
    /// Generic XDP (`XDP_FLAGS_SKB_MODE`): any interface, after socket buffer allocation.
    Skb,
    /// Native driver XDP (`XDP_FLAGS_DRV_MODE`): driver support needed.
    Drv,
}

/// `AF_XDP` socket on one interface queue: UMEM shape, bind mode, redirect.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AfxdpConfig {
    /// Interface name, 1 to 15 bytes.
    pub ifname: String,
    /// Receive queue to bind; key of socket in XSKMAP.
    pub queue: u32,
    /// UMEM frames, also fill and receive ring entries. Power of two. Default 4096.
    pub frames: NonZeroU32,
    /// Bytes per frame (UMEM chunk). Power of two, at least 2048; kernel rejects
    /// above page size. Default 2048.
    pub frame_size: NonZeroU32,
    /// Bytes reserved at frame start ahead of kernel's 256-byte XDP headroom;
    /// frame keeps `frame_size - headroom - 256` bytes for packet. Default 0.
    pub headroom: u32,
    /// Bind zero-copy (`XDP_ZEROCOPY`, driver support needed, unverified on real
    /// NICs) instead of copy mode. Default off.
    pub zero_copy: bool,
    /// Program steering queue to socket. Default `Builtin { mode: Skb }`.
    pub redirect: XdpRedirect,
}

impl AfxdpConfig {
    /// Config binding `queue` of `ifname`, every option at default.
    pub fn new(ifname: impl Into<String>, queue: u32) -> Self {
        Self {
            ifname: ifname.into(),
            queue,
            frames: DEFAULT_FRAMES,
            frame_size: DEFAULT_FRAME_SIZE,
            headroom: 0,
            zero_copy: false,
            redirect: XdpRedirect::Builtin { mode: XdpMode::Skb },
        }
    }

    // every rule kernel would enforce later with bare EINVAL, checked first
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        let name = self.ifname.as_bytes();
        if name.is_empty() {
            return invalid("ifname", "empty");
        }
        if name.len() >= libc::IFNAMSIZ {
            return invalid("ifname", "longer than 15 bytes");
        }
        if name.contains(&0) {
            return invalid("ifname", "contains NUL byte");
        }
        if !self.frames.is_power_of_two() {
            return invalid("frames", "not a power of two");
        }
        let size = self.frame_size.get();
        if !size.is_power_of_two() || size < MIN_FRAME_SIZE {
            return invalid("frame_size", "not a power of two of at least 2048");
        }
        if self.headroom >= size - XDP_PACKET_HEADROOM {
            return invalid("headroom", "leaves no packet room in frame");
        }
        match &self.redirect {
            // built-in XSKMAP holds `queue + 1` entries
            XdpRedirect::Builtin { .. } if self.queue == u32::MAX => {
                invalid("queue", "u32::MAX leaves no XSKMAP size")
            }
            XdpRedirect::Pinned { path } if path.as_os_str().is_empty() => {
                invalid("redirect.path", "empty")
            }
            XdpRedirect::Pinned { path } if path.as_os_str().as_bytes().contains(&0) => {
                invalid("redirect.path", "contains NUL byte")
            }
            _ => Ok(()),
        }
    }
}

fn invalid(field: &'static str, reason: &'static str) -> Result<(), TransportError> {
    Err(TransportError::InvalidConfig { field, reason })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected_field(cfg: &AfxdpConfig) -> Option<&'static str> {
        match cfg.validate() {
            Err(TransportError::InvalidConfig { field, .. }) => Some(field),
            _ => None,
        }
    }

    #[test]
    fn validate_rejects_each_bad_field_and_accepts_limits() {
        let base = AfxdpConfig::new("veth0", 0);
        let with = |edit: fn(&mut AfxdpConfig)| {
            let mut cfg = base.clone();
            edit(&mut cfg);
            rejected_field(&cfg)
        };
        assert_eq!(rejected_field(&base), None);
        assert_eq!(with(|c| c.ifname.clear()), Some("ifname"));
        assert_eq!(with(|c| c.ifname = "a".repeat(16)), Some("ifname"));
        assert_eq!(with(|c| c.ifname = "a".repeat(15)), None);
        assert_eq!(with(|c| c.ifname = "eth\0".into()), Some("ifname"));
        assert_eq!(
            with(|c| c.frames = NonZeroU32::new(3).unwrap()),
            Some("frames")
        );
        assert_eq!(
            with(|c| c.frame_size = NonZeroU32::new(1024).unwrap()),
            Some("frame_size")
        );
        assert_eq!(
            with(|c| c.frame_size = NonZeroU32::new(3000).unwrap()),
            Some("frame_size")
        );
        assert_eq!(with(|c| c.headroom = 2048 - 256), Some("headroom"));
        assert_eq!(with(|c| c.headroom = 2048 - 257), None);
        assert_eq!(with(|c| c.queue = u32::MAX), Some("queue"));
        let pinned = |path: &str| {
            let mut cfg = base.clone();
            cfg.queue = u32::MAX;
            cfg.redirect = XdpRedirect::Pinned { path: path.into() };
            rejected_field(&cfg)
        };
        // pinned map size comes from its creator, so any queue passes here
        assert_eq!(pinned("/sys/fs/bpf/xsks_map"), None);
        assert_eq!(pinned(""), Some("redirect.path"));
        assert_eq!(pinned("/sys/fs/bpf/x\0"), Some("redirect.path"));
    }
}
