//! XDP attach through `BPF_LINK_CREATE`: link fd is only handle, so closing it
//! (drop or process exit) detaches program. Netlink attach, which outlives crash,
//! is not used.

use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

use transport_core::TransportError;

use super::sys::{self, ATTACH_XDP, Attr, LinkCreate, XDP_FLAGS_DRV_MODE, XDP_FLAGS_SKB_MODE};
use crate::{XdpMode, unavailable};

/// Program attached to interface for as long as this lives.
pub(crate) struct XdpLink {
    _fd: OwnedFd,
}

impl XdpLink {
    /// Attach `prog` to interface `ifindex` in `mode`.
    pub(crate) fn attach(
        prog: BorrowedFd<'_>,
        ifindex: u32,
        mode: XdpMode,
    ) -> Result<Self, TransportError> {
        let mut attr = LinkCreate {
            prog_fd: prog.as_raw_fd().cast_unsigned(),
            target_ifindex: ifindex,
            attach_type: ATTACH_XDP,
            flags: match mode {
                XdpMode::Skb => XDP_FLAGS_SKB_MODE,
                XdpMode::Drv => XDP_FLAGS_DRV_MODE,
            },
        };
        // SAFETY: attr holds no address; `LINK_CREATE` returns new link fd
        match unsafe { sys::bpf_fd(&mut attr) } {
            Ok(fd) => Ok(Self { _fd: fd }),
            Err(error) if error.raw_os_error() == Some(libc::EBUSY) => Err(unavailable(
                "an XDP program is already attached; use Pinned",
                Some(error),
            )),
            Err(error) => Err(sys::error(LinkCreate::STAGE, error)),
        }
    }
}
