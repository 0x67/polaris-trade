//! XDP redirect over raw `bpf()`, no libbpf: program, XSKMAP, attach link.

mod link;
mod map;
mod program;
mod sys;

use std::os::fd::{AsFd, BorrowedFd};

pub(crate) use link::XdpLink;
use map::XskMap;
use transport_core::TransportError;

use crate::XdpRedirect;

/// Route frames of `queue` to bound socket `xsk`: map insert, then program
/// load, then attach. `Some` link in built-in mode (drop detaches), `None` in
/// pinned mode (external program stays attached).
pub(crate) fn install(
    redirect: &XdpRedirect,
    ifindex: u32,
    queue: u32,
    xsk: BorrowedFd<'_>,
) -> Result<Option<XdpLink>, TransportError> {
    match redirect {
        XdpRedirect::Builtin { mode } => {
            // config validation keeps `queue` below `u32::MAX`
            let map = XskMap::create(queue + 1)?;
            map.insert(queue, xsk)?;
            // program holds map, link holds program: both fds may close after attach
            let prog = program::load(map.as_fd())?;
            XdpLink::attach(prog.as_fd(), ifindex, *mode).map(Some)
        }
        XdpRedirect::Pinned { path } => {
            // entry leaves map when socket closes
            XskMap::open_pinned(path, queue)?.insert(queue, xsk)?;
            Ok(None)
        }
    }
}
