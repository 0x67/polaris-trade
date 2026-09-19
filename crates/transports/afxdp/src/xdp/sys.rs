//! Hand-written `bpf()` ABI: numbers, per-command attr prefixes, one syscall wrapper.
//!
//! Each command gets own `#[repr(C, align(8))]` prefix of kernel `union bpf_attr`
//! with every padding byte named; kernel zero-extends rest (uapi `linux/bpf.h`).
//! Const asserts pin sizes and offsets, so drift from uapi fails build.

use std::{
    ffi::{c_int, c_long},
    io,
    mem::offset_of,
    os::fd::{FromRawFd, OwnedFd, RawFd},
    ptr,
};

use transport_core::TransportError;

use crate::unavailable;

/// `BPF_MAP_TYPE_XSKMAP`.
pub(crate) const MAP_TYPE_XSKMAP: u32 = 17;
/// `BPF_PROG_TYPE_XDP`.
pub(crate) const PROG_TYPE_XDP: u32 = 6;
/// `BPF_XDP` attach type.
pub(crate) const ATTACH_XDP: u32 = 37;
/// `BPF_NOEXIST`: map update never replaces live entry.
pub(crate) const NOEXIST: u64 = 1;
/// `XDP_FLAGS_SKB_MODE`.
pub(crate) const XDP_FLAGS_SKB_MODE: u32 = 1 << 1;
/// `XDP_FLAGS_DRV_MODE`.
pub(crate) const XDP_FLAGS_DRV_MODE: u32 = 1 << 2;

/// Attr prefix of one `bpf()` command.
///
/// # Safety
///
/// Implementor is `#[repr(C, align(8))]` and laid out as kernel's `union bpf_attr`
/// member for [`CMD`](Self::CMD) up to its size, every byte a named field, so no
/// uninitialised padding reaches kernel.
pub(crate) unsafe trait Attr {
    /// `bpf()` command number.
    const CMD: c_int;
    /// Error stage naming command.
    const STAGE: &'static str;
}

/// `BPF_MAP_CREATE` prefix through `map_ifindex`.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct MapCreate {
    pub(crate) map_type: u32,
    pub(crate) key_size: u32,
    pub(crate) value_size: u32,
    pub(crate) max_entries: u32,
    pub(crate) map_flags: u32,
    pub(crate) inner_map_fd: u32,
    pub(crate) numa_node: u32,
    pub(crate) map_name: [u8; 16],
    pub(crate) map_ifindex: u32,
}

/// `BPF_MAP_UPDATE_ELEM` prefix; `key` and `value` are addresses.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct MapUpdateElem {
    pub(crate) map_fd: u32,
    pub(crate) pad: u32,
    pub(crate) key: u64,
    pub(crate) value: u64,
    pub(crate) flags: u64,
}

/// `BPF_PROG_LOAD` prefix through `expected_attach_type`.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct ProgLoad {
    pub(crate) prog_type: u32,
    pub(crate) insn_cnt: u32,
    pub(crate) insns: u64,
    pub(crate) license: u64,
    pub(crate) log_level: u32,
    pub(crate) log_size: u32,
    pub(crate) log_buf: u64,
    pub(crate) kern_version: u32,
    pub(crate) prog_flags: u32,
    pub(crate) prog_name: [u8; 16],
    pub(crate) prog_ifindex: u32,
    pub(crate) expected_attach_type: u32,
}

/// `BPF_OBJ_GET` prefix; `pathname` is address of NUL-terminated path.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct ObjGet {
    pub(crate) pathname: u64,
    pub(crate) bpf_fd: u32,
    pub(crate) file_flags: u32,
}

/// `BPF_OBJ_GET_INFO_BY_FD` prefix; `info` is address of `info_len`-byte buffer.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct ObjGetInfo {
    pub(crate) bpf_fd: u32,
    pub(crate) info_len: u32,
    pub(crate) info: u64,
}

/// `BPF_LINK_CREATE` prefix; target is interface index.
#[repr(C, align(8))]
#[derive(Default)]
pub(crate) struct LinkCreate {
    pub(crate) prog_fd: u32,
    pub(crate) target_ifindex: u32,
    pub(crate) attach_type: u32,
    pub(crate) flags: u32,
}

/// Bytes of [`MapInfo`] asked of kernel.
pub(crate) const MAP_INFO_LEN: u32 = 24;

/// `struct bpf_map_info` prefix through `map_flags`; kernel fills this much.
#[repr(C)]
#[derive(Default)]
pub(crate) struct MapInfo {
    pub(crate) map_type: u32,
    pub(crate) id: u32,
    pub(crate) key_size: u32,
    pub(crate) value_size: u32,
    pub(crate) max_entries: u32,
    pub(crate) map_flags: u32,
}

// uapi layout pins: sizes from research probes on 7.0, offsets after each padding
const _: () = {
    assert!(size_of::<MapCreate>() == 48);
    assert!(offset_of!(MapCreate, map_ifindex) == 44);
    assert!(size_of::<MapUpdateElem>() == 32);
    assert!(offset_of!(MapUpdateElem, key) == 8);
    assert!(size_of::<ProgLoad>() == 72);
    assert!(offset_of!(ProgLoad, prog_name) == 48);
    assert!(offset_of!(ProgLoad, expected_attach_type) == 68);
    assert!(size_of::<ObjGet>() == 16);
    assert!(size_of::<ObjGetInfo>() == 16);
    assert!(offset_of!(ObjGetInfo, info) == 8);
    assert!(size_of::<LinkCreate>() == 16);
    assert!(size_of::<MapInfo>() == MAP_INFO_LEN as usize);
};

// SAFETY: every impl below is `#[repr(C, align(8))]`, field order and widths as
// uapi member for its command, sizes and offsets pinned by const asserts above,
// padding named.
unsafe impl Attr for MapCreate {
    const CMD: c_int = 0;
    const STAGE: &'static str = "bpf(MAP_CREATE)";
}
// SAFETY: as `MapCreate`.
unsafe impl Attr for MapUpdateElem {
    const CMD: c_int = 2;
    const STAGE: &'static str = "bpf(MAP_UPDATE_ELEM)";
}
// SAFETY: as `MapCreate`.
unsafe impl Attr for ProgLoad {
    const CMD: c_int = 5;
    const STAGE: &'static str = "bpf(PROG_LOAD)";
}
// SAFETY: as `MapCreate`.
unsafe impl Attr for ObjGet {
    const CMD: c_int = 7;
    const STAGE: &'static str = "bpf(OBJ_GET)";
}
// SAFETY: as `MapCreate`.
unsafe impl Attr for ObjGetInfo {
    const CMD: c_int = 15;
    const STAGE: &'static str = "bpf(OBJ_GET_INFO_BY_FD)";
}
// SAFETY: as `MapCreate`.
unsafe impl Attr for LinkCreate {
    const CMD: c_int = 28;
    const STAGE: &'static str = "bpf(LINK_CREATE)";
}

/// Run `bpf(A::CMD, attr)`, returning its non-negative result.
///
/// # Safety
///
/// Every address field of `attr` is zero or points at memory valid for kernel
/// read or write of length its command implies (sizes in `attr` or map), live
/// until return.
pub(crate) unsafe fn bpf<A: Attr>(attr: &mut A) -> io::Result<c_long> {
    // SAFETY: `A` has kernel layout for `CMD` (`Attr` contract) and goes with its
    // exact size; addresses inside it valid per caller contract
    let ret = unsafe { libc::syscall(libc::SYS_bpf, A::CMD, ptr::from_mut(attr), size_of::<A>()) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

/// Run fd-returning command, adopting new descriptor.
///
/// # Safety
///
/// As [`bpf`]; `A::CMD` returns new file descriptor on success.
pub(crate) unsafe fn bpf_fd<A: Attr>(attr: &mut A) -> io::Result<OwnedFd> {
    // SAFETY: forwarded caller contract
    let ret = unsafe { bpf(attr) }?;
    let fd = RawFd::try_from(ret).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    // SAFETY: command returned fresh descriptor nothing else owns
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Address of `ptr` for attr address field; kernel reaches memory through it.
pub(crate) fn addr<T: ?Sized>(ptr: *const T) -> u64 {
    ptr.cast::<u8>().expose_provenance() as u64
}

/// Map `bpf()` failure: EPERM names missing capabilities, rest is `Io` at `stage`.
pub(crate) fn error(stage: &'static str, error: io::Error) -> TransportError {
    if error.raw_os_error() == Some(libc::EPERM) {
        unavailable("needs CAP_BPF and CAP_NET_ADMIN", Some(error))
    } else {
        TransportError::Io { stage, error }
    }
}
