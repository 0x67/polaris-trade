//! XSKMAP: queue index to `AF_XDP` socket, read by redirect program.

use std::{
    ffi::CString,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::Path,
};

use transport_core::TransportError;

use super::sys::{
    self, Attr, MAP_INFO_LEN, MAP_TYPE_XSKMAP, MapCreate, MapInfo, MapUpdateElem, NOEXIST, ObjGet,
    ObjGetInfo,
};
use crate::unavailable;

const NAME: [u8; 16] = *b"polaris_xsks\0\0\0\0";
// key is queue index, value socket fd
const ENTRY: u32 = 4;
const PATH_FIELD: &str = "redirect.path";

/// Open XSKMAP fd.
pub(crate) struct XskMap {
    fd: OwnedFd,
}

impl XskMap {
    /// New map of `entries` slots, keys `0..entries`.
    pub(crate) fn create(entries: u32) -> Result<Self, TransportError> {
        let mut attr = MapCreate {
            map_type: MAP_TYPE_XSKMAP,
            key_size: ENTRY,
            value_size: ENTRY,
            max_entries: entries,
            map_name: NAME,
            ..MapCreate::default()
        };
        // SAFETY: attr holds no address; `MAP_CREATE` returns new map fd
        let fd = unsafe { sys::bpf_fd(&mut attr) }
            .map_err(|error| sys::error(MapCreate::STAGE, error))?;
        Ok(Self { fd })
    }

    /// Map pinned at `path`, checked to be XSKMAP with 4-byte key and value
    /// holding key `queue`.
    pub(crate) fn open_pinned(path: &Path, queue: u32) -> Result<Self, TransportError> {
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            TransportError::InvalidConfig {
                field: PATH_FIELD,
                reason: "contains NUL byte",
            }
        })?;
        let mut attr = ObjGet {
            pathname: sys::addr(path.as_ptr()),
            ..ObjGet::default()
        };
        // SAFETY: `path` NUL-terminated and live for call; `OBJ_GET` returns new fd
        let fd = unsafe { sys::bpf_fd(&mut attr) }.map_err(|error| match error.raw_os_error() {
            Some(libc::ENOENT | libc::ENOTDIR | libc::EACCES) => TransportError::InvalidConfig {
                field: PATH_FIELD,
                reason: "no pinned object there, or access denied",
            },
            _ => sys::error(ObjGet::STAGE, error),
        })?;
        let map = Self { fd };
        let info = map.info()?;
        if info.map_type != MAP_TYPE_XSKMAP || info.key_size != ENTRY || info.value_size != ENTRY {
            return Err(TransportError::InvalidConfig {
                field: PATH_FIELD,
                reason: "not an XSKMAP with 4-byte key and value",
            });
        }
        if queue >= info.max_entries {
            return Err(TransportError::InvalidConfig {
                field: "queue",
                reason: "beyond pinned XSKMAP size",
            });
        }
        Ok(map)
    }

    // kernel fills `MapInfo` prefix; pinned program or link yields other type
    fn info(&self) -> Result<MapInfo, TransportError> {
        let mut info = MapInfo::default();
        let mut attr = ObjGetInfo {
            bpf_fd: self.fd.as_raw_fd().cast_unsigned(),
            info_len: MAP_INFO_LEN,
            info: sys::addr(&raw mut info),
        };
        // SAFETY: `info` holds `info_len` writable bytes of plain `u32`s, live for call
        unsafe { sys::bpf(&mut attr) }.map_err(|error| sys::error(ObjGetInfo::STAGE, error))?;
        Ok(info)
    }

    /// Point key `queue` at socket `xsk`; never replaces live entry.
    pub(crate) fn insert(&self, queue: u32, xsk: BorrowedFd<'_>) -> Result<(), TransportError> {
        let value = xsk.as_raw_fd().cast_unsigned();
        let mut attr = MapUpdateElem {
            map_fd: self.fd.as_raw_fd().cast_unsigned(),
            key: sys::addr(&raw const queue),
            value: sys::addr(&raw const value),
            flags: NOEXIST,
            ..MapUpdateElem::default()
        };
        // SAFETY: `queue` and `value` are 4 bytes each, matching map's key and
        // value size (created so or checked by `open_pinned`), live for call
        unsafe { sys::bpf(&mut attr) }.map_err(|error| match error.raw_os_error() {
            Some(libc::E2BIG) => TransportError::InvalidConfig {
                field: "queue",
                reason: "beyond XSKMAP size",
            },
            Some(libc::EEXIST) => unavailable("queue already served", Some(error)),
            _ => sys::error(MapUpdateElem::STAGE, error),
        })?;
        Ok(())
    }
}

impl AsFd for XskMap {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}
