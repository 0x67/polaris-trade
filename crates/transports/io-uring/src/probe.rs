//! Ring setup and receive-path detection.
//!
//! Only opcodes are probeable. Buffer rings are detected by registering one
//! whose arguments are valid by construction, so EINVAL can only mean
//! unsupported. Multishot recv is detected by arming one against that empty
//! ring: prep rejects unknown multishot flag with EINVAL inline, while
//! supporting kernel completes it at once with ENOBUFS, before any data moves.

use std::{io, mem, os::fd::RawFd};

use io_uring::{IoUring, Probe, opcode, types};
use transport_core::TransportError;

use crate::{BACKEND, driver::UD_PROBE, io_error, ring_mem::RingMem};

// never driver's own group, so probe ring cannot collide with it
const PROBE_BGID: u16 = 1;

/// Receive path: how buffers reach kernel and how recvs are armed. Ordered
/// by preference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum RecvPath {
    /// Provided-buffer group refilled by `ProvideBuffers`, single-shot recvs.
    Legacy,
    /// Registered buffer ring refilled from user space, single-shot recvs.
    BufRing,
    /// Registered buffer ring, one multishot recv.
    Multishot,
}

/// Paths running kernel supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Support {
    pub(crate) legacy: bool,
    pub(crate) buf_ring: bool,
    pub(crate) multishot: bool,
}

impl Support {
    fn has(self, path: RecvPath) -> bool {
        match path {
            RecvPath::Legacy => self.legacy,
            RecvPath::BufRing => self.buf_ring,
            RecvPath::Multishot => self.multishot,
        }
    }
}

/// Ring with `sq` submission and `cq` completion entries, default setup flags.
///
/// # Errors
///
/// [`TransportError::Unavailable`] on EPERM or ENOSYS (`io_uring` disabled,
/// filtered or absent); [`TransportError::Io`] otherwise.
pub(crate) fn open_ring(sq: u32, cq: u32) -> Result<IoUring, TransportError> {
    IoUring::builder()
        .setup_cqsize(cq)
        .build(sq)
        .map_err(|error| match error.raw_os_error() {
            Some(libc::EPERM | libc::ENOSYS) => TransportError::Unavailable {
                backend: BACKEND,
                reason: "io_uring disabled or absent",
                error: Some(error),
            },
            _ => TransportError::Io {
                stage: "io_uring_setup",
                error,
            },
        })
}

/// Detect every path `ring` supports for socket `fd`. Leaves no buffer group
/// registered and no request that can consume data.
///
/// # Errors
///
/// [`TransportError::Io`] when detection itself fails for reason other than
/// missing support.
pub(crate) fn probe(ring: &mut IoUring, fd: RawFd) -> Result<Support, TransportError> {
    let legacy = legacy(ring);
    let probe_ring = RingMem::new(1)?;
    // SAFETY: `probe_ring` outlives registration: unregistered below before it
    // drops, or leaked when unregister fails.
    let buf_ring = match unsafe { probe_ring.register(&ring.submitter(), PROBE_BGID) } {
        Ok(()) => true,
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => false,
        Err(error) => {
            return Err(TransportError::Io {
                stage: "io_uring register buf ring",
                error,
            });
        }
    };
    if !buf_ring {
        return Ok(Support {
            legacy,
            buf_ring,
            multishot: false,
        });
    }
    let multishot = multishot(ring, fd);
    if let Err(error) = ring.submitter().unregister_buf_ring(PROBE_BGID) {
        // group still registered, kernel may read ring: never unmap it
        mem::forget(probe_ring);
        return Err(TransportError::Io {
            stage: "io_uring unregister buf ring",
            error,
        });
    }
    Ok(Support {
        legacy,
        buf_ring,
        multishot: multishot?,
    })
}

// probe failure reads as unsupported: kernels without probe lack every path
fn legacy(ring: &IoUring) -> bool {
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe).is_ok()
        && probe.is_supported(opcode::Recv::CODE)
        && probe.is_supported(opcode::ProvideBuffers::CODE)
        && ring.params().is_feature_fast_poll()
}

// immediate ENOBUFS or no completion: supported. Request left armed ends with
// ENOBUFS once probe group is gone, under `UD_PROBE`, which driver ignores
fn multishot(ring: &mut IoUring, fd: RawFd) -> Result<bool, TransportError> {
    let entry = opcode::RecvMulti::new(types::Fd(fd), PROBE_BGID)
        .flags(libc::MSG_TRUNC)
        .build()
        .user_data(UD_PROBE);
    // SAFETY: request names no memory; buffers come from probe group, empty.
    unsafe { ring.submission().push(&entry) }.map_err(|_| TransportError::Io {
        stage: "io_uring multishot probe",
        error: io::ErrorKind::WouldBlock.into(),
    })?;
    ring.submit().map_err(io_error("io_uring_enter"))?;
    let rejected = ring.completion().any(|cqe| {
        cqe.user_data() == UD_PROBE && cqe.result() < 0 && cqe.result() != -libc::ENOBUFS
    });
    Ok(!rejected)
}

/// Pick receive path: `forced` when kernel has it, else best supported.
///
/// # Errors
///
/// [`TransportError::Unsupported`] when `forced` path is absent;
/// [`TransportError::Unavailable`] when auto mode finds no path.
pub(crate) fn select(
    forced: Option<RecvPath>,
    support: Support,
) -> Result<RecvPath, TransportError> {
    match forced {
        Some(path) if support.has(path) => Ok(path),
        Some(_) => Err(TransportError::Unsupported {
            backend: BACKEND,
            op: "recv path",
        }),
        None => [RecvPath::Legacy, RecvPath::BufRing, RecvPath::Multishot]
            .into_iter()
            .filter(|&path| support.has(path))
            .max()
            .ok_or(TransportError::Unavailable {
                backend: BACKEND,
                reason: "kernel supports no recv path",
                error: None,
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE: Support = Support {
        legacy: false,
        buf_ring: false,
        multishot: false,
    };
    const LEGACY: Support = Support {
        legacy: true,
        ..NONE
    };
    const RINGS: Support = Support {
        legacy: true,
        buf_ring: true,
        ..NONE
    };
    const ALL: Support = Support {
        multishot: true,
        ..RINGS
    };

    #[test]
    fn select_honours_forced_path_only_when_present() {
        assert_eq!(
            select(Some(RecvPath::Legacy), LEGACY).ok(),
            Some(RecvPath::Legacy)
        );
        assert_eq!(
            select(Some(RecvPath::BufRing), ALL).ok(),
            Some(RecvPath::BufRing)
        );
        for absent in [RecvPath::BufRing, RecvPath::Multishot] {
            assert!(
                matches!(
                    select(Some(absent), LEGACY),
                    Err(TransportError::Unsupported {
                        op: "recv path",
                        ..
                    })
                ),
                "{absent:?} forced on legacy-only kernel"
            );
        }
    }

    #[test]
    fn select_auto_picks_best_supported() {
        let ring_only = Support {
            buf_ring: true,
            ..NONE
        };
        let cases = [
            (ALL, RecvPath::Multishot),
            (RINGS, RecvPath::BufRing),
            (ring_only, RecvPath::BufRing),
            (LEGACY, RecvPath::Legacy),
        ];
        for (support, want) in cases {
            assert_eq!(select(None, support).ok(), Some(want), "{support:?}");
        }
        assert!(matches!(
            select(None, NONE),
            Err(TransportError::Unavailable { .. })
        ));
    }
}
