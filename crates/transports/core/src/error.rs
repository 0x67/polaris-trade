//! One error type for every transport operation.
//!
//! OS error sits in typed field named `error`, never `source`: workspace logs
//! format errors with `%err` (`Display` only), so its text belongs in `Display`,
//! and exposing it through `source()` too would print it twice in chain formatters.

use std::{fmt, io, net::SocketAddr};

/// Failure of any transport operation.
///
/// Reasons are `&'static str`, so building an error never allocates; dynamic
/// context (path, errno) travels inside the `io::Error`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Binding local socket failed.
    #[error("bind {addr}: {error}")]
    Bind {
        /// Local address requested.
        addr: SocketAddr,
        /// OS error.
        error: io::Error,
    },
    /// Connecting to remote peer failed.
    #[error("connect {addr}: {error}")]
    Connect {
        /// Remote address requested.
        addr: SocketAddr,
        /// OS error.
        error: io::Error,
    },
    /// Configuration rejected before any allocation or syscall.
    #[error("invalid config: {field}: {reason}")]
    InvalidConfig {
        /// Config field at fault.
        field: &'static str,
        /// Why value was rejected.
        reason: &'static str,
    },
    /// OS call failed at `stage`.
    #[error("{stage}: {error}")]
    Io {
        /// Operation that failed, e.g. `"recv_from"`.
        stage: &'static str,
        /// OS error; match on `error.kind()`.
        error: io::Error,
    },
    /// Data pending but no free buffer to land it.
    #[error("buffer pool exhausted ({in_use}/{capacity})")]
    PoolExhausted {
        /// Buffers taken when receive failed.
        in_use: usize,
        /// Buffers pool owns.
        capacity: usize,
    },
    /// Stream peer closed connection.
    #[error("peer closed")]
    PeerClosed,
    /// Backend does not offer requested operation variant.
    #[error("{backend}: {op} not supported")]
    Unsupported {
        /// Backend name, as [`Transport::name`](crate::Transport::name).
        backend: &'static str,
        /// Operation requested.
        op: &'static str,
    },
    /// Backend cannot run on this host or with these privileges.
    #[error(fmt = fmt_unavailable)]
    Unavailable {
        /// Backend name, as [`Transport::name`](crate::Transport::name).
        backend: &'static str,
        /// What is missing, e.g. `"needs CAP_NET_RAW"`.
        reason: &'static str,
        /// OS error behind it, when one exists.
        error: Option<io::Error>,
    },
}

#[expect(
    clippy::ref_option,
    reason = "thiserror fmt path passes each field by reference"
)]
fn fmt_unavailable(
    backend: &&'static str,
    reason: &&'static str,
    error: &Option<io::Error>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(f, "{backend} unavailable: {reason}")?;
    if let Some(error) = error {
        write!(f, ": {error}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_display_appends_os_error_only_when_present() {
        let bare = TransportError::Unavailable {
            backend: "io-uring",
            reason: "io_uring disabled or absent",
            error: None,
        };
        assert_eq!(
            bare.to_string(),
            "io-uring unavailable: io_uring disabled or absent"
        );

        let with_os = TransportError::Unavailable {
            backend: "afxdp",
            reason: "needs CAP_NET_RAW",
            error: Some(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "operation not permitted",
            )),
        };
        assert_eq!(
            with_os.to_string(),
            "afxdp unavailable: needs CAP_NET_RAW: operation not permitted"
        );
    }
}
