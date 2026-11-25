//! Configuration building blocks shared by every backend.
//!
//! Each backend owns its config type; core keeps only [`MulticastInterface`] and
//! [`validate`] helpers. Sizes use `std::num::NonZero*` directly, so zero is
//! unrepresentable.

use std::net::Ipv4Addr;

/// Interface selection for multicast join.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MulticastInterface {
    /// IPv4 interface address; `None` lets OS pick.
    pub v4: Option<Ipv4Addr>,
    /// IPv6 interface scope id; `None` lets OS pick.
    pub v6_scope_id: Option<u32>,
}

/// Checks constructors run before first allocation or syscall.
pub mod validate {
    use crate::error::TransportError;

    /// Product `a * b`, rejecting overflow.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] naming `field` when product overflows `usize`.
    pub fn checked_product(
        field: &'static str,
        a: usize,
        b: usize,
    ) -> Result<usize, TransportError> {
        a.checked_mul(b).ok_or(TransportError::InvalidConfig {
            field,
            reason: "size product overflows usize",
        })
    }

    /// Reject `value` above backend limit `max`; `max` itself passes.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidConfig`] naming `field` when `value > max`.
    pub fn at_most(field: &'static str, value: u32, max: u32) -> Result<(), TransportError> {
        if value > max {
            return Err(TransportError::InvalidConfig {
                field,
                reason: "above backend limit",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::validate;
    use crate::TransportError;

    #[test]
    fn checked_product_rejects_overflow_naming_field() {
        assert_eq!(
            validate::checked_product("region", 4096, 2048).ok(),
            Some(8_388_608)
        );
        assert!(matches!(
            validate::checked_product("region", usize::MAX, 2),
            Err(TransportError::InvalidConfig {
                field: "region",
                ..
            })
        ));
    }

    #[test]
    fn at_most_accepts_limit_rejects_above() {
        assert!(validate::at_most("slots", 32_768, 32_768).is_ok());
        assert!(matches!(
            validate::at_most("slots", 32_769, 32_768),
            Err(TransportError::InvalidConfig { field: "slots", .. })
        ));
    }
}
