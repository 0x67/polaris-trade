//! Statistic-band logic layered on the generated `StatType`.
//!
//! The typed field enums (`Action`, `Side`, `LevelAction`, `BreakRefKind`,
//! `ControlKind`, `StatType`) are generated into `crate::generated`; only the band
//! derivation is hand-written logic.

use crate::generated::StatType;

/// Band a [`StatType`] falls in. `Unassigned` marks a value outside every
/// reserved range and must never be reachable from a defined variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatBand {
    Equities,
    DerivativesCrypto,
    VenueEscape,
    Unassigned,
}

impl StatType {
    /// Reserved band this stat kind belongs to.
    pub const fn band(self) -> StatBand {
        match self as u16 {
            0x0000..=0x00FF => StatBand::Equities,
            0x0100..=0x01FF => StatBand::DerivativesCrypto,
            0xFF00..=0xFFFF => StatBand::VenueEscape,
            _ => StatBand::Unassigned,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Action, StatBand, StatType};

    // Vocabulary is fixed and each action has a stable discriminant; the record
    // set and every adapter encode against these values.
    #[test]
    fn action_vocabulary() {
        let all = [
            Action::Add,
            Action::Cancel,
            Action::Modify,
            Action::Fill,
            Action::FillWithPrice,
            Action::Trade,
            Action::Reset,
        ];
        // discriminants are dense 0..7 and unique
        let mut seen = [false; 7];
        for a in all {
            let d = a as usize;
            assert!(!seen[d], "duplicate action discriminant {d}");
            seen[d] = true;
        }
        assert!(
            seen.iter().all(|&s| s),
            "action discriminants not dense 0..7"
        );
    }

    // Every defined stat kind resolves to a real reserved band, never
    // Unassigned; that invariant is what makes adding a kind additive.
    #[test]
    fn stat_type_bands() {
        let equities = [
            StatType::PriceLimits,
            StatType::ReferencePrice,
            StatType::IndexPrice,
            StatType::Vwap,
        ];
        for s in equities {
            assert_eq!(s.band(), StatBand::Equities);
        }
        let derivs = [
            StatType::FundingRate,
            StatType::OpenInterest,
            StatType::NetAssetValue,
        ];
        for s in derivs {
            assert_eq!(s.band(), StatBand::DerivativesCrypto);
        }
        assert_eq!(StatType::VenueSpecific.band(), StatBand::VenueEscape);
    }
}
