//! Participant (broker) identity. The alpha code is the primary key; venue
//! numeric ids are aliases; per-venue capability flags declare where attribution
//! exists so consumers never assume uniform coverage.

use std::collections::HashMap;

use super::registry::VenueId;

/// Minted participant id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ParticipantId(pub u32);

/// Where a venue attributes broker identity. Some venues attribute on order
/// adds, some only on executions, some not at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct AttributionCapability {
    /// Attribution present on order adds.
    pub on_adds: bool,
    /// Attribution present on executions.
    pub on_fills: bool,
}

/// Mints one participant id per alpha broker code, aliases venue numeric ids to
/// it, and tracks per-venue attribution capability.
pub struct ParticipantRegistry {
    by_alpha: HashMap<[u8; 7], ParticipantId>,
    numeric: HashMap<(VenueId, u32), ParticipantId>,
    capability: HashMap<VenueId, AttributionCapability>,
    next: u32,
}

impl Default for ParticipantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ParticipantRegistry {
    /// Empty registry; minting starts at id 1.
    pub fn new() -> Self {
        ParticipantRegistry {
            by_alpha: HashMap::new(),
            numeric: HashMap::new(),
            capability: HashMap::new(),
            next: 1,
        }
    }

    /// Mint (or return the existing) id for an alpha code, optionally aliasing a
    /// venue numeric id to it.
    pub fn upsert(
        &mut self,
        alpha: [u8; 7],
        venue: VenueId,
        numeric: Option<u32>,
    ) -> ParticipantId {
        let id = match self.by_alpha.get(&alpha) {
            Some(&id) => id,
            None => {
                let id = ParticipantId(self.next);
                self.next += 1;
                self.by_alpha.insert(alpha, id);
                id
            }
        };
        if let Some(n) = numeric {
            self.numeric.insert((venue, n), id);
        }
        id
    }

    /// Resolve by alpha code.
    pub fn resolve_alpha(&self, alpha: [u8; 7]) -> Option<ParticipantId> {
        self.by_alpha.get(&alpha).copied()
    }

    /// Resolve by a venue's numeric participant id.
    pub fn resolve_numeric(&self, venue: VenueId, numeric: u32) -> Option<ParticipantId> {
        self.numeric.get(&(venue, numeric)).copied()
    }

    /// Declare a venue's attribution capability.
    pub fn set_capability(&mut self, venue: VenueId, cap: AttributionCapability) {
        self.capability.insert(venue, cap);
    }

    /// A venue's attribution capability; unset venues report no attribution.
    pub fn capability(&self, venue: VenueId) -> AttributionCapability {
        self.capability.get(&venue).copied().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Alpha code is the primary key; a venue numeric id aliases to it; a second
    // venue's numeric alias for the same broker resolves to the same id; and
    // capability is reported per venue (default: no attribution).
    #[test]
    fn participant_capability() {
        let mut r = ParticipantRegistry::new();
        let (v1, v2) = (VenueId(1), VenueId(2));

        let id = r.upsert(*b"FIRM001", v1, Some(42));
        assert_eq!(r.resolve_alpha(*b"FIRM001"), Some(id));
        assert_eq!(
            r.resolve_numeric(v1, 42),
            Some(id),
            "numeric aliases to the alpha id"
        );

        let id2 = r.upsert(*b"FIRM001", v2, Some(900));
        assert_eq!(id, id2, "same broker, one id across venues");
        assert_eq!(r.resolve_numeric(v2, 900), Some(id));

        r.set_capability(
            v1,
            AttributionCapability {
                on_adds: true,
                on_fills: false,
            },
        );
        r.set_capability(
            v2,
            AttributionCapability {
                on_adds: false,
                on_fills: true,
            },
        );
        assert!(r.capability(v1).on_adds && !r.capability(v1).on_fills);
        assert!(!r.capability(v2).on_adds && r.capability(v2).on_fills);
        assert_eq!(
            r.capability(VenueId(9)),
            AttributionCapability::default(),
            "unset venue"
        );
    }
}
