//! Versioned venue rule config. Effective-date-ranged versions, fail-fast
//! load (refuses overlap, interior gap, unknown vocabulary), and date-keyed
//! selection via [`VersionedConfig::at`]. Wire-carried state (prices, sizes,
//! timestamps, sequence numbers) is never duplicated into config; config only
//! carries rules an operator changes on a schedule (tiers, board routing,
//! status flag vocabulary, index membership).

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use time::Date;

/// Human-friendly calendar date for the serde edge. Range math internally
/// uses [`time::Date`] for correct ordering and day-successor across
/// month/leap boundaries; config files read `{year, month, day}` instead of
/// `time::Date`'s own encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CivilDate {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl CivilDate {
    /// Convert to `time::Date`. Fails on impossible calendar dates (month 13,
    /// Feb 30, ...).
    pub fn to_date(self) -> Result<Date, ConfigError> {
        let bad = || ConfigError::BadDate {
            year: self.year,
            month: self.month,
            day: self.day,
        };
        let month = time::Month::try_from(self.month).map_err(|_| bad())?;
        Date::from_calendar_date(self.year, month, self.day).map_err(|_| bad())
    }

    /// Convert from `time::Date`.
    pub fn from_date(d: Date) -> Self {
        CivilDate {
            year: d.year(),
            month: d.month() as u8,
            day: d.day(),
        }
    }
}

/// Inclusive effective-date range for one config version: both `start` and
/// `end` are covered. Caller-constructed ranges are assumed `start <= end`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EffectiveRange {
    pub start: Date,
    pub end: Date,
}

/// Serde shadow for `EffectiveRange`, carrying dates as `CivilDate`.
#[derive(Serialize, Deserialize)]
struct EffectiveRangeWire {
    start: CivilDate,
    end: CivilDate,
}

impl Serialize for EffectiveRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        EffectiveRangeWire {
            start: CivilDate::from_date(self.start),
            end: CivilDate::from_date(self.end),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EffectiveRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = EffectiveRangeWire::deserialize(deserializer)?;
        let start = wire.start.to_date().map_err(serde::de::Error::custom)?;
        let end = wire.end.to_date().map_err(serde::de::Error::custom)?;
        Ok(EffectiveRange { start, end })
    }
}

impl EffectiveRange {
    /// True if `date` falls within `[start, end]`.
    pub fn contains(&self, date: Date) -> bool {
        date >= self.start && date <= self.end
    }

    /// True if the two ranges share at least one date.
    pub fn overlaps(&self, other: &EffectiveRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

/// Which vocabulary a config token draws from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VocabKind {
    Board,
    State,
    StatusFlag,
}

impl core::fmt::Display for VocabKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let s = match self {
            VocabKind::Board => "board",
            VocabKind::State => "state",
            VocabKind::StatusFlag => "status_flag",
        };
        write!(f, "{s}")
    }
}

/// Load or lookup failure. `at()` finding no covering version is not an
/// error, it is a normal lookup miss (see [`VersionedConfig::at`]).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("config has no versions")]
    Empty,
    #[error("config versions overlap: [{a_start} .. {a_end}] and [{b_start} .. {b_end}]")]
    Overlap {
        a_start: Date,
        a_end: Date,
        b_start: Date,
        b_end: Date,
    },
    #[error("config has gap between {prev_end} and {next_start}")]
    Gap { prev_end: Date, next_start: Date },
    #[error("unknown {kind} token: {token}")]
    UnknownVocab { kind: VocabKind, token: String },
    #[error("bad calendar date: {year:04}-{month:02}-{day:02}")]
    BadDate { year: i32, month: u8, day: u8 },
}

/// Adapter-declared allowed token set, one per [`VocabKind`]. `load` cross-
/// checks every version's [`DeclaresVocabulary::referenced`] tokens against
/// this before accepting the config.
#[derive(Clone, Debug, Default)]
pub struct Vocabulary {
    allowed: HashMap<VocabKind, HashSet<String>>,
}

impl Vocabulary {
    /// Empty vocabulary; nothing is allowed until declared.
    pub fn new() -> Self {
        Vocabulary {
            allowed: HashMap::new(),
        }
    }

    /// Declare tokens allowed for `kind`.
    pub fn declare(
        &mut self,
        kind: VocabKind,
        tokens: impl IntoIterator<Item = String>,
    ) -> &mut Self {
        self.allowed.entry(kind).or_default().extend(tokens);
        self
    }

    fn allows(&self, kind: VocabKind, token: &str) -> bool {
        self.allowed
            .get(&kind)
            .is_some_and(|set| set.contains(token))
    }
}

/// Tokens one config version references, checked against a [`Vocabulary`] at
/// load time. Payload kinds with nothing to check return an empty vec.
pub trait DeclaresVocabulary {
    fn referenced(&self) -> Vec<(VocabKind, String)>;
}

/// Effective-date-ranged config versions for one payload type `T`. Built only
/// through [`VersionedConfig::load`], which refuses overlap, interior gap,
/// and unknown-vocabulary tokens, so every instance in hand is already valid.
#[derive(Clone, Debug)]
pub struct VersionedConfig<T> {
    versions: Vec<(EffectiveRange, T)>,
}

impl<T> VersionedConfig<T> {
    /// The version effective on `trade_date`, the processed event's date, not
    /// wall-clock time. `None` means `trade_date` predates the first version
    /// or postdates the last; a lookup miss, not a load-time gap (a gap
    /// between two configured ranges is refused at `load`).
    pub fn at(&self, trade_date: Date) -> Option<&T> {
        self.versions
            .iter()
            .find(|(r, _)| r.contains(trade_date))
            .map(|(_, t)| t)
    }
}

impl<T: DeclaresVocabulary> VersionedConfig<T> {
    /// Sort `versions` by start date, then refuse: no versions at all, any
    /// overlap between consecutive ranges, any interior gap between
    /// consecutive ranges, or any token a version references that `vocab`
    /// does not allow.
    pub fn load(
        mut versions: Vec<(EffectiveRange, T)>,
        vocab: &Vocabulary,
    ) -> Result<Self, ConfigError> {
        if versions.is_empty() {
            return Err(ConfigError::Empty);
        }
        versions.sort_by_key(|(r, _)| r.start);

        for w in versions.windows(2) {
            let prev = &w[0].0;
            let next = &w[1].0;
            if next.overlaps(prev) {
                return Err(ConfigError::Overlap {
                    a_start: prev.start,
                    a_end: prev.end,
                    b_start: next.start,
                    b_end: next.end,
                });
            }
            // next.start > prev.end already established above, so prev.end
            // is strictly below a valid date and next_day() cannot overflow.
            let expected = prev
                .end
                .next_day()
                .expect("prev.end < next.start rules out Date::MAX here");
            if next.start > expected {
                return Err(ConfigError::Gap {
                    prev_end: prev.end,
                    next_start: next.start,
                });
            }
        }

        for (_, payload) in &versions {
            for (kind, token) in payload.referenced() {
                if !vocab.allows(kind, &token) {
                    return Err(ConfigError::UnknownVocab { kind, token });
                }
            }
        }

        Ok(VersionedConfig { versions })
    }
}

/// One price-band tier: prices in `[price_min, price_max]` (venue fixed-point
/// scale) reject moves beyond `upper_band_bps`/`lower_band_bps` above and below
/// the reference price.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tier {
    pub price_min: i64,
    pub price_max: i64,
    pub upper_band_bps: u16,
    pub lower_band_bps: u16,
}

/// Price-band tier table for one effective version.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TierTable {
    pub tiers: Vec<Tier>,
}

impl DeclaresVocabulary for TierTable {
    fn referenced(&self) -> Vec<(VocabKind, String)> {
        Vec::new()
    }
}

/// What a board's records feed downstream. Local to config; adapters convert
/// to `orderbook-engine`'s own type (pipeline does not depend on
/// orderbook-engine).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Purpose {
    Book,
    Ohlcv,
    Chart,
    Tape,
    Rankings,
}

/// One board's routing rule: which purposes its records feed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoardRule {
    pub board: String,
    pub purposes: Vec<Purpose>,
}

/// Board-to-purpose routing map for one effective version.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BoardMap {
    pub boards: Vec<BoardRule>,
}

impl DeclaresVocabulary for BoardMap {
    fn referenced(&self) -> Vec<(VocabKind, String)> {
        self.boards
            .iter()
            .map(|b| (VocabKind::Board, b.board.clone()))
            .collect()
    }
}

/// One status flag token and whether it carries call-auction membership.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusFlagRule {
    pub token: String,
    pub call_auction: bool,
}

/// Status flag vocabulary for one effective version.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StatusFlagVocab {
    pub flags: Vec<StatusFlagRule>,
}

impl DeclaresVocabulary for StatusFlagVocab {
    fn referenced(&self) -> Vec<(VocabKind, String)> {
        self.flags
            .iter()
            .map(|n| (VocabKind::StatusFlag, n.token.clone()))
            .collect()
    }
}

/// One index member and its weight, in basis points.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMember {
    pub index: String,
    pub symbol: String,
    pub weight_bps: u32,
}

/// Index composition for one effective version.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexComposition {
    pub members: Vec<IndexMember>,
}

impl DeclaresVocabulary for IndexComposition {
    fn referenced(&self) -> Vec<(VocabKind, String)> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use time::macros::date;

    use super::*;

    fn range(start: Date, end: Date) -> EffectiveRange {
        EffectiveRange { start, end }
    }

    fn board_map(boards: &[&str]) -> BoardMap {
        BoardMap {
            boards: boards
                .iter()
                .map(|b| BoardRule {
                    board: b.to_string(),
                    purposes: vec![Purpose::Book],
                })
                .collect(),
        }
    }

    fn vocab_with_boards(boards: &[&str]) -> Vocabulary {
        let mut v = Vocabulary::new();
        v.declare(VocabKind::Board, boards.iter().map(|b| b.to_string()));
        v
    }

    // Contiguous versions (fed out of order) load fine once sorted, and each
    // version wins over exactly its own effective range.
    #[test]
    fn load_contiguous_multi_version_and_selects_by_date() {
        let vocab = vocab_with_boards(&["MAIN", "AUX1"]);
        let versions = vec![
            (
                range(date!(2026 - 02 - 01), date!(2026 - 12 - 31)),
                board_map(&["AUX1"]),
            ),
            (
                range(date!(2026 - 01 - 01), date!(2026 - 01 - 31)),
                board_map(&["MAIN"]),
            ),
        ];
        let cfg = VersionedConfig::load(versions, &vocab).expect("valid contiguous config");

        let jan = cfg.at(date!(2026 - 01 - 15)).unwrap();
        assert_eq!(jan.boards[0].board, "MAIN");
        let feb = cfg.at(date!(2026 - 02 - 15)).unwrap();
        assert_eq!(feb.boards[0].board, "AUX1");
    }

    // Two versions sharing a date refuse to load.
    #[test]
    fn load_rejects_overlap() {
        let vocab = vocab_with_boards(&["MAIN"]);
        let versions = vec![
            (
                range(date!(2026 - 01 - 01), date!(2026 - 01 - 20)),
                board_map(&["MAIN"]),
            ),
            (
                range(date!(2026 - 01 - 15), date!(2026 - 02 - 01)),
                board_map(&["MAIN"]),
            ),
        ];
        let err = VersionedConfig::load(versions, &vocab).unwrap_err();
        assert!(matches!(err, ConfigError::Overlap { .. }), "{err}");
    }

    // A day with no covering version between two configured ranges is a
    // load-time gap, refused up front rather than surfacing as a silent miss.
    #[test]
    fn load_rejects_interior_gap() {
        let vocab = vocab_with_boards(&["MAIN"]);
        let versions = vec![
            (
                range(date!(2026 - 01 - 01), date!(2026 - 01 - 10)),
                board_map(&["MAIN"]),
            ),
            (
                range(date!(2026 - 01 - 12), date!(2026 - 01 - 31)),
                board_map(&["MAIN"]),
            ),
        ];
        let err = VersionedConfig::load(versions, &vocab).unwrap_err();
        assert!(matches!(err, ConfigError::Gap { .. }), "{err}");
    }

    // A board token the adapter never declared refuses load by name.
    #[test]
    fn load_rejects_unknown_board_token() {
        let vocab = vocab_with_boards(&["MAIN"]);
        let versions = vec![(
            range(date!(2026 - 01 - 01), date!(2026 - 12 - 31)),
            board_map(&["XX"]),
        )];
        let err = VersionedConfig::load(versions, &vocab).unwrap_err();
        match err {
            ConfigError::UnknownVocab { kind, token } => {
                assert_eq!(kind, VocabKind::Board);
                assert_eq!(token, "XX");
            }
            other => panic!("expected UnknownVocab, got {other}"),
        }
    }

    // No versions at all refuses load rather than silently having no
    // coverage.
    #[test]
    fn load_rejects_empty() {
        let vocab = Vocabulary::new();
        let err = VersionedConfig::<BoardMap>::load(Vec::new(), &vocab).unwrap_err();
        assert!(matches!(err, ConfigError::Empty), "{err}");
    }

    // Both range endpoints are inclusive, and a date outside every configured
    // range is a lookup miss (`None`), not an error.
    #[test]
    fn at_boundaries_inclusive_and_outside_range_is_none() {
        let vocab = vocab_with_boards(&["MAIN"]);
        let versions = vec![(
            range(date!(2026 - 03 - 01), date!(2026 - 03 - 31)),
            board_map(&["MAIN"]),
        )];
        let cfg = VersionedConfig::load(versions, &vocab).unwrap();

        assert!(cfg.at(date!(2026 - 03 - 01)).is_some(), "start inclusive");
        assert!(cfg.at(date!(2026 - 03 - 31)).is_some(), "end inclusive");
        assert!(cfg.at(date!(2026 - 02 - 28)).is_none(), "before first");
        assert!(cfg.at(date!(2026 - 04 - 01)).is_none(), "after last");
    }

    // The manual CivilDate-backed serde impl round-trips a real EffectiveRange
    // through JSON, proving the wire shape (not just the in-memory struct).
    #[test]
    fn effective_range_json_round_trip() {
        let r = range(date!(2026 - 01 - 01), date!(2026 - 12 - 31));
        let json = serde_json::to_string(&r).unwrap();
        let back: EffectiveRange = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

    // An impossible calendar date crossing the serde boundary surfaces as a
    // deserialize error rather than panicking or silently clamping.
    #[test]
    fn civil_date_rejects_bad_calendar_date() {
        let json =
            r#"{"start":{"year":2026,"month":2,"day":30},"end":{"year":2026,"month":3,"day":1}}"#;
        let result: Result<EffectiveRange, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Feb 30 must not deserialize");
    }
}
