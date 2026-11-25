//! Venue registry: the per-venue metadata the datafeed needs, as data keyed by
//! the config name rather than a closed enum. Adding a venue adds a descriptor
//! here and touches no other module.

use md_pipeline::{
    ClosureModel, DynAdapter, RollModel, SessionModel, StreamId, VenueAdapter, VenueId,
};
use proto_nasdaq::adapter::{self as nasdaq, NasdaqAdapter};
use serde::{Deserialize, Deserializer, Serializer};

use crate::config::DatafeedConfig;

/// One venue the datafeed can run, found by its config `name`. Identity is the
/// name: no `PartialEq`, since comparing the `make` pointer is unpredictable.
#[derive(Debug)]
pub struct VenueDescriptor {
    /// Config-facing name, the `venue:` value in a deployment config.
    pub name: &'static str,
    /// Lowercase venue tag for the storage tree and derived filenames.
    pub tag: &'static str,
    /// Feed version token for the storage tree. Proto-independent, so a
    /// dual-channel venue is one version (its output merges both channels).
    pub version: &'static str,
    /// MDC1 header feed id, owned by each venue crate. The tap stamps it and
    /// replay reconcile checks a capture's own feed against it; two feeds of
    /// one venue can share a venue_id, so this is what separates them.
    pub feed_id: u8,
    /// Trading-day model: the session anchor windows floor from and when the
    /// capture date dir rolls. Must match what the venue's adapter declares.
    pub session: SessionModel,
    /// Ingest streams the venue's adapter consumes, one per wire channel. A
    /// replay container that carries stream 0 only cannot feed more than one.
    /// Order is part of the contract: the first stream is the one whose trades
    /// feed the session's bars, VWAP, and trade memo (`session::SOURCE_STREAM`),
    /// and a two-stream live venue tags its primary leg with the first and its secondary
    /// leg with the second.
    pub streams: &'static [StreamId],
    /// Whether a live run dials the venue over SoupBinTCP, one session per
    /// stream. NASDAQ TotalView publishes over MoldUDP64, which has no live
    /// source here.
    pub soupbin_live: bool,
    /// Builds the venue's adapter for one session from the deployment config.
    pub make: fn(&DatafeedConfig) -> Box<dyn DynAdapter>,
}

/// NASDAQ TotalView-ITCH 5.0, single channel.
pub const NASDAQ: VenueDescriptor = VenueDescriptor {
    name: "nasdaq",
    tag: "nasdaq",
    version: "v5",
    feed_id: proto_nasdaq::FEED_NASDAQ,
    // ET midnight moves between 04:00 and 05:00 UTC with DST, so the anchor
    // resolves per trade date through a zone database.
    session: SessionModel {
        roll: RollModel::DailyZoned {
            tz: "America/New_York",
            local_ns: 0,
        },
        closure: ClosureModel::None,
    },
    streams: &[StreamId(0)],
    soupbin_live: false,
    make: make_nasdaq,
};

fn make_nasdaq(config: &DatafeedConfig) -> Box<dyn DynAdapter> {
    Box::new(NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(config.venue_id),
        session_midnight_ns: config.session_midnight_ns,
    }))
}

/// Every venue this binary ships, found by config name.
pub const VENUES: &[VenueDescriptor] = &[NASDAQ];

/// Deserialize a config venue name into its built-in descriptor, so an unknown
/// venue fails config load at startup instead of at first dispatch.
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    de: D,
) -> Result<&'static VenueDescriptor, D::Error> {
    let name = String::deserialize(de)?;
    VENUES.iter().find(|v| v.name == name).ok_or_else(|| {
        let known: Vec<&str> = VENUES.iter().map(|v| v.name).collect();
        serde::de::Error::custom(format!("unknown venue {name:?}, expected one of {known:?}"))
    })
}

/// Serialize a descriptor back to its config name.
pub(crate) fn serialize<S: Serializer>(
    venue: &&'static VenueDescriptor,
    ser: S,
) -> Result<S::Ok, S::Error> {
    ser.serialize_str(venue.name)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The tap windows on the descriptor's session and dispatch reads its streams,
    // so both must match what the venue's adapter declares. An adapter's
    // `streams()` is a set while descriptor order carries its own contract, so
    // the streams compare as sorted lists.
    #[test]
    fn builtin_descriptors_agree_with_their_adapters() {
        for venue in VENUES {
            let config: DatafeedConfig = serde_json::from_str(&format!(
                r#"{{"venue":"{}","venue_id":1,"session_midnight_ns":0,
                    "source":{{"kind":"live","endpoint":"127.0.0.1:1"}}}}"#,
                venue.name
            ))
            .expect("config parses");
            assert_eq!(
                (venue.make)(&config).session(),
                venue.session,
                "{} session",
                venue.name
            );
        }
        let sorted = |streams: &[StreamId]| {
            let mut sorted = streams.to_vec();
            sorted.sort();
            sorted
        };
        assert_eq!(sorted(NASDAQ.streams), sorted(NasdaqAdapter::streams()));
    }

    // The session feeds bars, the VWAP, and the trade memo from `SOURCE_STREAM`
    // alone, and a live dual venue tags its primary leg with the first stream. A
    // venue leading with another id would publish trades that reach no bar, or
    // tag primary frames as secondary.
    #[test]
    fn every_venue_leads_with_the_aggregate_stream() {
        for venue in VENUES {
            assert_eq!(
                venue.streams.first(),
                Some(&crate::session::SOURCE_STREAM),
                "{}",
                venue.name
            );
        }
    }
}
