use core::fmt;
use std::{
    cell::RefCell, collections::{HashMap, HashSet}, net::IpAddr, path::PathBuf, rc::Rc,
};

use chrono::serde::ts_microseconds;
use chrono::Utc;
use inetnum::{addr::Prefix, asn::Asn};
use log::debug;
use routecore::bgp::{
    message::UpdateMessage, nlri::afisafi::Nlri, types::AfiSafiType,
};
use serde::{Deserialize, Serialize};

use crate::{
    ingress::IngressId,
    manager,
    payload::{RotondaPaMap, RotondaRoute},
};

use super::MutLogEntry;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FilterName(String);

impl Default for FilterName {
    fn default() -> Self {
        FilterName("".into())
    }
}

// XXX LH: not a fan of calling load_filter_name from here, quite a surprising
// side effect of deserializing a config parameter.
impl<'de> Deserialize<'de> for FilterName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // This has to be a String, even though we pass a &str to
        // ShortString::from(), because of the way that newer versions of the
        // toml crate work.
        //
        // See: https://github.com/toml-rs/toml/issues/597
        let s: String = Deserialize::deserialize(deserializer)?;
        let filter_name = FilterName(s);
        Ok(manager::load_filter_name(filter_name))
    }
}

impl From<String> for FilterName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Display for FilterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RotoScripts {
    scripts: HashMap<FilterName, PathBuf>,
}

impl RotoScripts {
    pub fn new(scripts: HashMap<FilterName, PathBuf>) -> Self {
        Self { scripts }
    }

    pub fn get(&self, name: &FilterName) -> Option<&PathBuf> {
        self.scripts.get(name)
    }

    pub fn get_filter_names(&self) -> HashSet<FilterName> {
        self.scripts.keys().cloned().collect::<HashSet<_>>()
    }

    pub fn get_script_origins(&self) -> HashSet<PathBuf> {
        self.scripts.values().cloned().collect::<HashSet<_>>()
    }
}

pub type RotoPackage = std::sync::Mutex<roto::Package>;

#[derive(Default)]
pub struct OutputStream<M> {
    msgs: Vec<M>,
    entry: MutLogEntry,
}

pub type RotoOutputStream = OutputStream<Output>;

impl<M> OutputStream<M> {
    pub fn new() -> Self {
        Self::with_vec(vec![])
    }

    pub fn with_vec(v: Vec<M>) -> Self {
        Self {
            msgs: v,
            entry: Rc::new(RefCell::new(LogEntry::new())),
        }
    }

    /// Create a new `OutputStream` wrapped in an `Rc<RefCell<>>`
    pub fn new_rced() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new()))
    }

    pub fn push(&mut self, msg: M) {
        self.msgs.push(msg);
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, M> {
        self.msgs.drain(..)
    }

    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }

    pub fn entry(&mut self) -> MutLogEntry {
        self.entry.clone()
    }

    pub fn take_entry(&mut self) -> MutLogEntry {
        std::mem::take(&mut self.entry)
    }

    pub fn print(&self, msg: impl AsRef<str>) {
        eprintln!("{}", msg.as_ref());
    }
}

impl<M> IntoIterator for OutputStream<M> {
    type Item = M;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.msgs.into_iter()
    }
}

#[derive(Clone, Debug)]
pub enum Output {
    /// Community observed in Path Attributes.
    Community(u32),

    /// ASN observed in the AS_PATH Path Attribute.
    Asn(Asn),

    /// ASN observed as right-most AS in the AS_PATH.
    Origin(Asn),

    // TODO stick the PeerIp in here from roto, if we can, otherwise get it
    // from elsewhere in Rotonda
    /// A BMP PeerDownNotification was observed.
    PeerDown,
    /// Prefix observed in the BGP or BMP message.
    Prefix(Prefix),

    /// Variant to support user-defined log entries.
    Custom((u32, u32)),

    /// Extensive, composable log entry, see [`LogEntry`].
    Entry(LogEntry),
}

#[derive(Copy, Clone, Debug)]
pub struct InsertionInfo {
    pub prefix_new: bool,
    pub new_peer: bool,
    //is_new_best: bool,
    //replaced_route: RotondaRoute,
}


//------------ PeerRibType ---------------------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PeerRibType {
    InPre,
    InPost,
    Loc,
    OutPre,
    #[default]
    OutPost, // This is the default for BGP messages
}

// XXX LH: perhaps we can/should get rid of PeerRibType here if we extend the
// one in routecore?
impl From<(bool, routecore::bmp::message::RibType)> for PeerRibType {
    fn from(
        (is_post_policy, rib_type): (bool, routecore::bmp::message::RibType),
    ) -> Self {
        match rib_type {
            routecore::bmp::message::RibType::AdjRibIn => {
                if is_post_policy {
                    PeerRibType::InPost
                } else {
                    PeerRibType::InPre
                }
            }
            routecore::bmp::message::RibType::AdjRibOut => {
                if is_post_policy {
                    PeerRibType::OutPost
                } else {
                    PeerRibType::OutPre
                }
            }
            routecore::bmp::message::RibType::LocRib => PeerRibType::Loc,
        }
    }
}

impl fmt::Display for PeerRibType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerRibType::InPre => write!(f, "adj-RIB-in-pre"),
            PeerRibType::InPost => write!(f, "adj-RIB-in-post"),
            PeerRibType::Loc => write!(f, "RIB-loc"),
            PeerRibType::OutPre => write!(f, "adj-RIB-out-pre"),
            PeerRibType::OutPost => write!(f, "adj-RIB-out-pre"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
#[serde(untagged)]
pub enum OutputStreamMessageRecord {
    Route(Option<RotondaRoute>),
    Peerdown(IpAddr, Asn),
    Custom(CustomLogEntry),
    Entry(LogEntry),
}

impl OutputStreamMessageRecord {
    pub fn into_timestamped(
        self,
        timestamp: chrono::DateTime<Utc>,
    ) -> TimestampedOSMR {
        TimestampedOSMR {
            timestamp: timestamp.timestamp(),
            record: self,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct TimestampedOSMR {
    timestamp: i64,
    record: OutputStreamMessageRecord,
}

impl From<OutputStreamMessageRecord> for TimestampedOSMR {
    fn from(value: OutputStreamMessageRecord) -> Self {
        Self {
            timestamp: Utc::now().timestamp(),
            record: value,
        }
    }
}

impl From<OutputStreamMessage> for TimestampedOSMR {
    fn from(value: OutputStreamMessage) -> Self {
        let value = value.record;
        value.into()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
pub struct CustomLogEntry {
    id: u32,
    value: u32,
}

impl From<(u32, u32)> for CustomLogEntry {
    fn from(t: (u32, u32)) -> Self {
        Self {
            id: t.0,
            value: t.1,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct LogEntry {
    #[serde(with = "ts_microseconds")]
    pub timestamp: chrono::DateTime<Utc>,
    pub origin_as: Option<Asn>,
    pub peer_as: Option<Asn>,
    pub as_path_hops: Option<usize>,
    pub conventional_reach: usize,
    pub conventional_unreach: usize,
    pub mp_reach: Option<usize>,
    pub mp_reach_afisafi: Option<AfiSafiType>,
    pub mp_unreach: Option<usize>,
    pub mp_unreach_afisafi: Option<AfiSafiType>,
    pub custom: Option<String>,
}

use serde_with;

#[serde_with::skip_serializing_none]
#[derive(serde::Serialize)]
pub struct MinimalLogEntry {
    #[serde(with = "ts_microseconds")]
    pub timestamp: chrono::DateTime<Utc>,
    pub origin_as: Option<Asn>,
    pub peer_as: Option<Asn>,
    pub as_path_hops: Option<usize>,
    pub conventional_reach: usize,
    pub conventional_unreach: usize,
    pub mp_reach: Option<usize>,
    pub mp_reach_afisafi: Option<AfiSafiType>,
    pub mp_unreach: Option<usize>,
    pub mp_unreach_afisafi: Option<AfiSafiType>,
}

impl From<LogEntry> for MinimalLogEntry {
    fn from(value: LogEntry) -> Self {
        Self {
            timestamp: value.timestamp,
            origin_as: value.origin_as,
            peer_as: value.peer_as,
            as_path_hops: value.as_path_hops,
            conventional_reach: value.conventional_reach,
            conventional_unreach: value.conventional_unreach,
            mp_reach: value.mp_reach,
            mp_reach_afisafi: value.mp_reach_afisafi,
            mp_unreach: value.mp_unreach,
            mp_unreach_afisafi: value.mp_unreach_afisafi,
        }
    }
}

impl LogEntry {
    pub fn new() -> Self {
        Self {
            //timestamp: Utc::now(),
            ..Default::default()
        }
    }
    pub fn into_minimal(self) -> MinimalLogEntry {
        self.into()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OutputStreamMessage {
    name: String,
    topic: String,
    record: OutputStreamMessageRecord,
    ingress_id: Option<IngressId>,
}

const MQTT_NAME: &str = "mqtt";
impl OutputStreamMessage {
    pub fn prefix(
        record: Option<RotondaRoute>,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "prefix".into(),
            record: OutputStreamMessageRecord::Route(record),
            ingress_id,
        }
    }

    pub fn community(
        record: Option<RotondaRoute>,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "community".into(),
            record: OutputStreamMessageRecord::Route(record),
            ingress_id,
        }
    }

    pub fn asn(
        record: Option<RotondaRoute>,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "asn".into(),
            record: OutputStreamMessageRecord::Route(record),
            ingress_id,
        }
    }

    pub fn origin(
        record: Option<RotondaRoute>,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "origin".into(),
            record: OutputStreamMessageRecord::Route(record),
            ingress_id,
        }
    }

    pub fn peer_down(
        name: String,
        topic: String,
        peer_ip: IpAddr,
        peer_asn: Asn,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name,
            topic,
            record: OutputStreamMessageRecord::Peerdown(peer_ip, peer_asn),
            ingress_id,
        }
    }
    pub fn custom(
        id: u32,
        value: u32,
        ingress_id: Option<IngressId>,
    ) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "custom".into(),
            record: OutputStreamMessageRecord::Custom((id, value).into()),
            ingress_id,
        }
    }

    pub fn entry(entry: LogEntry, ingress_id: Option<IngressId>) -> Self {
        Self {
            name: MQTT_NAME.into(),
            topic: "log_entry".into(),
            record: OutputStreamMessageRecord::Entry(entry),
            ingress_id,
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_topic(&self) -> &String {
        &self.topic
    }

    pub fn get_record(&self) -> &OutputStreamMessageRecord {
        &self.record
    }

    pub fn into_record(self) -> OutputStreamMessageRecord {
        self.record
    }

    pub fn get_ingress_id(&self) -> Option<IngressId> {
        self.ingress_id
    }
}

impl<O> TryFrom<(Nlri<O>, RotondaPaMap)> for RotondaRoute {
    type Error = ();
    fn try_from(value: (Nlri<O>, RotondaPaMap)) -> Result<Self, Self::Error> {
        let res = match value.0 {
            Nlri::Ipv4Unicast(n) => RotondaRoute::Ipv4Unicast(n, value.1),
            Nlri::Ipv4Multicast(n) => RotondaRoute::Ipv4Multicast(n, value.1),
            Nlri::Ipv6Unicast(n) => RotondaRoute::Ipv6Unicast(n, value.1),
            Nlri::Ipv6Multicast(n) => RotondaRoute::Ipv6Multicast(n, value.1),

            Nlri::Ipv4UnicastAddpath(..)
            | Nlri::Ipv4MulticastAddpath(..)
            | Nlri::Ipv4MplsUnicast(..)
            | Nlri::Ipv4MplsUnicastAddpath(..)
            | Nlri::Ipv4MplsVpnUnicast(..)
            | Nlri::Ipv4MplsVpnUnicastAddpath(..)
            | Nlri::Ipv4RouteTarget(..)
            | Nlri::Ipv4RouteTargetAddpath(..)
            | Nlri::Ipv4FlowSpec(..)
            | Nlri::Ipv4FlowSpecAddpath(..)
            | Nlri::Ipv6UnicastAddpath(..)
            | Nlri::Ipv6MulticastAddpath(..)
            | Nlri::Ipv6MplsUnicast(..)
            | Nlri::Ipv6MplsUnicastAddpath(..)
            | Nlri::Ipv6MplsVpnUnicast(..)
            | Nlri::Ipv6MplsVpnUnicastAddpath(..)
            | Nlri::Ipv6FlowSpec(..)
            | Nlri::Ipv6FlowSpecAddpath(..)
            | Nlri::L2VpnVpls(..)
            | Nlri::L2VpnVplsAddpath(..)
            | Nlri::L2VpnEvpn(..)
            | Nlri::L2VpnEvpnAddpath(..) => {
                debug!(
                    "AFI/SAFI {} not yet supported in RotondaRoute",
                    value.0
                );
                return Err(());
            }
        };

        Ok(res)
    }
}

pub(crate) fn explode_announcements(
    bgp_update: &UpdateMessage<impl routecore::Octets>,
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pas = bgp_update.path_attributes()?;
    let pamap = RotondaPaMap::new(pas.into());

    for a in bgp_update.announcements()? {
        let a = a?;
        if let Ok(r) = (a, pamap.clone()).try_into() {
            res.push(r);
        } else {
            debug!("unsupported AFI/SAFI in explode_announcements");
        }
    }
    Ok(res)
}

pub(crate) fn explode_withdrawals(
    bgp_update: &UpdateMessage<impl routecore::Octets>,
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pamap = RotondaPaMap::new(
        routecore::bgp::path_attributes::OwnedPathAttributes::new(
            bgp_update.pdu_parse_info(),
            vec![],
        ),
    );

    for w in bgp_update.withdrawals()? {
        let w = w?;
        if let Ok(r) = (w, pamap.clone()).try_into() {
            res.push(r);
        } else {
            debug!("unsupported AFI/SAFI in explode_withdrawals");
        }
    }
    Ok(res)
}

//------------ Temporary types -----------------------------------------------

// PeerId was part of the old roto, but used throughout the BMP state machine.
// This should go (elsewhere), eventually.

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PeerId {
    pub addr: IpAddr,
    pub asn: Asn,
}

impl PeerId {
    pub fn new(addr: IpAddr, asn: Asn) -> Self {
        Self { addr, asn }
    }
}
