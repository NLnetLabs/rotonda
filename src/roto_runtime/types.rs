use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    path::PathBuf, sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use chrono::serde::ts_microseconds;
use inetnum::{addr::Prefix, asn::Asn};
use log::debug;
use rotonda_store::prelude::multi::RouteStatus;
use routecore::bgp::{
    message::UpdateMessage,
    nlri::afisafi::Nlri,
    types::AfiSafiType
};
use serde::Deserialize;

use crate::{
    ingress::IngressId,
    manager,
    payload::{RotondaPaMap, RotondaRoute},
};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FilterName(String);

impl Default for FilterName {
    fn default() -> Self {
        FilterName("".into())
    }
}

// XXX LH: not a fan of calling load_filter_name from here, quite a surprising
// side effect of deserializing a config parameter.
impl<'a, 'de: 'a> Deserialize<'de> for FilterName {
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
        Self { 0: value }
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

pub type CompiledRoto = std::sync::Mutex<roto::Compiled>;

#[derive(Default)]
pub struct OutputStream<M> {
    msgs: Vec<M>,
    entry: LogEntry,
}

pub type RotoOutputStream = OutputStream<Output>;

impl<M> OutputStream<M> {
    pub fn new() -> Self {
        Self {
            msgs: vec![],
            entry: LogEntry::new(),
        }
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

    pub fn entry(&mut self) -> &mut LogEntry {
        &mut self.entry
    }
    pub fn take_entry(&mut self) -> LogEntry {
        std::mem::take(&mut self.entry)
    }

    pub fn print(&self, msg: impl AsRef<str>) {
        eprintln!("roto output: {}", msg.as_ref());
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FreshRouteContext {
    pub bgp_msg: UpdateMessage<Bytes>,
    pub status: RouteStatus,
    pub provenance: Provenance,
    // reprocessing: bool // true if this RouteContext is attached to values
    // facilitating a query (and thus the bgp_msg itself likely is  None).
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MrtContext {
    pub status: RouteStatus,
    pub provenance: Provenance,
}
impl MrtContext {
    pub fn provenance(&self) -> Provenance {
        self.provenance
    }
}

// LH: attempt to capture both fresh and re-process contexts with an enum:
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteContext {
    // Fresh, Realtime,
    Fresh(FreshRouteContext),
    Mrt(MrtContext),
    // Data coming form a pRIB somewhere West. XXX could/should this
    // contain an IngressId/MUI ?
    Reprocess,
}

impl From<FreshRouteContext> for RouteContext {
    fn from(context: FreshRouteContext) -> Self {
        RouteContext::Fresh(context)
    }
}

impl RouteContext {
    pub fn for_reprocessing() -> Self {
        Self::Reprocess
    }

    pub fn for_mrt_dump(provenance: Provenance) -> Self {
        Self::Mrt(MrtContext {
            status: RouteStatus::Active,
            provenance,
        })
    }

    pub fn new(
        bgp_msg: UpdateMessage<Bytes>,
        status: RouteStatus,
        provenance: Provenance,
    ) -> Self {
        FreshRouteContext::new(bgp_msg, status, provenance).into()
    }

    // XXX add setter for withdrawals?

    pub fn ingress_id(&self) -> u32 {
        match self {
            Self::Fresh(ctx) => ctx.provenance().ingress_id,
            Self::Mrt(ctx) => ctx.provenance().ingress_id,
            Self::Reprocess => todo!(),
        }
    }
}

impl FreshRouteContext {
    pub fn new(
        bgp_msg: UpdateMessage<Bytes>,
        status: RouteStatus,
        provenance: Provenance,
    ) -> Self {
        Self {
            bgp_msg,
            status,
            provenance,
        }
    }

    pub fn message(&self) -> &UpdateMessage<Bytes> {
        &self.bgp_msg
    }

    pub fn provenance(&self) -> Provenance {
        self.provenance
    }

    pub fn status(&self) -> RouteStatus {
        self.status
    }

    pub fn update_status(&mut self, status: RouteStatus) {
        self.status = status;
    }
}

//------------ Provenance ----------------------------------------------------

/// A sized struct containing session/state information for BMP and/or BGP.
///
/// The Provenance struct holds information that pertains to the session. This
/// information comes from configuration, or is exchanged in the first stage
/// of a session prior to the actual routing information is exchanged.
/// Typically, the information in Provenance is not available in the
/// individual routing information messages (e.g. BGP UPDATE PDUs), but is
/// useful or necessary to process such messages.
///
/// For BGP, this means information from the BGP OPEN message. For BMP, that
/// is information from the PerPeerHeader: as we currently split up the
/// encapsulated BGP UPDATE message per NLRI into N `PrefixRoutes` typevalues,
/// we lose the PerPeerHeader after the filter in the connector Unit.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Provenance {
    //#[serde(skip)]
    pub timestamp: chrono::DateTime<Utc>,

    /// The unique ID for the session.
    ///
    /// For the first 'control' messages in a BGP/BMP session, this ingress_id
    /// might be a general ID registered by the connector, not a session
    /// specific ID.
    pub ingress_id: u32, // rotonda::ingress::IngressId

    /// The remote address of the BGP session.
    ///
    /// If this is not yet available (e.g. for the first messages of a BMP
    /// session), this holds the remote IP of the BMP session itself, i.e. the
    /// monitored router.
    pub peer_ip: IpAddr,

    /// The remote ASN of the BGP session.
    ///
    /// If this is not yet available (e.g. for the first messages of a BMP
    /// session), this holds ASN(0).
    pub peer_asn: Asn,

    /// The remote ip address for the TCP connection.
    ///
    /// For BGP, the connection_ip and peer_ip are the same. For BMP, the
    /// connection_ip holds the IP address of the monitored router.
    pub connection_ip: IpAddr,

    // pub peer_bgp_id: routecore::bgp::path_attributes::BgpIdentifier,
    /// The BMP PeerType (1 byte) and PeerDistuingisher (8 bytes).
    ///
    /// These are stored together as the combination of the two is used to
    /// disambiguate peers in certain scenarios. PeerType can be 0, 1 or 2,
    /// and only for 1 or 2 the RouteDistinguisher is set. So for the
    /// majority, the value of peer_distuingisher will be a 0 for PeerType ==
    /// Global Instance Peer, followed by 8 more zeroes.
    pub peer_distuingisher: [u8; 9],

    pub peer_rib_type: PeerRibType,
}

impl Provenance {
    pub fn for_bgp(ingress_id: u32, peer_ip: IpAddr, peer_asn: Asn) -> Self {
        Self::new(
            ingress_id,
            peer_ip,
            peer_asn,
            peer_ip, // connection ==~ peer_ip
            [0u8; 9],
            PeerRibType::OutPost,
        )
    }

    pub fn for_bmp(
        ingress_id: u32,
        peer_ip: IpAddr,
        peer_asn: Asn,
        connection_ip: IpAddr,
        peer_distuingisher: [u8; 9],
        peer_rib_type: PeerRibType,
    ) -> Self {
        Self::new(
            ingress_id,
            peer_ip,
            peer_asn,
            connection_ip,
            peer_distuingisher,
            peer_rib_type,
        )
    }

    pub fn new(
        ingress_id: u32,
        peer_ip: IpAddr,
        peer_asn: Asn,
        connection_ip: IpAddr,
        peer_distuingisher: [u8; 9],
        peer_rib_type: PeerRibType,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            ingress_id,
            peer_ip,
            peer_asn,
            connection_ip,
            peer_distuingisher,
            peer_rib_type,
        }
    }

    pub fn mock() -> Self {
        todo!()
    }
}

//------------ PeerRibType ---------------------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Default)]
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
            routecore::bmp::message::RibType::Unimplemented(_) => {
                PeerRibType::OutPost
            }
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
        timestamp: chrono::DateTime<Utc>
    ) -> TimestampedOSMR {
        TimestampedOSMR {
            timestamp: timestamp.timestamp(),
            record: self
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
            record: value
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
            mp_unreach_afisafi: value.mp_unreach_afisafi
        }
    }
}


impl LogEntry {
    pub fn new() -> Self {
        Self { 
            timestamp: Utc::now(),
            .. Default::default()
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

    pub fn entry(
        entry: LogEntry,
        ingress_id: Option<IngressId>,
    ) -> Self {
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
                    value.0.to_string()
                );
                return Err(());
            }
        };

        Ok(res)
    }
}

pub(crate) fn explode_announcements(
    bgp_update: &UpdateMessage<Bytes>,
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pas = bgp_update.path_attributes()?;
    let pamap = RotondaPaMap(pas.into());

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
    bgp_update: &UpdateMessage<Bytes>,
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pamap = RotondaPaMap(
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
