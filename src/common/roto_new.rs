use core::fmt;
use std::{cell::RefCell, collections::{HashMap, HashSet}, ffi::OsString, net::IpAddr, path::PathBuf};

use bytes::Bytes;
use chrono::Utc;
use inetnum::{addr::Prefix, asn::Asn};
use rotonda_store::prelude::multi::RouteStatus;
use routecore::{bgp::{message::UpdateMessage, nlri::afisafi::Nlri, workshop::route::RouteWorkshop}, bmp::message::PerPeerHeader};
use serde::Deserialize;

use crate::{manager, payload::{RotondaPaMap, RotondaRoute}};

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
        // This has to be a String, even though we pass a &str to ShortString::from(), because of the way that newer
        // versions of the toml crate work. See: https://github.com/toml-rs/toml/issues/597
        let s: String = Deserialize::deserialize(deserializer)?;
        let filter_name = FilterName(s);
        Ok(manager::load_filter_name(filter_name))
    }
}

impl From<String> for FilterName {
    fn from(value: String) -> Self {
        Self {0: value}
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


//pub type CompiledRoto = RefCell<Option<roto::Compiled>>;
pub type CompiledRoto = std::sync::Mutex<roto::Compiled>;

pub fn ensure_compiled(
    cr: &mut Option<roto::Compiled>,
    filter: impl Into<PathBuf>,
) -> &mut roto::Compiled {
    if cr.is_none() {
        cr.replace(
            roto::read_files([filter.into().to_string_lossy()]).unwrap()
            .compile(rotonda_roto_runtime().unwrap(), usize::BITS / 8)
            .unwrap()
        );
    }
    cr.as_mut().unwrap()
}

pub use super::roto_runtime::rotonda_roto_runtime;

#[derive(Default)]
pub struct OutputStream<M>{
    msgs: Vec<M>,
}

pub type RotoOutputStream = OutputStream<Output>;

impl<M> OutputStream<M> {
    pub fn new() -> Self {
        Self { msgs: vec![] }
    }

    pub fn push(&mut self, msg: M) {
        self.msgs.push(msg);
    }
    pub fn drain(&mut self) -> std::vec::Drain<'_, M> {
        self.msgs.drain(..)
    }
}

impl<M> IntoIterator for OutputStream<M> {
    type Item = M;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.msgs.into_iter()
    }

}

#[derive(Copy, Clone, Debug)]
pub enum Output {
    /// Community observed in Path Attributes.
    Community(u32),

    /// ASN observed in the AS_PATH Path Attribute. 
    Asn(Asn),

    /// ASN observed as right-most AS in the AS_PATH.
    Origin(Asn),

    /// A BMP PeerDownNotification was observed. 
    PeerDown, // TODO stick the PeerIp in here from roto, if we can, otherwise
              // get it from elsewhere in Rotonda
    /// Prefix observed in the BGP or BMP message.
    Prefix(Prefix),

    /// Variant to support user-defined log entries.
    Custom(u32, u32),
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
    Fresh(FreshRouteContext), // Fresh, Realtime,
    Mrt(MrtContext),
    Reprocess // Data coming form a pRIB somewhere West. XXX could/should this
              // contain an IngressId/MUI ?
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
            provenance
        })
    }

    pub fn new(
        bgp_msg: UpdateMessage<Bytes>,
        status: RouteStatus,
        provenance: Provenance,
    ) -> Self {
        FreshRouteContext::new(
            bgp_msg,
            status,
            provenance,
        ).into()
    }

    // XXX add setter for withdrawals?

    pub fn ingress_id(&self) -> u32 {
        match self {
            Self::Fresh(ctx) => ctx.provenance().ingress_id,
            Self::Mrt(ctx) => ctx.provenance().ingress_id,
            Self::Reprocess => todo!()
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

    /*
    pub fn get_attrs_builder(&self) -> Result<PaMap, VmError> {
        if let Some(msg) = &self.bgp_msg {
            PaMap::from_update_pdu(&msg.clone().into_inner())
                .map_err(|_| VmError::InvalidMsgType)
        } else {
            Err(VmError::InvalidPayload)
        }
    }
    */
}


//------------ Provenance ----------------------------------------------------

/// A sized struct containing session/state information for BMP and/or BGP.
///
/// The Provenance struct holds information that pertains to the session. This
/// information comes from configuration, or is exchanged in the first
/// stage of a session prior to the actual routing information is exchanged.
/// Typically, the information in Provenance is not available in the
/// individual routing information messages (e.g. BGP UPDATE PDUs), but is
/// useful or necessary to process such messages.
///
/// For BGP, this means information from the BGP OPEN message.
/// For BMP, that is information from the PerPeerHeader: as we currently split
/// up the encapsulated BGP UPDATE message per NLRI into N `PrefixRoutes`
/// typevalues, we lose the PerPeerHeader after the filter in the connector
/// Unit.
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
    /// For BGP, the connection_ip and peer_ip are the same.
    /// For BMP, the connection_ip holds the IP address of the monitored
    /// router.
    pub connection_ip: IpAddr,

    // pub peer_bgp_id: routecore::bgp::path_attributes::BgpIdentifier,

    /// The BMP PeerType (1 byte) and PeerDistuingisher (8 bytes).
    ///
    /// These are stored together as the combination of the two is used to
    /// disambiguate peers in certain scenarios.
    /// PeerType can be 0, 1 or 2, and only for 1 or 2 the RouteDistinguisher
    /// is set. So for the majority, the value of peer_distuingisher will be a
    /// 0 for PeerType == Global Instance Peer, followed by 8 more zeroes.
    pub peer_distuingisher: [u8; 9],

    pub peer_rib_type: PeerRibType,
}

impl Provenance {

    pub fn for_bgp(
        ingress_id: u32,
        peer_ip: IpAddr,
        peer_asn: Asn,
    ) -> Self {
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


#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OutputStreamMessage {
    name: String,
    topic: String,
    record: RotondaRoute,
}

impl OutputStreamMessage {
    // pub fn new(name: ShortString, topic: String, record: TypeValue) -> Self {
    //     Self {
    //         name, topic, record
    //     }
    // }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_topic(&self) -> &String {
        &self.topic
    }

    pub fn get_record(&self) -> &RotondaRoute {
        &self.record
    }
}

impl<O> From<(Nlri<O>, RotondaPaMap)> for RotondaRoute {
    fn from(value: (Nlri<O>, RotondaPaMap)) -> Self {
        match value.0 {
            Nlri::Ipv4Unicast(n) => RotondaRoute::Ipv4Unicast(n, value.1),
            Nlri::Ipv4UnicastAddpath(_) => todo!(),
            Nlri::Ipv4Multicast(_) => todo!(),
            Nlri::Ipv4MulticastAddpath(_) => todo!(),
            Nlri::Ipv4MplsUnicast(_) => todo!(),
            Nlri::Ipv4MplsUnicastAddpath(_) => todo!(),
            Nlri::Ipv4MplsVpnUnicast(_) => todo!(),
            Nlri::Ipv4MplsVpnUnicastAddpath(_) => todo!(),
            Nlri::Ipv4RouteTarget(_) => todo!(),
            Nlri::Ipv4RouteTargetAddpath(_) => todo!(),
            Nlri::Ipv4FlowSpec(_) => todo!(),
            Nlri::Ipv4FlowSpecAddpath(_) => todo!(),
            Nlri::Ipv6Unicast(n) => RotondaRoute::Ipv6Unicast(n, value.1),
            Nlri::Ipv6UnicastAddpath(_) => todo!(),
            Nlri::Ipv6Multicast(_) => todo!(),
            Nlri::Ipv6MulticastAddpath(_) => todo!(),
            Nlri::Ipv6MplsUnicast(_) => todo!(),
            Nlri::Ipv6MplsUnicastAddpath(_) => todo!(),
            Nlri::Ipv6MplsVpnUnicast(_) => todo!(),
            Nlri::Ipv6MplsVpnUnicastAddpath(_) => todo!(),
            Nlri::Ipv6FlowSpec(_) => todo!(),
            Nlri::Ipv6FlowSpecAddpath(_) => todo!(),
            Nlri::L2VpnVpls(_) => todo!(),
            Nlri::L2VpnVplsAddpath(_) => todo!(),
            Nlri::L2VpnEvpn(_) => todo!(),
            Nlri::L2VpnEvpnAddpath(_) => todo!(),
        }
    }
}
/*
impl<O> From<Nlri<O>> for RotondaRoute {
    fn from(nlri: Nlri<O>) -> Self {
        match nlri {
            Nlri::Ipv4Unicast(n) => RotondaRoute::Ipv4Unicast(RouteWorkshop::new(n)),
            Nlri::Ipv4UnicastAddpath(_) => todo!(),
            Nlri::Ipv4Multicast(_) => todo!(),
            Nlri::Ipv4MulticastAddpath(_) => todo!(),
            Nlri::Ipv6Unicast(n) => RotondaRoute::Ipv6Unicast(RouteWorkshop::new(n)),
            Nlri::Ipv6UnicastAddpath(_) => todo!(),
            Nlri::Ipv6Multicast(_) => todo!(),
            Nlri::Ipv6MulticastAddpath(_) => todo!(),
            _ => unimplemented!(),
        /*
            Nlri::Ipv4MplsUnicast(_) => todo!(),
            Nlri::Ipv4MplsUnicastAddpath(_) => todo!(),
            Nlri::Ipv4MplsVpnUnicast(_) => todo!(),
            Nlri::Ipv4MplsVpnUnicastAddpath(_) => todo!(),
            Nlri::Ipv4RouteTarget(_) => todo!(),
            Nlri::Ipv4RouteTargetAddpath(_) => todo!(),
            Nlri::Ipv4FlowSpec(_) => todo!(),
            Nlri::Ipv4FlowSpecAddpath(_) => todo!(),
            Nlri::Ipv6Unicast(_) => todo!(),
            Nlri::Ipv6UnicastAddpath(_) => todo!(),
            Nlri::Ipv6Multicast(_) => todo!(),
            Nlri::Ipv6MulticastAddpath(_) => todo!(),
            Nlri::Ipv6MplsUnicast(_) => todo!(),
            Nlri::Ipv6MplsUnicastAddpath(_) => todo!(),
            Nlri::Ipv6MplsVpnUnicast(_) => todo!(),
            Nlri::Ipv6MplsVpnUnicastAddpath(_) => todo!(),
            Nlri::Ipv6FlowSpec(_) => todo!(),
            Nlri::Ipv6FlowSpecAddpath(_) => todo!(),
            Nlri::L2VpnVpls(_) => todo!(),
            Nlri::L2VpnVplsAddpath(_) => todo!(),
            Nlri::L2VpnEvpn(_) => todo!(),
            Nlri::L2VpnEvpnAddpath(_) => todo!(),
        */
        }
    }
}
*/

pub(crate) fn explode_announcements(
    bgp_update: &UpdateMessage<Bytes>
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pas = bgp_update.path_attributes()?;
    let pamap = RotondaPaMap(pas.into());

    for a in bgp_update.announcements()? {
        let a = a?;
        res.push((a, pamap.clone()).into());
    }
    Ok(res)
}

pub(crate) fn explode_withdrawals(
    bgp_update: &UpdateMessage<Bytes>
) -> Result<Vec<RotondaRoute>, routecore::bgp::ParseError> {
    let mut res = vec![];

    let pamap = RotondaPaMap(
        routecore::bgp::path_attributes::OwnedPathAttributes::new(
            bgp_update.pdu_parse_info(),
            vec![]
        )
    );

    for w in bgp_update.withdrawals()? {
        let w = w?;
        res.push((w, pamap.clone()).into());
    }
    Ok(res)
}

//------------ Temporary types ------------------------------------------------


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

