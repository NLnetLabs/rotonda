use log::debug;
use rotonda_store::match_options::QueryResult;

use rotonda_store::prefix_record::Meta;
use routecore::bgp::communities::{Community, HumanReadableCommunity};
use routecore::bgp::message::PduParseInfo;
use routecore::bgp::path_attributes::{OwnedPathAttributes, PathAttribute};
use routecore::bgp::path_selection::TiebreakerInfo;
use routecore::bgp::types::AfiSafiType;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};
use smallvec::{smallvec, SmallVec};
use std::fmt;
use uuid::Uuid;

use crate::ingress::{self, IngressId};
use crate::roto_runtime::types::{OutputStreamMessage, RouteContext};

// TODO: make this a reference
pub type RouterId = String;

//------------ UpstreamStatus ------------------------------------------------

#[derive(Clone, Debug)]
pub enum UpstreamStatus {
    /// No more data will be sent for the specified source.
    ///
    /// This could be because a network connection has been lost, or at the
    /// protocol level a session has been terminated, but need not be network
    /// related. E.g. it could be that the last message in a replay file has
    /// been loaded and replayed, or the last message in a test set has been
    /// pushed into the pipeline, etc.
    EndOfStream { ingress_id: ingress::IngressId },
}

//------------ Payload -------------------------------------------------------

// TODO macrofy
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RotondaRoute {
    Ipv4Unicast(routecore::bgp::nlri::afisafi::Ipv4UnicastNlri, RotondaPaMap),
    Ipv6Unicast(routecore::bgp::nlri::afisafi::Ipv6UnicastNlri, RotondaPaMap),
    Ipv4Multicast(
        routecore::bgp::nlri::afisafi::Ipv4MulticastNlri,
        RotondaPaMap,
    ),
    Ipv6Multicast(
        routecore::bgp::nlri::afisafi::Ipv6MulticastNlri,
        RotondaPaMap,
    ),
    // TODO support all routecore AfiSafiTypes
}

impl Serialize for RotondaRoute {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Route", 2)?;
        match self {
            RotondaRoute::Ipv4Unicast(n, _) => s.serialize_field("prefix", n),
            RotondaRoute::Ipv6Unicast(n, _) => s.serialize_field("prefix", n),
            RotondaRoute::Ipv4Multicast(n, _) => {
                s.serialize_field("prefix", n)
            }
            RotondaRoute::Ipv6Multicast(n, _) => {
                s.serialize_field("prefix", n)
            }
        }?;

        s.serialize_field("attributes", self.rotonda_pamap())?;
        s.end()
    }
}

impl RotondaRoute {
    pub fn owned_map(
        &self,
    ) -> routecore::bgp::path_attributes::OwnedPathAttributes {
        match self {
            RotondaRoute::Ipv4Unicast(_, p) => p.path_attributes(),
            RotondaRoute::Ipv6Unicast(_, p) => p.path_attributes(),
            RotondaRoute::Ipv4Multicast(_, p) => p.path_attributes(),
            RotondaRoute::Ipv6Multicast(_, p) => p.path_attributes(),
        }
    }

    pub fn rotonda_pamap(&self) -> &RotondaPaMap {
        match self {
            RotondaRoute::Ipv4Unicast(_, p) => p,
            RotondaRoute::Ipv6Unicast(_, p) => p,
            RotondaRoute::Ipv4Multicast(_, p) => p,
            RotondaRoute::Ipv6Multicast(_, p) => p,
        }
    }

    pub fn rotonda_pamap_mut(&mut self) -> &mut RotondaPaMap {
        match self {
            RotondaRoute::Ipv4Unicast(_, ref mut p) => p,
            RotondaRoute::Ipv6Unicast(_, ref mut p) => p,
            RotondaRoute::Ipv4Multicast(_, ref mut p) => p,
            RotondaRoute::Ipv6Multicast(_, ref mut p) => p,
        }
    }
}

impl fmt::Display for RotondaRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RotondaRoute::Ipv4Unicast(p, ..) => {
                write!(f, "RR-Ipv4Unicast {}", p)
            }
            RotondaRoute::Ipv6Unicast(p, ..) => {
                write!(f, "RR-Ipv6Unicast {}", p)
            }
            RotondaRoute::Ipv4Multicast(p, ..) => {
                write!(f, "RR-Ipv4Multicast {}", p)
            }
            RotondaRoute::Ipv6Multicast(p, ..) => {
                write!(f, "RR-Ipv6Multicast {}", p)
            }
        }
    }
}

impl Meta for RotondaPaMap {
    type Orderable<'a> = routecore::bgp::path_selection::OrdRoute<
        'a,
        routecore::bgp::path_selection::Rfc4271,
    >;

    type TBI = TiebreakerInfo;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

impl From<Vec<u8>> for RotondaPaMap {
    fn from(value: Vec<u8>) -> Self {
        OwnedPathAttributes::new(PduParseInfo::modern(), value).into()
    }
}

impl AsRef<[u8]> for RotondaPaMap {
    fn as_ref(&self) -> &[u8] {
        self.raw.as_ref()
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
#[derive(Serialize)]
pub struct RpkiInfo {
    rov: RovStatus,
}

impl From<u8> for RpkiInfo {
    fn from(value: u8) -> Self {
        let rov = match value {
            1 => RovStatus::NotFound,
            2 => RovStatus::Valid,
            4 => RovStatus::Invalid,
            _ => RovStatus::NotChecked
        };

        Self { rov }
    }
}

impl From<RpkiInfo> for u8 {
    fn from(value: RpkiInfo) -> Self {
        match value.rov {
            RovStatus::NotChecked => 0,
            RovStatus::NotFound => 1,
            RovStatus::Valid => 2,
            RovStatus::Invalid => 4,
        }
    }
}




impl From<RovStatus> for RpkiInfo {
    fn from(value: RovStatus) -> Self {
        Self { rov: value }
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
#[derive(Serialize)]
pub enum RovStatus {
    #[default]
    NotChecked,
    NotFound,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RotondaPaMap{
    // raw[0] is RpkiInfo
    // raw[1] is PduParseInfo
    // raw[2..] contains the path attributes blob
    raw: Vec<u8>,
}

// These from/to byte functions should ideally live in routecore, but as we
// will refactor many routecore types to zerocopy structs soon(tm), we define
// these here for now.
fn ppi_to_byte(ppi: PduParseInfo) -> u8 {
    match ppi.four_octet_enabled() {
        true => 1,
        false => 0,
    }
}

fn byte_to_ppi(byte: u8) -> PduParseInfo {
    if byte == 0x01 {
        PduParseInfo::modern()
    } else {
        PduParseInfo::legacy()
    }
}


impl RotondaPaMap {
    pub fn new(path_attributes: OwnedPathAttributes) -> Self {
        let ppi = path_attributes.pdu_parse_info();
        let mut pas = path_attributes.into_vec();
        let mut raw = Vec::with_capacity(2 + pas.len());
        
        let rpki_info = RpkiInfo::default();
        raw.push(rpki_info.into());
        raw.push(ppi_to_byte(ppi));

        raw.append(&mut pas);
        Self { raw }
    }

    pub fn set_rpki_info(&mut self, rpki_info: RpkiInfo) {
        self.raw[0] = rpki_info.into();
    }

    pub fn rpki_info(&self) -> RpkiInfo {
        self.raw[0].into()
    }

    pub fn path_attributes(&self) -> OwnedPathAttributes {
        let ppi = byte_to_ppi(self.raw[1]);
        OwnedPathAttributes::new(ppi, self.raw[2..].to_vec())
    }
}

impl fmt::Display for RotondaPaMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.path_attributes())
    }
}

impl Serialize for RotondaPaMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(None)?;
        let mut communities: Vec<HumanReadableCommunity> = vec![];
        for pa in self.path_attributes().iter().flatten() {
            match pa.to_owned().unwrap() {
                PathAttribute::StandardCommunities(list) => {
                    for c in list.communities() {
                        communities.push(HumanReadableCommunity(
                            Community::from(*c),
                        ));
                    }
                }
                PathAttribute::ExtendedCommunities(list) => {
                    for c in list.communities() {
                        communities.push(HumanReadableCommunity(
                            Community::from(*c),
                        ));
                    }
                }
                PathAttribute::LargeCommunities(list) => {
                    for c in list.communities() {
                        communities.push(HumanReadableCommunity(
                            Community::from(*c),
                        ));
                    }
                }
                PathAttribute::Ipv6ExtendedCommunities(list) => {
                    for c in list.communities() {
                        communities.push(HumanReadableCommunity(
                            Community::from(*c),
                        ));
                    }
                }

                pa => {
                    if pa.type_code() == 14 || pa.type_code() == 15 {
                        debug!("not including MP_REACH/MP_UNREACH path attributes in serialized output");
                    } else {
                        s.serialize_element(&pa)?;
                    }
                }
            }
        }

        // We need to wrap our collected communities in a struct with a field
        // named 'communities' to get the proper JSON output.
        // For the other path attributes, as they are actually variants of the
        // PathAttribute enum type, their name/type is included in the JSON
        // already.
        // See also https://serde.rs/json.html

        if !communities.is_empty() {
            #[derive(Serialize)]
            struct Communities {
                communities: Vec<HumanReadableCommunity>,
            }

            let c = Communities { communities };
            s.serialize_element(&c)?;
        }
        s.end()
    }
}

impl From<OwnedPathAttributes> for RotondaPaMap {
    fn from(value: OwnedPathAttributes) -> Self {
        RotondaPaMap::new(value)
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Payload {
    pub rx_value: RotondaRoute, //RouteWorkshop<N>, //was: TypeValue,
    pub context: RouteContext,
    pub trace_id: Option<u8>,
    pub received: std::time::Instant,
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        // Don't compare the received timestamp
        // self.source_id == other.source_id &&
        self.rx_value == other.rx_value && self.trace_id == other.trace_id
    }
}

impl Payload {
    pub fn new(
        rx_value: RotondaRoute,
        context: RouteContext,
        trace_id: Option<u8>,
    ) -> Self {
        Self {
            rx_value,
            context,
            trace_id,
            received: std::time::Instant::now(),
        }
    }

    pub fn with_received(
        rx_value: RotondaRoute,
        context: RouteContext,
        trace_id: Option<u8>,
        received: std::time::Instant,
    ) -> Self {
        Self {
            rx_value,
            context,
            trace_id,
            received,
        }
    }

    pub fn trace_id(&self) -> Option<u8> {
        self.trace_id
    }
}

//------------ Update --------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Update {
    Single(Payload),
    Bulk(SmallVec<[Payload; 8]>),
    // Withdraw everything or a particular AFISAFI because the session ended.
    // Not to be used for 'normal' withdrawals.
    Withdraw(IngressId, Option<AfiSafiType>),
    // Withdraw everything for multiple sessions. This is used when a BMP
    // connection goes down and everything for the monitored sessions has to
    // be marked Withdrawn.
    WithdrawBulk(SmallVec<[IngressId; 8]>),
    QueryResult(
        Uuid,
        Result<QueryResult<crate::payload::RotondaPaMap>, String>,
    ),
    UpstreamStatusChange(UpstreamStatus),

    OutputStream(SmallVec<[OutputStreamMessage; 2]>),
    Rtr(crate::units::RtrUpdate),
}

impl Update {
    pub fn trace_ids(&self) -> SmallVec<[&Payload; 1]> {
        match self {
            Update::Single(payload) => {
                if payload.trace_id().is_some() {
                    [payload].into()
                } else {
                    smallvec![]
                }
            }
            Update::Bulk(payloads) => {
                payloads.iter().filter(|p| p.trace_id().is_some()).collect()
            }
            Update::Withdraw(_ingress_id, _maybe_afisafi) => smallvec![],
            Update::WithdrawBulk(..) => smallvec![],
            Update::QueryResult(_, _) => smallvec![],
            Update::UpstreamStatusChange(_) => smallvec![],
            Update::OutputStream(..) => smallvec![],
            Update::Rtr(..) => smallvec![],
        }
    }
}

impl From<Payload> for Update {
    fn from(payload: Payload) -> Self {
        Update::Single(payload)
    }
}

impl<const N: usize> From<[Payload; N]> for Update {
    fn from(payloads: [Payload; N]) -> Self {
        Update::Bulk(payloads.as_slice().into())
    }
}

impl From<SmallVec<[Payload; 8]>> for Update {
    fn from(payloads: SmallVec<[Payload; 8]>) -> Self {
        Update::Bulk(payloads)
    }
}
