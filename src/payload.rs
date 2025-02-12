use log::debug;
use rotonda_store::QueryResult;

use routecore::bgp::communities::{Community, HumanReadableCommunity};
use routecore::bgp::path_attributes::PathAttribute;
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
    ) -> &routecore::bgp::path_attributes::OwnedPathAttributes {
        match self {
            RotondaRoute::Ipv4Unicast(_, p) => &p.0,
            RotondaRoute::Ipv6Unicast(_, p) => &p.0,
            RotondaRoute::Ipv4Multicast(_, p) => &p.0,
            RotondaRoute::Ipv6Multicast(_, p) => &p.0,
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

impl rotonda_store::Meta for RotondaPaMap {
    type Orderable<'a> = routecore::bgp::path_selection::OrdRoute<
        'a,
        routecore::bgp::path_selection::Rfc4271,
    >;

    type TBI = rotonda_store::prelude::multi::TiebreakerInfo;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RotondaPaMap(
    pub routecore::bgp::path_attributes::OwnedPathAttributes,
);

impl fmt::Display for RotondaPaMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Serialize for RotondaPaMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(None)?;
        let mut communities: Vec<HumanReadableCommunity> = vec![];
        for pa in self.0.iter().flatten() {
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
