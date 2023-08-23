use bytes::Bytes;
use chrono::{DateTime, Utc};
use roto::types::builtin::{BuiltinTypeValue, RawRouteWithDeltas};
use roto::types::typevalue::TypeValue;
use roto::vm::OutputStreamQueue;
use rotonda_store::QueryResult;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use uuid::Uuid;

use crate::units::RibValue;

// TODO: make this a reference
pub type RouterId = String;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Action {
    Announce,
    Withdraw,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RoutingInformationBase {
    PrePolicy,
    PostPolicy,
    Unknown,
}

impl Display for RoutingInformationBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingInformationBase::PrePolicy => f.write_str("Pre-Policy-RIB"),
            RoutingInformationBase::PostPolicy => f.write_str("Post-Policy-RIB"),
            RoutingInformationBase::Unknown => f.write_str("Unknown-RIB"),
        }
    }
}

impl FromStr for RoutingInformationBase {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pre-Policy-RIB" => Ok(Self::PrePolicy),
            "Post-Policy-RIB" => Ok(Self::PostPolicy),
            _ => Ok(Self::Unknown),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RawBmpPayload {
    Eof,
    Msg(Bytes),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Payload {
    // Still needed by bmp-tcp-in -> bmp-in. Should be replaced by TypeValue when it supports raw BMP messages?
    // Also still accepted as input by mqtt-out...
    RawBmp {
        received: DateTime<Utc>,
        router_addr: SocketAddr,
        msg: RawBmpPayload,
    },

    TypeValue(TypeValue),
}

impl Payload {
    pub fn bmp_eof(router_addr: SocketAddr) -> Self {
        Self::RawBmp {
            received: Utc::now(),
            router_addr,
            msg: RawBmpPayload::Eof,
        }
    }

    pub fn bmp_msg(router_addr: SocketAddr, msg_bytes: Bytes) -> Self {
        Self::RawBmp {
            received: Utc::now(),
            router_addr,
            msg: RawBmpPayload::Msg(msg_bytes),
        }
    }

    pub fn is_eof(&self) -> bool {
        matches!(
            self,
            Payload::RawBmp {
                msg: RawBmpPayload::Eof,
                ..
            }
        )
    }
}

impl From<RawRouteWithDeltas> for Payload {
    fn from(route: RawRouteWithDeltas) -> Self {
        Payload::TypeValue(TypeValue::Builtin(BuiltinTypeValue::Route(route)))
    }
}

//------------ Update --------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Update {
    Single(Payload),
    Bulk(SmallVec<[Payload; 8]>),
    QueryResult(Uuid, Result<QueryResult<RibValue>, String>),
    OutputStreamMessage(OutputStreamQueue),
}
