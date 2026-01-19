use std::{collections::HashMap, fmt::Display, net::{IpAddr, Ipv4Addr, Ipv6Addr}};

use axum::{extract::{Path, Query, State}, response::IntoResponse};
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, warn};
use routecore::{bgp::{communities::{LargeCommunity, StandardCommunity}, path_attributes::PathAttributeType, types::AfiSafiType}, bmp::message::RibType};
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::formats::CommaSeparator;
use serde_with::StringWithSeparator;

use crate::{http_ng::{Api, ApiError, ApiState}, ingress::IngressId, roto_runtime::types::PeerRibType, units::rib_unit::rpki::RovStatus};

/// Add ingress register specific endpoints to a HTTP API
pub fn register_routes(router: &mut Api) {
    router.add_get("/ribs/ipv4unicast/routes/{prefix}/{prefix_len}", search_ipv4unicast);
    router.add_get("/ribs/ipv4unicast/routes", search_ipv4unicast_all);
    router.add_get("/ribs/ipv6unicast/routes/{prefix}/{prefix_len}", search_ipv6unicast);
    router.add_get("/ribs/ipv6unicast/routes", search_ipv6unicast_all);

    // The 'hardcoded' afisafis above take precedence over this 'catch-all' one.
    router.add_get("/ribs/{afisafi}/routes", generic_afisafi_all);


    // Possible shortcuts:
    //router.add_get("/origin_asn/{asn}", search_origin_asn_shortcut);
    //router.add_get("/ipv4unicast/origin_asn/{asn}", search_origin_asn);
    // or, should we do this per afisafi, a la:
    // Because with a /origin_asn (without afisafi), we have to decide and hardcode for which
    // address families we'll do the lookups.
    // Perhaps, if we offer both, the /origin_asn can default to unicast stuff?
    //
    // Or, should all of this go as a URL query parameter?
    // so we get /ipv4unicast/0/0?origin=211321
}

#[derive(Debug, Deserialize)]
enum SupportedAfiSafi {
    #[serde(rename = "ipv4unicast")]
    Ipv4Unicast,
    #[serde(rename = "ipv6unicast")]
    Ipv6Unicast,
}


#[serde_as]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct QueryFilter {
    
    #[serde(default)]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, Include>")]
    pub include: Vec<Include>,


    pub ingress_id: Option<IngressId>,

    #[serde(rename = "filter[originAsn]")]
    pub origin_asn: Option<Asn>, 

    #[serde(rename = "filter[otc]")]
    pub otc: Option<Asn>, 

    #[serde(rename = "filter[community]")]
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub community: Option<StandardCommunity>, 

    #[serde(rename = "filter[largeCommunity]")]
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub large_community: Option<LargeCommunity>, 

    #[serde(rename = "filter[ribType]")]
    pub rib_type: Option<PeerRibType>,


    #[serde(rename = "filter[rovStatus]")]
    pub rov_status: Option<RovStatus>,

    #[serde(rename = "filter[peerAsn]")]
    pub peer_asn: Option<Asn>,

    #[serde(rename = "filter[peerAddress]")]
    pub peer_addr: Option<IpAddr>,

    // TODO: RouteDistinguisher, 

    // content parameter (defaulting to 'all') to request only the nlri without path attributes, or
    // perhaps only specific path attributes?
    // rfc8040 (RESTCONF) describes content=all|config|nonconfig , but we could divert from that?
    //
    // json:api describes 'fields[]', e.g.:
    // ?include=author&fields[articles]=title,body&fields[people]=name
    //
    // We could go for e.g. fields[pathAttributes]=asPath,otc 
    //
    // Then to alter representation, i.e. offer 'plain' communities and the exploded human readable
    // representation from the old API, .. what do we do/
    //
    // fields[communities]=humanReadable?
    // or do we use content for that? downside of 'content' is that it seems to be less
    // fine-grained, while fields[$foo] allows defining things on the $foo level

    //#[serde_as(as = "StringWithSeparator::<CommaSeparator, PathAttributeType>")]
    // TODO instead of u8, base this on strings
    // for that, add impl FromStr for PathAttributeType in routecore
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, u8>>")]
    #[serde(rename = "fields[pathAttributes]")]
    pub fields_path_attributes: Option<Vec<u8>>,



    #[serde(rename = "function[roto]")]
    pub roto_function: Option<String>
}

impl QueryFilter {
    pub fn enable_more_specifics(&mut self) {
        if !self.include.contains(&Include::MoreSpecifics) {
            self.include.push(Include::MoreSpecifics);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Include {
    MoreSpecifics,
    LessSpecifics,
}

#[derive(Debug)]
pub struct UnknownInclude;
impl Display for UnknownInclude {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown include")
    }
}
impl std::str::FromStr for Include {
    type Err = UnknownInclude;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "moreSpecifics" => Ok(Include::MoreSpecifics),
            "lessSpecifics" => Ok(Include::LessSpecifics),
            _ => Err(UnknownInclude)
        }
    }
}

async fn generic_afisafi_all(
    Path(afisafi): Path<SupportedAfiSafi>,
    filter: Query<QueryFilter>,
    _state: State<ApiState>
) -> Result<Vec<u8>, ApiError> {

    dbg!(afisafi, filter);
    warn!("searching routes other than unicast not yet implemented");
    Err(ApiError::InternalServerError("TODO".into()))
}

async fn search_ipv4unicast(
    Path((prefix, prefix_len)): Path<(Ipv4Addr, u8)>,
    Query(filter): Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let prefix = Prefix::new_v4(prefix, prefix_len).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let s = state.store.clone();
    let mut res = Vec::new();
    match s.get().map(|store|
        store.search_and_output_routes(
            crate::representation::Json(&mut res),
            AfiSafiType::Ipv4Unicast,
            prefix,
            filter
        )
    ) {
        Some(Ok(())) => Ok(([("content-type", "application/json")], res)),
        Some(Err(e)) => Err(ApiError::BadRequest(e)),
        None => Err(ApiError::InternalServerError("store unavailable".into()))
    }
}

// Search all routes, we mimic a 0.0.0.0/0 search, but most (or all) results will actually be
// more-specifics. These go into the "included" part of the response.
async fn search_ipv4unicast_all(
    mut filter: Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {
    filter.enable_more_specifics();
    search_ipv4unicast(Path((0.into(), 0)), filter, state).await
}

async fn search_ipv6unicast(
    Path((prefix, prefix_len)): Path<(Ipv6Addr, u8)>,
    Query(filter): Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let prefix = Prefix::new_v6(prefix, prefix_len).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let s = state.store.clone();
    let mut res = Vec::new();
    match s.get().map(|store|
        store.search_and_output_routes(
            crate::representation::Json(&mut res),
            AfiSafiType::Ipv6Unicast,
            prefix,
            filter
        )
    ) {
        Some(Ok(())) => Ok(([("content-type", "application/json")], res)),
        Some(Err(e)) => Err(ApiError::BadRequest(e)),
        None => Err(ApiError::InternalServerError("store unavailable".into()))
    }
}

// Search all routes, we mimic a ::/0 search, but most (or all) results will actually be
// more-specifics. These go into the "included" part of the response.
async fn search_ipv6unicast_all(
    mut filter: Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {
    filter.enable_more_specifics();
    search_ipv6unicast(Path((0.into(), 0)), filter, state).await
}
