use std::{fmt::Display, io, net::{IpAddr, Ipv4Addr, Ipv6Addr}};
use std::io::Write;

use axum::{body::Body, extract::{Path, Query, State}, response::IntoResponse};
use bytes::Bytes;
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, warn};
use routecore::{bgp::{communities::{LargeCommunity, StandardCommunity}, message_ng::{common::AfiSafiType as AfiSafiTypeNg, nlri::{Nlri, OwnedIpv4UnicastNlri, OwnedIpv6UnicastNlri}}, path_attributes::PathAttributeType, types::AfiSafiType}, bmp::message::RibType};
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::formats::CommaSeparator;
use serde_with::StringWithSeparator;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{http_ng::{Api, ApiError, ApiState}, ingress::{IngressId, IngressType}, representation::{GenOutput, Json}, roto_runtime::types::PeerRibType, units::rib_unit::rpki::RovStatus};

/// Add ingress register specific endpoints to a HTTP API
pub fn register_routes(router: &mut Api) {
    router.add_get("/ribs/ipv4unicast/routes/{prefix}/{prefix_len}", search_ipv4unicast);
    router.add_get("/ribs/ipv4unicast/routes", search_ipv4unicast_all);
    router.add_get("/ribs/ipv6unicast/routes/{prefix}/{prefix_len}", search_ipv6unicast);
    router.add_get("/ribs/ipv6unicast/routes", search_ipv6unicast_all);

    // The 'hardcoded' afisafis above take precedence over this 'catch-all' one.
    router.add_get("/ribs/{afisafi}/routes", generic_afisafi_all);


    router.add_get("/ribs/ng", routedb_test);

    router.add_get("/ng/ribs/ipv4unicast/routes", routedb_ipv4unicast_all);
    router.add_get("/ng/ribs/ipv4unicast/routes/{prefix}/{prefix_len}", routedb_ipv4unicast_search);
    router.add_get("/ng/ribs/ipv6unicast/routes", routedb_ipv6unicast_all);
    router.add_get("/ng/ribs/ipv6unicast/routes/{prefix}/{prefix_len}", routedb_ipv6unicast_search);

    //router.add_get("/ng/ribs/{afisafi}/routes", routedb_generic_all);


        

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
#[derive(Clone, Debug, Default, Deserialize)]
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

    // XXX this would allow us to easily filter on type=bgpOut,
    // but is that the way to go?
    #[serde(rename = "filter[ingressType]")]
    pub ingress_type: Option<IngressType>,

    pub include_local_announcements: Option<bool>,

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

const STREAM_CHUNK_SIZE: usize = 256 * 1024;

struct StreamResponseWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    buffer: Vec<u8>,
}

impl StreamResponseWriter {
    fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(STREAM_CHUNK_SIZE),
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::copy_from_slice(&self.buffer);
        self.buffer.clear();
        self.sender
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "receiver dropped"))
    }
}

impl io::Write for StreamResponseWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= STREAM_CHUNK_SIZE {
            self.send_buffer()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

fn stream_search_result(
    search_result: super::rib::SearchResult,
) -> impl IntoResponse {
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(64);
    let stream = ReceiverStream::new(rx);

    tokio::task::spawn_blocking(move || {
        let mut writer = StreamResponseWriter::new(tx);
        let _ = search_result.write(&mut Json(&mut writer));
        let _ = writer.flush();
    });

    ([("content-type", "application/json")], Body::from_stream(stream))
}

fn stream_routedb_routing_tables(
    tables: super::serialize::RouteDbRoutingTables,
) -> impl IntoResponse {
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(64);
    let stream = ReceiverStream::new(rx);

    tokio::task::spawn_blocking(move || {
        let mut writer = StreamResponseWriter::new(tx);
        let _ = tables.write(&mut Json(&mut writer));
        let _ = writer.flush();
    });

    ([("content-type", "application/json")], Body::from_stream(stream))
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
    let s = state.store.load();
    let search_result = match *s {
        Some(ref store) => store.search_routes(AfiSafiType::Ipv4Unicast, prefix, filter)
            .map_err(ApiError::BadRequest)?,
        None => return Err(ApiError::InternalServerError("store unavailable".into())),
    };

    Ok(stream_search_result(search_result))
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
    let s = state.store.load();
    let search_result = match *s {
        Some(ref store) => store.search_routes(AfiSafiType::Ipv6Unicast, prefix, filter)
            .map_err(ApiError::BadRequest)?,
        None => return Err(ApiError::InternalServerError("store unavailable".into())),
    };

    Ok(stream_search_result(search_result))
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


async fn routedb_test(

    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let s = state.store.load();
    let Some(ref rib) = *s else {
        return Err(ApiError::InternalServerError("routedb unavailable".into()))
    };

    let mut res = String::new();
    for rt in rib.routedb.routing_tables().iter() {
        res.push_str(&format!("{:?}\n", rt.props().unwrap()));
        res.push_str(&format!("{} nlri\n", rt.iter().count()));
        for route in rt.iter() {
            let nlri = route.nlri();
            let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(route.pa_hints()); // XXX why is this not [u8; 10]?
            //dbg!(&pa_header);
            let attrs = route.path_attrs();
            // TODO in routecore, make a version of PreppedAttributes that is
            // not zerocopy, takes an owned Header, and an
            // UncheckedPathAttributes
            // -> There is this PreppedAttributes2 now. If that suffices,
            // rename to PreppedAttributes and move all methods to it
            // Can we do without it being zerocopy?
            

            let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
                header: &pa_header,
                path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
            };
            res.push_str(&format!("{nlri:?}\n"));
            res.push_str(&serde_json::to_string(&prepped_attrs).unwrap());
            //res.push_str(&format!("{attrs:?}\n"));
            //PreppedAttributes
        }
    }

    Ok(res)
}



async fn routedb_ipv4unicast_all(

    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let s = state.store.load();
    let Some(rib) = (*s).clone() else {
        return Err(ApiError::InternalServerError("routedb unavailable".into()))
    };


    //TODO next up: count number of results, compare with old endpoint

    //let ids = rib.routedb.routing_tables().iter().filter(|rt| {
    //    if let Ok(props) = rt.props() {
    //        props.afi_safi_type == routecore::bgp::message_ng::common::AfiSafiType::IPV4UNICAST
    //    } else {
    //        false
    //    }
    //})
    //.map(|rt| rt.table_id()).collect::<Vec<_>>();


    // attempt based on iter_grouped_by_table_id
    let ids = rib.routedb.routes().iter_grouped_by_table_id().filter_map(|(rt_props,_)|
        (rt_props.afi_safi() == Ok(AfiSafiTypeNg::IPV4UNICAST)).then(|| rt_props.table_id())
    ).collect::<Vec<_>>();


    Ok(
        stream_routedb_routing_tables(
            super::serialize::RouteDbRoutingTables {
                routedb: rib.routedb.clone(),
                ids
            }
        )
    )
    
}

async fn routedb_ipv6unicast_all(

    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let s = state.store.load();
    let Some(rib) = (*s).clone() else {
        return Err(ApiError::InternalServerError("routedb unavailable".into()))
    };


    //TODO next up: count number of results, compare with old endpoint

    let ids = rib.routedb.routing_tables().iter().filter(|rt| {
        if let Ok(props) = rt.props() {
            props.afi_safi_type == AfiSafiTypeNg::IPV6UNICAST
        } else {
            false
        }
    })
    .map(|rt| rt.table_id()).collect::<Vec<_>>();

    Ok(
        stream_routedb_routing_tables(
            super::serialize::RouteDbRoutingTables {
                routedb: rib.routedb.clone(),
                ids,
            }
        )
    )
    
}

async fn routedb_ipv4unicast_search(
    Path((prefix, prefix_len)): Path<(Ipv4Addr, u8)>,
    Query(filter): Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let s = state.store.load();
    let Some(rib) = (*s).clone() else {
        return Err(ApiError::InternalServerError("routedb unavailable".into()))
    };

    let Ok(nlri) = OwnedIpv4UnicastNlri::try_from((prefix, prefix_len)) else {
        return Err(ApiError::BadRequest("invalid prefix".into()));
    };

    debug!("searching for {} {:?}", nlri.as_ref(), nlri.as_ref().as_ref());
    let mut res = String::new();
    for rt in rib.routedb.routing_tables().iter().filter(|rt| rt.props().unwrap().afi_safi_type == AfiSafiTypeNg::IPV4UNICAST) {

        dbg!(&rt.props());

        if let Some(r) = rt.get(nlri.as_ref().as_ref())  {
            debug!("got a route");

            assert_eq!(nlri.as_ref().as_ref(), r.nlri());

            res.push_str(&format!("nlri: {}\n", nlri.as_ref()));

            let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(r.pa_hints());
            let attrs = r.path_attrs();
            let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
                header: &pa_header,
                path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
            };

            res.push_str(&format!("pathAttributes: {}\n", serde_json::to_string(&prepped_attrs).unwrap()));
        }

    }


    Ok(res)
}

async fn routedb_ipv6unicast_search(
    Path((prefix, prefix_len)): Path<(Ipv6Addr, u8)>,
    Query(filter): Query<QueryFilter>,
    state: State<ApiState>
) -> Result<impl IntoResponse, ApiError> {

    let s = state.store.load();
    let Some(rib) = (*s).clone() else {
        return Err(ApiError::InternalServerError("routedb unavailable".into()))
    };

    let Ok(nlri) = OwnedIpv6UnicastNlri::try_from((prefix, prefix_len)) else {
        return Err(ApiError::BadRequest("invalid prefix".into()));
    };

    debug!("searching for {} {:?}", nlri.as_ref(), nlri.as_ref().as_ref());
    let mut res = String::new();
    for rt in rib.routedb.routing_tables().iter().filter(|rt| rt.props().unwrap().afi_safi_type == AfiSafiTypeNg::IPV6UNICAST) {

        dbg!(&rt.props());

        if let Some(r) = rt.get(nlri.as_ref().as_ref())  {
            debug!("got a route");

            assert_eq!(nlri.as_ref().as_ref(), r.nlri());

            res.push_str(&format!("nlri: {}\n", nlri.as_ref()));

            let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(r.pa_hints());
            let attrs = r.path_attrs();
            let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
                header: &pa_header,
                path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
            };

            res.push_str(&format!("pathAttributes: {}\n", serde_json::to_string(&prepped_attrs).unwrap()));
        }

    }


    Ok(res)
}
