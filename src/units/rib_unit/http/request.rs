use std::{ops::Deref, str::FromStr, sync::Arc};

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};
use log::trace;
use rotonda_store::{epoch, prelude::Prefix, MatchOptions};
use routecore::{asn::Asn, bgp::communities::Community};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    comms::{Link, TriggerData},
    http::{
        extract_params, get_all_params, get_param, MatchedParam,
        PercentDecodedPath, ProcessRequest, QueryParams,
    },
    units::{
        rib_unit::{
            http::types::{FilterKind, FilterOp},
            rib::HashedRib,
            unit::{PendingVirtualRibQueryResults, QueryLimits},
        },
        RibType,
    },
};

use super::types::{Details, Filter, FilterMode, Filters, Includes, SortKey};

pub struct PrefixesApi {
    rib: Arc<ArcSwap<HashedRib>>,
    http_api_path: Arc<String>,
    query_limits: Arc<ArcSwap<QueryLimits>>,
    rib_type: RibType,
    vrib_upstream: Arc<ArcSwapOption<Link>>,
    pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
}

impl PrefixesApi {
    pub fn new(
        rib: Arc<ArcSwap<HashedRib>>,
        http_api_path: Arc<String>,
        query_limits: Arc<ArcSwap<QueryLimits>>,
        rib_type: RibType,
        vrib_upstream: Option<Link>,
        pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
    ) -> Self {
        Self {
            rib,
            http_api_path,
            query_limits,
            rib_type,
            vrib_upstream: Arc::new(ArcSwapOption::from_pointee(
                vrib_upstream,
            )),
            pending_vrib_query_results,
        }
    }

    pub fn http_api_path(&self) -> &String {
        self.http_api_path.as_ref()
    }

    pub fn set_vrib_upstream(&self, vrib_upstream: Option<Link>) {
        self.vrib_upstream.store(vrib_upstream.map(Arc::new));
    }
}

#[async_trait]
impl ProcessRequest for PrefixesApi {
    async fn process_request(
        &self,
        request: &Request<Body>,
    ) -> Option<Response<Body>> {
        // Percent decoding the path shouldn't be necessary for the requests we support at the moment, but later
        // it may matter, and it shouldn't hurt, and it's been demonstrated that there are clients that encode the
        // URL path component when perhaps (not that clear from RFC 3986) they don't have to (e.g. when the path
        // component contains a ':' as in an IPv6 address). Let's be lenient about the UTF-8 decoding as well while
        // we are at it...
        let req_path = &request.uri().decoded_path();

        // e.g. req_path = "/prefixes/2804:1398:100::/48"
        if request.method() == Method::GET
            && req_path.starts_with(self.http_api_path.deref())
        {
            match self.handle_prefix_query(req_path, request).await {
                Ok(res) => Some(res),
                Err(err) => Some(
                    Response::builder()
                        .status(hyper::StatusCode::BAD_REQUEST)
                        .header("Content-Type", "text/plain")
                        .body(err.into())
                        .unwrap(),
                ),
            }
        } else {
            // Start of HTTP relative URL did not match the one defined for this processor
            None
        }
    }
}

impl PrefixesApi {
    async fn handle_prefix_query(
        &self,
        req_path: &str,
        request: &Request<Body>,
    ) -> Result<Response<Body>, String> {
        let prefix = Prefix::from_str(
            req_path.strip_prefix(self.http_api_path.as_str()).unwrap(),
        ) // SAFETY: unwrap() safe due to starts_with() check above
        .map_err(|err| err.to_string())?;

        //
        // Handle query parameters
        //
        let params = extract_params(request);

        let includes = Self::parse_include_param(
            &params,
            self.query_limits.clone(),
            prefix,
        )?;
        let details = Self::parse_details_param(&params)?;
        let filters = Self::parse_filter_params(&params)?;
        let sort = Self::parse_sort_params(&params)?;
        let format = get_param(&params, "format");

        //
        // Check for unused params
        //
        let unused_params: Vec<&str> = params
            .iter()
            .filter(|param| !param.used())
            .map(|param| param.name())
            .collect();
        if !unused_params.is_empty() {
            return Err(format!(
                "Unrecognized query parameters: {}",
                unused_params.join(",")
            ));
        }

        //
        // Query the prefix store
        //
        // For a physical RIB we own the store and can query it directly.
        // For a virtual RIB we must send a query command to store nearest to our Western edge,
        // which we've been given a Link to so we can send it commands. Once we've sent it the
        // query command we then have to wait to receive the query result back.
        //
        let options = MatchOptions {
            match_type: rotonda_store::MatchType::ExactMatch,
            include_less_specifics: includes.less_specifics,
            include_more_specifics: includes.more_specifics,
            include_all_records: true,
        };

        let res = match self.rib_type {
            RibType::Physical => {
                let guard = &epoch::pin();
                if let Ok(store) = self.rib.load().store() {
                    store.match_prefix(&prefix, &options, guard)
                } else {
                    let res = Response::builder()
                        .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "text/plain")
                        .body(
                            "Cannot query non-existent RIB store"
                                .to_string()
                                .into(),
                        )
                        .unwrap();
                    return Ok(res);
                }
            }
            RibType::GeneratedVirtual(_) | RibType::Virtual => {
                trace!("Handling virtual RIB query");
                // Generate a unique query ID to tie the request and later response together.
                let uuid = Uuid::new_v4();
                let data = TriggerData::MatchPrefix(uuid, prefix, options);

                // Create a oneshot channel and store the sender to it by the query ID we generated.
                let (tx, rx) = oneshot::channel();
                self.pending_vrib_query_results.insert(uuid, Arc::new(tx));

                // Send a command asynchronously to the upstream physical RIB unit to perform the desired query on its
                // store and to send the result back to us through the pipeline (being operated on as necessary by any
                // intermediate virtual RIB roto scripts). This command includes the query ID that we generated.
                trace!("Triggering upstream physical RIB {} to do the actual query", self.vrib_upstream.load().as_ref().unwrap().id());
                self.vrib_upstream
                    .load()
                    .as_ref()
                    .unwrap()
                    .trigger(data)
                    .await;

                // Wait for the main unit which operates the Gate to receive a Query Result that matches the query ID
                // we just generated, and to pick the oneshot channel by query ID and use it to pass the results to us.
                trace!("Waiting for physical RIB query result to arrive");
                match rx.await {
                    Ok(Ok(query_result)) => {
                        trace!("Physical RIB query result received");
                        query_result
                    }

                    Ok(Err(err)) => {
                        trace!("Internal error: {}", err);
                        let res = Response::builder()
                            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "text/plain")
                            .body(err.into())
                            .unwrap();
                        return Ok(res);
                    }

                    Err(err) => {
                        trace!("Internal error: {}", err);
                        let res = Response::builder()
                            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "text/plain")
                            .body(err.to_string().into())
                            .unwrap();
                        return Ok(res);
                    }
                }
            }
        };

        //
        // Format the response
        //
        let res = match format {
            None => {
                // default format
                Self::mk_json_response(res, includes, details, filters, sort)
            }

            Some(format) if format.value() == "dump" => {
                // internal diagnostic dump format
                Self::mk_dump_response(&res)
            }

            Some(other) => {
                // unknown format
                Response::builder()
                    .status(hyper::StatusCode::BAD_REQUEST)
                    .header("Content-Type", "text/plain")
                    .body(
                        format!("Unsupported value '{}' for query parameter 'format'", other)
                            .into(),
                    )
                    .unwrap()
            }
        };

        Ok(res)
    }

    fn parse_include_param(
        params: &QueryParams,
        query_limits: Arc<ArcSwap<QueryLimits>>,
        prefix: Prefix,
    ) -> Result<Includes, String> {
        let mut includes = Includes::default();

        if let Some(requested_includes) = get_param(params, "include") {
            for include in requested_includes.value().split(',') {
                match include {
                    "lessSpecifics" => includes.less_specifics = true,
                    "moreSpecifics" => includes.more_specifics = true,
                    _ => {
                        return Err(format!(
                            "'{}' is not a valid value for query parameter 'include'",
                            include
                        ))
                    }
                }
            }
        }

        if includes.more_specifics {
            let query_limits = query_limits.load();
            let shortest_prefix_permitted = query_limits
                .more_specifics
                .shortest_prefix_permitted(&prefix);
            if prefix.len() < shortest_prefix_permitted {
                let err = if prefix.is_v4() {
                    format!(
                        "Query prefix '{}' is shorter than the allowed minimum allowed ({}) when including more specifics",
                        prefix,
                        query_limits.more_specifics.shortest_prefix_ipv4
                    )
                } else if prefix.is_v6() {
                    format!(
                        "Query prefix '{}' is shorter than the allowed minimum allowed ({}) when including more specifics",
                        prefix,
                        query_limits.more_specifics.shortest_prefix_ipv6
                    )
                } else {
                    format!(
                        "Internal error: prefix '{}' is neither IPv4 nor IPv6",
                        prefix
                    )
                };
                return Err(err);
            }
        }

        Ok(includes)
    }

    fn parse_details_param(params: &QueryParams) -> Result<Details, String> {
        let mut details = Details::default();

        if let Some(requested_details) = get_param(params, "details") {
            for detail in requested_details.value().split(',') {
                match detail {
                    "communities" => details.communities = true,
                    _ => {
                        return Err(format!(
                            "'{}' is not a valid value for query parameter 'details'",
                            detail
                        ))
                    }
                }
            }
        }

        Ok(details)
    }

    fn parse_filter_params(params: &QueryParams) -> Result<Filters, String> {
        let mut filters = vec![];

        for filter in get_all_params(params, "select") {
            let filter_kind = extract_filter_kind(filter)?;
            filters.push(Filter::new(filter_kind, FilterMode::Select));
        }

        for filter in get_all_params(params, "discard") {
            let filter_kind = extract_filter_kind(filter)?;
            filters.push(Filter::new(filter_kind, FilterMode::Discard));
        }

        let op = match get_param(params, "filter_op")
            .as_ref()
            .map(MatchedParam::value)
        {
            Some("any") => FilterOp::Any,
            Some("all") => FilterOp::All,
            Some(other) => {
                return Err(format!("Unknown filter_op value '{}'", other))
            }
            None => FilterOp::default(),
        };

        Ok(Filters::new(op, filters))
    }

    fn parse_sort_params(params: &QueryParams) -> Result<SortKey, String> {
        match get_param(params, "sort").as_ref().map(MatchedParam::value) {
            Some(json_pointer) => Ok(SortKey::Some(json_pointer.into())),
            _ => Ok(SortKey::None),
        }
    }
}

fn extract_filter_kind(filter: MatchedParam) -> Result<FilterKind, String> {
    let extracted_filter = match filter {
        MatchedParam::Family("as_path", v) => {
            let mut asns = Vec::new();
            for asn_str in v.split(',') {
                let asn = Asn::from_str(asn_str).map_err(|err| {
                    format!(
                        "Invalid ASN value '{}' in 'as_path' filter: {}",
                        asn_str, err
                    )
                })?;
                asns.push(asn);
            }
            Ok(FilterKind::AsPath(asns))
        }

        MatchedParam::Family("peer_as", v) => match Asn::from_str(v) {
            Ok(asn) => Ok(FilterKind::PeerAs(asn)),
            Err(err) => Err(format!(
                "Invalid value '{}' for 'peer_as' filter: {}",
                v, err
            )),
        },

        MatchedParam::Family("community", v) => {
            match Community::from_str(v) {
                Ok(community) => Ok(FilterKind::Community(community)),
                Err(err) => Err(format!(
                    "Invalid value '{}' for 'community' filter: {}",
                    v, err
                )),
            }
        }

        other => Err(format!("Unrecognized filter family '{}'", other)),
    }?;
    Ok(extracted_filter)
}
