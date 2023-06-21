use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};

use hyper::{Method, Request, Response};

use crate::{
    common::frim::FrimMap,
    http::{extract_params, get_param, MatchedParam, PercentDecodedPath},
    units::bmp_in::{
        metrics::BmpInMetrics,
        state_machine::metrics::BmpMetrics,
        types::RouterInfo,
        util::{calc_u8_pc, format_router_id},
    },
};

pub struct RouterListApi;

impl RouterListApi {
    pub fn mk_http_processor(
        http_api_path: Arc<String>,
        router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>,
        router_metrics: Arc<BmpInMetrics>,
        bmp_metrics: Arc<BmpMetrics>,
        router_id_template: Arc<String>,
    ) -> Arc<dyn crate::http::ProcessRequest> {
        Arc::new(move |request: &Request<_>| {
            let req_path = request.uri().decoded_path();
            if request.method() == Method::GET && req_path == *http_api_path {
                let http_api_path =
                    html_escape::encode_double_quoted_attribute(http_api_path.deref());

                // If sorting has been requested, extract the value to sort on and the key, store them as a vec of
                // tuples, and sort the vec, then lookup each router_info key in the order of the keys in the sorted
                // vec.
                let params = extract_params(request);
                let sort_by = get_param(&params, "sort_by");
                let sort_order = get_param(&params, "sort_order");

                let response = match Self::sort_routers(
                    sort_by,
                    sort_order,
                    router_info.clone(),
                    router_id_template.clone(),
                    bmp_metrics.clone(),
                    router_metrics.clone(),
                ) {
                    Ok(keys) => Self::build_response(
                        keys,
                        router_info.clone(),
                        router_id_template.clone(),
                        bmp_metrics.clone(),
                        router_metrics.clone(),
                        http_api_path,
                    ),

                    Err(err) => Response::builder()
                        .status(hyper::StatusCode::BAD_REQUEST)
                        .header("Content-Type", "text/plain")
                        .body(err.into())
                        .unwrap(),
                };

                Some(response)
            } else {
                None
            }
        })
    }

    fn sort_routers(
        sort_by: Option<MatchedParam>,
        sort_order: Option<MatchedParam>,
        router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>,
        router_id_template: Arc<String>,
        bmp_metrics: Arc<BmpMetrics>,
        router_metrics: Arc<BmpInMetrics>,
    ) -> Result<Vec<SocketAddr>, String> {
        let sort_by = sort_by.as_ref().map(MatchedParam::value);
        let mut keys: Vec<SocketAddr> = match sort_by {
            None | Some("addr") => router_info.guard().iter().map(|(k, _v)| *k).collect(),

            Some("sys_name") | Some("sys_desc") => {
                let mut sort_tmp: Vec<(String, SocketAddr)> = router_info
                    .guard()
                    .iter()
                    .map(|(k, v)| {
                        let resolved_v = match sort_by {
                            Some("sys_name") => v.sys_name.as_ref(),
                            Some("sys_desc") => v.sys_desc.as_ref(),
                            _ => unreachable!(),
                        }
                        .map_or_else(|| "-", |v| v)
                        .to_string();
                        (resolved_v, *k)
                    })
                    .collect();

                sort_tmp.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                sort_tmp.iter().map(|v| v.1).collect()
            }

            Some("state")
            | Some("peers_up")
            | Some("peers_up_eor_capable")
            | Some("peers_up_dumping")
            | Some("peers_up_eor_capable_pc")
            | Some("peers_up_dumping_pc")
            | Some("invalid_messages")
            | Some("soft_parse_errors")
            | Some("hard_parse_errors") => {
                let mut sort_tmp: Vec<(usize, SocketAddr)> = router_info
                    .guard()
                    .iter()
                    .map(|(k, v)| {
                        let resolved_v = if let Some(sys_name) = &v.sys_name {
                            let sys_name = html_escape::encode_safe(sys_name);

                            let router_id = Arc::new(format_router_id(router_id_template.clone(), &sys_name, k));
                            let metrics = bmp_metrics.router_metrics(router_id.clone());

                            match sort_by {
                                Some("state") => metrics.bmp_state_machine_state.load(Ordering::Relaxed) as usize,

                                Some("peers_up") => metrics.num_peers_up.load(Ordering::Relaxed),

                                Some("peers_up_eor_capable") => metrics.num_peers_up_eor_capable.load(Ordering::Relaxed),

                                Some("peers_up_dumping") => metrics.num_peers_up_dumping.load(Ordering::Relaxed),

                                Some("peers_up_eor_capable_pc") => {
                                    let total = metrics.num_peers_up.load(Ordering::Relaxed);
                                    let v = metrics.num_peers_up_eor_capable.load(Ordering::Relaxed);
                                    calc_u8_pc(total, v).into()
                                }
                                Some("peers_up_dumping_pc") => {
                                    let total = metrics.num_peers_up_eor_capable.load(Ordering::Relaxed);
                                    let v = metrics.num_peers_up_dumping.load(Ordering::Relaxed);
                                    calc_u8_pc(total, v).into()
                                }

                                Some("invalid_messages") => {
                                    if let Some(router_metrics) = router_metrics.router_metrics(router_id) {
                                        router_metrics.num_invalid_bmp_messages.load(Ordering::Relaxed)
                                    } else {
                                        0
                                    }
                                }

                                Some("soft_parse_errors") => {
                                    metrics.num_bgp_updates_with_recoverable_parsing_failures_for_known_peers.load(Ordering::Relaxed) +
                                    metrics.num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers.load(Ordering::Relaxed)
                                }

                                Some("hard_parse_errors") => {
                                    metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers.load(Ordering::Relaxed) +
                                    metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers.load(Ordering::Relaxed)
                                }

                                _ => unreachable!()
                            }
                        } else {
                            0
                        };

                        (resolved_v, *k)
                    })
                    .collect();

                sort_tmp.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                sort_tmp.iter().map(|v| v.1).collect()
            }

            Some(other) => {
                return Err(format!(
                    "Unknown value '{}' for query parameter 'sort_by'",
                    other
                ));
            }
        };

        match sort_order.as_ref().map(MatchedParam::value) {
            None | Some("asc") => { /* nothing to do */ }

            Some("desc") => keys.reverse(),

            Some(other) => {
                return Err(format!(
                    "Unknown value '{}' for query parameter 'sort_order'",
                    other
                ));
            }
        }

        Ok(keys)
    }
}
