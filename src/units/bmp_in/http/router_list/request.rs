use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};

use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};

use crate::{
    common::frim::FrimMap,
    http::{extract_params, get_param, MatchedParam, PercentDecodedPath, ProcessRequest},
    payload::SourceId,
    units::bmp_in::{
        metrics::BmpInMetrics,
        state_machine::{
            machine::{BmpState, BmpStateDetails},
            metrics::BmpMetrics,
        },
        types::RouterInfo,
        util::{calc_u8_pc, format_source_id},
    },
};

pub struct RouterListApi {
    http_api_path: Arc<String>,
    pub router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
    pub router_metrics: Arc<BmpInMetrics>,
    pub bmp_metrics: Arc<BmpMetrics>,
    pub router_id_template: Arc<String>,
    pub router_states: Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>,
}

#[async_trait]
impl ProcessRequest for RouterListApi {
    async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>> {
        let req_path = request.uri().decoded_path();
        if request.method() == Method::GET && req_path == *self.http_api_path {
            let http_api_path =
                html_escape::encode_double_quoted_attribute(self.http_api_path.deref());

            // If sorting has been requested, extract the value to sort on and the key, store them as a vec of
            // tuples, and sort the vec, then lookup each router_info key in the order of the keys in the sorted
            // vec.
            let params = extract_params(request);
            let sort_by = get_param(&params, "sort_by");
            let sort_order = get_param(&params, "sort_order");

            let response = match self.sort_routers(sort_by, sort_order).await {
                Ok(keys) => self.build_response(keys, http_api_path).await,

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
    }
}

impl RouterListApi {
    pub fn new(
        http_api_path: Arc<String>,
        router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
        router_metrics: Arc<BmpInMetrics>,
        bmp_metrics: Arc<BmpMetrics>,
        router_id_template: Arc<String>,
        router_states: Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>,
    ) -> Self {
        Self {
            http_api_path,
            router_info,
            router_metrics,
            bmp_metrics,
            router_id_template,
            router_states,
        }
    }

    async fn sort_routers<'a>(
        &self,
        sort_by: Option<MatchedParam<'a>>,
        sort_order: Option<MatchedParam<'a>>,
    ) -> Result<Vec<SourceId>, String> {
        let sort_by = sort_by.as_ref().map(MatchedParam::value);

        let mut keys: Vec<SourceId> = match sort_by {
            None | Some("addr") => self
                .router_info
                .guard()
                .iter()
                .map(|(k, _v)| k.clone())
                .collect(),

            Some("sys_name") | Some("sys_desc") => {
                let mut sort_tmp: Vec<(String, SourceId)> = Vec::new();

                for (source_id, state_machine) in self.router_states.guard().iter() {
                    if let Some(sm) = state_machine.lock().await.as_ref() {
                        let (sys_name, sys_desc) = match sm {
                            BmpState::Dumping(BmpStateDetails { details, .. }) => {
                                (Some(&details.sys_name), Some(&details.sys_desc))
                            }
                            BmpState::Updating(BmpStateDetails { details, .. }) => {
                                (Some(&details.sys_name), Some(&details.sys_desc))
                            }
                            _ => {
                                // no TLVs available
                                (None, None)
                            }
                        };

                        let resolved_v = match sort_by {
                            Some("sys_name") => sys_name,
                            Some("sys_desc") => sys_desc,
                            _ => None,
                        }
                        .map_or_else(|| "-", |v| v)
                        .to_string();

                        sort_tmp.push((resolved_v, source_id.clone()));
                    }
                }

                sort_tmp.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                sort_tmp.iter().map(|v| v.1.clone()).collect()
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
                let mut sort_tmp: Vec<(usize, SourceId)> = Vec::new();

                for (source_id, state_machine) in self.router_states.guard().iter() {
                    if let Some(sm) = state_machine.lock().await.as_ref() {
                        let sys_name = match sm {
                            BmpState::Dumping(BmpStateDetails { details, .. }) => {
                                Some(&details.sys_name)
                            }
                            BmpState::Updating(BmpStateDetails { details, .. }) => {
                                Some(&details.sys_name)
                            }
                            _ => {
                                // no TLVs available
                                None
                            }
                        };

                        let resolved_v = if let Some(sys_name) = sys_name {
                            let sys_name = html_escape::encode_safe(sys_name);

                            let router_id = Arc::new(format_source_id(
                                self.router_id_template.clone(),
                                &sys_name,
                                source_id,
                            ));
                            let metrics = self.bmp_metrics.router_metrics(router_id.clone());

                            match sort_by {
                                Some("state") => metrics.bmp_state_machine_state.load(Ordering::SeqCst) as usize,

                                Some("peers_up") => metrics.num_peers_up.load(Ordering::SeqCst),

                                Some("peers_up_eor_capable") => metrics.num_peers_up_eor_capable.load(Ordering::SeqCst),

                                Some("peers_up_dumping") => metrics.num_peers_up_dumping.load(Ordering::SeqCst),

                                Some("peers_up_eor_capable_pc") => {
                                    let total = metrics.num_peers_up.load(Ordering::SeqCst);
                                    let v = metrics.num_peers_up_eor_capable.load(Ordering::SeqCst);
                                    calc_u8_pc(total, v).into()
                                }
                                Some("peers_up_dumping_pc") => {
                                    let total = metrics.num_peers_up_eor_capable.load(Ordering::SeqCst);
                                    let v = metrics.num_peers_up_dumping.load(Ordering::SeqCst);
                                    calc_u8_pc(total, v).into()
                                }

                                Some("invalid_messages") => {
                                    if let Some(router_metrics) = self.router_metrics.router_metrics(router_id) {
                                        router_metrics.num_invalid_bmp_messages.load(Ordering::SeqCst)
                                    } else {
                                        0
                                    }
                                }

                                Some("soft_parse_errors") => {
                                    metrics.num_bgp_updates_with_recoverable_parsing_failures_for_known_peers.load(Ordering::SeqCst) +
                                    metrics.num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers.load(Ordering::SeqCst)
                                }

                                Some("hard_parse_errors") => {
                                    metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers.load(Ordering::SeqCst) +
                                    metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers.load(Ordering::SeqCst)
                                }

                                _ => unreachable!()
                            }
                        } else {
                            0
                        };

                        sort_tmp.push((resolved_v, source_id.clone()));
                    }
                }

                sort_tmp.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                sort_tmp.iter().map(|v| v.1.clone()).collect()
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