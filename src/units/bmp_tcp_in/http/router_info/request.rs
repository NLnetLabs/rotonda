use std::{
    ops::Deref,
    sync::{Arc, RwLock, Weak},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hyper::{Body, Method, Request, Response};
use tokio::sync::Mutex;
//use roto::types::builtin::SourceId;

use crate::{
    http::{self, PercentDecodedPath, ProcessRequest},
    ingress,
    units::bmp_tcp_in::{
        metrics::BmpTcpInMetrics,
        state_machine::{BmpState, BmpStateDetails, BmpStateMachineMetrics},
    },
};

use super::response::Focus;

pub struct RouterInfoApi {
    http_resources: http::Resources,
    http_api_path: Arc<String>,
    //source_id: SourceId,
    ingress_id: ingress::IngressId,
    conn_metrics: Arc<BmpTcpInMetrics>,
    bmp_metrics: Arc<BmpStateMachineMetrics>,
    connected_at: DateTime<Utc>,
    last_message_at: Arc<RwLock<DateTime<Utc>>>,
    state_machine: Weak<Mutex<Option<BmpState>>>,
    ingresses: Arc<ingress::Register>,
}

impl RouterInfoApi {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        http_resources: http::Resources,
        http_api_path: Arc<String>,
        //source_id: SourceId,
        ingress_id: ingress::IngressId,
        conn_metrics: Arc<BmpTcpInMetrics>,
        bmp_metrics: Arc<BmpStateMachineMetrics>,
        connected_at: DateTime<Utc>,
        last_message_at: Arc<RwLock<DateTime<Utc>>>,
        state_machine: Weak<Mutex<Option<BmpState>>>,
        ingresses: Arc<ingress::Register>,
    ) -> Self {
        Self {
            http_resources,
            http_api_path,
            //source_id,
            ingress_id,
            conn_metrics,
            bmp_metrics,
            connected_at,
            last_message_at,
            state_machine,
            ingresses,
        }
    }
}

#[async_trait]
impl ProcessRequest for RouterInfoApi {
    async fn process_request(
        &self,
        request: &Request<Body>,
    ) -> Option<Response<Body>> {
        let req_path = request.uri().decoded_path();
        if request.method() == Method::GET
            && req_path.starts_with(self.http_api_path.deref())
        {
            let (base_path, router) =
                req_path.split_at(self.http_api_path.len());
            if !router.is_empty() {
                if let Some(state_machine) = self.state_machine.upgrade() {
                    let lock = state_machine.lock().await;
                    let sm = lock.as_ref().unwrap();

                    let (router, focus) = if let Some((router, peer)) =
                        router.split_once("/prefixes/")
                    {
                        (router, Focus::Prefixes(peer.to_string()))
                    } else if let Some((router, peer)) =
                        router.split_once("/flags/")
                    {
                        (router, Focus::Flags(peer.to_string()))
                    } else {
                        (router, Focus::None)
                    };

                    #[allow(non_snake_case)]
                    let EMPTY_STR: String = String::new();
                    #[allow(non_snake_case)]
                    let EMPTY_VEC: Vec<String> = vec![];

                    let router_id = sm.router_id();
                    let (sys_name, sys_desc, sys_extra, peer_states) =
                        match sm {
                            BmpState::Dumping(BmpStateDetails {
                                details,
                                ..
                            }) => (
                                &details.sys_name,
                                &details.sys_desc,
                                &details.sys_extra,
                                Some(&details.peer_states),
                            ),
                            BmpState::Updating(BmpStateDetails {
                                details,
                                ..
                            }) => (
                                &details.sys_name,
                                &details.sys_desc,
                                &details.sys_extra,
                                Some(&details.peer_states),
                            ),
                            _ => {
                                // no TLVs available
                                (&EMPTY_STR, &EMPTY_STR, &EMPTY_VEC, None)
                            }
                        };

                    let ingress_info = self.ingresses.get(self.ingress_id);
                    let addr = ingress_info
                        .and_then(|i| i.remote_addr)
                        .map(|a| a.to_string())
                        .unwrap_or("".to_string());
                    //if router == self.source_id.to_string()
                    if router == self.ingress_id.to_string()
                        || router == router_id.as_str()
                        || router == sys_name
                        || router == addr
                    {
                        return Some(Self::build_response(
                            self.http_resources.clone(),
                            format!("{}{}", base_path, router),
                            //self.source_id.clone(),
                            self.ingress_id,
                            router_id,
                            sys_name,
                            sys_desc,
                            sys_extra,
                            peer_states,
                            focus,
                            &self.conn_metrics,
                            &self.bmp_metrics,
                            &self.connected_at,
                            &self.last_message_at,
                        ));
                    }
                }
            }
        }

        None
    }
}
