use std::{
    ops::Deref,
    sync::{Arc, RwLock, Weak},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hyper::{Body, Method, Request, Response};
use tokio::sync::Mutex;

use crate::{
    http::{PercentDecodedPath, ProcessRequest, self},
    units::bmp_in::{
        metrics::BmpInMetrics,
        state_machine::{
            machine::{BmpState, BmpStateDetails},
            metrics::BmpMetrics,
        },
    }, payload::SourceId,
};

use super::response::Focus;

pub struct RouterInfoApi {
    http_resources: http::Resources,
    http_api_path: Arc<String>,
    source_id: SourceId,
    conn_metrics: Arc<BmpInMetrics>,
    bmp_metrics: Arc<BmpMetrics>,
    connected_at: DateTime<Utc>,
    last_message_at: Arc<RwLock<DateTime<Utc>>>,
    state_machine: Weak<Mutex<Option<BmpState>>>,
}

impl RouterInfoApi {
    pub fn new(
        http_resources: http::Resources,
        http_api_path: Arc<String>,
        source_id: SourceId,
        conn_metrics: Arc<BmpInMetrics>,
        bmp_metrics: Arc<BmpMetrics>,
        connected_at: DateTime<Utc>,
        last_message_at: Arc<RwLock<DateTime<Utc>>>,
        state_machine: Weak<Mutex<Option<BmpState>>>,
    ) -> Self {
        Self {
            http_resources,
            http_api_path,
            source_id,
            conn_metrics,
            bmp_metrics,
            connected_at,
            last_message_at,
            state_machine,
        }
    }
}

#[async_trait]
impl ProcessRequest for RouterInfoApi {
    async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>> {
        let req_path = request.uri().decoded_path();
        if request.method() == Method::GET && req_path.starts_with(self.http_api_path.deref()) {
            let (base_path, router) = req_path.split_at(self.http_api_path.len());
            let router = router.trim_start_matches('/');
            if let Some(state_machine) = self.state_machine.upgrade() {
                let lock = state_machine.lock().await;
                let sm = lock.as_ref().unwrap();

                let (router, focus) = if let Some((router, peer)) = router.split_once("/prefixes/")
                {
                    (router, Focus::Prefixes(peer.to_string()))
                } else if let Some((router, peer)) = router.split_once("/flags/") {
                    (router, Focus::Flags(peer.to_string()))
                } else {
                    (router, Focus::None)
                };

                #[allow(non_snake_case)]
                let EMPTY_STR: String = String::new();
                #[allow(non_snake_case)]
                let EMPTY_VEC: Vec<String> = vec![];

                let router_id = sm.router_id();
                let (sys_name, sys_desc, sys_extra, peer_states) = match sm {
                    BmpState::Dumping(BmpStateDetails { details, .. }) => (
                        &details.sys_name,
                        &details.sys_desc,
                        &details.sys_extra,
                        Some(&details.peer_states),
                    ),
                    BmpState::Updating(BmpStateDetails { details, .. }) => (
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

                if router == self.source_id.to_string()
                    || router == router_id.as_str()
                    || router == sys_name
                {
                    return Some(Self::build_response(
                        self.http_resources.clone(),
                        format!("{}/{}", base_path, router),
                        self.source_id.clone(),
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

        None
    }
}
