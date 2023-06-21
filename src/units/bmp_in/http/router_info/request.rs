use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use hyper::{Method, Request};

use crate::{
    http::PercentDecodedPath,
    units::bmp_in::{metrics::BmpInMetrics, state_machine::metrics::BmpMetrics},
};

pub struct RouterInfoApi;

impl RouterInfoApi {
    #[allow(clippy::too_many_arguments)]
    pub fn mk_http_processor(
        http_api_path: Arc<String>,
        router_addr: SocketAddr,
        router_id: Arc<String>,
        sys_name: String,
        sys_desc: String,
        sys_extra: Vec<String>,
        conn_metrics: Arc<BmpInMetrics>,
        bmp_metrics: Arc<BmpMetrics>,
        connected_at: DateTime<Utc>,
        last_message_at: Arc<RwLock<DateTime<Utc>>>,
    ) -> Arc<dyn crate::http::ProcessRequest> {
        Arc::new(move |request: &Request<_>| {
            let req_path = request.uri().decoded_path();
            if request.method() == Method::GET && req_path.starts_with(http_api_path.deref()) {
                let (_, router) = req_path.split_at(http_api_path.len());
                if router == router_addr.to_string()
                    || router == sys_name
                    || router == router_id.deref()
                {
                    return Some(Self::build_response(
                        router_addr,
                        router_id.clone(),
                        &sys_name,
                        &sys_desc,
                        &sys_extra,
                        conn_metrics.clone(),
                        bmp_metrics.clone(),
                        connected_at,
                        last_message_at.clone(),
                    ));
                }
            }

            None
        })
    }
}
