use std::net::IpAddr;

use axum::{extract::{Path, Query, State}, response::IntoResponse};
use inetnum::asn::Asn;
use serde::Deserialize;

use crate::{http_ng::{Api, ApiError, ApiState}, ingress::{IngressId, IngressInfo, IngressType}, representation::Json, roto_runtime::types::PeerRibType};

pub struct IngressApi { }

impl IngressApi {


    /// Add ingress register specific endpoints to a HTTP API
    pub fn register_routes(router: &mut Api) {
        router.add_get("/ingresses", Self::ingresses);
        router.add_get("/ingresses/{ingress_id}", Self::ingress_id);
        
    }

    async fn ingress_id(Path(ingress_id): Path<IngressId>, state: State<ApiState>)
        -> Result<impl IntoResponse, ApiError>
    {
        //let mut res = Vec::new();
        //let _ = state.ingress_register.get_and_output(ingress_id, Json(&mut res));
        //Ok(res.into())

        let raw = serde_json::json!(
            {"data": state.ingress_register.get(ingress_id)
                .as_ref().map(|ingress_info| crate::ingress::register::IdAndInfo{ingress_id, ingress_info})
                    
            }
        );
        Ok(([("content-type", "application/json")], raw.to_string()))

    }

    async fn ingresses(Query(filter): Query<QueryFilter>, state: State<ApiState>)
        -> Result<impl IntoResponse, ApiError>
    {
        // Approach #1:
        //let raw = serde_json::json!(
        //    {"data": state.ingress_register.search()}
        //);
        //Ok(raw.to_string())

        // Alternative approach:
        let mut raw = String::from("{\"data\":").into_bytes();
        let _ = state.ingress_register.search_and_output(filter, Json(&mut raw));

        raw.push(b'}');
        Ok(([("content-type", "application/json")], raw))
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct QueryFilter {
    
    #[serde(rename = "filter[type]")]
    pub ingress_type: Option<IngressType>,
    
    #[serde(rename = "filter[ribType]")]
    pub rib_type: Option<PeerRibType>,
    
    #[serde(rename = "filter[peerAddress]")]
    pub remote_addr: Option<IpAddr>,
    
    #[serde(rename = "filter[peerAsn]")]
    pub remote_asn: Option<Asn>,
}

impl QueryFilter {
    /// Returns true if `ingress_info` matches the  set fields in Self. 
    ///
    /// This can be used in a call to `iter().filter()`.
    pub fn filter(&self, ingress_info: &IngressInfo) -> bool {
        if self.remote_addr.is_some() &&
            self.remote_addr != ingress_info.remote_addr
        {
                return false
        }
        if self.remote_asn.is_some() &&
            self.remote_asn != ingress_info.remote_asn
        {
            return false
        }
        if self.ingress_type.is_some() &&
            self.ingress_type != ingress_info.ingress_type
        {
            return false
        }
        if self.rib_type.is_some() &&
            self.rib_type != ingress_info.peer_rib_type
        {
            return false
        }
        true
    }
}
