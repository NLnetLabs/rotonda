use std::net::IpAddr;

use axum::{
    extract::{Json, Path, State},
    response::IntoResponse,
};
use base64::prelude::*;
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, warn};
use serde::Deserialize;
//use serde_json::Value;

use crate::{
    http_ng::{Api, ApiError, ApiState},
    ingress::{IngressId, IngressType},
};

pub fn register_routes(router: &mut Api) {
    router.add_post(
        "/bgp/announce/ingress/{ingress_id}",
        send_pdu_for_ingress_id,
    );
    router.add_post(
        "/bgp/announce/peer/{remote_asn}/{remote_addr}",
        send_pdu_for_peer,
    );
}

#[derive(Debug, Deserialize)]
struct Announce {
    prefix: Prefix,
    // base64 encoded path attributes
    raw_attributes: String,
}

async fn send_pdu_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    // fetch asn+addr from ingress
    // call send_pdu
    let Some(ingress_info) = state
        .ingress_register
        .get(ingress_id)
        .filter(|ii| ii.ingress_type == Some(IngressType::Bgp))
    else {
        return Err(ApiError::BadRequest(
            "no BGP ingress for id {ingress_id}".into(),
        ));
    };

    let Some((asn, addr)) =
        ingress_info.remote_asn.zip(ingress_info.remote_addr)
    else {
        return Err(ApiError::BadRequest(
            "unexpected: remote ASN and/or IP address missing".into(),
        ));
    };

    send_pdu(asn, addr, state, body)
}

async fn send_pdu_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    send_pdu(remote_asn, remote_addr, state, body)
}

fn send_pdu(
    remote_asn: Asn,
    remote_addr: IpAddr,
    state: State<ApiState>,
    body: Announce,
) -> Result<impl IntoResponse, ApiError> {
    let Ok(sessions) = state.global_bgp_sessions.lock() else {
        return Err(ApiError::InternalServerError(
            "could not get lock on BGP live sessions".into(),
        ));
    };

    debug!("in send_pdu, global_live_sessions: {:#?}", sessions.keys());

    if let Some(_tx) = sessions.get(&(remote_addr, remote_asn)) {
        debug!(
            "found a session for {remote_asn}@{remote_addr} to send a PDU to"
        );
        let decoded = BASE64_STANDARD
            .decode(body.raw_attributes.as_bytes())
            .unwrap();

        Ok(format!(
            "found a session, TODO send announcement for {} containing {:?}",
            body.prefix, decoded
        ))
    } else {
        warn!("no live BGP session for {remote_asn}@{remote_addr}");
        Err(ApiError::InternalServerError(
            "no live BGP session for {remote_asn}@{remote_addr}".into(),
        ))
    }
}
