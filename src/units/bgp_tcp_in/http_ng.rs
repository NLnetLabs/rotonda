use std::net::IpAddr;

use axum::{
    extract::{Json, Path, State},
    response::IntoResponse,
};
use base64::prelude::*;
use bytes::Bytes;
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, warn};
use routecore::bgp::{
    message::{
        update_builder::UpdateBuilder, PduParseInfo, SessionConfig,
        UpdateMessage,
    },
    nlri::afisafi::{Ipv4UnicastNlri, Ipv6UnicastNlri},
    path_attributes::{OwnedPathAttributes, PaMap},
};
use serde::Deserialize;
use tokio::sync::oneshot;

use crate::{
    http_ng::{Api, ApiError, ApiState},
    ingress::{IngressId, IngressType},
};

pub fn register_routes(router: &mut Api) {
    router.add_post(
        "/bgp/announce/ingress/{ingress_id}",
        send_announce_for_ingress_id,
    );
    router.add_post(
        "/bgp/announce/peer/{remote_asn}/{remote_addr}",
        send_announce_for_peer,
    );
    router.add_post(
        "/bgp/withdraw/ingress/{ingress_id}",
        send_withdraw_for_ingress_id,
    );
    router.add_post(
        "/bgp/withdraw/peer/{remote_asn}/{remote_addr}",
        send_withdraw_for_peer,
    );
}

#[derive(Debug, Deserialize)]
struct Announce {
    prefix: Vec<Prefix>,
    nexthop: IpAddr,
    // base64 encoded path attributes
    raw_attributes: String,
}

#[derive(Debug, Deserialize)]
struct Withdraw {
    prefix: Vec<Prefix>,
}

enum Action {
    Announce(Announce),
    Withdraw(Withdraw),
}

fn ingress_to_peer(
    ingress_id: IngressId,
    state: &State<ApiState>,
) -> Result<(Asn, IpAddr), ApiError> {
    let Some(ingress_info) = state
        .ingress_register
        .get(ingress_id)
        .filter(|ii| ii.ingress_type == Some(IngressType::Bgp))
    else {
        return Err(ApiError::BadRequest(
            "no BGP ingress for id {ingress_id}".into(),
        ));
    };

    ingress_info.remote_asn.zip(ingress_info.remote_addr).ok_or(
        ApiError::BadRequest(
            "unexpected: remote ASN and/or IP address missing".into(),
        ),
    )
}

async fn send_announce_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    let (asn, addr) = ingress_to_peer(ingress_id, &state)?;
    send_pdu(asn, addr, state, Action::Announce(body)).await
}

async fn send_announce_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    send_pdu(remote_asn, remote_addr, state, Action::Announce(body)).await
}

async fn send_withdraw_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<Withdraw>,
) -> Result<impl IntoResponse, ApiError> {
    let (asn, addr) = ingress_to_peer(ingress_id, &state)?;
    send_pdu(asn, addr, state, Action::Withdraw(body)).await
}

async fn send_withdraw_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<Withdraw>,
) -> Result<impl IntoResponse, ApiError> {
    send_pdu(remote_asn, remote_addr, state, Action::Withdraw(body)).await
}

async fn send_pdu(
    remote_asn: Asn,
    remote_addr: IpAddr,
    state: State<ApiState>,
    action: Action,
) -> Result<impl IntoResponse, ApiError> {
    let (tx_cmds, tx_pdus) = state.get_session(remote_addr, remote_asn)?;

    let (tx, rx) = oneshot::channel();
    let session_config = {
        tx_cmds
            .send(routecore::bgp::fsm::session::Command::GetSessionConfig {
                resp: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap_or_else(|_| {
            warn!(
                "could not get SessionConfig from BGP handler, \
                defaulting to SessionConfig::modern()"
            );
            SessionConfig::modern()
        })
    };

    debug!("got session_config: {:#?}", session_config);

    match action {
        Action::Announce(announce) => {
            let decoded = BASE64_STANDARD
                .decode(announce.raw_attributes.as_bytes())
                .unwrap();

            let mut pamap = PaMap::empty();
            let opa =
                OwnedPathAttributes::new(PduParseInfo::modern(), decoded);
            for pa in opa.iter() {
                if let Ok(pa) = pa {
                    pamap.set_from_enum(pa.to_owned().unwrap());
                } else {
                    warn!("can't interpret path attribute: {pa:#?}")
                }
            }

            if announce.prefix.first().is_some_and(|p| p.is_v4()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv4UnicastNlri>::from_attributes_builder(pamap);
                if let Err(e) = builder.announcements_from_iter(
                    announce
                        .prefix
                        .into_iter()
                        .map(|p| Ipv4UnicastNlri::try_from(p).unwrap()),
                ) {
                    return Err(ApiError::InternalServerError(e.to_string()));
                };
                let _ = builder.set_nexthop(
                    routecore::bgp::types::NextHop::Unicast(announce.nexthop),
                );
                let pdu = match builder.into_message(&session_config) {
                    Ok(pdu) => pdu,
                    Err(e) => {
                        return Err(ApiError::InternalServerError(
                            e.to_string(),
                        ));
                    }
                };

                let bytes = Bytes::from(pdu.as_ref().to_vec());
                match tx_pdus.try_send(
                    routecore::bgp::message::Message::Update(
                        UpdateMessage::from_octets(bytes, &session_config)
                            .unwrap(),
                    ),
                ) {
                    Ok(_) => Ok("ok"),
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else if announce.prefix.first().is_some_and(|p| p.is_v6()) {
                todo!()
                //if let Err(e) = builder
                //    .add_announcement(Ipv6UnicastNlri::try_from(body.prefix).unwrap())
                //{
                //    return Err(ApiError::InternalServerError(e.to_string()));
                //};
            } else {
                Err(ApiError::InternalServerError(
                    "no prefixes or unexpected prefix type".into(),
                ))
            }
        }
        Action::Withdraw(withdraw) => {
            if withdraw.prefix.first().is_some_and(|p| p.is_v4()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv4UnicastNlri>::new_vec();
                if let Err(e) = builder.withdrawals_from_iter(
                    withdraw
                        .prefix
                        .into_iter()
                        .map(|p| Ipv4UnicastNlri::try_from(p).unwrap()),
                ) {
                    return Err(ApiError::InternalServerError(e.to_string()));
                };
                let pdu = match builder.into_message(&session_config) {
                    Ok(pdu) => pdu,
                    Err(e) => {
                        return Err(ApiError::InternalServerError(
                            e.to_string(),
                        ));
                    }
                };

                let bytes = Bytes::from(pdu.as_ref().to_vec());
                match tx_pdus.try_send(
                    routecore::bgp::message::Message::Update(
                        UpdateMessage::from_octets(bytes, &session_config)
                            .unwrap(),
                    ),
                ) {
                    Ok(_) => Ok("ok"),
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else if withdraw.prefix.first().is_some_and(|p| p.is_v6()) {
                todo!()
                //if let Err(e) = builder
                //    .add_announcement(Ipv6UnicastNlri::try_from(body.prefix).unwrap())
                //{
                //    return Err(ApiError::InternalServerError(e.to_string()));
                //};
            } else {
                Err(ApiError::InternalServerError(
                    "no prefixes or unexpected prefix type".into(),
                ))
            }
        }
    }
}
