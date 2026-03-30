use std::net::IpAddr;

use axum::{
    extract::{Json, Path, State},
    response::IntoResponse,
};
use base64::prelude::*;
use bytes::Bytes;
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, warn};
use rotonda_store::prefix_record::{Record, RouteStatus};
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
    ingress::{
        http_ng::QueryFilter, register::IngressState, IngressId, IngressInfo,
        IngressType,
    },
    payload::{RotondaPaMap, RotondaRoute},
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

    router.add_post("/bgp/raw/ingress/{ingress_id}", send_raw_for_ingress_id);
    router.add_post(
        "/bgp/raw/peer/{remote_asn}/{remote_addr}",
        send_raw_for_peer,
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

#[derive(Debug, Deserialize)]
struct RawPdu {
    full_pdu: String,
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

// XXX make this and ingress_to_peer live on ApiState itself?
fn peer_to_ingress(
    peer_asn: Asn,
    peer_addr: IpAddr,
    state: &State<ApiState>,
) -> Result<IngressId, ApiError> {
    let res = state.ingress_register.search(QueryFilter {
        ingress_type: Some(IngressType::Bgp),
        ingress_state: Some(IngressState::Connected),
        remote_addr: Some(peer_addr),
        remote_asn: Some(peer_asn),
        ..Default::default()
    });
    if res.len() > 1 {
        return Err(ApiError::InternalServerError(format!(
            "more than one candidate BGP session for \
            {peer_asn}@{peer_addr}, rotonda bug"
        )));
    }

    if res.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "no active BGP session for {peer_asn}@{peer_addr}"
        )));
    }

    Ok(res[0].ingress_id)
}

async fn send_announce_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    let (asn, addr) = ingress_to_peer(ingress_id, &state)?;
    send_pdu(ingress_id, asn, addr, state, Action::Announce(body)).await
}

async fn send_announce_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<Announce>,
) -> Result<impl IntoResponse, ApiError> {
    let ingress_id = peer_to_ingress(remote_asn, remote_addr, &state)?;
    send_pdu(
        ingress_id,
        remote_asn,
        remote_addr,
        state,
        Action::Announce(body),
    )
    .await
}

async fn send_withdraw_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<Withdraw>,
) -> Result<impl IntoResponse, ApiError> {
    let (asn, addr) = ingress_to_peer(ingress_id, &state)?;
    send_pdu(ingress_id, asn, addr, state, Action::Withdraw(body)).await
}

async fn send_withdraw_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<Withdraw>,
) -> Result<impl IntoResponse, ApiError> {
    let ingress_id = peer_to_ingress(remote_asn, remote_addr, &state)?;
    send_pdu(
        ingress_id,
        remote_asn,
        remote_addr,
        state,
        Action::Withdraw(body),
    )
    .await
}

async fn send_pdu(
    ingress_id: IngressId,
    remote_asn: Asn,
    remote_addr: IpAddr,
    state: State<ApiState>,
    action: Action,
) -> Result<impl IntoResponse, ApiError> {
    let (tx_cmds, tx_pdus) = state.get_session(remote_addr, remote_asn)?;

    // TODO D-R-Y, extract this into function
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
                .map_err(|e| {
                    ApiError::BadRequest(format!("invalid base64: {e}"))
                })?;

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

            // find an existing mui, or register one.
            // the mui ('ingress_id') is not really pointing to an
            // ingress, but rather a adj-RIB-out for Rotonda
            // itself.
            let adj_rib_out_id = if let Some((ingress_id, _ingress_info)) =
                state.ingress_register.find_rib_out_for_ingress(ingress_id)
            {
                ingress_id
            } else {
                let new_id = state.ingress_register.register();
                state.ingress_register.update_info(
                    new_id,
                    IngressInfo::new()
                        .with_parent_ingress(ingress_id)
                        .with_ingress_type(IngressType::BgpOut)
                        .with_remote_asn(remote_asn)
                        .with_remote_addr(remote_addr),
                );
                new_id
            };

            let store = state.store.load();
            let Some(ref rib) = *store else {
                return Err(ApiError::InternalServerError(
                    "store not ready".into(),
                ));
            };

            if announce.prefix.first().is_some_and(|p| p.is_v4()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv4UnicastNlri>::from_attributes_builder(pamap);
                if let Err(e) = builder.announcements_from_iter(
                    announce
                        .prefix
                        .iter()
                        .map(|p| Ipv4UnicastNlri::try_from(*p).unwrap()),
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
                    Ok(_) => {
                        let ltime = 0;
                        let route_status = RouteStatus::Active;
                        for p in announce.prefix.into_iter() {
                            let rr = RotondaRoute::Ipv4Unicast(
                                Ipv4UnicastNlri::try_from(p).unwrap(),
                                RotondaPaMap::new(opa.clone()),
                            );
                            if let Err(e) = rib.insert(
                                &rr,
                                route_status,
                                ltime,
                                adj_rib_out_id,
                            ) {
                                return Err(ApiError::InternalServerError(
                                    format!("store error: {e}"),
                                ));
                            }
                        }
                        Ok("ok")
                    }
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else if announce.prefix.first().is_some_and(|p| p.is_v6()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv6UnicastNlri>::from_attributes_builder(pamap);
                if let Err(e) = builder.announcements_from_iter(
                    announce
                        .prefix
                        .iter()
                        .map(|p| Ipv6UnicastNlri::try_from(*p).unwrap()),
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
                    Ok(_) => {
                        let ltime = 0;
                        let route_status = RouteStatus::Active;
                        for p in announce.prefix.into_iter() {
                            let rr = RotondaRoute::Ipv6Unicast(
                                Ipv6UnicastNlri::try_from(p).unwrap(),
                                RotondaPaMap::new(opa.clone()),
                            );
                            if let Err(e) = rib.insert(
                                &rr,
                                route_status,
                                ltime,
                                adj_rib_out_id,
                            ) {
                                return Err(ApiError::InternalServerError(
                                    format!("store error: {e}"),
                                ));
                            }
                        }
                        Ok("ok")
                    }
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else {
                Err(ApiError::InternalServerError(
                    "no prefixes or unexpected prefix type".into(),
                ))
            }
        }
        Action::Withdraw(withdraw) => {
            let adj_rib_out_id = if let Some((ingress_id, _ingress_info)) =
                state.ingress_register.find_rib_out_for_ingress(ingress_id)
            {
                ingress_id
            } else {
                // If there is no such an existing BgpOut, there
                // can't be anything to withdraw in the first
                // place.
                return Err(ApiError::BadRequest(format!(
                    "no BgpOut for \
                                {remote_asn}@{remote_addr} \
                                (ingress {ingress_id})"
                )));
            };

            let store = state.store.load();
            let Some(ref rib) = *store else {
                return Err(ApiError::InternalServerError(
                    "store not ready".into(),
                ));
            };

            if withdraw.prefix.first().is_some_and(|p| p.is_v4()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv4UnicastNlri>::new_vec();
                if let Err(e) = builder.withdrawals_from_iter(
                    withdraw
                        .prefix
                        .iter()
                        .map(|p| Ipv4UnicastNlri::try_from(*p).unwrap()),
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
                    Ok(_) => {
                        let ltime = 0;
                        let route_status = RouteStatus::Withdrawn;
                        for p in withdraw.prefix.into_iter() {
                            let rr = RotondaRoute::Ipv4Unicast(
                                Ipv4UnicastNlri::try_from(p).unwrap(),
                                RotondaPaMap::empty(),
                            );
                            if let Err(e) = rib.insert(
                                &rr,
                                route_status,
                                ltime,
                                adj_rib_out_id,
                            ) {
                                return Err(ApiError::InternalServerError(
                                    format!("store error: {e}"),
                                ));
                            }
                        }
                        Ok("ok")
                    }
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else if withdraw.prefix.first().is_some_and(|p| p.is_v6()) {
                let mut builder =
                    UpdateBuilder::<Vec<u8>, Ipv6UnicastNlri>::new_vec();
                if let Err(e) = builder.withdrawals_from_iter(
                    withdraw
                        .prefix
                        .iter()
                        .map(|p| Ipv6UnicastNlri::try_from(*p).unwrap()),
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
                    Ok(_) => {
                        let ltime = 0;
                        let route_status = RouteStatus::Withdrawn;
                        for p in withdraw.prefix.into_iter() {
                            let rr = RotondaRoute::Ipv6Unicast(
                                Ipv6UnicastNlri::try_from(p).unwrap(),
                                RotondaPaMap::empty(),
                            );
                            if let Err(e) = rib.insert(
                                &rr,
                                route_status,
                                ltime,
                                adj_rib_out_id,
                            ) {
                                return Err(ApiError::InternalServerError(
                                    format!("store error: {e}"),
                                ));
                            }
                        }
                        Ok("ok")
                    }
                    Err(e) => {
                        Err(ApiError::InternalServerError(e.to_string()))
                    }
                }
            } else {
                Err(ApiError::InternalServerError(
                    "no prefixes or unexpected prefix type".into(),
                ))
            }
        }
    }
}

async fn send_raw_for_ingress_id(
    Path(ingress_id): Path<IngressId>,
    state: State<ApiState>,
    Json(body): Json<RawPdu>,
) -> Result<impl IntoResponse, ApiError> {
    let (asn, addr) = ingress_to_peer(ingress_id, &state)?;
    send_raw(asn, addr, state, body).await
}

async fn send_raw_for_peer(
    Path((remote_asn, remote_addr)): Path<(Asn, IpAddr)>,
    state: State<ApiState>,
    Json(body): Json<RawPdu>,
) -> Result<impl IntoResponse, ApiError> {
    send_raw(remote_asn, remote_addr, state, body).await
}

async fn send_raw(
    remote_asn: Asn,
    remote_addr: IpAddr,
    state: State<ApiState>,
    raw_pdu: RawPdu,
) -> Result<impl IntoResponse, ApiError> {
    let (tx_cmds, tx_pdus) = state.get_session(remote_addr, remote_asn)?;

    // TODO D-R-Y, extract this into function
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
    let decoded = BASE64_STANDARD
        .decode(raw_pdu.full_pdu.as_bytes())
        .map_err(|e| ApiError::BadRequest(format!("invalid base64: {e}")))?;
    let bytes = Bytes::from(decoded);
    let pdu = UpdateMessage::from_octets(bytes, &session_config)
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    match tx_pdus.try_send(routecore::bgp::message::Message::Update(pdu)) {
        Ok(_) => {
            // TODO update RIB?
            // We do not know whether this is an announcement or a withdrawal,
            // or what AFISAFI this PDU pertains to anyway.
            Ok("ok")
        }
        Err(e) => Err(ApiError::InternalServerError(e.to_string())),
    }
}
