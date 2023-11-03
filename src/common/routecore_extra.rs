//! Things not implemented in the routecore crate that we use.
use std::{iter::Peekable, net::IpAddr, sync::Arc};

use bytes::Bytes;
use log::error;
use roto::types::builtin::{
    BgpUpdateMessage, RawRouteWithDeltas, RotondaId, RouteStatus,
};
use routecore::{
    addr::Prefix,
    asn::Asn,
    bgp::message::{
        nlri::{BasicNlri, Nlri},
        update::{AddPath, FourOctetAsn},
        update_builder::{ComposeError, UpdateBuilder},
        SessionConfig, UpdateMessage,
    },
};
use smallvec::SmallVec;

use crate::payload::{Payload, SourceId};

// Based on code in bgmp::main.rs:
pub fn generate_alternate_config(
    peer_config: &SessionConfig,
) -> Option<SessionConfig> {
    let mut alt_peer_config = *peer_config;
    if peer_config.four_octet_asn == FourOctetAsn::Disabled {
        alt_peer_config.enable_four_octet_asn();
    } else if peer_config.add_path == AddPath::Disabled {
        alt_peer_config.enable_addpath();
    } else if peer_config.add_path == AddPath::Enabled {
        alt_peer_config.disable_addpath();
    } else if peer_config.four_octet_asn == FourOctetAsn::Enabled {
        alt_peer_config.disable_four_octet_asn();
    } else {
        return None;
    }
    Some(alt_peer_config)
}

pub fn mk_withdrawals_for_peers_announced_prefixes<'a, I>(
    prefixes: I,
    router_id: Arc<String>,
    peer_address: IpAddr,
    peer_asn: Asn,
    source_id: SourceId,
) -> SmallVec<[Payload; 8]>
where
    I: Iterator<Item = &'a Prefix>,
{
    // From https://datatracker.ietf.org/doc/html/rfc7854#section-4.9
    //
    //   "4.9.  Peer Down Notification
    //
    //    ...
    //
    //    A Peer Down message implicitly withdraws all routes that
    //    were associated with the peer in question.  A BMP
    //    implementation MAY omit sending explicit withdraws for such
    //    routes."
    //
    // So, we must act as if we had received route withdrawals for
    // all of the routes previously received for this peer.

    let possible_num_payloads = match prefixes.size_hint() {
        (_, Some(upper_bound)) => upper_bound,
        (lower_bound, None) => lower_bound,
    };
    let mut payloads = SmallVec::with_capacity(possible_num_payloads);

    // create all the withdrawals for the prefixes we need to withdraw
    let mut withdrawals = prefixes
        .map(|p| Nlri::Unicast::<Bytes>(BasicNlri::new(*p)))
        .collect::<Vec<_>>();

    // create a new UpdateBuilder and insert all the withdrawals
    let mut builder = UpdateBuilder::new_bytes();
    let _ = builder.append_withdrawals(&mut withdrawals);

    // turn all the withdrawals into possibly several PDUs (if the amount of
    // withdrawals will exceed the max PDU size). We only care about these
    // PDUs since we want to reference them in our routes.
    if let Ok(pdus) = builder.into_messages() {
        for pdu in pdus {
            for nlri in pdu.withdrawals().unwrap().flatten() {
                if let Nlri::Unicast(BasicNlri { prefix, .. }) = nlri {
                    let route = mk_route_for_prefix(
                        router_id.clone(),
                        pdu.clone(),
                        peer_address,
                        peer_asn,
                        prefix,
                        RouteStatus::Withdrawn,
                    );
                    let payload =
                        Payload::new(source_id.clone(), route, None);
                    payloads.push(payload);
                }
            }
        }
    }

    payloads
}

fn mk_bgp_update<I>(
    withdrawals: &mut Peekable<I>,
) -> Result<UpdateMessage<Bytes>, ComposeError>
where
    I: Iterator<Item = Nlri<Vec<u8>>>,
{
    let mut builder = UpdateBuilder::new_bytes();
    match builder.withdrawals_from_iter(withdrawals) {
        Ok(_) | Err(ComposeError::PduTooLarge(_)) => builder.into_message(),
        Err(err) => Err(err),
    }
}

pub fn mk_route_for_prefix(
    router_id: Arc<String>,
    update: UpdateMessage<Bytes>,
    peer_address: IpAddr,
    peer_asn: Asn,
    prefix: Prefix,
    route_status: RouteStatus,
) -> RawRouteWithDeltas {
    let delta_id = (RotondaId(0), 0); // TODO
    let roto_update_msg = roto::types::builtin::UpdateMessage(update);
    let raw_msg = Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
    RawRouteWithDeltas::new_with_message_ref(
        delta_id,
        prefix.into(),
        &raw_msg,
        route_status,
    )
    .with_peer_ip(peer_address)
    .with_peer_asn(peer_asn)
    .with_router_id(router_id)
}
