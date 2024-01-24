//! Things not implemented in the routecore crate that we use.
use std::{iter::Peekable, net::IpAddr, sync::Arc};

use bytes::Bytes;
use chrono::Utc;
use roto::types::builtin::{
    PeerId, PeerRibType, Provenance, RawRouteWithDeltas, RouteContext, RouteProperties, RouteStatus
};
use routecore::{
    addr::Prefix,
    asn::Asn,
    bgp::{message::{
        nlri::{BasicNlri, Nlri, PathId},
        update::FourOctetAsn,
        update_builder::{ComposeError, UpdateBuilder},
        SessionConfig, UpdateMessage,
    }, path_attributes::BgpIdentifier, types::AfiSafi}
};
use smallvec::SmallVec;

use roto::types::builtin::SourceId;

use crate::payload::Payload;

// Originally based on code in bgmp::main.rs.
pub fn generate_alternate_config(
    peer_config: &SessionConfig,
) -> Option<SessionConfig> {
    let mut alt_peer_config = *peer_config;
    if peer_config.four_octet_asn == FourOctetAsn::Disabled {
        alt_peer_config.enable_four_octet_asn();
    } else if peer_config.four_octet_asn == FourOctetAsn::Enabled {
        alt_peer_config.disable_four_octet_asn();
    }
    // We could try to be smart and toggle addpath settings for all or some
    // address families like below, but there is a chance we start storing
    // incorrect data. The proper solution is to fix the exporting side.
    //
    //    alt_peer_config.inverse_addpaths();
    //    or
    //    alt_peer_config.inverse_addpath(AfiSafi::Ipv4Unicast);

    Some(alt_peer_config)
}

pub fn mk_withdrawals_for_peers_announced_prefixes<'a, I>(
    prefixes: I,
    router_id: Arc<String>,
    peer_address: IpAddr,
    peer_asn: Asn,
    // peer_bgp_id: BgpIdentifier,
    // peer_distuingisher: [u8; 8],
    source_id: SourceId,
) -> Result<SmallVec<[Payload; 8]>, ComposeError>
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
    builder.append_withdrawals(&mut withdrawals)?;

    // turn all the withdrawals into possibly several Update Messages (if the
    // amount of withdrawals will exceed the max PDU size). We only care about
    // these messages since we want to reference them in our routes.
    for bgp_msg in builder.into_iter().flatten() {
        for basic_nlri in bgp_msg.unicast_withdrawals_vec()? {
            let afi_safi = if basic_nlri.prefix.is_v4() { AfiSafi::Ipv4Unicast } else { AfiSafi::Ipv6Unicast };

            let route: RawRouteWithDeltas = RouteContext {
                    msg: bgp_msg.clone(),
                    provenance: Provenance {
                        timestamp: Utc::now(),
                        router_id: router_id.clone(),
                        source_id: source_id.clone(),
                        peer_id: PeerId::new(peer_address, peer_asn),
                        peer_bgp_id: BgpIdentifier::from([0,0,0,0]),
                        peer_distuingisher: [0,0,0,0,0,0,0,0],
                        peer_rib_type: PeerRibType::default(),
                    },
                    route_properties: RouteProperties {
                        prefix: basic_nlri.prefix,
                        path_id: basic_nlri.path_id(),
                        afi_safi,
                        status: RouteStatus::Withdrawn,
                    },
                }.into();

            let payload =
                Payload::new(source_id.clone(), route, None);
            payloads.push(payload);
        }
    }

    Ok(payloads)
}

// TODO: This probably lives in routes, get it from there.
fn _mk_bgp_update<I>(
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
