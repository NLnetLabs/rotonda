//! Things not implemented in the routecore crate that we use.
// XXX we can get rid of most if not all of the things here?

/*
use bytes::Bytes;
use chrono::Utc;
use roto::types::builtin::{
    Nlri, NlriStatus, PeerId, PeerRibType, Provenance
};
use inetnum::{addr::Prefix, asn::Asn};
use crate::units::bgp_tcp_in::peer_config::ConfigExt;
*/

/*
use routecore::
    bgp::{fsm::session::BgpConfig, message::{update::FourOctetAsns,
        update_builder::{ComposeError, UpdateBuilder},
        SessionConfig, UpdateMessage,
    }, nlri::afisafi::{AfiSafiNlri, NlriParse, NlriCompose}, path_attributes::{BgpIdentifier, PathAttribute, PathAttributeType}, types::AfiSafiType, workshop::route::RouteWorkshop};
use smallvec::SmallVec;

use roto::types::builtin::SourceId;

use crate::payload::Payload;
*/

use routecore::bgp::message::{update::FourOctetAsns, SessionConfig};

// Originally based on code in bgmp::main.rs.
pub fn generate_alternate_config(
    peer_config: &SessionConfig,
) -> Option<SessionConfig> {
    let mut alt_peer_config = peer_config.clone();
    alt_peer_config.set_four_octet_asns(FourOctetAsns(
        !peer_config.four_octet_enabled(),
    ));
    // We could try to be smart and toggle addpath settings for all or some
    // address families like below, but there is a chance we start storing
    // incorrect data. The proper solution is to fix the exporting side.
    //
    //    alt_peer_config.inverse_addpaths();
    //    or
    //    alt_peer_config.inverse_addpath(AfiSafi::Ipv4Unicast);

    Some(alt_peer_config)
}

/*
pub fn mk_withdrawals_for_peers_announced_prefixes<'a, N, NI>(
    nlri: NI,
    // router_id: Arc<String>,
    provenance: Provenance,
    session_config: SessionConfig,
    // peer_address: IpAddr,
    // peer_asn: Asn,
    // peer_bgp_id: BgpIdentifier,
    // peer_distuingisher: [u8; 8],
    // source_id: SourceId,
) -> Result<SmallVec<[Payload; 8]>, ComposeError>
where
    NI: Iterator<Item = &'a N>,
    N: AfiSafiNlri + NlriCompose + Clone + 'a
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

    todo!()
    /*
    let possible_num_payloads = match nlri.size_hint() {
        (_, Some(upper_bound)) => upper_bound,
        (lower_bound, None) => lower_bound,
    };
    let mut payloads = SmallVec::with_capacity(possible_num_payloads);

    // create all the withdrawals for the prefixes we need to withdraw
    let mut withdrawals = nlri
        .map(|n| *n)
        .collect::<Vec<N>>();

    // create a new UpdateBuilder and insert all the withdrawals
    let mut builder = UpdateBuilder::new_bytes();
    builder.append_withdrawals(withdrawals)?;

    // let mut router_hash = DefaultHasher::new();
    // router_id.hash(&mut router_hash);

    // let provenance = Provenance {
    //     timestamp: Utc::now(),
    //     peer_id: PeerId::new(peer_address, peer_asn),
    //     peer_bgp_id: BgpIdentifier::from([0,0,0,0]),
    //     peer_distuingisher: [0,0,0,0,0,0,0,0],
    //     peer_rib_type: PeerRibType::default(),
    //     router_id: router_hash.finish() as u32,
    //     connection_id: 0,
    // };
    // turn all the withdrawals into possibly several Update Messages (if the
    // amount of withdrawals will exceed the max PDU size). We only care about
    // these messages since we want to reference them in our routes.
    for pdu in builder.into_pdu_iter(&session_config).flatten() {
        for nlri in pdu.withdrawals_vec()? {
            // let afi_safi = if basic_nlri.prefix.is_v4() { AfiSafi::Ipv4Unicast } else { AfiSafi::Ipv6Unicast };
            // let router_id_hash: u32 = router_id.into();
            // let route = RouteWorkshop::new(
            //     nlri
            // );

            let payload =
                Payload::new::<Nlri>(
                    nlri.into(),
                    Some(provenance),
                    None
                );
            payloads.push(payload);
        }
    }

    Ok(payloads)
    */
}
*/

/*
// TODO: This probably lives in routes, get it from there.
fn _mk_bgp_update<'a, N, NI>(
    withdrawals: &mut Peekable<NI>,
    session_config: SessionConfig
) -> Result<UpdateMessage<Bytes>, ComposeError>
where
    NI: Iterator<Item = N>,
    N: AfiSafiNlri + NlriCompose + Clone + 'a
{
    let mut builder = UpdateBuilder::new_bytes();
    match builder.withdrawals_from_iter(withdrawals) {
        Ok(_) | Err(ComposeError::PduTooLarge(_)) => builder.into_message(&session_config),
        Err(err) => Err(err),
    }
}
*/
