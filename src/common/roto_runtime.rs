use bytes::Bytes;
use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use routecore::bgp::aspath::{Hop, HopPath};
use routecore::bgp::communities::Community;
use routecore::bgp::message::SessionConfig;
use routecore::bgp::nlri::afisafi::IsPrefix;
use routecore::bmp::message::PerPeerHeader;
use routecore::bmp::message::{Message as BmpMsg, MessageType as BmpMsgType};
use routecore::bgp::message::{Message as BgpMsg, UpdateMessage as BgpUpdateMessage};

use crate::common::roto_new::{RotoOutputStream, RouteContext};
use crate::payload::RotondaRoute;

use super::roto_new::{FreshRouteContext, InsertionInfo, Output, OutputStream, Provenance};
use roto::roto_method;



pub(crate) type Log = *mut RotoOutputStream;

pub fn rotonda_roto_runtime() -> Result<roto::Runtime, String> {
    let mut rt = roto::Runtime::basic()?;

    // --- General types
    rt.register_clone_type_with_name::<RotondaRoute>(
        "Route",
        "A single announced or withdrawn path",
    )?;
    rt.register_clone_type_with_name::<RouteContext>(
        "RouteContext",
        "Contextual information pertaining to the Route"
    )?;
    rt.register_copy_type::<Provenance>(
        "Session/state information"
    )?;
    rt.register_clone_type_with_name::<Log>(
        "Log",
        "Machinery to create output entries"
    )?;
    rt.register_copy_type::<InsertionInfo>(
        "Information from the RIB on an inserted route"
    )?;

    // --- BGP types / methods
    rt.register_clone_type_with_name::<BgpUpdateMessage<Bytes>>(
        "BgpMsg",
        "BGP UPDATE message"
    )?;


    // --- RotondaRoute methods

    #[roto_method(rt, RotondaRoute)]
    fn prefix_matches(rr: *const RotondaRoute, to_match: *const Prefix) -> bool {
        let rr = unsafe { &*rr };
        let to_match = unsafe { &*to_match };
        let rr_prefix = match rr {
            RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
            RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
            RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
            RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
        };
        rr_prefix == *to_match
    }

    #[roto_method(rt, RotondaRoute, aspath_origin)]
    fn rr_aspath_origin(
        rr: *const RotondaRoute,
        to_match: Asn,
    ) -> bool {
        let rr = unsafe { &*rr };
        if let Some(hoppath) = rr.owned_map().get::<HopPath>(){
            if let Some(Hop::Asn(asn)) = hoppath.origin() {
                return *asn == to_match
            }
        }
        false

    }


    #[roto_method(rt, RotondaRoute, has_attribute)]
    fn rr_has_attribute(rr: *const RotondaRoute, to_match: u8) -> bool {
        let rr = unsafe { &*rr };
        rr.owned_map()
            .iter()
            .any(|pa| pa.ok().map(|pa| pa.type_code() == to_match).is_some())
    }

    // --- BGP message methods

    #[roto_method(rt, BgpUpdateMessage<Bytes>, aspath_contains)]
    fn bgp_aspath_contains(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        aspath_contains(msg, to_match)
    }

    #[roto_method(rt, BgpUpdateMessage<Bytes>, aspath_origin)]
    fn bgp_aspath_origin(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        aspath_origin(msg, to_match)
    }

    #[roto_method(rt, BgpUpdateMessage<Bytes>, contains_community)]
    fn bgp_contains_community(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: u32,
    ) -> bool {
        let msg = unsafe { &*msg };

        contains_community(msg, to_match)
    }

    #[roto_method(rt, BgpUpdateMessage<Bytes>, has_attribute)]
    fn bgp_has_attribute(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: u8,
    ) -> bool {
        let msg = unsafe { &*msg };

        has_attribute(msg, to_match)
    }



    // --- BMP types / methods
    rt.register_clone_type_with_name::<BmpMsg<Bytes>>(
        "BmpMsg",
        "BMP Message"
    )?;
    rt.register_clone_type::<PerPeerHeader<Bytes>>(
        "BMP Per Peer Header"
    )?;


    // Return true if asn matches the asn in the BmpMsg.
    //
    // returns false if no PPH is present.
    #[roto_method(rt, BmpMsg<Bytes>)]
    fn is_ibgp(msg: *const BmpMsg<Bytes>, asn: Asn) -> bool {
        let msg = unsafe { &*msg };
        let asn_in_msg = match msg {
            BmpMsg::RouteMonitoring(m) => m.per_peer_header().asn(),
            BmpMsg::StatisticsReport(m) => m.per_peer_header().asn(),
            BmpMsg::PeerDownNotification(m) => m.per_peer_header().asn(),
            BmpMsg::PeerUpNotification(m) => m.per_peer_header().asn(),
            BmpMsg::InitiationMessage(_) => return false,
            BmpMsg::TerminationMessage(_) => return false,
            BmpMsg::RouteMirroring(m) => m.per_peer_header().asn(),
        };
        asn == asn_in_msg
    }

    //rt.register_method::<BmpMsg<Bytes>, _, _>("is_peer_down", is_peer_down)?;
    #[roto_method(rt, BmpMsg<Bytes>)]
    fn is_peer_down(msg: *const BmpMsg<Bytes>) -> bool {
        let msg = unsafe { &*msg };
        msg.msg_type() == BmpMsgType::PeerDownNotification
    }

    //rt.register_method::<BmpMsg<Bytes>, _, _>("aspath_contains", bmp_aspath_contains)?;
    #[roto_method(rt, BmpMsg<Bytes>, aspath_contains)]
    fn bmp_aspath_contains(
        msg: *const BmpMsg<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return false;
            }
        } else {
            return false;
        };

        aspath_contains(&update, to_match)
    }


    #[roto_method(rt, BmpMsg<Bytes>, aspath_origin)]
    fn bmp_aspath_origin(
        msg: *const BmpMsg<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return false;
            }
        } else {
            return false;
        };

        aspath_origin(&update, to_match)
    }

    #[roto_method(rt, BmpMsg<Bytes>, contains_community)]
    fn bmp_contains_community(
        msg: *const BmpMsg<Bytes>,
        to_match: u32,
    ) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return false;
            }
        } else {
            return false;
        };

        contains_community(&update, to_match)
    }

    #[roto_method(rt, BmpMsg<Bytes>, has_attribute)]
    fn bmp_has_attribute(
        msg: *const BmpMsg<Bytes>,
        to_match: u8,
    ) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return false;
            }
        } else {
            return false;
        };

        has_attribute(&update, to_match)
    }


    // --- Output / logging / 'south'-wards artifacts methods
    #[roto_method(rt, Log)]
    fn log_prefix(
        stream: *mut Log,
        prefix: *const Prefix,
    ) {
        let stream = unsafe { &mut **stream };
        let prefix = unsafe { &*prefix };
        stream.push(Output::Prefix(*prefix));
    }


    #[roto_method(rt, Log, log_matched_asn)]
    fn log_asn(
        stream: *mut Log,
        asn: Asn,
    ) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Asn(asn));
    }

    #[roto_method(rt, Log, log_matched_origin)]
    fn log_origin(
        stream: *mut Log,
        origin: Asn,
    ) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Origin(origin));
    }

    //rt.register_method::<Log, _, _>("log_matched_community", log_community)?;
    #[roto_method(rt, Log, log_matched_community)]
    fn log_community(
        stream: *mut Log,
        community: u32,
    ) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Community(community));
    }

    //rt.register_method::<Log, _, _>("log_peer_down", log_peer_down)?;
    #[roto_method(rt, Log)]
    fn log_peer_down(
        stream: *mut Log,
        //addr: *mut IpAddr,
    ) {
        let stream = unsafe { &mut **stream };
        //let addr = unsafe { &mut *addr };
        stream.push(Output::PeerDown);
    }

    //rt.register_method::<Log, _, _>("log_custom", log_custom)?;
    #[roto_method(rt, Log)]
    fn log_custom(
        stream: *mut Log,
        id: u32,
        local: u32,
    ) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Custom((id, local)));
    }


    // --- InsertionInfo methods
    #[roto_method(rt, InsertionInfo)]
    fn new_peer(info: *const InsertionInfo) -> bool {
        unsafe { &*info}.new_peer
    }

    #[roto_method(rt, InsertionInfo)]
    fn prefix_new(info: *const InsertionInfo) -> bool {
        unsafe { &*info}.prefix_new
    }


    Ok(rt)
}

fn has_attribute(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: u8,
) -> bool {
    if let Ok(mut pas) = bgp_update.path_attributes() {
        pas.any(|p| p.ok().map(|p| p.type_code() == to_match).is_some())
    } else {
        false
    }
}

fn contains_community(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: u32
) -> bool {
    if let Some(mut iter) = bgp_update.communities().ok().flatten() {
        iter.any(|c|
            Community::Standard(to_match.into()) == c
        )

    } else {
        false
    }
}

fn aspath_contains(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: Asn,
) -> bool {
    if let Some(aspath) = bgp_update.aspath().ok().flatten() {
        aspath.hops().any(|h| h == to_match.into())
    } else {
        false
    }
}

fn aspath_origin(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: Asn,
) -> bool {
    if let Some(aspath) = bgp_update.aspath().ok().flatten() {
        aspath.origin() == Some(to_match.into())
    } else {
        false
    }
}

