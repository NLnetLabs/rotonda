use bytes::Bytes;
use inetnum::asn::Asn;
use routecore::bgp::communities::Community;
use routecore::bgp::message::SessionConfig;
use routecore::bmp::message::PerPeerHeader;
use routecore::bmp::message::{Message as BmpMsg, MessageType as BmpMsgType};
use routecore::bgp::message::{Message as BgpMsg, UpdateMessage as BgpUpdateMessage};

use crate::payload::RotondaRoute;

use super::roto_new::{FreshRouteContext, InsertionInfo, Output, OutputStream, Provenance};

pub fn rotonda_roto_runtime() -> Result<roto::Runtime, String> {
    let mut rt = roto::Runtime::basic()?;

    // --- General types
    rt.register_type_with_name::<RotondaRoute>("Route")?;
    rt.register_type_with_name::<FreshRouteContext>("RouteContext")?;
    rt.register_type::<Provenance>()?;
    rt.register_type_with_name::<OutputStream<Output>>("Log")?;
    rt.register_type::<InsertionInfo>()?;

    // --- BGP types / methods
    //rt.register_type_with_name::<BgpMsg<Bytes>>("BgpMsg")?;
    rt.register_type_with_name::<BgpUpdateMessage<Bytes>>("BgpMsg")?;
    rt.register_method::<BgpUpdateMessage<Bytes>, _, _>("aspath_contains", bgp_aspath_contains )?;
    rt.register_method::<BgpUpdateMessage<Bytes>, _, _>("aspath_origin", bgp_aspath_origin )?;
    rt.register_method::<BgpUpdateMessage<Bytes>, _, _>("contains_community", bgp_contains_community )?;

    // --- BMP types / methods
    rt.register_type_with_name::<BmpMsg<Bytes>>("BmpMsg")?;
    rt.register_type::<PerPeerHeader<Bytes>>()?;

    rt.register_method::<BmpMsg<Bytes>, _, _>("is_ibgp", is_ibgp )?;
    rt.register_method::<BmpMsg<Bytes>, _, _>("is_peer_down", is_peer_down )?;
    rt.register_method::<BmpMsg<Bytes>, _, _>("aspath_contains", bmp_aspath_contains )?;
    rt.register_method::<BmpMsg<Bytes>, _, _>("aspath_origin", bmp_aspath_origin )?;
    rt.register_method::<BmpMsg<Bytes>, _, _>("contains_community", bmp_contains_community )?;

    // --- Output / loggging / 'south'-wards artifacts methods
    rt.register_method::<OutputStream<Output>, _, _>("log_matched_asn", log_asn )?;
    rt.register_method::<OutputStream<Output>, _, _>("log_matched_origin", log_origin )?;
    rt.register_method::<OutputStream<Output>, _, _>("log_matched_community", log_community )?;
    rt.register_method::<OutputStream<Output>, _, _>("log_peer_down", log_peer_down )?;
    rt.register_method::<OutputStream<Output>, _, _>("log_custom", log_custom )?;

    // --- InsertionInfo methods
    rt.register_method::<InsertionInfo, _, _>("new_peer", new_peer )?;
    rt.register_method::<InsertionInfo, _, _>("prefix_new", prefix_new )?;


    Ok(rt)
}


// Return true if asn matches the asn in the BmpMsg.
//
// returns false if no PPH is present.
//extern "C" fn is_ibgp(msg: *const BmpMsg<Bytes>, asn: *const Asn) -> bool {
extern "C" fn is_ibgp(msg: *const BmpMsg<Bytes>, asn: Asn) -> bool {
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

extern "C" fn log_asn(
    stream: *mut OutputStream<Output>,
    asn: Asn,
) {
    let stream = unsafe { &mut *stream };
    stream.push(Output::Asn(asn));
}

extern "C" fn log_origin(
    stream: *mut OutputStream<Output>,
    origin: Asn,
) {
    let stream = unsafe { &mut *stream };
    stream.push(Output::Origin(origin));
}

extern "C" fn log_community(
    stream: *mut OutputStream<Output>,
    community: u32,
) {
    let stream = unsafe { &mut *stream };
    stream.push(Output::Community(community));
}

extern "C" fn log_peer_down(
    stream: *mut OutputStream<Output>,
    //addr: *mut IpAddr,
) {
    let stream = unsafe { &mut *stream };
    //let addr = unsafe { &mut *addr };
    stream.push(Output::PeerDown);
}

extern "C" fn is_peer_down(msg: *const BmpMsg<Bytes>) -> bool {
    let msg = unsafe { &*msg };
    msg.msg_type() == BmpMsgType::PeerDownNotification
}

extern "C" fn log_custom(
    stream: *mut OutputStream<Output>,
    id: u32,
    local: u32,
) {
    let stream = unsafe { &mut *stream };
    stream.push(Output::Custom(id, local));
}


extern "C" fn bgp_contains_community(
    msg: *const BgpUpdateMessage<Bytes>,
    to_match: u32,
) -> bool {
    let msg = unsafe { &*msg };

    contains_community(msg, to_match)
}

extern "C" fn bgp_aspath_contains(
    msg: *const BgpUpdateMessage<Bytes>,
    to_match: Asn,
) -> bool {
    let msg = unsafe { &*msg };

    aspath_contains(msg, to_match)
}

extern "C" fn bgp_aspath_origin(
    msg: *const BgpUpdateMessage<Bytes>,
    to_match: Asn,
) -> bool {
    let msg = unsafe { &*msg };

    aspath_origin(msg, to_match)
}

extern "C" fn bmp_contains_community(
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

extern "C" fn bmp_aspath_contains(
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

extern "C" fn bmp_aspath_origin(
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

extern "C" fn new_peer(info: *const InsertionInfo) -> bool {
    unsafe { &*info}.new_peer
}

extern "C" fn prefix_new(info: *const InsertionInfo) -> bool {
    unsafe { &*info}.prefix_new
}
