use std::sync::Arc;

use bytes::Bytes;
use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use routecore::bgp::aspath::{AsPath, Hop, HopPath};
use routecore::bgp::communities::{Community, LargeCommunity, StandardCommunity, Wellknown};
use routecore::bgp::message::update_builder::StandardCommunitiesList;
use routecore::bgp::message::SessionConfig;
use routecore::bgp::message::UpdateMessage as BgpUpdateMessage;
use routecore::bgp::nlri::afisafi::IsPrefix;
use routecore::bgp::path_attributes::LargeCommunitiesList;
use routecore::bmp::message::PerPeerHeader;
use routecore::bmp::message::{Message as BmpMsg, MessageType as BmpMsgType};

use roto::{Context, roto_method, roto_function};

use super::types::{
    InsertionInfo, Output, Provenance, RotoOutputStream, RouteContext,
};
use crate::payload::RotondaRoute;
use crate::roto_runtime::types::LogEntry;

pub(crate) type Log = *mut RotoOutputStream;


/// Context used for all components.
///
/// Currently, the Provenance is not stored so it is not guaranteed to be
/// available (in case of reprocessing/queries).
#[derive(Context, Clone)]
pub struct Ctx {
    pub output: Log,
}

unsafe impl Send for Ctx {}

impl Ctx {
    pub fn new(log: Log) -> Self {
        Self {
            output: log,
        }
    }
}

pub fn create_runtime() -> Result<roto::Runtime, String> {
    let mut rt = roto::Runtime::basic()?;

    // --- General types
    rt.register_clone_type_with_name::<RotondaRoute>(
        "Route",
        "A single announced or withdrawn path",
    )?;
    rt.register_clone_type_with_name::<RouteContext>(
        "RouteContext",
        "Contextual information pertaining to the Route",
    )?;
    rt.register_copy_type::<Provenance>("Session/state information")?;
    rt.register_clone_type_with_name::<Log>(
        "Log",
        "Machinery to create output entries",
    )?;

    rt.register_context_type::<Ctx>()?;

    rt.register_copy_type::<InsertionInfo>(
        "Information from the RIB on an inserted route",
    )?;

    // XXX can we get away with registering only one of these, somehow?
    rt.register_clone_type::<LogEntry>("Entry to log to file/mqtt")?;
    rt.register_clone_type_with_name::<*mut LogEntry>(
        "LogEntryPtr",
        "Entry to log to file/mqtt"
    )?;

    // --- BGP types / methods
    rt.register_clone_type_with_name::<BgpUpdateMessage<Bytes>>(
        "BgpMsg",
        "BGP UPDATE message",
    )?;

    rt.register_copy_type_with_name::<StandardCommunity>(
        "Community",
        "A BGP Standard Community (RFC1997)"
    )?;

    rt.register_copy_type_with_name::<LargeCommunity>(
        "LargeCommunity",
        "A BGP Large Community (RFC8092)"
    )?;

    #[roto_function(rt, Community)]
    fn from_u32(raw: u32) -> StandardCommunity {
        StandardCommunity::from_u32(raw)
    }

    // --- Provenance methods

    /// Return the peer ASN
    #[roto_method(rt, Provenance)]
    fn peer_asn(
        provenance: *const Provenance
    ) -> Asn {
        let provenance = unsafe { &*provenance };
        provenance.peer_asn
    }

    /// Return the formatted string for `asn`
    #[roto_method(rt, Asn, fmt)]
    fn fmt_asn(
        asn: Asn
    ) -> Arc<str> {
        asn.to_string().into()
    }

    // --- RotondaRoute methods

    /// Check whether the prefix for this `RotondaRoute` matches
    #[roto_method(rt, RotondaRoute)]
    fn prefix_matches(
        rr: *const RotondaRoute,
        to_match: *const Prefix,
    ) -> bool {
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

    /// Check whether the AS_PATH contains the given `Asn`
    #[roto_method(rt, RotondaRoute, aspath_contains)]
    fn rr_aspath_contains(rr: *const RotondaRoute, to_match: Asn) -> bool {
        let rr = unsafe { &*rr };

        if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
            hoppath.into_iter().any(|h| h == to_match.into())
        } else {
            false
        }
    }

    /// Check whether the AS_PATH origin matches the given `Asn`
    #[roto_method(rt, RotondaRoute, match_aspath_origin)]
    fn rr_match_aspath_origin(rr: *const RotondaRoute, to_match: Asn) -> bool {
        let rr = unsafe { &*rr };
        if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
            if let Some(Hop::Asn(asn)) = hoppath.origin() {
                return *asn == to_match;
            }
        }
        false
    }

    /// Check whether this `RotondaRoute` contains the given Standard Community
    #[roto_method(rt, RotondaRoute, contains_community)]
    fn rr_contains_community(
        rr: *const RotondaRoute,
        to_match: *const StandardCommunity,
    ) -> bool {
        let rr = unsafe { &*rr };
        let to_match = unsafe { &*to_match };

        if let Some(list) = rr.owned_map().get::<StandardCommunitiesList>() {
            return list.communities().iter().any(|c| c == to_match)
        }
        false
    }

    /// Check whether this `RotondaRoute` contains the given Large Community
    #[roto_method(rt, RotondaRoute, contains_large_community)]
    fn rr_contains_large_community(
        rr: *const RotondaRoute,
        to_match: *const LargeCommunity,
    ) -> bool {
        let rr = unsafe { &*rr };
        let to_match = unsafe { &*to_match };

        if let Some(list) = rr.owned_map().get::<LargeCommunitiesList>() {
            return list.communities().iter().any(|c| c == to_match)
        }
        false
    }

    /// Check whether this `RotondaRoute` contains the given Path Attribute
    #[roto_method(rt, RotondaRoute, has_attribute)]
    fn rr_has_attribute(rr: *const RotondaRoute, to_match: u8) -> bool {
        let rr = unsafe { &*rr };
        rr.owned_map()
            .iter()
            .any(|pa| pa.ok().is_some_and(|pa| pa.type_code() == to_match))
    }


    /// Return a formatted string for the AS_PATH
    #[roto_method(rt, RotondaRoute, fmt_aspath)]
    fn rr_fmt_aspath(rr: *const RotondaRoute) -> Arc<str> {
        let rr = unsafe { &*rr };
        if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
            let Ok(as_path) = hoppath.to_as_path();
            _fmt_aspath(as_path)
        } else { 
            "".into()
        }
    }

    /// Return a formatted string for the AS_PATH origin
    #[roto_method(rt, RotondaRoute, fmt_aspath_origin)]
    fn rr_fmt_aspath_origin(rr: *const RotondaRoute) -> Arc<str> {
        let rr = unsafe { &*rr };
        if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
            let Ok(as_path) = hoppath.to_as_path();
            _fmt_aspath_origin(as_path)
        } else {
            "".into()
        }
    }

    /// Return a formatted string for the Standard Communities
    #[roto_method(rt, RotondaRoute, fmt_communities)]
    fn rr_fmt_communities(rr: *const RotondaRoute) -> Arc<str> {
        let rr = unsafe { &*rr };

        if let Some(iter) = rr.owned_map().get::<StandardCommunitiesList>() {
            iter.communities().iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>().join(", ")
                .into()
        } else {
            "".into()
        }
    }

    /// Return a formatted string for the Large Communities
    #[roto_method(rt, RotondaRoute, fmt_large_communities)]
    fn rr_fmt_large_communities(rr: *const RotondaRoute) -> Arc<str> {
        let rr = unsafe { &*rr };

        if let Some(iter) = rr.owned_map().get::<LargeCommunitiesList>() {
            iter.communities().iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>().join(", ")
                .into()
        } else {
            "".into()
        }
    }


    // --- BGP message methods

    /// Check whether the AS_PATH contains the given `Asn`
    #[roto_method(rt, BgpUpdateMessage<Bytes>, aspath_contains)]
    fn bgp_aspath_contains(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        aspath_contains(msg, to_match)
    }

    /// Check whether the AS_PATH origin matches the given `Asn`
    #[roto_method(rt, BgpUpdateMessage<Bytes>, match_aspath_origin)]
    fn bgp_match_aspath_origin(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: Asn,
    ) -> bool {
        let msg = unsafe { &*msg };

        match_aspath_origin(msg, to_match)
    }

    /// Check whether this message contains the given Standard Community
    #[roto_method(rt, BgpUpdateMessage<Bytes>, contains_community)]
    fn bgp_contains_community(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: *const StandardCommunity,
    ) -> bool {
        let msg = unsafe { &*msg };
        let to_match = unsafe { &*to_match };

        contains_community(msg, to_match)
    }

    /// Check whether this message contains the given Large Community
    #[roto_method(rt, BgpUpdateMessage<Bytes>, contains_large_community)]
    fn bgp_contains_large_community(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: *const LargeCommunity,
    ) -> bool {
        let msg = unsafe { &*msg };
        let to_match = unsafe { &*to_match };

        contains_large_community(msg, to_match)
    }

    /// Check whether this message contains the given Path Attribute
    #[roto_method(rt, BgpUpdateMessage<Bytes>, has_attribute)]
    fn bgp_has_attribute(
        msg: *const BgpUpdateMessage<Bytes>,
        to_match: u8,
    ) -> bool {
        let msg = unsafe { &*msg };

        has_attribute(msg, to_match)
    }

    /// Return the number of announcements in this message
    #[roto_method(rt, BgpUpdateMessage<Bytes>, announcements_count)]
    fn bgp_announcements_count(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> u32 {
        let msg = unsafe { &*msg };
        announcements_count(msg)
    }

    /// Return the number of withdrawals in this message
    #[roto_method(rt, BgpUpdateMessage<Bytes>, withdrawals_count)]
    fn bgp_withdrawals_count(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> u32 {
        let msg = unsafe { &*msg };
        withdrawals_count(msg)
    }

    
    /// Return a formatted string for the AS_PATH
    #[roto_method(rt, BgpUpdateMessage<Bytes>, fmt_aspath)]
    fn bgp_fmt_aspath(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_aspath(msg)
    }

    /// Return a formatted string for the AS_PATH origin
    #[roto_method(rt, BgpUpdateMessage<Bytes>, fmt_aspath_origin)]
    fn bgp_fmt_aspath_origin(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_aspath_origin(msg)
    }

    /// Return a formatted string for the Standard Communities
    #[roto_method(rt, BgpUpdateMessage<Bytes>, fmt_communities)]
    fn bgp_fmt_communities(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_communities(msg)
    }

    /// Return a formatted string for the Large Communities
    #[roto_method(rt, BgpUpdateMessage<Bytes>, fmt_large_communities)]
    fn bgp_fmt_large_communities(
        msg: *const BgpUpdateMessage<Bytes>,
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_large_communities(msg)
    }

    /// Format this message as hexadecimal Wireshark input
    #[roto_method(rt, BgpUpdateMessage<Bytes>, fmt_pcap)]
    fn bgp_fmt_pcap(
        msg: *const BgpUpdateMessage<Bytes>
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_pcap(msg.as_ref())
    }

    // --- BMP types / methods
    rt.register_clone_type_with_name::<BmpMsg<Bytes>>(
        "BmpMsg",
        "BMP Message",
    )?;
    rt.register_clone_type::<PerPeerHeader<Bytes>>("BMP Per Peer Header")?;

    /// Check whether this is an iBGP message based on a given `asn`
    ///
    /// Return true if `asn` matches the asn in the `BmpMsg`.
    /// returns false if no PPH is present.
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

    /// Check whether this message is of type 'RouteMonitoring'
    #[roto_method(rt, BmpMsg<Bytes>)] 
    fn is_route_monitoring(msg: *const BmpMsg<Bytes>) -> bool {
        let msg = unsafe { &*msg };
        matches!(msg, BmpMsg::RouteMonitoring(..))
    }


    /// Check whether this message is of type 'PeerDownNotification'
    #[roto_method(rt, BmpMsg<Bytes>)]
    fn is_peer_down(msg: *const BmpMsg<Bytes>) -> bool {
        let msg = unsafe { &*msg };
        msg.msg_type() == BmpMsgType::PeerDownNotification
    }

    /// Check whether the AS_PATH contains the given `Asn`
    #[roto_method(rt, BmpMsg<Bytes>, aspath_contains)]
    fn bmp_aspath_contains(msg: *const BmpMsg<Bytes>, to_match: Asn) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error?
                return false;
            }
        } else {
            return false;
        };

        aspath_contains(&update, to_match)
    }

    /// Check whether the AS_PATH origin matches the given `Asn`
    #[roto_method(rt, BmpMsg<Bytes>, match_aspath_origin)]
    fn bmp_match_aspath_origin(
        msg: *const BmpMsg<Bytes>,
        to_match: Asn
    ) -> bool {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error?
                return false;
            }
        } else {
            return false;
        };

        match_aspath_origin(&update, to_match)
    }

    /// Check whether this message contains the given Standard Community
    #[roto_method(rt, BmpMsg<Bytes>, contains_community)]
    fn bmp_contains_community(
        msg: *const BmpMsg<Bytes>,
        to_match: *const StandardCommunity,
    ) -> bool {
        let msg = unsafe { &*msg };
        let to_match = unsafe { &*to_match };

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

    /// Check whether this message contains the given Large Community
    #[roto_method(rt, BmpMsg<Bytes>, contains_large_community)]
    fn bmp_contains_large_community(
        msg: *const BmpMsg<Bytes>,
        to_match: *const LargeCommunity,
    ) -> bool {
        let msg = unsafe { &*msg };
        let to_match = unsafe { &*to_match };

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

        contains_large_community(&update, to_match)
    }

    /// Check whether this message contains the given Path Attribute
    #[roto_method(rt, BmpMsg<Bytes>, has_attribute)]
    fn bmp_has_attribute(msg: *const BmpMsg<Bytes>, to_match: u8) -> bool {
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

    /// Return the number of announcements in this message
    #[roto_method(rt, BmpMsg<Bytes>, announcements_count)]
    fn bmp_announcements_count(
        msg: *const BmpMsg<Bytes>,
    ) -> u32 {
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                return announcements_count(&upd);
            } else {
                // log error
                return 0;
            }
        };
        0
    }

    /// Return the number of withdrawals in this message
    #[roto_method(rt, BmpMsg<Bytes>, withdrawals_count)]
    fn bmp_withdrawals_count(msg: *const BmpMsg<Bytes>) -> u32 {
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                return withdrawals_count(&upd);
            } else {
                // log error
                return 0;
            }
        };
        0
    }

    /// Return a formatted string for the AS_PATH
    #[roto_method(rt, BmpMsg<Bytes>, fmt_aspath)]
    fn bmp_fmt_aspath(msg: *const BmpMsg<Bytes>) -> Arc<str> {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return "".into();
            }
        } else {
            return "".into();
        };

        fmt_aspath(&update)
    }

    /// Return a string of the AS_PATH origin for this `BmpMsg`.
    #[roto_method(rt, BmpMsg<Bytes>, fmt_aspath_origin)]
    fn bmp_fmt_aspath_origin(msg: *const BmpMsg<Bytes>) -> Arc<str> {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return "".into();
            }
        } else {
            return "".into();
        };

        fmt_aspath_origin(&update)
    }

    /// Return a string for the Standard Communities in this `BmpMsg`.
    #[roto_method(rt, BmpMsg<Bytes>, fmt_communities)]
    fn bmp_fmt_communities(msg: *const BmpMsg<Bytes>) -> Arc<str> {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return "".into();
            }
        } else {
            return "".into();
        };

        fmt_communities(&update)
    }

    /// Return a string for the Large Communities in this `BmpMsg`.
    #[roto_method(rt, BmpMsg<Bytes>, fmt_large_communities)]
    fn bmp_fmt_large_communities(msg: *const BmpMsg<Bytes>) -> Arc<str> {
        let msg = unsafe { &*msg };

        let update = if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                upd
            } else {
                // log error
                return "".into();
            }
        } else {
            return "".into();
        };

        fmt_large_communities(&update)
    }

    /// Format this message as hexadecimal Wireshark input
    #[roto_method(rt, BmpMsg<Bytes>, fmt_pcap)]
    fn bmp_fmt_pcap(
        msg: *const BmpMsg<Bytes>
    ) -> Arc<str> {
        let msg = unsafe { &*msg };
        fmt_pcap(msg.as_ref())
    }

    // --- Output / logging / 'south'-wards artifacts methods


    /// Log the given prefix (NB: this method will likely be removed)
    #[roto_method(rt, Log)]
    fn log_prefix(stream: *mut Log, prefix: *const Prefix) {
        let stream = unsafe { &mut **stream };
        let prefix = unsafe { &*prefix };
        stream.push(Output::Prefix(*prefix));
    }

    /// Log the given ASN (NB: this method will likely be removed)
    #[roto_method(rt, Log, log_matched_asn)]
    fn log_asn(stream: *mut Log, asn: Asn) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Asn(asn));
    }

    /// Log the given ASN as origin (NB: this method will likely be removed)
    #[roto_method(rt, Log, log_matched_origin)]
    fn log_origin(stream: *mut Log, origin: Asn) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Origin(origin));
    }

    /// Log the given community (NB: this method will likely be removed)
    #[roto_method(rt, Log, log_matched_community)]
    fn log_community(stream: *mut Log, community: u32) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Community(community));
    }


    /// Log a PeerDown event
    #[roto_method(rt, Log)]
    fn log_peer_down(
        stream: *mut Log,
    ) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::PeerDown);
    }

    /// Log a custom entry in forms of a tuple (NB: this method will likely be removed)
    #[roto_method(rt, Log)]
    fn log_custom(stream: *mut Log, id: u32, local: u32) {
        let stream = unsafe { &mut **stream };
        stream.push(Output::Custom((id, local)));
    }

    /// Print a message to standard error
    #[roto_method(rt, Log)]
    fn print(stream: *mut Log, msg: *mut Arc<str>) {
        let stream = unsafe { &mut **stream };
        let msg = unsafe { &*msg };
        stream.print(msg);
    }

    //------------ LogEntry --------------------------------------------------

    /// Get the current/new entry
    ///
    /// A `LogEntry` is only written to the output if [`write_entry`] is
    /// called on it after populating its fields.
    #[roto_method(rt, Log)]
    fn entry(stream: *mut Log) -> *mut LogEntry {
        let stream = unsafe { &mut **stream };
        stream.entry() as *mut _
    } 

    /// Log a custom message based on the given string
    ///
    /// By setting a custom message for a `LogEntry`, all other fields are
    /// ignored when the entry is written to the output. Combining the custom
    /// message with the built-in fields is currently not possible.
    #[roto_method(rt, *mut LogEntry)]
    fn custom(
        entry_ptr: *mut *mut LogEntry,
        custom_msg: *const Arc<str>
    ) {
        let entry = unsafe { &mut **entry_ptr };
        let custom_msg = unsafe { &*custom_msg };
        entry.custom = Some(custom_msg.to_string());
    }

    /// Log the AS_PATH origin ASN for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn origin_as(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };

        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                if let Some(asn) = upd.aspath().ok().flatten()
                    .and_then(|asp| asp.origin())
                    .and_then(|asp| asp.try_into_asn().ok()) {
                        entry.origin_as = Some(asn);
                }
            }
        }
        unsafe {*entry_ptr}
    }

    /// Log the peer ASN for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn peer_as(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            let asn = rm.per_peer_header().asn();
            entry.peer_as = Some(asn);
        }
        unsafe {*entry_ptr}
    }

    /// Log the number of AS_PATH hops for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn as_path_hops(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>,
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                let cnt = upd.aspath().ok().flatten().map(|asp|
                    asp.hops().count()
                );
                entry.as_path_hops = cnt;
            }

        }
        unsafe {*entry_ptr}
    }

    /// Log the number of conventional announcements for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn conventional_reach(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                let cnt = upd.conventional_announcements()
                    .ok()
                    .map(|iter| iter.count())
                    .unwrap_or(0);
                entry.conventional_reach = cnt;
            }
        }
        unsafe {*entry_ptr}
    }

    /// Log the number of conventional withdrawals for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn conventional_unreach(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                let cnt = upd.conventional_withdrawals()
                    .ok()
                    .map(|iter| iter.count())
                    .unwrap_or(0);
                entry.conventional_unreach = cnt;
            }
        }
        unsafe {*entry_ptr}
    }

    /// Log the number of MultiProtocol announcements for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn mp_reach(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                if let Some(iter) = upd.mp_announcements().ok().flatten() {
                    entry.mp_reach_afisafi = Some(iter.afi_safi());
                    entry.mp_reach = Some(iter.count());
                }
            }
        }
        unsafe {*entry_ptr}
    }

    /// Log the number of MultiProtocol withdrawals for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn mp_unreach(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };
        if let BmpMsg::RouteMonitoring(rm) = msg {
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                if let Some(iter) = upd.mp_withdrawals().ok().flatten() {
                    entry.mp_unreach_afisafi = Some(iter.afi_safi());
                    entry.mp_unreach = Some(iter.count());
                }
            }
        }
        unsafe {*entry_ptr}
    }

    /// Log all the built-in features for the given message
    #[roto_method(rt, *mut LogEntry)]
    fn log_all(
        entry_ptr: *mut *mut LogEntry,
        msg: *const BmpMsg<Bytes>
    ) -> *mut LogEntry {
        let entry = unsafe { &mut **entry_ptr };
        let msg = unsafe { &*msg };

        if let BmpMsg::RouteMonitoring(rm) = msg {
            let asn = rm.per_peer_header().asn();
            entry.peer_as = Some(asn);
            if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                if let Some(asp) = upd.aspath().ok().flatten() {
                    entry.as_path_hops = Some(asp.hops().count());
                    entry.origin_as = asp.hops().last()
                        .and_then(|h| (h).try_into_asn().ok());
                }
                entry.conventional_reach = upd.conventional_announcements()
                    .ok()
                    .map(|iter| iter.count())
                    .unwrap_or(0);
                
                entry.conventional_unreach = upd.conventional_withdrawals()
                    .ok()
                    .map(|iter| iter.count())
                    .unwrap_or(0);

                if let Some(iter) = upd.mp_announcements().ok().flatten() {
                    entry.mp_reach_afisafi = Some(iter.afi_safi());
                    entry.mp_reach = Some(iter.count());
                }

                if let Some(iter) = upd.mp_withdrawals().ok().flatten() {
                    entry.mp_unreach_afisafi = Some(iter.afi_safi());
                    entry.mp_unreach = Some(iter.count());
                }
            }
        }
        unsafe {*entry_ptr}
    }


    /// Finalize this entry and ensure it will be written to the output
    ///
    /// Calling this method will close the log entry that is currently being
    /// composed, and ensures a subsequent call to [`entry`] returns a new,
    /// empty `LogEntry`.
    #[roto_method(rt, Log)]
    fn write_entry(stream: *mut Log) {
        let stream = unsafe { &mut **stream };
        let entry = stream.take_entry();
        stream.push(Output::Entry(entry));
    }


    // currently unused
    //// --- InsertionInfo methods
    //#[roto_method(rt, InsertionInfo)]
    //fn new_peer(info: *const InsertionInfo) -> bool {
    //    unsafe { &*info }.new_peer
    //}

    //#[roto_method(rt, InsertionInfo)]
    //fn prefix_new(info: *const InsertionInfo) -> bool {
    //    unsafe { &*info }.prefix_new
    //}


    //------------ Constants -------------------------------------------------

    rt.register_constant(
        "NO_EXPORT",
        "The well-known NO_EXPORT community (RFC1997)",
        StandardCommunity::from_wellknown(Wellknown::NoExport)
    )?;

    rt.register_constant(
        "NO_ADVERTISE",
        "The well-known NO_ADVERTISE community (RFC1997)",
        StandardCommunity::from_wellknown(Wellknown::NoAdvertise)
    )?;

    rt.register_constant(
        "NO_EXPORT_SUBCONFED",
        "The well-known NO_EXPORT_SUBCONFED community (RFC1997)",
        StandardCommunity::from_wellknown(Wellknown::NoExportSubconfed)
    )?;

    rt.register_constant(
        "NO_PEER",
        "The well-known NO_PEER community (RFC3765)",
        StandardCommunity::from_wellknown(Wellknown::NoPeer)
    )?;

    Ok(rt)
}

//------------ Path Attributes helpers ----------------------------------------

fn has_attribute(bgp_update: &BgpUpdateMessage<Bytes>, to_match: u8) -> bool {
    if let Ok(mut pas) = bgp_update.path_attributes() {
        pas.any(|p| p.ok().is_some_and(|p| p.type_code() == to_match))
    } else {
        false
    }
}

fn contains_community(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: &StandardCommunity,
) -> bool {
    if let Some(mut iter) = bgp_update.communities().ok().flatten() {
        iter.any(|c| c == *to_match)
    } else {
        false
    }
}

fn contains_large_community(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: &LargeCommunity,
) -> bool {
    if let Some(mut iter) = bgp_update.large_communities().ok().flatten() {
        iter.any(|c| c == *to_match)
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

fn match_aspath_origin(
    bgp_update: &BgpUpdateMessage<Bytes>,
    to_match: Asn,
) -> bool {
    if let Some(aspath) = bgp_update.aspath().ok().flatten() {
        aspath.origin() == Some(to_match.into())
    } else {
        false
    }
}

fn announcements_count(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> u32 {
    if let Ok(iter) = bgp_update.announcements() {
        iter.count().try_into().unwrap_or(u32::MAX)
    } else {
        0
    }
}

fn withdrawals_count(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> u32 {
    if let Ok(iter) = bgp_update.withdrawals() {
        iter.count().try_into().unwrap_or(u32::MAX)
    } else {
        0
    }
}

//------------ Formatting/printing helpers ------------------------------------

fn fmt_aspath(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Arc<str> {
    if let Some(aspath) = bgp_update.aspath().ok().flatten() {
        _fmt_aspath(aspath)
    } else {
        "".into()
    }
}

fn _fmt_aspath(
    aspath: AsPath<Bytes>,
) -> Arc<str> {
    if let Ok(mut asns) = aspath.try_single_sequence_iter() {
        let mut res = String::new();
        if let Some(asn) = asns.next() {
            res.push_str(&format!("{}", asn.into_u32()));
        }
        for asn in asns {
            res.push_str(&format!(" {}", asn.into_u32()));
        }
        res.into()
    } else {
        eprintln!("not single sequence");
        aspath.to_string().into()
    }
}

fn fmt_aspath_origin(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Arc<str> {
    if let Some(asp) = bgp_update.aspath().ok().flatten() {
        _fmt_aspath_origin(asp)
    } else {
        "".into()
    }
}

fn _fmt_aspath_origin(
    aspath: AsPath<Bytes>,
) -> Arc<str> {
    if let Some(asn) = aspath.origin().and_then(|a| Asn::try_from(a).ok()) {
        asn.to_string().into()
    } else {
        "".into()
    }
}


fn fmt_communities(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Arc<str> {
    if let Some(iter) = bgp_update.communities().ok().flatten() {
        iter.map(|c| c.to_string()).collect::<Vec<_>>().join(", ").into()
    } else {
        "".into()
    }
}

fn fmt_large_communities(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Arc<str> {
    if let Some(iter) = bgp_update.large_communities().ok().flatten() {
        iter.map(|c| c.to_string()).collect::<Vec<_>>().join(", ").into()
    } else {
        "".into()
    }
}


fn fmt_pcap(buf: impl AsRef<[u8]>) -> Arc<str>{
    let mut res = String::with_capacity(7 + buf.as_ref().len());
    res.push_str("000000 ");
    for b in buf.as_ref() {
        res.push_str(&format!("{:02x} ", b));
    }
    res.into()
}
