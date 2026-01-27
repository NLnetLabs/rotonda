use std::cell::RefCell;
use std::net::{IpAddr, Ipv6Addr};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use log::{debug, warn};
use routecore::bgp::aspath::{AsPath, Hop, HopPath};
use routecore::bgp::communities::{
    LargeCommunity, StandardCommunity, Wellknown,
};
use routecore::bgp::message::update_builder::StandardCommunitiesList;
use routecore::bgp::message::SessionConfig;
use routecore::bgp::message::UpdateMessage as BgpUpdateMessage;
use routecore::bgp::nlri::afisafi::IsPrefix;
use routecore::bgp::path_attributes::LargeCommunitiesList;
use routecore::bgp::types::Otc;
use routecore::bmp::message::PerPeerHeader;
use routecore::bmp::message::{Message as BmpMsg, MessageType as BmpMsgType};

use roto::{Context, Val};

use super::lists::{MutNamedAsnLists, MutNamedPrefixLists};
use super::types::{
    InsertionInfo, Output, RotoOutputStream 
};
use crate::ingress::{self, IngressId, IngressInfo};
use crate::payload::{RotondaPaMap, RotondaRoute};
use crate::roto_runtime::lists::{AsnList, PrefixList};
use crate::roto_runtime::metrics::Metrics;
use crate::roto_runtime::types::LogEntry;
use crate::units::rib_unit::rpki::{RovStatus, RovStatusUpdate, RtrCache};
use crate::units::rtr::client::VrpUpdate;


pub type CompileListsFunc = roto::TypedFunc<roto::Ctx<RotondaCtx>, fn () -> ()>;
pub const COMPILE_LISTS_FUNC_NAME: &str = "compile_lists";


pub(crate) type Log = Rc<RefCell<RotoOutputStream>>;
pub(crate) type SharedRtrCache = Arc<RtrCache>;
pub(crate) type MutRotondaRoute = Rc<RefCell<RotondaRoute>>;
pub(crate) type RcRotondaPaMap = Rc<RotondaPaMap>;
pub(crate) type MutLogEntry = Rc<RefCell<LogEntry>>;

pub type MutMetrics = Arc<RwLock<Metrics>>;
pub type MutIngressInfoCache = Rc<RefCell<IngressInfoCache>>;

impl From<RotondaRoute> for MutRotondaRoute {
    fn from(value: RotondaRoute) -> Self {
        Rc::new(RefCell::new(value))
    }
}

/// Context used for all components.
#[derive(Context, Clone)]
pub struct RotondaCtx {
    pub output: Log,
    pub rpki: SharedRtrCache,
    pub asn_lists: MutNamedAsnLists,
    pub prefix_lists: MutNamedPrefixLists,
    pub metrics: MutMetrics,
}

pub struct IngressInfoCache {
    ingress_id: IngressId,
    register: Arc<ingress::Register>,
    ingress_info: Option<IngressInfo>
}

impl IngressInfoCache {
    pub fn new_rc(ingress_id: IngressId, register: Arc<ingress::Register>) -> MutIngressInfoCache {
        Rc::new(RefCell::new(Self {
            ingress_id,
            register,
            ingress_info: None
        }))
    }
    pub fn for_info_rc(ingress_id: IngressId, register: Arc<ingress::Register>, ingress_info: IngressInfo) -> MutIngressInfoCache {
        Rc::new(RefCell::new(Self {
            ingress_id,
            register,
            ingress_info: Some(ingress_info),
        }))
    }
    fn info(&mut self) -> &IngressInfo {
        if let Some(ref info) = self.ingress_info {
            info
        } else if let Some(fresh_info) = self.register.get(self.ingress_id) {
            self.ingress_info = Some(fresh_info);
            self.ingress_info.as_ref().unwrap()
        } else {
            warn!("No ingress_info for {}, this is a bug", self.ingress_id);
            panic!();
        }
    }
    fn peer_asn(&mut self) -> Asn {
        self.info().remote_asn.unwrap_or_else(|| {
            warn!("No remote_asn on ingress {}, this is a bug", self.ingress_id);
            Asn::from_u32(u32::MAX)
        })
    }
    fn peer_address(&mut self) -> IpAddr {
        self.info().remote_addr.unwrap_or_else(|| {
            warn!("No remote_address on ingress {}, this is a bug", self.ingress_id);
            Ipv6Addr::from(0).into()
        })
    }
}


unsafe impl Send for RotondaCtx {}

impl RotondaCtx {
    pub fn new(log: Log, rpki: SharedRtrCache) -> Self {
        Self {
            output: log,
            rpki,
            asn_lists: Default::default(),
            prefix_lists: Default::default(),
            metrics: Default::default(),
        }
    }
    pub fn empty() -> Self {
        Self {
            output: RotoOutputStream::new_rced(),
            rpki: Arc::<RtrCache>::default(),
            asn_lists: Default::default(),
            prefix_lists: Default::default(),
            metrics: Default::default(),
        }
    }

    pub fn set_metrics(&mut self, metrics: MutMetrics) {
        debug!("setting metrics in Ctx");
        self.metrics = metrics;
    }

    pub fn prepare(&mut self, roto_package: &mut roto::Package<roto::Ctx<RotondaCtx>>) {
        let f: Result<CompileListsFunc, _> = roto_package
            .get_function(COMPILE_LISTS_FUNC_NAME);
        if let Ok(f) = f {
            f.call(self);
        } else {
            debug!("No {COMPILE_LISTS_FUNC_NAME} to prepare");
        }
    }
}

/// Newtype for a possibly empty `Asn`.
///
/// NB: This type might become obsolete depending on the development of
/// Optional value handling in roto.
#[derive(Copy, Clone, Debug)]
pub struct OriginAsn(pub Option<Asn>);

pub fn create_runtime() -> Result<roto::Runtime<roto::Ctx<RotondaCtx>>, String> {

    let lib = roto::library! {

        // --- General types

        /// A single announced or withdrawn path
        #[clone] type Route  = Val<MutRotondaRoute>;
        impl Val<MutRotondaRoute> {

            /// Return the prefix for this `RotondaRoute`
            fn route_prefix(rr: Val<MutRotondaRoute>) -> Prefix {
                let rr = rr.borrow_mut();
                match *rr {
                    RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
                }
            }

            /// Check whether the prefix for this `RotondaRoute` matches
            fn prefix_matches(rr: Val<MutRotondaRoute>, to_match: Prefix) -> bool {
                let rr = rr.borrow_mut();
                let rr_prefix = match *rr {
                    RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
                };
                rr_prefix == to_match
            }

            /// Check whether the AS_PATH contains the given `Asn`
            fn aspath_contains(rr: Val<MutRotondaRoute>, to_match: Asn) -> bool {
                let rr = rr.borrow_mut();

                if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
                    hoppath.into_iter().any(|h| h == to_match.into())
                } else {
                    false
                }
            }

            /// Check whether the AS_PATH origin matches the given `Asn`
            fn match_aspath_origin(
                rr: Val<MutRotondaRoute>,
                to_match: Asn,
            ) -> bool {
                let rr = rr.borrow_mut();
                if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
                    if let Some(Hop::Asn(asn)) = hoppath.origin() {
                        return *asn == to_match;
                    }
                }
                false
            }

            /// Check whether this `RotondaRoute` contains the given Standard Community
            fn contains_community(
                rr: Val<MutRotondaRoute>,
                to_match: Val<StandardCommunity>,
            ) -> bool {
                let rr = rr.borrow_mut();

                if let Some(list) = rr.owned_map().get::<StandardCommunitiesList>() {
                    return list.communities().contains(&*to_match);
                }
                false
            }

            /// Check whether this `RotondaRoute` contains the given Large Community
            fn contains_large_community(
                rr: Val<MutRotondaRoute>,
                to_match: Val<LargeCommunity>,
            ) -> bool {
                let rr = rr.borrow_mut();

                if let Some(list) = rr.owned_map().get::<LargeCommunitiesList>() {
                    return list.communities().contains(&*to_match);
                }
                false
            }

            /// Check whether this `RotondaRoute` contains the given Path Attribute
            fn has_attribute(rr: Val<MutRotondaRoute>, to_match: u8) -> bool {
                let rr = rr.borrow_mut();
                rr.owned_map()
                    .iter()
                    .any(|pa| pa.ok().is_some_and(|pa| pa.type_code() == to_match))
            }


            /// Return a formatted string for the prefix
            fn fmt_prefix(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow();
                let prefix = match *rr {
                    RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
                };
                prefix.to_string().into()
            }

            /// Return a formatted string for the ROV status
            fn fmt_rov_status(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow();
                match rr.rotonda_pamap().rpki_info().rov_status() {
                    RovStatus::NotChecked => "not-checked",
                    RovStatus::NotFound => "not-found",
                    RovStatus::Valid => "valid",
                    RovStatus::Invalid => "invalid",
                }.into()
            }

            /// Return a formatted string for the AS_PATH
            fn fmt_aspath(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow_mut();
                if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
                    let Ok(as_path) = hoppath.to_as_path();
                    _fmt_aspath(as_path)
                } else {
                    "".into()
                }
            }

            /// Return a formatted string for the AS_PATH origin
            fn fmt_aspath_origin(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow_mut();
                if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
                    let Ok(as_path) = hoppath.to_as_path();
                    _fmt_aspath_origin(as_path)
                } else {
                    "".into()
                }
            }

            /// Return a formatted string for the Standard Communities
            fn fmt_communities(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow_mut();

                if let Some(iter) = rr.owned_map().get::<StandardCommunitiesList>() {
                    iter.communities()
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                        .into()
                } else {
                    "".into()
                }
            }

            /// Return a formatted string for the Large Communities
            fn fmt_large_communities(rr: Val<MutRotondaRoute>) -> Arc<str> {
                let rr = rr.borrow_mut();

                if let Some(iter) = rr.owned_map().get::<LargeCommunitiesList>() {
                    iter.communities()
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                        .into()
                } else {
                    "".into()
                }
            }
        }

        /// The Path attributes pertaining to a certain Route
        #[clone] type PathAttributes = Val<RcRotondaPaMap>;
        impl Val<RcRotondaPaMap> {
            fn otc(pamap: Val<RcRotondaPaMap>) -> Option<Asn> {
                pamap.path_attributes().get::<Otc>().map(|a| a.0)
            }

            fn contains_community(pamap: Val<RcRotondaPaMap>, to_match: Val<StandardCommunity>) -> bool {
                if let Some(pa) = pamap.path_attributes()
                    .get::<StandardCommunitiesList>()
                {
                    pa.communities().contains(&*to_match)
                } else {
                    false
                }
            }

            fn contains_large_community(pamap: Val<RcRotondaPaMap>, to_match: Val<LargeCommunity>) -> bool {
                if let Some(pa) = pamap.path_attributes()
                    .get::<LargeCommunitiesList>()
                {
                    pa.communities().contains(&*to_match)
                } else {
                    false
                }
            }

            fn aspath(pamap: Val<RcRotondaPaMap>) -> Option<Val<HopPath>> {
                pamap.path_attributes().get::<HopPath>().map(Val)
            }

        }


        //#[clone] type Output = Val<Output>;

        /// Machinery to create output entries
        #[clone] type Log = Val<Log>;
        impl Val<Log> {
            /// Log the given prefix (NB: this method will likely be removed)
            fn log_prefix(stream: Val<Log>, prefix: Prefix) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::Prefix(prefix));
            }

            /// Log the given ASN (NB: this method will likely be removed)
            fn log_matched_asn(stream: Val<Log>, asn: Asn) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::Asn(asn));
            }

            /// Log the given ASN as origin (NB: this method will likely be removed)
            fn log_matched_origin(stream: Val<Log>, origin: Asn) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::Origin(origin));
            }

            /// Log the given community (NB: this method will likely be removed)
            fn log_matched_community(stream: Val<Log>, community: Val<StandardCommunity>) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::Community(community.to_u32()));
            }

            /// Log a PeerDown event
            fn log_peer_down(stream: Val<Log>) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::PeerDown);
            }

            /// Log a custom entry in forms of a tuple (NB: this method will likely be removed)
            fn log_custom(stream: Val<Log>, id: u32, local: u32) {
                let mut stream = stream.borrow_mut();
                stream.push(Output::Custom((id, local)));
            }

            /// Print a message to standard error
            fn print(stream: Val<Log>, msg: Arc<str>) {
                let stream = stream.borrow();
                stream.print(&*msg);
            }

            /// Print a timestamped message to standard error
            fn timestamped_print(stream: Val<Log>, msg: Arc<str>) {
                let stream = stream.borrow();
                stream.print(
                    format!("[{}] {}",
                        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                        &*msg
                    )
                );
            }

            /// Finalize this entry and ensure it will be written to the output
            ///
            /// Calling this method will close the log entry that is currently being
            /// composed, and ensures a subsequent call to [`entry`] returns a new,
            /// empty `LogEntry`.
            fn write_entry(stream: Val<Log>) {
                let mut stream = stream.borrow_mut();
                let entry = stream.take_entry();
                let entry = Rc::unwrap_or_clone(entry).into_inner();
                stream.push(Output::Entry(entry));
            }

            //------------ LogEntry --------------------------------------------------

            /// Get the current/new entry
            ///
            /// A `LogEntry` is only written to the output if [`write_entry`] is
            /// called on it after populating its fields.
            fn entry(stream: Val<Log>) -> Val<MutLogEntry> {
                let mut stream = stream.borrow_mut();
                Val(stream.entry())
            }

        }

        /// RPKI information retrieved via RTR
        #[clone] type Rpki = Val<SharedRtrCache>;
        impl Val<SharedRtrCache> {

            /// Perform Route Origin Validation on the route
            ///
            /// This sets the 'rpki_info' for this Route to Valid, Invalid or
            /// NotFound (RFC6811).
            ///
            /// In order for this method to have effect, a 'rtr-in' connector should
            /// be configured, and it should have received VRP data from the connected
            /// RP software.
            fn check_rov(rpki: Val<SharedRtrCache>, rr: Val<MutRotondaRoute>) -> Val<RovStatus> {
                let mut rr = rr.borrow_mut();
                let prefix = match *rr {
                    RotondaRoute::Ipv4Unicast(nlri, _) => nlri.prefix(),
                    RotondaRoute::Ipv6Unicast(nlri, _) => nlri.prefix(),
                    _=> { return Val(RovStatus::NotChecked) ; } // defaults to 'NotChecked'
                };

                let mut rov_status = RovStatus::default();

                if let Some(hoppath) = rr.owned_map().get::<HopPath>() {
                    if let Some(origin) = hoppath.origin()
                        .and_then(|o| Hop::try_into_asn(o.clone()).ok())
                    {
                        rov_status = rpki.check_rov(&prefix, origin);
                    }
                }

                rr.rotonda_pamap_mut().set_rpki_info(rov_status.into());
                Val(rov_status)
            }

        }

        /// A single announced or withdrawn VRP
        #[clone] type VrpUpdate = Val<VrpUpdate>;
        impl Val<VrpUpdate> {

            /// Returns the `Asn` for this `VrpUpdate`
            fn asn(vrp_update: Val<VrpUpdate>) -> Asn {
                // We need to convert the rpki-rs Asn into the inetnum Asn, hence the
                // into_u32->from_u32 calls.
                Asn::from_u32(vrp_update.vrp.asn.into_u32())
            }

            /// Returns the prefix of the updated route
            fn prefix(vrp_update: Val<VrpUpdate>) -> Prefix {
                let maxlen_pref = vrp_update.vrp.prefix;
                Prefix::new(
                    maxlen_pref.addr(),
                    maxlen_pref.prefix_len()
                ).unwrap()
            }

            /// Return a formatted string for `vrp_update`
            fn fmt(vrp_update: Val<VrpUpdate>) -> Arc<str> {
                vrp_update.to_string().into()
            }
        }

        /// Named lists of ASNs
        #[clone] type AsnLists = Val<MutNamedAsnLists>;
        impl Val<MutNamedAsnLists> {
            /// Add a named ASN list
            fn add(lists: Val<MutNamedAsnLists>, name: Arc<str>, s: Arc<str>) {
                let mut lists = lists.lock().unwrap();
                let res = AsnList::from_str(&s).unwrap_or_default();
                lists.add(name, res);
            }


            /// Returns 'true' if `asn` is in the named list
            fn contains(asn_list: Val<MutNamedAsnLists>, name: Arc<str>, asn: Asn) -> bool {
                let asn_list = asn_list.lock().unwrap();
                if let Some(list) = asn_list.inner.get(&*name.clone()) {
                    list.contains(asn)
                } else {
                    false
                }
            }

            /// Returns 'true' if the named list contains `origin`
            ///
            /// This method returns false if the list does not exist, or if `origin`
            /// does not actually contain an `Asn`. The latter could occur for
            /// announcements with an empty 'AS_PATH' attribute (iBGP).
            fn contains_origin(asn_list: Val<MutNamedAsnLists>, name: Arc<str>, origin: Val<OriginAsn>) -> bool {
                let asn = match (*origin).0 {
                    Some(asn) => asn,
                    None => { return false }
                };
                let asn_list = asn_list.lock().unwrap();
                if let Some(list) = asn_list.inner.get(&*name.clone()) {
                    list.contains(asn)
                } else {
                    false
                }
            }

        }

        /// Named lists of prefixes
        #[clone] type PrefixLists = Val<MutNamedPrefixLists>;
        impl Val<MutNamedPrefixLists> {
            /// Add a named prefix list
            fn add(lists: Val<MutNamedPrefixLists>, name: Arc<str>, s: Arc<str>) {
                let mut lists = lists.lock().unwrap();
                let res = PrefixList::from_str(&s).unwrap_or_default();
                lists.add(name, res);
            }

            /// Returns 'true' if `prefix` is in the named list
            fn contains(prefix_list: Val<MutNamedPrefixLists>, name: Arc<str>, prefix: Prefix) -> bool {
                let prefix_list = prefix_list.lock().unwrap();
                if let Some(list) = prefix_list.inner.get(&*name.clone()) {
                    list.contains(prefix)
                } else {
                    false
                }
            }

            /// Returns 'true' if `prefix` or a less-specific is in the named list 
            fn covers(prefix_list: Val<MutNamedPrefixLists>, name: Arc<str>, prefix: Prefix) -> bool {
                let prefix_list = prefix_list.lock().unwrap();
                if let Some(list) = prefix_list.inner.get(&*name.clone()) {
                    list.covers(prefix)
                } else {
                    false
                }
            }
        }

        /// User-defined Prometheus style metrics
        #[clone] type Metrics = Val<MutMetrics>;
        impl Val<MutMetrics> {
            fn increase_counter(metrics: Val<MutMetrics>, name: Arc<str>, value: u64) {
                // first try with only a read-lock (for already existing keys)
                // if that fails, try again with a write lock so the new key can get inserted.
                if value == 0 {
                    return
                }
                let updated = {
                    let readlock = metrics.read().unwrap();
                    readlock.try_inc_counter(name.clone(), value).is_ok()
                };
                if !updated {
                    metrics.write().unwrap().inc_counter(name, value);
                }
            }

            fn set_gauge(metrics: Val<MutMetrics>, name: Arc<str>, value: u64) {
                // first try with only a read-lock (for already existing keys)
                // if that fails, try again with a write lock so the new key can get inserted.
                let updated = {
                    let readlock = metrics.read().unwrap();
                    readlock.try_set_gauge(name.clone(), value).is_ok()
                };
                if !updated {
                    metrics.write().unwrap().set_gauge(name.clone(), value);
                }
            }

        }

        /// Information pertaining to the source of the Message or Route
        #[clone] type IngressInfo = Val<MutIngressInfoCache>;
        impl Val<MutIngressInfoCache> {
            fn peer_asn(iic: Val<MutIngressInfoCache>) -> Asn {
                let mut iic = iic.borrow_mut();
                iic.peer_asn()
            }

            fn peer_address(iic: Val<MutIngressInfoCache>) -> IpAddr {
                let mut iic = iic.borrow_mut();
                iic.peer_address()
            }

        }

        /// Entry to log to file/mqtt
        #[clone] type LogEntry = Val<MutLogEntry>;
        impl Val<MutLogEntry> {

            /// Log a custom message based on the given string
            ///
            /// By setting a custom message for a `LogEntry`, all other fields are
            /// ignored when the entry is written to the output. Combining the custom
            /// message with the built-in fields is currently not possible.
            fn custom(entry_ptr: Val<MutLogEntry>, custom_msg: Arc<str>) {
                let mut entry = entry_ptr.borrow_mut();
                entry.custom = Some(custom_msg.to_string());
            }

            /// Log a custom, timestamped message based on the given string
            /// 
            /// Also see [`custom`].
            fn timestamped_custom(entry_ptr: Val<MutLogEntry>, custom_msg: Arc<str>) {
                let mut entry = entry_ptr.borrow_mut();
                entry.timestamp = chrono::Utc::now();
                entry.custom = Some(custom_msg.to_string());
            }

            /// Log the AS_PATH origin ASN for the given message
            fn origin_as(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();

                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        if let Some(asn) = upd
                            .aspath()
                                .ok()
                                .flatten()
                                .and_then(|asp| asp.origin())
                                .and_then(|asp| asp.try_into_asn().ok())
                        {
                            entry.origin_as = Some(asn);
                        }
                    }
                }
                entry_ptr.clone()
            }

            /// Log the peer ASN for the given message
            fn peer_as(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    let asn = rm.per_peer_header().asn();
                    entry.peer_as = Some(asn);
                }
                entry_ptr.clone()
            }

            /// Log the number of AS_PATH hops for the given message
            fn as_path_hops(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        let cnt =
                            upd.aspath().ok().flatten().map(|asp| asp.hops().count());
                        entry.as_path_hops = cnt;
                    }
                }
                entry_ptr.clone()
            }

            /// Log the number of conventional announcements for the given message
            fn conventional_reach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        let cnt = upd
                            .conventional_announcements()
                            .ok()
                            .map(|iter| iter.count())
                            .unwrap_or(0);
                        entry.conventional_reach = cnt;
                    }
                }
                entry_ptr.clone()
            }

            /// Log the number of conventional withdrawals for the given message
            fn conventional_unreach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        let cnt = upd
                            .conventional_withdrawals()
                            .ok()
                            .map(|iter| iter.count())
                            .unwrap_or(0);
                        entry.conventional_unreach = cnt;
                    }
                }
                entry_ptr.clone()
            }

            /// Log the number of MultiProtocol announcements for the given message
            fn mp_reach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        if let Some(iter) = upd.mp_announcements().ok().flatten() {
                            entry.mp_reach_afisafi = Some(iter.afi_safi());
                            entry.mp_reach = Some(iter.count());
                        }
                    }
                }
                entry_ptr.clone()
            }

            /// Log the number of MultiProtocol withdrawals for the given message
            fn mp_unreach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        if let Some(iter) = upd.mp_withdrawals().ok().flatten() {
                            entry.mp_unreach_afisafi = Some(iter.afi_safi());
                            entry.mp_unreach = Some(iter.count());
                        }
                    }
                }
                entry_ptr.clone()
            }

            /// Log all the built-in features for the given message
            fn log_all(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.borrow_mut();

                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    let asn = rm.per_peer_header().asn();
                    entry.peer_as = Some(asn);
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        if let Some(asp) = upd.aspath().ok().flatten() {
                            entry.as_path_hops = Some(asp.hops().count());
                            entry.origin_as = asp
                                .hops()
                                .last()
                                .and_then(|h| (h).try_into_asn().ok());
                        }
                        entry.conventional_reach = upd
                            .conventional_announcements()
                            .ok()
                            .map(|iter| iter.count())
                            .unwrap_or(0);

                        entry.conventional_unreach = upd
                            .conventional_withdrawals()
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

                entry_ptr.clone()
            }
        }



        /// BGP UPDATE message
        #[clone] type BgpMsg = Val<BgpUpdateMessage<Bytes>>;
        impl Val<BgpUpdateMessage<Bytes>> {
            /// Check whether the AS_PATH contains the given `Asn`
            fn aspath_contains(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: Asn,
            ) -> bool {
                aspath_contains(&msg, to_match)
            }

            /// Returns the right-most `Asn` in the 'AS_PATH' attribute
            ///
            /// Note that the returned value is of type `OriginAsn`, which optionally
            /// contains an `Asn`. In case of empty an 'AS_PATH' (e.g. in iBGP) this
            /// method will still return an `OriginAsn`, though representing 'None'.
            fn aspath_origin(
                msg: Val<BgpUpdateMessage<Bytes>>,
            ) -> Val<OriginAsn> {
                Val(aspath_origin(&msg))
            }

            /// Check whether the AS_PATH origin matches the given `Asn`
            fn match_aspath_origin(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: Asn,
            ) -> bool {
                match_aspath_origin(&msg, to_match)
            }

            /// Check whether this message contains the given Standard Community
            fn contains_community(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: Val<StandardCommunity>,
            ) -> bool {
                contains_community(&msg, &to_match)
            }

            /// Check whether this message contains the given Large Community
            fn contains_large_community(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: Val<LargeCommunity>,
            ) -> bool {
                contains_large_community(&msg, &to_match)
            }

            /// Check whether this message contains the given Path Attribute
            fn has_attribute(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: u8,
            ) -> bool {
                has_attribute(&msg, to_match)
            }

            /// Return the number of announcements in this message
            fn announcements_count(msg: Val<BgpUpdateMessage<Bytes>>) -> u64 {
                announcements_count(&msg)
            }

            /// Return the number of withdrawals in this message
            fn withdrawals_count(msg: Val<BgpUpdateMessage<Bytes>>) -> u64 {
                withdrawals_count(&msg)
            }

            /// Return a formatted string for the AS_PATH
            fn fmt_aspath(msg: Val<BgpUpdateMessage<Bytes>>) -> Arc<str> {
                fmt_aspath(&msg)
            }

            /// Return a formatted string for the AS_PATH origin
            fn fmt_aspath_origin(
                msg: Val<BgpUpdateMessage<Bytes>>,
            ) -> Arc<str> {
                fmt_aspath_origin(&msg)
            }

            /// Return a formatted string for the Standard Communities
            fn fmt_communities(msg: Val<BgpUpdateMessage<Bytes>>) -> Arc<str> {
                fmt_communities(&msg)
            }

            /// Return a formatted string for the Large Communities
            fn fmt_large_communities(
                msg: Val<BgpUpdateMessage<Bytes>>,
            ) -> Arc<str> {
                fmt_large_communities(&msg)
            }

            /// Format this message as hexadecimal Wireshark input
            fn fmt_pcap(msg: Val<BgpUpdateMessage<Bytes>>) -> Arc<str> {
                fmt_pcap(msg.as_ref())
            }

        }

        /// BMP message
        #[clone] type BmpMsg = Val<BmpMsg<Bytes>>;
        impl Val<BmpMsg<Bytes>> {

            /// Check whether this is an iBGP message based on a given `asn`
            ///
            /// Return true if `asn` matches the asn in the `BmpMsg`.
            /// returns false if no PPH is present.
            fn is_ibgp(msg: Val<BmpMsg<Bytes>>, asn: Asn) -> bool {
                let asn_in_msg = match &*msg {
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
            fn is_route_monitoring(msg: Val<BmpMsg<Bytes>>) -> bool {
                matches!(*msg, BmpMsg::RouteMonitoring(..))
            }

            /// Check whether this message is of type 'PeerDownNotification'
            fn is_peer_down(msg: Val<BmpMsg<Bytes>>) -> bool {
                msg.msg_type() == BmpMsgType::PeerDownNotification
            }

            /// Check whether this message is of type 'PeerUpNotification'
            fn is_peer_up(msg: Val<BmpMsg<Bytes>>) -> bool {
                msg.msg_type() == BmpMsgType::PeerUpNotification
            }

            /// Check whether the AS_PATH contains the given `Asn`
            fn aspath_contains(msg: Val<BmpMsg<Bytes>>, to_match: Asn) -> bool {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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

            /// Returns the right-most `Asn` in the 'AS_PATH' attribute
            ///
            /// Note that the returned value is of type `OriginAsn`, which optionally
            /// contains an `Asn`. In case of empty an 'AS_PATH' (e.g. in iBGP) this
            /// method will still return an `OriginAsn`, though representing 'None'.
            ///
            /// When called on BMP messages not of type 'RouteMonitoring', the
            /// 'None'-variant is returned as well.
            fn aspath_origin(
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<OriginAsn> {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        upd
                    } else {
                        return Val(OriginAsn(None));
                    }
                } else {
                    return Val(OriginAsn(None));
                };

                Val(aspath_origin(&update))
            }


            /// Check whether the AS_PATH origin matches the given `Asn`
            fn match_aspath_origin(
                msg: Val<BmpMsg<Bytes>>,
                to_match: Asn,
            ) -> bool {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn contains_community(
                msg: Val<BmpMsg<Bytes>>,
                to_match: Val<StandardCommunity>,
            ) -> bool {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        upd
                    } else {
                        // log error
                        return false;
                    }
                } else {
                    return false;
                };

                contains_community(&update, &to_match)
            }

            /// Check whether this message contains the given Large Community
            fn contains_large_community(
                msg: Val<BmpMsg<Bytes>>,
                to_match: Val<LargeCommunity>,
            ) -> bool {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        upd
                    } else {
                        // log error
                        return false;
                    }
                } else {
                    return false;
                };

                contains_large_community(&update, &to_match)
            }

            /// Check whether this message contains the given Path Attribute
            fn has_attribute(msg: Val<BmpMsg<Bytes>>, to_match: u8) -> bool {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn announcements_count(msg: Val<BmpMsg<Bytes>>) -> u64 {
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn withdrawals_count(msg: Val<BmpMsg<Bytes>>) -> u64 {
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn fmt_aspath(msg: Val<BmpMsg<Bytes>>) -> Arc<str> {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn fmt_aspath_origin(msg: Val<BmpMsg<Bytes>>) -> Arc<str> {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn fmt_communities(msg: Val<BmpMsg<Bytes>>) -> Arc<str> {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn fmt_large_communities(msg: Val<BmpMsg<Bytes>>) -> Arc<str> {
                let update = if let BmpMsg::RouteMonitoring(rm) = &*msg {
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
            fn fmt_pcap(msg: Val<BmpMsg<Bytes>>) -> Arc<str> {
                fmt_pcap(msg.as_ref())
            }
        }


        /// BMP Per Peer Header
        #[clone] type PerPeerHeader = Val<PerPeerHeader<Bytes>>;

        /// AS_PATH path attribute
        #[clone] type AsPath = Val<HopPath>;
        impl Val<HopPath> {
            fn contains(hoppath: Val<HopPath>, asn: Asn) -> bool {
                hoppath.contains(&asn.into())
            }
        }


        /// Represents an optional ASN
        #[copy] type OriginAsn = Val<OriginAsn>;

        /// Information from the RIB on an inserted route
        #[copy] type InsertionInfo = Val<InsertionInfo>;

        /// A BGP Standard Community (RFC1997)
        #[copy] type Community = Val<StandardCommunity>;

        fn community_from_str(s: Arc<str>) -> Val<StandardCommunity> {
            Val(StandardCommunity::from_str(&s).unwrap_or(StandardCommunity::from_u32(0)))
        }

        /// A BGP Large Community (RFC8092)
        #[copy] type LargeCommunity = Val<LargeCommunity>;

        fn large_community_from_str(s: Arc<str>) -> Val<LargeCommunity> {
            Val(LargeCommunity::from_str(&s).unwrap_or(LargeCommunity::from([0u8;12])))
        }

        /// ROV status of a `Route`
        #[copy] type RovStatus = Val<RovStatus>;
        impl Val<RovStatus> {
            /// Returns 'true' if the status is 'Valid'
            fn is_valid(status: Val<RovStatus>) -> bool {
                *status == RovStatus::Valid
            }

            /// Returns 'true' if the status is 'Invalid'
            fn is_invalid(status: Val<RovStatus>) -> bool {
                *status == RovStatus::Invalid
            }

            /// Returns 'true' if the status is 'NotFound'
            fn is_not_found(status: Val<RovStatus>) -> bool {
                *status == RovStatus::NotFound
            }
        }

        /// ROV update of a `Route`
        #[copy] type RovStatusUpdate = Val<RovStatusUpdate>;
        impl Val<RovStatusUpdate> {

            /// Returns the prefix of the updated route
            fn prefix(rov_update: Val<RovStatusUpdate>) -> Prefix {
                rov_update.prefix
            }

            /// Returns the origin `asn` from the 'AS_PATH' of the updated route
            fn origin(rov_update: Val<RovStatusUpdate>) -> Asn {
                rov_update.origin
            }

            /// Returns the peer `asn` from which the route was received
            fn peer_asn(rov_update: Val<RovStatusUpdate>) -> Asn {
                rov_update.peer_asn
            }

            /// Returns 'true' if the new status differs from the old status
            fn has_changed(rov_update: Val<RovStatusUpdate>) -> bool {
                rov_update.previous_status != rov_update.current_status
            }

            /// Returns the old status of the route
            fn previous_status(rov_update: Val<RovStatusUpdate>) -> Val<RovStatus> {
                Val(rov_update.previous_status)
            }

            /// Returns the new status of the route
            fn current_status(rov_update: Val<RovStatusUpdate>) -> Val<RovStatus> {
                Val(rov_update.current_status)
            }

            /// Return a formatted string for `rov_update`
            fn fmt(rov_update: Val<RovStatusUpdate>) -> Arc<str> {
                format!(
                    "[{:?}] -> [{:?}] {} originated by {}, learned from {}",
                    rov_update.previous_status,
                    rov_update.current_status,
                    rov_update.prefix,
                    rov_update.origin,
                    rov_update.peer_asn,
                ).as_str().into()
            }
        }



        /// The well-known NO_EXPORT community (RFC1997)
        const NO_EXPORT: Val<StandardCommunity> = Val(StandardCommunity::from_wellknown(Wellknown::NoExport));

        /// The well-known NO_ADVERTISE community (RFC1997)
        const NO_ADVERTISE: Val<StandardCommunity> = Val(StandardCommunity::from_wellknown(Wellknown::NoAdvertise));

        /// The well-known NO_EXPORT_SUBCONFED community (RFC1997)
        const NO_EXPORT_SUBCONFED: Val<StandardCommunity> = Val(StandardCommunity::from_wellknown(Wellknown::NoExportSubconfed));

        /// The well-known NO_PEER community (RFC1997)
        const NO_PEER: Val<StandardCommunity> = Val(StandardCommunity::from_wellknown(Wellknown::NoPeer));




        // XXX do we still need these with all new string/formatting functionality in roto itself?
        fn fmt_asn(asn: Asn) -> Arc<str> {
            asn.to_string().into()
        }

        impl u32 {
            fn fmt(n: u32) -> Arc<str> {
                format!("{n}").into()
            }
        }


    };

    // --- RotondaRoute methods






    // --- BGP message methods


    // --- BMP types / methods



    // --- Output / logging / 'south'-wards artifacts methods





    //------------ RPKI / RTR methods ----------------------------------------





    //--- RovStatusUpdate



    //------------ Lists -----------------------------------------------------




    //------------ IngressInfo -----------------------------------------------


    //------------ Metrics ---------------------------------------------------


    //---------





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



    roto::Runtime::from_lib(lib)
        .map_err(|e| e.to_string())?
        .with_context_type::<RotondaCtx>()
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

fn aspath_origin(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> OriginAsn {
    OriginAsn(
        if let Some(aspath) = bgp_update.aspath().ok().flatten() {
            aspath.origin().and_then(|o| o.try_into_asn().ok())
        } else {
            None
        }
    )
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

fn announcements_count(bgp_update: &BgpUpdateMessage<Bytes>) -> u64 {
    if let Ok(iter) = bgp_update.announcements() {
        iter.count().try_into().unwrap_or(u32::MAX)
    } else {
        0
    }.into()
}

fn withdrawals_count(bgp_update: &BgpUpdateMessage<Bytes>) -> u64 {
    if let Ok(iter) = bgp_update.withdrawals() {
        let res = iter.count().try_into().unwrap_or(u32::MAX);
        if res > 0 {
            dbg!(res, bgp_update.afi_safis());
            eprintln!("{}", bgp_update.fmt_pcap_string());
        }
        res
    } else {
        0
    }.into()
}

//------------ Formatting/printing helpers ------------------------------------

fn fmt_aspath(bgp_update: &BgpUpdateMessage<Bytes>) -> Arc<str> {
    if let Some(aspath) = bgp_update.aspath().ok().flatten() {
        _fmt_aspath(aspath)
    } else {
        "".into()
    }
}

fn _fmt_aspath(aspath: AsPath<Bytes>) -> Arc<str> {
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
        aspath.to_string().into()
    }
}

fn fmt_aspath_origin(bgp_update: &BgpUpdateMessage<Bytes>) -> Arc<str> {
    if let Some(asp) = bgp_update.aspath().ok().flatten() {
        _fmt_aspath_origin(asp)
    } else {
        "".into()
    }
}

fn _fmt_aspath_origin(aspath: AsPath<Bytes>) -> Arc<str> {
    if let Some(asn) = aspath.origin().and_then(|a| Asn::try_from(a).ok()) {
        asn.to_string().into()
    } else {
        "".into()
    }
}

fn fmt_communities(bgp_update: &BgpUpdateMessage<Bytes>) -> Arc<str> {
    if let Some(iter) = bgp_update.communities().ok().flatten() {
        iter.map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ")
            .into()
    } else {
        "".into()
    }
}

fn fmt_large_communities(bgp_update: &BgpUpdateMessage<Bytes>) -> Arc<str> {
    if let Some(iter) = bgp_update.large_communities().ok().flatten() {
        iter.map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ")
            .into()
    } else {
        "".into()
    }
}

fn fmt_pcap(buf: impl AsRef<[u8]>) -> Arc<str> {
    let mut res = String::with_capacity(7 + buf.as_ref().len());
    res.push_str("000000 ");
    for b in buf.as_ref() {
        res.push_str(&format!("{:02x} ", b));
    }
    res.into()
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn packaged_roto_script() {
        use crate::units::bgp_tcp_in::unit::{
            RotoFunc as BgpInFunc,
            ROTO_FUNC_FILTER_NAME as ROTO_FUNC_BGP_IN_NAME
        };
        use crate::units::bmp_tcp_in::unit::{
            RotoFunc as BmpInFunc,
            ROTO_FUNC_FILTER_NAME as ROTO_FUNC_BMP_IN_NAME
        };
        use crate::units::rib_unit::unit::{
            RotoFuncPre as RibInPreFunc,
            ROTO_FUNC_PRE_FILTER_NAME as ROTO_FUNC_RIB_IN_PRE_NAME,
            RotoFuncVrpUpdate, ROTO_FUNC_VRP_UPDATE_FILTER_NAME,
            RotoFuncRovStatusUpdate, ROTO_FUNC_ROV_STATUS_UPDATE_NAME,
        };

        let roto_script = "etc/examples/filters.roto.example";
        let i = roto::FileTree::single_file(roto_script);
        let mut roto_package = i.compile(&create_runtime().unwrap())
            .inspect_err(|e| eprintln!("{e}"))
            .unwrap();

        let _: CompileListsFunc = roto_package.get_function(COMPILE_LISTS_FUNC_NAME).unwrap();
        let _: BgpInFunc = roto_package.get_function(ROTO_FUNC_BGP_IN_NAME).unwrap();
        let _: BmpInFunc = roto_package.get_function(ROTO_FUNC_BMP_IN_NAME).unwrap();
        let _: RibInPreFunc = roto_package.get_function(ROTO_FUNC_RIB_IN_PRE_NAME).unwrap();
        let _: RotoFuncVrpUpdate = roto_package.get_function(ROTO_FUNC_VRP_UPDATE_FILTER_NAME).unwrap();
        let _: RotoFuncRovStatusUpdate = roto_package.get_function(ROTO_FUNC_ROV_STATUS_UPDATE_NAME).unwrap();
    }
}
