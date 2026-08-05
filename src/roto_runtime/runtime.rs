use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
use std::ops::RangeInclusive;
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use base64::prelude::*;
use roto::RotoString;

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use log::{debug, error, info, warn};
use routecore::bgp::aspath::{Hop, HopPath};
use routecore::bgp::communities::{
    LargeCommunity, StandardCommunity, Wellknown,
};
use routecore::bgp::message::SessionConfig;
use routecore::bgp::message::UpdateMessage as BgpUpdateMessage;
use routecore::bgp::message::update_builder::StandardCommunitiesList;
use routecore::bgp::nlri::afisafi::IsPrefix;
use routecore::bgp::path_attributes::{
    LargeCommunitiesList, OwnedPathAttributes,
};
use routecore::bgp::types::Otc;
use routecore::bmp::message::{Message as BmpMsg, MessageType as BmpMsgType};

use roto::{Context, List, Val};

use super::types::{Output, RotoOutputStream};
use crate::ingress::{self, IngressId, IngressInfo};
use crate::payload::{RotondaPaMap, RotondaRoute};
use crate::roto_runtime::metrics::MutMetrics;
use crate::roto_runtime::types::LogEntry;
use crate::units::rib_unit::rpki::{RovStatus, RovStatusUpdate, RtrCache};
use crate::units::rtr::client::VrpUpdate;

#[derive(Clone)]
pub struct Log(Arc<Mutex<RotoOutputStream>>);

impl Log {
    pub fn new() -> Self {
        Self(RotoOutputStream::new_arced())
    }
}

impl Default for Log {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Log {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check
        false
    }
}
impl std::ops::Deref for Log {
    type Target = Arc<Mutex<RotoOutputStream>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) type SharedRtrCache = Arc<RtrCache>;

#[derive(Clone)]
pub struct MutRotondaRoute(Arc<Mutex<RotondaRoute>>);
impl PartialEq for MutRotondaRoute {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check
        false
    }
}
impl MutRotondaRoute {
    //pub fn inner(&self) -> &RotondaRoute {
    //    &self.0.lock().unwrap()
    //}
    pub fn cloned_inner(&self) -> RotondaRoute {
        self.0.lock().unwrap().clone()
    }
}
impl std::ops::Deref for MutRotondaRoute {
    type Target = Arc<Mutex<RotondaRoute>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type ArcRotondaPaMap = Arc<RotondaPaMap>;
#[derive(Clone, Default)]
pub struct MutLogEntry(Arc<Mutex<LogEntry>>);

impl MutLogEntry {
    pub fn cloned_inner(&self) -> LogEntry {
        self.0.lock().unwrap().clone()
    }
}

impl std::ops::Deref for MutLogEntry {
    type Target = Arc<Mutex<LogEntry>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for MutLogEntry {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check
        false
    }
}

#[derive(Clone)]
pub struct MutIngressInfoCache(Arc<Mutex<IngressInfoCache>>);

impl std::ops::Deref for MutIngressInfoCache {
    type Target = Arc<Mutex<IngressInfoCache>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RotondaRoute> for MutRotondaRoute {
    fn from(value: RotondaRoute) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }
}

/// Context used for all components.
#[derive(Context, Clone)]
pub struct RotondaCtx {
    pub output: Val<Log>,
    pub rpki: Val<SharedRtrCache>,
    pub metrics: Val<MutMetrics>,
}
impl PartialEq for RotondaCtx {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check
        false
    }
}

pub struct IngressInfoCache {
    ingress_id: IngressId,
    register: Arc<ingress::Register>,
    ingress_info: Option<IngressInfo>,
}

impl PartialEq for MutIngressInfoCache {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check
        false
    }
}

impl IngressInfoCache {
    pub fn new_arc(
        ingress_id: IngressId,
        register: Arc<ingress::Register>,
    ) -> MutIngressInfoCache {
        MutIngressInfoCache(Arc::new(Mutex::new(Self {
            ingress_id,
            register,
            ingress_info: None,
        })))
    }
    pub fn for_info_arc(
        ingress_id: IngressId,
        register: Arc<ingress::Register>,
        ingress_info: IngressInfo,
    ) -> MutIngressInfoCache {
        MutIngressInfoCache(Arc::new(Mutex::new(Self {
            ingress_id,
            register,
            ingress_info: Some(ingress_info),
        })))
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
            warn!(
                "No remote_asn on ingress {}, this is a bug",
                self.ingress_id
            );
            Asn::from_u32(u32::MAX)
        })
    }
    fn peer_address(&mut self) -> IpAddr {
        self.info().remote_addr.unwrap_or_else(|| {
            warn!(
                "No remote_address on ingress {}, this is a bug",
                self.ingress_id
            );
            Ipv6Addr::from(0).into()
        })
    }
}

unsafe impl Send for RotondaCtx {}

impl RotondaCtx {
    pub fn new(log: Log, rpki: SharedRtrCache) -> Self {
        Self {
            output: Val(log),
            rpki: Val(rpki),
            metrics: Val(Default::default()),
        }
    }
    pub fn empty() -> Self {
        Self {
            output: Val(Log(RotoOutputStream::new_arced())),
            rpki: Val(Arc::<RtrCache>::default()),
            metrics: Val(Default::default()),
        }
    }

    pub fn set_metrics(&mut self, metrics: MutMetrics) {
        debug!("setting metrics in Ctx");
        self.metrics = Val(metrics);
    }
}

// Inclusive range of [`Asn`]s.
#[derive(Clone, Eq, PartialEq)]
pub struct AsnRange(RangeInclusive<Asn>);

impl AsnRange {
    /// Create a new **inclusive** range
    pub fn new(start: Asn, end: Asn) -> Self {
        Self(start..=end)
    }

    pub fn contains(&self, asn: &Asn) -> bool {
        self.0.contains(asn)
    }
}

pub fn create_runtime() -> Result<roto::Runtime<roto::Ctx<RotondaCtx>>, String>
{
    let lib = roto::library! {

        /// Inclusive range of [`Asn`](Asn)'s.
        #[clone] type AsnRange = Val<AsnRange>;

        // Extends the roto std lib Asn type
        impl Asn {
            /// Create an `Asn` from a [`u32`](u32).
            fn from_u32(v: u32) -> Asn {
                Asn::from_u32(v)
            }

            /// Check whether this ASN is in `list`.
            fn appears_on(asn: Asn, list: List<Val<AsnRange>>) -> bool {
                list.into_iter().any(|r| r.contains(&asn))
            }
        }

        impl Val<AsnRange> {
            /// Create a new range from `start` to `end`, inclusive.
            fn new(start: Asn, end: Asn) -> Val<AsnRange> {
                Val(AsnRange::new(start, end))
            }

            /// Create a new range for a single Asn..
            fn single(asn: Asn) -> Val<AsnRange> {
                Val(AsnRange::new(asn, asn))
            }
        }


        // Extends the roto std lib Prefix type
        impl Prefix {
            fn covered_by(prefix: Prefix, list: List<Prefix>) -> bool {
                list.into_iter().any(|p| p.covers(prefix))
            }
        }


        /// Execute an external command.
        ///
        /// The `args` list of arguments is passed to the command.
        /// See also <https://doc.rust-lang.org/std/process/struct.Command.html#method.args>.
        fn command(cmd: RotoString, args: List<RotoString>) -> bool {
            let Ok(output) = Command::new(cmd.as_ref())
                .args(args.into_iter().map(|s| s.to_string()))
                .output()
                else {
                    error!("error running command() from roto");
                    return false;
                };

            if !output.stdout.is_empty() {
                info!("roto command {cmd}: {}", String::from_utf8_lossy(&output.stdout));
            }
            if !output.stderr.is_empty() {
                warn!("roto command {cmd} stderr: {}", String::from_utf8_lossy(&output.stderr));
            }
            output.status.success()
        }


        // --- General types

        /// A single announced or withdrawn path.
        #[clone] type Route  = Val<MutRotondaRoute>;
        impl Val<MutRotondaRoute> {

            /// Return the prefix for this `RotondaRoute`.
            fn prefix(rr: Val<MutRotondaRoute>) -> Prefix {
                let rr = rr.cloned_inner();
                match rr {
                    RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
                }
            }

            /// Check whether the prefix for this `RotondaRoute` matches.
            fn prefix_matches(rr: Val<MutRotondaRoute>, to_match: Prefix) -> bool {
                let rr = rr.cloned_inner();
                let rr_prefix = match rr {
                    RotondaRoute::Ipv4Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Unicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv4Multicast(n, ..) => n.prefix(),
                    RotondaRoute::Ipv6Multicast(n, ..) => n.prefix(),
                };
                rr_prefix == to_match
            }

            /// Check whether this `RotondaRoute` contains the given Path
            /// Attribute.
            fn has_attribute(rr: Val<MutRotondaRoute>, to_match: u8) -> bool {
                let rr = rr.lock().unwrap();
                rr.owned_map()
                    .iter()
                    .any(|pa| pa.ok().is_some_and(|pa| pa.type_code() == to_match))
            }


            /// Return the RPKI [`RovStatus`] for this Route.
            fn rov_status(rr: Val<MutRotondaRoute>) -> Val<RovStatus> {
                Val(rr.lock().unwrap().rotonda_pamap().rpki_info().rov_status())
            }

            /// Return the [`AsPath`](AsPath).
            fn aspath(rr: Val<MutRotondaRoute>) -> Option<Val<HopPath>> {
                rr.lock().unwrap().owned_map().get::<HopPath>().map(Val)
            }

            /// Return a `List` of `Community`s.
            fn communities(rr: Val<MutRotondaRoute>) -> Option<List<Val<StandardCommunity>>> {
                let communities = rr.lock().unwrap().owned_map().get::<StandardCommunitiesList>()?;
                Some(communities.communities().iter().cloned().map(Val).collect())
            }

            /// Return a `List` of `LargeCommunity`s.
            fn large_communities(rr: Val<MutRotondaRoute>) -> Option<List<Val<LargeCommunity>>> {
                let communities = rr.lock().unwrap().owned_map().get::<LargeCommunitiesList>()?;
                Some(communities.communities().iter().cloned().map(Val).collect())
            }

            /// Format the NLRI and path attributes as JSON.
            fn fmt_json(rr: Val<MutRotondaRoute>) -> RotoString {
                serde_json::to_string(&rr.cloned_inner()).unwrap().into()
            }

            /// Format the path attributes as hex.
            fn fmt_hex(rr: Val<MutRotondaRoute>) -> RotoString {
                RawHex(rr.cloned_inner().rotonda_pamap().as_ref()).to_string().into()
            }

            /// Format the path attributes as base64.
            fn fmt_base64(rr: Val<MutRotondaRoute>) -> RotoString {
                BASE64_STANDARD.encode(rr.cloned_inner().rotonda_pamap().as_ref()).into()
            }

        }

        /// The Path attributes pertaining to a certain Route.
        ///
        /// Currently only used in custom HTTP endpoint `filter`s.
        #[clone] type PathAttributes = Val<ArcRotondaPaMap>;
        impl Val<ArcRotondaPaMap> {
            /// Return the OTC attribute.
            fn otc(pamap: Val<ArcRotondaPaMap>) -> Option<Asn> {
                pamap.path_attributes().get::<Otc>().map(|a| a.0)
            }

            /// Return the [`AsPath`](AsPath).
            fn aspath(pamap: Val<ArcRotondaPaMap>) -> Option<Val<HopPath>> {
                pamap.path_attributes().get::<HopPath>().map(Val)
            }

            /// Return a [`List[T]`](List) of [`Community`](Community).
            fn communities(pamap: Val<ArcRotondaPaMap>) -> Option<List<Val<StandardCommunity>>> {
                let communities = pamap.path_attributes().get::<StandardCommunitiesList>()?;
                Some(communities.communities().iter().cloned().map(Val).collect())
            }

            /// Return a [`List[T]`](List) of [`LargeCommunity`](LargeCommunity).
            fn large_communities(pamap: Val<ArcRotondaPaMap>) -> Option<List<Val<LargeCommunity>>> {
                let communities = pamap.path_attributes().get::<LargeCommunitiesList>()?;
                Some(communities.communities().iter().cloned().map(Val).collect())
            }
        }


        //#[clone] type Output = Val<Output>;

        /// Machinery to create output entries.
        #[clone] type Log = Val<Log>;
        impl Val<Log> {
            /// Log the given prefix (NB: this method will likely be removed).
            fn log_prefix(stream: Val<Log>, prefix: Prefix) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::Prefix(prefix));
            }

            /// Log the given ASN (NB: this method will likely be removed).
            fn log_matched_asn(stream: Val<Log>, asn: Asn) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::Asn(asn));
            }

            /// Log the given ASN as origin (NB: this method will likely be
            /// removed).
            fn log_matched_origin(stream: Val<Log>, origin: Asn) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::Origin(origin));
            }

            /// Log the given community (NB: this method will likely be
            /// removed).
            fn log_matched_community(stream: Val<Log>, community: Val<StandardCommunity>) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::Community(community.to_u32()));
            }

            /// Log a PeerDown event.
            fn log_peer_down(stream: Val<Log>) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::PeerDown);
            }

            /// Log a custom entry in forms of a tuple (NB: this method will
            /// likely be removed).
            fn log_custom(stream: Val<Log>, id: u32, local: u32) {
                let mut stream = stream.lock().unwrap();
                stream.push(Output::Custom((id, local)));
            }

            /// Print a message to standard error.
            fn print(stream: Val<Log>, msg: RotoString) {
                let stream = stream.lock().unwrap();
                stream.print(&*msg);
            }

            /// Print a timestamped message to standard error.
            fn timestamped_print(stream: Val<Log>, msg: RotoString) {
                let stream = stream.lock().unwrap();
                stream.print(
                    format!("[{}] {}",
                        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                        &*msg
                    )
                );
            }

            /// Finalize this entry and ensure it will be written to the
            /// output.
            ///
            /// Calling this method will close the log entry that is currently
            /// being composed, and ensures a subsequent call to [`entry`]
            /// returns a new, empty `LogEntry`.
            fn write_entry(stream: Val<Log>) {
                let mut stream = stream.lock().unwrap();
                let entry = stream.take_entry();
                let entry = entry.cloned_inner();
                stream.push(Output::Entry(entry));
            }

            //------------ LogEntry -----------------------------------------

            /// Get the current/new entry.
            ///
            /// A `LogEntry` is only written to the output if [`write_entry`]
            /// is called on it after populating its fields.
            fn entry(stream: Val<Log>) -> Val<MutLogEntry> {
                let mut stream = stream.lock().unwrap();
                Val(stream.entry())
            }

        }

        /// RPKI information retrieved via RTR.
        #[clone] type Rpki = Val<SharedRtrCache>;
        impl Val<SharedRtrCache> {

            /// Perform Route Origin Validation on the route.
            ///
            /// This sets the 'rpki_info' for this Route to Valid, Invalid or
            /// NotFound (RFC6811).
            ///
            /// In order for this method to have effect, a 'rtr-in' connector
            /// should be configured, and it should have received VRP data
            /// from the connected RP software.
            fn check_rov(rpki: Val<SharedRtrCache>, rr: Val<MutRotondaRoute>) -> Val<RovStatus> {
                let mut rr = rr.lock().unwrap();
                let prefix = match *rr {
                    RotondaRoute::Ipv4Unicast(nlri, _) => nlri.prefix(),
                    RotondaRoute::Ipv6Unicast(nlri, _) => nlri.prefix(),
                    // defaults to 'NotChecked'
                    _=> { return Val(RovStatus::NotChecked) ; }
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

        /// A single announced or withdrawn VRP.
        #[clone] type VrpUpdate = Val<VrpUpdate>;
        impl Val<VrpUpdate> {

            /// Returns the `Asn` for this `VrpUpdate`.
            fn asn(vrp_update: Val<VrpUpdate>) -> Asn {
                // We need to convert the rpki-rs Asn into the inetnum Asn,
                // hence the into_u32->from_u32 calls.
                Asn::from_u32(vrp_update.vrp.asn.into_u32())
            }

            /// Returns the prefix of the updated route.
            fn prefix(vrp_update: Val<VrpUpdate>) -> Prefix {
                let maxlen_pref = vrp_update.vrp.prefix;
                Prefix::new(
                    maxlen_pref.addr(),
                    maxlen_pref.prefix_len()
                ).unwrap()
            }

            /// Return a formatted string for `vrp_update`.
            fn to_string(vrp_update: Val<VrpUpdate>) -> RotoString {
                vrp_update.to_string().into()
            }
        }

        /// User-defined Prometheus style metrics.
        #[clone] type Metrics = Val<MutMetrics>;
        impl Val<MutMetrics> {
            /// Increase the counter for key `name` with `value`.
            fn increase_counter(metrics: Val<MutMetrics>, name: RotoString, value: u64) {
                // first try with only a read-lock (for already existing keys)
                // if that fails, try again with a write lock so the new key
                // can get inserted.
                if value == 0 {
                    return
                }
                let updated = {
                    let readlock = metrics.read().unwrap();
                    readlock.try_inc_counter(&*name, value).is_ok()
                };
                if !updated {
                    metrics.write().unwrap().inc_counter(&*name, value);
                }
            }

            /// Set the gauge for key `name` to `value`.
            fn set_gauge(metrics: Val<MutMetrics>, name: RotoString, value: u64) {
                // first try with only a read-lock (for already existing keys)
                // if that fails, try again with a write lock so the new key can get inserted.
                let updated = {
                    let readlock = metrics.read().unwrap();
                    readlock.try_set_gauge(&*name, value).is_ok()
                };
                if !updated {
                    metrics.write().unwrap().set_gauge(&*name, value);
                }
            }

        }

        /// Information pertaining to the source of the Message or Route.
        #[clone] type IngressInfo = Val<MutIngressInfoCache>;
        impl Val<MutIngressInfoCache> {
            /// Return the peer [`Asn`](Asn).
            fn peer_asn(iic: Val<MutIngressInfoCache>) -> Asn {
                let mut iic = iic.lock().unwrap();
                iic.peer_asn()
            }

            /// Return the peer [`IpAddr`](IpAddr).
            fn peer_address(iic: Val<MutIngressInfoCache>) -> IpAddr {
                let mut iic = iic.lock().unwrap();
                iic.peer_address()
            }

        }

        /// Entry to log to file/mqtt.
        #[clone] type LogEntry = Val<MutLogEntry>;
        impl Val<MutLogEntry> {

            /// Log a custom message based on the given string.
            ///
            /// By setting a custom message for a `LogEntry`, all other fields
            /// are ignored when the entry is written to the output. Combining
            /// the custom message with the built-in fields is currently not
            /// possible.
            fn custom(entry_ptr: Val<MutLogEntry>, custom_msg: RotoString) {
                let mut entry = entry_ptr.lock().unwrap();
                entry.custom = Some(custom_msg.to_string());
            }

            /// Log a custom, timestamped message based on the given string.
            ///
            /// Also see [`custom`].
            fn timestamped_custom(entry_ptr: Val<MutLogEntry>, custom_msg: RotoString) {
                let mut entry = entry_ptr.lock().unwrap();
                entry.timestamp = chrono::Utc::now();
                entry.custom = Some(custom_msg.to_string());
            }

            /// Log the AS_PATH origin ASN for the given message.
            fn origin_as(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();

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

            /// Log the peer ASN for the given message.
            fn peer_as(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    let asn = rm.per_peer_header().asn();
                    entry.peer_as = Some(asn);
                }
                entry_ptr.clone()
            }

            /// Log the number of AS_PATH hops for the given message.
            fn as_path_hops(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    if let Ok(upd) = rm.bgp_update(&SessionConfig::modern()) {
                        let cnt =
                            upd.aspath().ok().flatten().map(|asp| asp.hops().count());
                        entry.as_path_hops = cnt;
                    }
                }
                entry_ptr.clone()
            }

            /// Log the number of conventional announcements for the given
            /// message.
            fn conventional_reach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
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

            /// Log the number of conventional withdrawals for the given
            /// message.
            fn conventional_unreach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
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

            /// Log the number of MultiProtocol announcements for the given
            /// message.
            fn mp_reach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
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

            /// Log the number of MultiProtocol withdrawals for the given
            /// message.
            fn mp_unreach(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();
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

            /// Log all the built-in features for the given message.
            fn log_all(
                entry_ptr: Val<MutLogEntry>,
                msg: Val<BmpMsg<Bytes>>,
            ) -> Val<MutLogEntry> {
                let mut entry = entry_ptr.lock().unwrap();

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



        /// BGP UPDATE message.
        #[clone] type BgpMsg = Val<BgpUpdateMessage<Bytes>>;
        impl Val<BgpUpdateMessage<Bytes>> {
            /// Check whether this message contains the given Path Attribute.
            fn has_attribute(
                msg: Val<BgpUpdateMessage<Bytes>>,
                to_match: u8,
            ) -> bool {
                has_attribute(&msg, to_match)
            }

            /// Return the number of announcements in this message.
            fn announcements_count(msg: Val<BgpUpdateMessage<Bytes>>) -> u64 {
                announcements_count(&msg)
            }

            /// Return the number of withdrawals in this message.
            fn withdrawals_count(msg: Val<BgpUpdateMessage<Bytes>>) -> u64 {
                withdrawals_count(&msg)
            }

            /// Return the [`AsPath`](AsPath) in this message.
            fn aspath(msg: Val<BgpUpdateMessage<Bytes>>) -> Option<Val<HopPath>> {
                hoppath(&msg)
            }

            /// Return a [`List`](List[T]) of [`Community`](Community) in this message.
            fn communities(msg: Val<BgpUpdateMessage<Bytes>>) -> Option<List<Val<StandardCommunity>>> {
                _communities(&msg)
            }

            /// Return a [`List`](List[T]) of [`LargeCommunity`](LargeCommunity) in this message.
            fn large_communities(msg: Val<BgpUpdateMessage<Bytes>>) -> Option<List<Val<LargeCommunity>>> {
                _large_communities(&msg)
            }

            /// Format this message as hexadecimal Wireshark input.
            fn fmt_pcap(msg: Val<BgpUpdateMessage<Bytes>>) -> RotoString {
                fmt_pcap(msg.as_ref())
            }

            /// Format this message as hex.
            fn fmt_hex(msg: Val<BgpUpdateMessage<Bytes>>) -> RotoString {
                RawHex(msg.as_ref()).to_string().into()
            }

            /// Format this message as base64.
            fn fmt_base64(msg: Val<BgpUpdateMessage<Bytes>>) -> RotoString {
                BASE64_STANDARD.encode(msg.as_ref()).into()
            }

        }

        /// BMP message.
        #[clone] type BmpMsg = Val<BmpMsg<Bytes>>;
        impl Val<BmpMsg<Bytes>> {

            /// Check whether this is an iBGP message based on a given `asn`.
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


            /// Check whether this message is of type 'RouteMonitoring'.
            fn is_route_monitoring(msg: Val<BmpMsg<Bytes>>) -> bool {
                matches!(*msg, BmpMsg::RouteMonitoring(..))
            }

            /// Check whether this message is of type 'PeerDownNotification'.
            fn is_peer_down(msg: Val<BmpMsg<Bytes>>) -> bool {
                msg.msg_type() == BmpMsgType::PeerDownNotification
            }

            /// Check whether this message is of type 'PeerUpNotification'.
            fn is_peer_up(msg: Val<BmpMsg<Bytes>>) -> bool {
                msg.msg_type() == BmpMsgType::PeerUpNotification
            }

            /// Check whether this message contains the given Path Attribute.
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

            /// Return the number of announcements in this message.
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


            /// Return the number of withdrawals in this message.
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

            /// Return the [`AsPath`](AsPath).
            fn aspath(msg: Val<BmpMsg<Bytes>>) -> Option<Val<HopPath>> {
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    rm.bgp_update(&SessionConfig::modern())
                        .ok()
                        .and_then(|ref upd| hoppath(upd))
                } else {
                    None
                }
            }

            /// Return a [`List`](List[T]) of [`Community`](Community) in this message.
            fn communities(msg: Val<BmpMsg<Bytes>>) -> Option<List<Val<StandardCommunity>>> {
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    rm.bgp_update(&SessionConfig::modern())
                        .ok()
                        .and_then(|ref upd| _communities(upd))
                } else {
                    None
                }
            }

            /// Return a [`List`](List[T]) of [`LargeCommunity`](LargeCommunity) in this message.
            fn large_communities(msg: Val<BmpMsg<Bytes>>) -> Option<List<Val<LargeCommunity>>> {
                if let BmpMsg::RouteMonitoring(rm) = &*msg {
                    rm.bgp_update(&SessionConfig::modern())
                        .ok()
                        .and_then(|ref upd| _large_communities(upd))
                } else {
                    None
                }
            }

            /// Format this message as hexadecimal Wireshark input.
            fn fmt_pcap(msg: Val<BmpMsg<Bytes>>) -> RotoString {
                fmt_pcap(msg.as_ref())
            }

            /// Format this message as hex.
            fn fmt_hex(msg: Val<BgpUpdateMessage<Bytes>>) -> RotoString {
                RawHex(msg.as_ref()).to_string().into()
            }

            /// Format this message as base64.
            fn fmt_base64(msg: Val<BgpUpdateMessage<Bytes>>) -> RotoString {
                BASE64_STANDARD.encode(msg.as_ref()).into()
            }
        }


        ///// BMP Per Peer Header.
        //#[clone] type PerPeerHeader = Val<PerPeerHeader<Bytes>>;

        /// AS_PATH path attribute.
        #[clone] type AsPath = Val<HopPath>;
        impl Val<HopPath> {

            /// Return the (right-most) originator [`Asn`](Asn).
            fn origin(hoppath: Val<HopPath>) -> Option<Asn> {
                hoppath.origin().cloned()
                    .and_then(|o| Asn::try_from(o).ok())
            }

            /// Return true if `asn` occurs in this [`AsPath`](AsPath).
            fn contains(hoppath: Val<HopPath>, asn: Asn) -> bool {
                hoppath.contains(&asn.into())
            }

            /// Return a string representation.
            fn to_string(hoppath: Val<HopPath>) -> RotoString {
                hoppath.to_string().into()
            }

        }

        ///// Information from the RIB on an inserted route.
        //#[copy] type InsertionInfo = Val<InsertionInfo>;

        /// A BGP Standard Community (RFC1997).
        #[copy] type Community = Val<StandardCommunity>;

        impl Val<StandardCommunity> {
            const NO_EXPORT: Val<StandardCommunity> = Val(Wellknown::NoExport.into());
            const NO_ADVERTISE: Val<StandardCommunity> = Val(Wellknown::NoAdvertise.into());
            const NO_EXPORT_SUBCONFED: Val<StandardCommunity> = Val(Wellknown::NoExportSubconfed.into());
            const NO_PEER: Val<StandardCommunity> = Val(Wellknown::NoPeer.into());
            const BLACKHOLE: Val<StandardCommunity> = Val(Wellknown::Blackhole.into());

            // TODO get rid of unwrap_or
            /// Parse a `Community` from a string.
            fn from(s: RotoString) -> Val<StandardCommunity> {
                Val(StandardCommunity::from_str(&s)
                    .unwrap_or(StandardCommunity::from_u32(0))
                )
            }

            /// Return the string representation.
            fn to_string(c: Val<StandardCommunity>) -> RotoString {
                c.to_string().into()
            }
        }

        /// A BGP Large Community (RFC8092).
        #[copy] type LargeCommunity = Val<LargeCommunity>;
        impl Val<LargeCommunity> {
            // TODO get rid of unwrap_or
            /// Parse a `LargeCommunity` from a string.
            fn from(s: RotoString) -> Val<LargeCommunity> {
                Val(LargeCommunity::from_str(&s)
                    .unwrap_or(LargeCommunity::from([0u8;12]))
                )
            }

            /// Return the string representation.
            fn to_string(c: Val<LargeCommunity>) -> RotoString {
                c.to_string().into()
            }
        }

        /// ROV status of a `Route`.
        #[copy] type RovStatus = Val<RovStatus>;
        impl Val<RovStatus> {
            /// Returns 'true' if the status is 'Valid'.
            fn is_valid(status: Val<RovStatus>) -> bool {
                *status == RovStatus::Valid
            }

            /// Returns 'true' if the status is 'Invalid'.
            fn is_invalid(status: Val<RovStatus>) -> bool {
                *status == RovStatus::Invalid
            }

            /// Returns 'true' if the status is 'NotFound'.
            fn is_not_found(status: Val<RovStatus>) -> bool {
                *status == RovStatus::NotFound
            }

            /// Return the string representation.
            fn to_string(status: Val<RovStatus>) -> RotoString {
                status.to_string().into()
            }
        }

        /// ROV update of a `Route`.
        #[copy] type RovStatusUpdate = Val<RovStatusUpdate>;
        impl Val<RovStatusUpdate> {

            /// Returns the prefix of the updated route.
            fn prefix(rov_update: Val<RovStatusUpdate>) -> Prefix {
                rov_update.prefix
            }

            /// Returns the origin `asn` from the 'AS_PATH' of the updated
            /// route.
            fn origin(rov_update: Val<RovStatusUpdate>) -> Asn {
                rov_update.origin
            }

            /// Returns the peer `asn` from which the route was received.
            fn peer_asn(rov_update: Val<RovStatusUpdate>) -> Asn {
                rov_update.peer_asn
            }

            /// Returns 'true' if the new status differs from the old status.
            fn has_changed(rov_update: Val<RovStatusUpdate>) -> bool {
                rov_update.previous_status != rov_update.current_status
            }

            /// Returns the old status of the route.
            fn previous_status(rov_update: Val<RovStatusUpdate>) -> Val<RovStatus> {
                Val(rov_update.previous_status)
            }

            /// Returns the new status of the route.
            fn current_status(rov_update: Val<RovStatusUpdate>) -> Val<RovStatus> {
                Val(rov_update.current_status)
            }
        }
    };

    roto::Runtime::from_lib(lib)
        .map_err(|e| e.to_string())?
        .with_context_type::<RotondaCtx>()
}

//------------ Path Attributes helpers --------------------------------------

fn has_attribute(bgp_update: &BgpUpdateMessage<Bytes>, to_match: u8) -> bool {
    if let Ok(mut pas) = bgp_update.path_attributes() {
        pas.any(|p| p.ok().is_some_and(|p| p.type_code() == to_match))
    } else {
        false
    }
}

fn _communities(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Option<List<Val<StandardCommunity>>> {
    let iter = bgp_update.communities().ok().flatten()?;
    Some(iter.map(Val).collect())
}

fn _large_communities(
    bgp_update: &BgpUpdateMessage<Bytes>,
) -> Option<List<Val<LargeCommunity>>> {
    let iter = bgp_update.large_communities().ok().flatten()?;
    Some(iter.map(Val).collect())
}

fn announcements_count(bgp_update: &BgpUpdateMessage<Bytes>) -> u64 {
    if let Ok(iter) = bgp_update.announcements() {
        iter.count().try_into().unwrap_or(u32::MAX)
    } else {
        0
    }
    .into()
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
    }
    .into()
}

//------------ printing helpers ----------------------------------------------

fn hoppath(bgp_update: &BgpUpdateMessage<Bytes>) -> Option<Val<HopPath>> {
    bgp_update
        .path_attributes()
        .ok()
        .and_then(|pas| OwnedPathAttributes::from(pas).get::<HopPath>())
        .map(Val)
}

fn fmt_pcap(buf: impl AsRef<[u8]>) -> RotoString {
    let mut res = String::with_capacity(7 + buf.as_ref().len());
    res.push_str("000000 ");
    for b in buf.as_ref() {
        res.push_str(&format!("{:02x} ", b));
    }
    res.into()
}

struct RawHex<'a>(&'a [u8]);
impl fmt::Display for RawHex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for &b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn generate_documentation() {
        let runtime = create_runtime().unwrap();
        let tmpdir = tempfile::tempdir().unwrap();
        let _ = runtime.print_documentation(tmpdir.path());
    }

    #[test]
    fn packaged_roto_script() {
        use crate::units::bgp_tcp_in::unit::{
            ROTO_FUNC_FILTER_NAME as ROTO_FUNC_BGP_IN_NAME,
            RotoFunc as BgpInFunc,
        };
        use crate::units::bmp_tcp_in::unit::{
            ROTO_FUNC_FILTER_NAME as ROTO_FUNC_BMP_IN_NAME,
            RotoFunc as BmpInFunc,
        };
        use crate::units::rib_unit::unit::{
            ROTO_FUNC_PRE_FILTER_NAME as ROTO_FUNC_RIB_IN_PRE_NAME,
            ROTO_FUNC_ROV_STATUS_UPDATE_NAME,
            ROTO_FUNC_VRP_UPDATE_FILTER_NAME, RotoFuncPre as RibInPreFunc,
            RotoFuncRovStatusUpdate, RotoFuncVrpUpdate,
        };

        let roto_script = "etc/examples/filters.roto.example";
        let mut roto_package = roto::FileTree::single_file(roto_script)
            .unwrap()
            .compile(&create_runtime().unwrap())
            .inspect_err(|e| eprintln!("{e}"))
            .unwrap();

        let _: BgpInFunc =
            roto_package.get_function(ROTO_FUNC_BGP_IN_NAME).unwrap();
        let _: BmpInFunc =
            roto_package.get_function(ROTO_FUNC_BMP_IN_NAME).unwrap();
        let _: RibInPreFunc = roto_package
            .get_function(ROTO_FUNC_RIB_IN_PRE_NAME)
            .unwrap();
        let _: RotoFuncVrpUpdate = roto_package
            .get_function(ROTO_FUNC_VRP_UPDATE_FILTER_NAME)
            .unwrap();
        let _: RotoFuncRovStatusUpdate = roto_package
            .get_function(ROTO_FUNC_ROV_STATUS_UPDATE_NAME)
            .unwrap();
    }
}
