use atomic_enum::atomic_enum;
use bytes::{BufMut, Bytes, BytesMut};
use futures::FutureExt;
use log::error;
use roto::types::builtin::{BgpUpdateMessage, RawRouteWithDeltas, RotondaId, RouteStatus};

/// RFC 7854 BMP processing.
///
/// This module includes a BMP state machine and handling of cases defined in
/// RFC 7854 such as issuing withdrawals on receipt of a Peer Down
/// Notification message, and also extracts routes from Route Monitoring
/// messages.
///
/// # Known Issues
///
/// Unfortunately at present these are mixed together. Callers who want to
/// extract different properties of a Route Monitoring message or store it in
/// a different form cannot currently re-use this code.
///
/// The route extraction, storage and issuance of withdrawals should be
/// extracted from the state machine itself to higher level code that uses the
/// state machine.
///
/// Also, while some common logic has been extracted from the different state
/// handling enum variant code, some duplicate or almost duplicate code remains
/// such as the implementation of `fn get_peer_config()`. Duplicate code could
/// lead to fixes in one place and not in another which should be avoided be
/// factoring the common code out.
use routecore::{
    addr::Prefix,
    bgp::{
        message::{
            open::CapabilityType,
            update::{AddPath, FourOctetAsn},
            SessionConfig, UpdateMessage,
        },
        types::{PathAttributeType, AFI, SAFI},
    },
    bmp::message::{
        InformationTlvType, InitiationMessage, PeerDownNotification, PeerUpNotification,
        PerPeerHeader, RibType, RouteMonitoring, TerminationMessage,
    },
};
use smallvec::SmallVec;

use std::{
    collections::{hash_map::Keys, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
    panic::UnwindSafe,
    sync::Arc,
};

use crate::{
    common::{routecore_extra::generate_alternate_config, status_reporter::AnyStatusReporter},
    payload::{Payload, RouterId, Update},
};

use super::{
    metrics::BmpMetrics,
    processing::{MessageType, ProcessingResult},
    states::{
        dumping::Dumping, initiating::Initiating, terminated::Terminated, updating::Updating,
    },
    status_reporter::BmpStatusReporter,
};

//use octseq::Octets;
use routecore::Octets;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EoRProperties {
    pub afi: AFI,
    pub safi: SAFI,
    pub post_policy: bool, // post-policy if 1, or pre-policy if 0
    pub adj_rib_out: bool, // rfc8671: adj-rib-out if 1, adj-rib-in if 0
}

impl EoRProperties {
    pub fn new<T: AsRef<[u8]>>(pph: &PerPeerHeader<T>, afi: AFI, safi: SAFI) -> Self {
        EoRProperties {
            afi,
            safi,
            post_policy: pph.is_post_policy(),
            adj_rib_out: pph.adj_rib_type() == RibType::AdjRibOut,
        }
    }
}

pub struct PeerState {
    /// The settings needed to correctly parse BMP UPDATE messages sent
    /// for this peer.
    pub session_config: SessionConfig,

    /// Did the peer advertise the GracefulRestart capability in its BGP OPEN message?
    pub eor_capable: bool,

    /// The set of End-of-RIB markers that we expect to see for this peer,
    /// based on received Peer Up Notifications.
    pub pending_eors: HashSet<EoRProperties>,

    /// RFC 7854 section "4.9. Peer Down Notification" states:
    ///     "A Peer Down message implicitly withdraws all routes that were
    ///      associated with the peer in question.  A BMP implementation MAY
    ///      omit sending explicit withdraws for such routes."
    ///
    /// RFC 4271 section "4.3. UPDATE Message Format" states:
    ///     "An UPDATE message can list multiple routes that are to be withdrawn
    ///      from service.  Each such route is identified by its destination
    ///      (expressed as an IP prefix), which unambiguously identifies the route
    ///      in the context of the BGP speaker - BGP speaker connection to which
    ///      it has been previously advertised.
    ///      
    ///      An UPDATE message might advertise only routes that are to be
    ///      withdrawn from service, in which case the message will not include
    ///      path attributes or Network Layer Reachability Information."
    ///
    /// So, we need to generate synthetic withdrawals for the routes announced by a peer when that peer goes down, and
    /// the only information needed to announce a withdrawal is the peer identity (represented by the PerPeerHeader and
    /// the prefix that is no longer routed to. We only need to keep the set of announced prefixes here as PeerStates
    /// stores the PerPeerHeader.
    pub announced_prefixes: HashSet<Prefix>,
}

impl std::fmt::Debug for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerState")
            .field("session_config", &self.session_config)
            .field("pending_eors", &self.pending_eors)
            .finish()
    }
}

/// RFC 7854 BMP state machine.
///
/// Allowed transitions:
///
/// ```text
/// Initiating -> Dumping -> Updating -> Terminated
///                │                        ▲
///                └────────────────────────┘
/// ```
///
/// See: <https://datatracker.ietf.org/doc/html/rfc7854#section-3.3>
#[derive(Debug)]
pub enum BmpState {
    Initiating(BmpStateDetails<Initiating>),
    Dumping(BmpStateDetails<Dumping>),
    Updating(BmpStateDetails<Updating>),
    Terminated(BmpStateDetails<Terminated>),
    Aborted(SocketAddr, Arc<String>),
}

// Rust enums with fields cannot have custom discriminant values assigned to them so we have to use separate
// constants or another enum instead, or use something like https://crates.io/crates/discrim. See also:
//   - https://internals.rust-lang.org/t/pre-rfc-enum-from-integer/6348/23
//   - https://github.com/rust-lang/rust/issues/60553
#[atomic_enum]
#[derive(Default, PartialEq, Eq, Hash)]
pub enum BmpStateIdx {
    #[default]
    Initiating = 0,
    Dumping = 1,
    Updating = 2,
    Terminated = 3,
    Aborted = 4,
}

impl Default for AtomicBmpStateIdx {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl std::fmt::Display for BmpStateIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BmpStateIdx::Initiating => write!(f, "Initiating"),
            BmpStateIdx::Dumping => write!(f, "Dumping"),
            BmpStateIdx::Updating => write!(f, "Updating"),
            BmpStateIdx::Terminated => write!(f, "Terminated"),
            BmpStateIdx::Aborted => write!(f, "Aborted"),
        }
    }
}

#[derive(Debug)]
pub struct BmpStateDetails<T>
where
    BmpState: From<BmpStateDetails<T>>,
{
    pub addr: SocketAddr,
    pub router_id: Arc<String>,
    pub status_reporter: Arc<BmpStatusReporter>,
    pub details: T,
}

impl BmpState {
    pub fn addr(&self) -> SocketAddr {
        match self {
            BmpState::Initiating(v) => v.addr,
            BmpState::Dumping(v) => v.addr,
            BmpState::Updating(v) => v.addr,
            BmpState::Terminated(v) => v.addr,
            BmpState::Aborted(addr, _) => *addr,
        }
    }

    pub fn router_id(&self) -> Arc<String> {
        match self {
            BmpState::Initiating(v) => v.router_id.clone(),
            BmpState::Dumping(v) => v.router_id.clone(),
            BmpState::Updating(v) => v.router_id.clone(),
            BmpState::Terminated(v) => v.router_id.clone(),
            BmpState::Aborted(_, router_id) => router_id.clone(),
        }
    }

    pub fn state_idx(&self) -> BmpStateIdx {
        match self {
            BmpState::Initiating(_) => BmpStateIdx::Initiating,
            BmpState::Dumping(_) => BmpStateIdx::Dumping,
            BmpState::Updating(_) => BmpStateIdx::Updating,
            BmpState::Terminated(_) => BmpStateIdx::Terminated,
            BmpState::Aborted(_, _) => BmpStateIdx::Aborted,
        }
    }

    pub fn status_reporter(&self) -> Option<Arc<BmpStatusReporter>> {
        match self {
            BmpState::Initiating(v) => Some(v.status_reporter.clone()),
            BmpState::Dumping(v) => Some(v.status_reporter.clone()),
            BmpState::Updating(v) => Some(v.status_reporter.clone()),
            BmpState::Terminated(v) => Some(v.status_reporter.clone()),
            BmpState::Aborted(_, _) => None,
        }
    }

    #[rustfmt::skip]
    pub async fn terminate(self) -> ProcessingResult {
        match self {
            BmpState::Initiating(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None),
            BmpState::Dumping(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None).await,
            BmpState::Updating(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None).await,
            BmpState::Terminated(_) => BmpStateDetails::<Terminated>::mk_state_transition_result(self),
            BmpState::Aborted(..) => BmpStateDetails::<Terminated>::mk_state_transition_result(self),
        }
    }
}

impl<T> BmpStateDetails<T>
where
    BmpState: From<BmpStateDetails<T>>,
{
    pub fn mk_invalid_message_result<U: Into<String>>(
        self,
        err: U,
        known_peer: Option<bool>,
        msg_bytes: Option<Bytes>,
    ) -> ProcessingResult {
        ProcessingResult::new(
            MessageType::InvalidMessage {
                err: err.into(),
                known_peer,
                msg_bytes,
            },
            self.into(),
        )
    }

    pub fn mk_other_result(self) -> ProcessingResult {
        ProcessingResult::new(MessageType::Other, self.into())
    }

    pub fn mk_routing_update_result(self, update: Update) -> ProcessingResult {
        ProcessingResult::new(MessageType::RoutingUpdate { update }, self.into())
    }

    pub fn mk_final_routing_update_result(
        next_state: BmpState,
        update: Update,
    ) -> ProcessingResult {
        ProcessingResult::new(MessageType::RoutingUpdate { update }, next_state)
    }

    pub fn mk_state_transition_result(next_state: BmpState) -> ProcessingResult {
        ProcessingResult::new(MessageType::StateTransition, next_state)
    }
}

pub trait Initiable {
    /// Set the initiating's sys name.
    fn set_information_tlvs(&mut self, sys_name: String, sys_desc: String, sys_extra: Vec<String>);

    fn sys_name(&self) -> Option<&str>;
}

impl<T> BmpStateDetails<T>
where
    T: Initiable,
    BmpState: From<BmpStateDetails<T>>,
{
    pub async fn initiate<Octs: Octets>(
        mut self,
        msg: InitiationMessage<Octs>,
    ) -> ProcessingResult {
        // https://datatracker.ietf.org/doc/html/rfc7854#section-4.3
        //    "The initiation message consists of the common
        //     BMP header followed by two or more Information
        //     TLVs (Section 4.4) containing information about
        //     the monitored router.  The sysDescr and sysName
        //     Information TLVs MUST be sent, any others are
        //     optional."
        let sys_name = msg
            .information_tlvs()
            .filter(|tlv| tlv.typ() == InformationTlvType::SysName)
            .map(|tlv| String::from_utf8_lossy(tlv.value()).into_owned())
            .collect::<Vec<_>>()
            .join("|");

        if sys_name.is_empty() {
            self.mk_invalid_message_result(
                "Invalid BMP InitiationMessage: Missing or empty sysName Information TLV",
                None,
                Some(Bytes::copy_from_slice(msg.as_ref())),
            )
        } else {
            let sys_desc = msg
                .information_tlvs()
                .filter(|tlv| tlv.typ() == InformationTlvType::SysDesc)
                .map(|tlv| String::from_utf8_lossy(tlv.value()).into_owned())
                .collect::<Vec<_>>()
                .join("|");

            let extra = msg
                .information_tlvs()
                .filter(|tlv| tlv.typ() == InformationTlvType::String)
                .map(|tlv| String::from_utf8_lossy(tlv.value()).into_owned())
                .collect::<Vec<_>>();

            self.details.set_information_tlvs(sys_name, sys_desc, extra);
            self.mk_other_result()
        }
    }
}

pub trait PeerAware {
    /// Remember this peer and the configuration we will need to use later to
    /// correctly parse and interpret subsequent messages for this peer. EOR
    /// is an abbreviation of End-of-RIB [1].
    ///
    /// Returns true if the configuration was recorded, false if configuration
    /// for the peer already exists.
    ///
    /// [1]: https://datatracker.ietf.org/doc/html/rfc4724#section-2
    fn add_peer_config(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        config: SessionConfig,
        eor_capable: bool,
    ) -> bool;

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState>;

    fn update_peer_config(&mut self, pph: &PerPeerHeader<Bytes>, config: SessionConfig) -> bool;

    /// Get a reference to a previously inserted configuration.
    fn get_peer_config(&self, pph: &PerPeerHeader<Bytes>) -> Option<&SessionConfig>;

    /// Remove previously recorded peer configuration.
    ///
    /// Returns true if the configuration was removed, false if configuration
    /// for the peer does not exist.
    fn remove_peer_config(&mut self, pph: &PerPeerHeader<Bytes>) -> bool;

    fn num_peer_configs(&self) -> usize;

    fn is_peer_eor_capable(&self, pph: &PerPeerHeader<Bytes>) -> Option<bool>;

    fn add_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI);

    /// Remove previously recorded pending End-of-RIB note for a peer.
    ///
    /// Returns true if the configuration removed was the last one, i.e. this
    /// is the end of the initial table dump, false otherwise.
    fn remove_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI) -> bool;

    fn num_pending_eors(&self) -> usize;

    fn add_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: Prefix) -> bool;

    fn remove_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: &Prefix);

    fn get_announced_prefixes(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<std::collections::hash_set::Iter<Prefix>>;
}

impl<T> BmpStateDetails<T>
where
    T: PeerAware,
    BmpState: From<BmpStateDetails<T>>,
{
    pub async fn peer_up(mut self, msg: PeerUpNotification<Bytes>) -> ProcessingResult {
        let pph = msg.per_peer_header();
        let config = msg.session_config();

        // Will this peer send End-of-RIB?
        let eor_capable = msg
            .bgp_open_rcvd()
            .capabilities()
            .any(|cap| cap.typ() == CapabilityType::GracefulRestart);

        if !self.details.add_peer_config(pph, config, eor_capable) {
            // This is unexpected. How can we already have an entry in
            // the map for a peer which is currently up (i.e. we have
            // already seen a PeerUpNotification for the peer but have
            // not seen a PeerDownNotification for the same peer)?
            return self.mk_invalid_message_result(
                format!(
                    "PeerUpNotification received for peer that is already 'up': {}",
                    msg.per_peer_header()
                ),
                Some(true),
                Some(Bytes::copy_from_slice(msg.as_ref())),
            );
        }

        // TODO: pass the peer up message to the status reporter so that it can log/count/capture anything of interest
        // and not just what we pass it here, e.g. what information TLVs were sent with the peer up, which capabilities
        // did the peer announce support for, etc?
        self.status_reporter
            .peer_up(self.router_id.clone(), eor_capable);

        self.mk_other_result()
    }

    pub async fn peer_down(mut self, msg: PeerDownNotification<Bytes>) -> ProcessingResult {
        // Compatibility Note: RFC-7854 doesn't seem to indicate that a Peer
        // Down Notification has a Per Peer Header, but if it doesn't how can
        // we know which peer of the remote has gone down? Also, we see the
        // Per Peer Header attached to Peer Down Notification BMP messages in
        // packet captures so it seems that it is indeed sent.
        let pph = msg.per_peer_header();

        let routes = self.do_peer_down(&pph);

        if !self.details.remove_peer_config(&pph) {
            // This is unexpected, we should have had configuration
            // stored for this peer but apparently don't. Did we
            // receive a Peer Down Notification without a
            // corresponding prior Peer Up Notification for the same
            // peer?
            return self.mk_invalid_message_result(
                format!("PeerDownNotification received for peer that was not 'up'",),
                Some(false),
                // TODO: Silly to_copy the bytes, but PDN won't give us the octets back..
                Some(Bytes::copy_from_slice(msg.as_ref())),
            );
        } else if routes.is_empty() {
            self.mk_other_result()
        } else {
            self.mk_routing_update_result(Update::Bulk(routes))
        }
    }

    pub fn do_peer_down(&mut self, pph: &PerPeerHeader<Bytes>) -> SmallVec<[Payload; 8]> {
        let eor_capable = self.details.is_peer_eor_capable(pph);

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

        self.status_reporter
            .peer_down(self.router_id.clone(), eor_capable);

        let mut routes = SmallVec::new();
        if let Some(prefixes_to_withdraw) = self.details.get_announced_prefixes(pph) {
            if prefixes_to_withdraw.clone().peekable().next().is_some() {
                match mk_bgp_update(prefixes_to_withdraw.clone()) {
                    Ok(bgp_update) => {
                        match UpdateMessage::from_octets(
                            bgp_update.clone(),
                            SessionConfig::modern(),
                        ) {
                            Ok(update) => {
                                for prefix in prefixes_to_withdraw {
                                    let route = Self::mk_route_for_prefix(
                                        self.router_id.clone(),
                                        update.clone(),
                                        &pph,
                                        *prefix,
                                        RouteStatus::Withdrawn,
                                    )
                                    .into();
                                    routes.push(route);
                                }
                            }

                            Err(err) => {
                                let mut pcap_text = "000000 ".to_string();
                                for b in bgp_update.as_ref() {
                                    pcap_text.push_str(&format!("{:02x} ", b));
                                }
                                error!("Internal error: Failed to issue internal BGP UPDATE to withdraw routes for a down peer. Reason: BGP UPDATE encoding error: {err}. PCAP TEXT: {pcap_text}");
                            }
                        }
                    }

                    Err(err) => {
                        error!("Internal error: Failed to issue internal BGP UPDATE to withdraw routes for a down peer. Reason: BGP UPDATE construction error: {err}");
                    }
                }
            }
        }

        routes
    }

    /// `filter` should return `None` if the BGP message should be ignored, i.e. be filtered out, otherwise `Some(msg)`
    /// where `msg` is either the original unmodified `msg` or a modified or completely new message.
    pub async fn route_monitoring<CB, F, D>(
        mut self,
        msg: RouteMonitoring<Bytes>,
        route_status: RouteStatus,
        filter_data: Option<D>,
        filter_map: F,
        do_state_specific_pre_processing: CB,
    ) -> ProcessingResult
    where
        CB: Fn(
            BmpStateDetails<T>,
            &PerPeerHeader<Bytes>,
            &UpdateMessage<Bytes>,
        ) -> ControlFlow<ProcessingResult, Self>,
        F: Fn(BgpUpdateMessage, Option<D>) -> Result<ControlFlow<(), BgpUpdateMessage>, String>,
    {
        let mut tried_peer_configs = SmallVec::<[SessionConfig; 4]>::new();

        self.status_reporter
            .bmp_update_message_processed(self.router_id.clone());

        let pph = msg.per_peer_header();
        let (known_peer, chosen_peer_config) = self
            .details
            .get_peer_config(&pph)
            .map(|v| (true, v))
            .unwrap_or_else(|| {
                self.status_reporter.peer_unknown(self.router_id.clone());
                // TODO: use SessionConfig::modern() when it is const
                static FALLBACK_CONFIG: SessionConfig = SessionConfig {
                    four_octet_asn: FourOctetAsn::Enabled,
                    add_path: AddPath::Disabled,
                };
                (false, &FALLBACK_CONFIG)
            });

        let mut chosen_peer_config = *chosen_peer_config;
        let mut retry_due_to_err: Option<String> = None;
        loop {
            let res = match msg.bgp_update(chosen_peer_config) {
                Ok(update) => {
                    if let Some(err_str) = retry_due_to_err {
                        self.status_reporter.bgp_update_parse_soft_fail(
                            self.router_id.clone(),
                            Some(known_peer),
                            err_str,
                            Some(Bytes::copy_from_slice(msg.as_ref())),
                        );

                        // use this config in future (#128)
                        self.details.update_peer_config(&pph, chosen_peer_config);
                    }

                    let mut saved_self = match do_state_specific_pre_processing(self, &pph, &update)
                    {
                        ControlFlow::Break(res) => return res,
                        ControlFlow::Continue(saved_self) => saved_self,
                    };

                    let update_msg = roto::types::builtin::UpdateMessage(update); // clone is cheap here
                    let delta_id = (RotondaId(0), 0); // TODO
                    let roto_bgp_msg = BgpUpdateMessage::new(delta_id, update_msg);
                    let update = match filter_map(roto_bgp_msg, filter_data) {
                        Ok(ControlFlow::Continue(new_or_modified_msg)) => {
                            new_or_modified_msg.raw_message().0.clone()
                        }
                        _ => return saved_self.mk_other_result(),
                    };

                    let (routes, n_new_prefixes, n_announcements, n_withdrawals) = saved_self
                        .extract_route_monitoring_routes(pph.clone(), &update, route_status);

                    if n_announcements > 0
                        && saved_self.details.is_peer_eor_capable(&pph) == Some(true)
                    {
                        let nlris = update.nlris();
                        saved_self
                            .details
                            .add_pending_eor(&pph, nlris.afi(), nlris.safi());
                    }

                    saved_self.status_reporter.routing_update(
                        saved_self.router_id.clone(),
                        n_new_prefixes,
                        n_announcements,
                        n_withdrawals,
                        0, //self.details.get_stored_routes().prefixes_len(), // TODO
                    );

                    saved_self.mk_routing_update_result(Update::Bulk(routes))
                }

                Err(err) => {
                    tried_peer_configs.push(chosen_peer_config);
                    if let Some(alt_config) = generate_alternate_config(&chosen_peer_config) {
                        if !tried_peer_configs.contains(&alt_config) {
                            chosen_peer_config = alt_config;
                            if retry_due_to_err.is_none() {
                                retry_due_to_err = Some(err.to_string());
                            }
                            continue;
                        }
                    }

                    self.mk_invalid_message_result(
                        format!("Invalid BMP RouteMonitoring BGP UPDATE message: {}", err),
                        Some(known_peer),
                        Some(Bytes::copy_from_slice(msg.as_ref())),
                    )
                }
            };

            break res;
        }
    }

    pub fn extract_route_monitoring_routes<R>(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        update: &UpdateMessage<Bytes>,
        route_status: RouteStatus,
    ) -> (SmallVec<[R; 8]>, usize, usize, usize)
    where
        R: From<RawRouteWithDeltas>,
    {
        let mut num_new_prefixes = 0;
        let num_announcements: usize = update.nlris().iter().count();
        let mut num_withdrawals: usize = update.withdrawn_routes_len().into();
        let mut routes = SmallVec::with_capacity(num_announcements + num_withdrawals);

        let nlris = &update.nlris();

        for nlri in nlris.iter() {
            if let Some(prefix) = nlri.prefix() {
                if self.details.add_announced_prefix(&pph, prefix) {
                    num_new_prefixes += 1;
                }

                // clone is cheap due to use of Bytes
                routes.push(
                    Self::mk_route_for_prefix(
                        self.router_id.clone(),
                        update.clone(),
                        &pph,
                        prefix,
                        route_status,
                    )
                    .into(),
                );
            }
        }

        for nlri in update.withdrawals().iter() {
            if let Some(prefix) = nlri.prefix() {
                // RFC 4271 section "4.3 UPDATE Message Format" states:
                //
                // "An UPDATE message SHOULD NOT include the same address prefix in the
                //  WITHDRAWN ROUTES and Network Layer Reachability Information fields.
                //  However, a BGP speaker MUST be able to process UPDATE messages in
                //  this form.  A BGP speaker SHOULD treat an UPDATE message of this form
                //  as though the WITHDRAWN ROUTES do not contain the address prefix.
                if nlris.iter().all(|nlri| nlri.prefix() != Some(prefix)) {
                    // clone is cheap due to use of Bytes
                    routes.push(
                        Self::mk_route_for_prefix(
                            self.router_id.clone(),
                            update.clone(),
                            &pph,
                            prefix,
                            RouteStatus::Withdrawn,
                        )
                        .into(),
                    );

                    self.details.remove_announced_prefix(&pph, &prefix);
                } else {
                    num_withdrawals -= 1;
                }
            }
        }

        (routes, num_new_prefixes, num_announcements, num_withdrawals)
    }

    fn mk_route_for_prefix(
        router_id: Arc<String>,
        update: UpdateMessage<Bytes>,
        pph: &PerPeerHeader<Bytes>,
        prefix: Prefix,
        route_status: RouteStatus,
    ) -> RawRouteWithDeltas {
        let delta_id = (RotondaId(0), 0); // TODO
        let roto_update_msg = roto::types::builtin::UpdateMessage(update);
        let raw_msg = Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
        RawRouteWithDeltas::new_with_message_ref(delta_id, prefix.into(), &raw_msg, route_status)
            .with_peer_ip(pph.address())
            .with_peer_asn(pph.asn())
            .with_router_id(router_id)
    }
}

impl BmpState {
    pub fn new<T: AnyStatusReporter>(
        addr: SocketAddr,
        router_id: Arc<RouterId>,
        parent_status_reporter: Arc<T>,
        metrics: Arc<BmpMetrics>,
    ) -> Self {
        let child_name = parent_status_reporter.link_names("bmp_state");
        let status_reporter = Arc::new(BmpStatusReporter::new(child_name, metrics));

        BmpState::Initiating(BmpStateDetails::<Initiating>::new(
            addr,
            router_id,
            status_reporter,
        ))
    }

    #[allow(dead_code)]
    pub async fn process_msg(self, msg_buf: Bytes) -> ProcessingResult {
        self.process_msg_with_filter(msg_buf, None::<()>, |msg, _| Ok(ControlFlow::Continue(msg)))
            .await
    }

    /// `filter` should return true if the BGP message should be ignored, i.e. be filtered out.
    pub async fn process_msg_with_filter<F, D>(
        self,
        msg_buf: Bytes,
        filter_data: Option<D>,
        filter: F,
    ) -> ProcessingResult
    where
        D: UnwindSafe,
        F: Fn(BgpUpdateMessage, Option<D>) -> Result<ControlFlow<(), BgpUpdateMessage>, String>
            + UnwindSafe,
    {
        let saved_addr = self.addr();
        let saved_router_id = self.router_id().clone();
        let saved_state_idx = self.state_idx();

        // Attempt to prevent routecore BMP/BGP parsing code panics blowing up
        // process. However, there are two issues with this:
        //   1. We don't keep a copy of the BMP message and we pass ownership
        //      of it to the code that can panic, so we lose the ability on
        //      catching the panic to say which bytes couldn't be processed.
        //      This could be improved by moving the panic catching closer to
        //      the invocation of the routecore code, but ideally routecore
        //      shouldn't panic on parsing failure anyway.
        //   2. I don't know if the panic blows up an entire process thread.
        //      If that happens it will also presumably kill any tokio async
        //      task management happening in that thread and thus we don't
        //      know for sure what state we are in, even though we caught the
        //      panic. This could be worked around by running the logic below
        //      in a tokio worker thread instead of on its core threads.
        //   3. If someone, with good intentions, adds 'panic = "abort"' to
        //      Cargo.toml we will no longer be able to catch this panic and
        //      this defensive logic will become useless.
        #[rustfmt::skip]
        let may_panic = async {
            match self {
                BmpState::Initiating(inner) => inner.process_msg_with_filter(msg_buf, filter_data, filter).await,
                BmpState::Dumping(inner) => inner.process_msg_with_filter(msg_buf, filter_data, filter).await,
                BmpState::Updating(inner) => inner.process_msg_with_filter(msg_buf, filter_data, filter).await,
                BmpState::Terminated(inner) => inner.process_msg_with_filter(msg_buf, filter_data, filter),
                BmpState::Aborted(addr, router_id) => {
                    ProcessingResult::new(MessageType::Aborted, BmpState::Aborted(addr, router_id))
                }
            }
        };

        let may_panic_res = may_panic.catch_unwind().await;

        match may_panic_res {
            Ok(process_res) => {
                if process_res.next_state.state_idx() != saved_state_idx {
                    if let Some(reporter) = process_res.next_state.status_reporter() {
                        reporter.change_state(
                            process_res.next_state.router_id(),
                            saved_state_idx,
                            process_res.next_state.state_idx(),
                        );
                    }
                }

                process_res
            }
            Err(err) => {
                let err = format!("Internal error while processing BMP message: {:?}", err);
                // We've lost the bmp state machine and associated state...
                // including things like detected peer configurations which we
                // need in order to be able to process subsequent messages.
                // With the current design it is difficult to recover from
                // this, on the other hand we don't know what we've missed by
                // a panic occuring in the first place so maybe it's just
                // better to blow up at this point (just without a panic that
                // will take out other Tokio tasks being handled by the same
                // worker thread!).
                ProcessingResult::new(
                    MessageType::InvalidMessage {
                        err,
                        known_peer: None,
                        msg_bytes: None,
                    },
                    BmpState::Aborted(saved_addr, saved_router_id.clone()),
                )
            }
        }
    }
}

impl From<BmpStateDetails<Initiating>> for BmpState {
    fn from(v: BmpStateDetails<Initiating>) -> Self {
        Self::Initiating(v)
    }
}

impl From<BmpStateDetails<Dumping>> for BmpState {
    fn from(v: BmpStateDetails<Dumping>) -> Self {
        Self::Dumping(v)
    }
}

impl From<BmpStateDetails<Updating>> for BmpState {
    fn from(v: BmpStateDetails<Updating>) -> Self {
        Self::Updating(v)
    }
}

impl From<BmpStateDetails<Terminated>> for BmpState {
    fn from(v: BmpStateDetails<Terminated>) -> Self {
        Self::Terminated(v)
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs::File, io::Read, time::Instant};

    use bytes::BytesMut;
    // use chrono::{DateTime, NaiveDateTime};
    use futures::future::join_all;

    use crate::{
        metrics::{OutputFormat, Source},
        units::bmp_in::state_machine::metrics::BmpMetrics,
    };

    use super::*;

    /// This test replays data captured by BmpRawDumper.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn replay_test() {
        const USIZE_BYTES: usize = (usize::BITS as usize) >> 3;
        let bmp_metrics = Arc::new(BmpMetrics::default());
        let status_reporter =
            Arc::new(BmpStatusReporter::new("some reporter", bmp_metrics.clone()));

        let mut inputs = Vec::new();
        for entry in std::fs::read_dir("bmp-dump").unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension() == Some(OsStr::new("bin")) {
                inputs.push(path);
            }
        }

        let mut join_handles = Vec::new();

        for path in inputs {
            if let Some(fname) = path.file_name().and_then(OsStr::to_str) {
                let pieces: Vec<&str> = fname.splitn(3, '-').collect();
                let ip = pieces[1];
                let port = pieces[2].split('.').next().unwrap();
                let client_addr = SocketAddr::new(ip.parse().unwrap(), port.parse().unwrap());
                let router_id = Arc::new(format!("{}-{}", client_addr.ip(), client_addr.port()));
                let status_reporter = status_reporter.clone();
                let mut bmp_state = BmpState::new(
                    client_addr,
                    router_id.clone(),
                    status_reporter.clone(),
                    bmp_metrics.clone(),
                );

                let handle = tokio::task::spawn(async move {
                    let mut file = File::open(path).unwrap();
                    let mut ts_bytes: [u8; 16] = [0; 16];
                    let mut num_bmp_bytes: [u8; USIZE_BYTES] = [0; USIZE_BYTES];
                    let mut last_ts = None;
                    let mut last_push = Instant::now();

                    loop {
                        let mut bmp_bytes = BytesMut::with_capacity(64 * 1024);

                        // Each line has the form <timestamp:u128><num bytes:usize><bmp bytes>
                        file.read_exact(&mut ts_bytes).unwrap();
                        let ts = u128::from_be_bytes(ts_bytes);

                        file.read_exact(&mut num_bmp_bytes).unwrap();
                        let num_bytes_to_read = usize::from_be_bytes(num_bmp_bytes);
                        bmp_bytes.resize(num_bytes_to_read, 0);

                        file.read_exact(&mut bmp_bytes).unwrap();

                        // Did the original stream contain the message at this
                        // point in time or do we need to wait a bit so as to
                        // more closely emulate the original rate at which the
                        // data was seen?
                        if let Some(last_ts) = last_ts {
                            let millis_between_messages = ts - last_ts;
                            let millis_since_last_push = last_push.elapsed().as_millis();
                            if millis_since_last_push < millis_between_messages {
                                let _millis_to_sleep =
                                    millis_between_messages - millis_since_last_push;
                                //sleep(Duration::from_millis(_millis_to_sleep.try_into().unwrap()));
                            }
                        }

                        let mut res = bmp_state.process_msg(bmp_bytes.freeze()).await;

                        match res.processing_result {
                            MessageType::InvalidMessage { err, .. } => {
                                log::warn!(
                                    "{}: Invalid message: {}",
                                    res.next_state.router_id(),
                                    err
                                );
                            }

                            MessageType::StateTransition => {
                                if let BmpState::Dumping(next_state) = &mut res.next_state {
                                    next_state.router_id =
                                        Arc::new(next_state.details.sys_name.clone());

                                    // This will enable metrics to be stored for the router id.
                                    // status_reporter.router_id_changed(next_state.router_id.clone());
                                }
                            }

                            MessageType::Aborted => {
                                log::warn!("{}: Aborted", res.next_state.router_id());
                                break;
                            }

                            _ => {}
                        }

                        bmp_state = res.next_state;

                        last_ts = Some(ts);
                        last_push = Instant::now();
                    }
                });

                join_handles.push(handle);
            }
        }

        join_all(join_handles).await;

        let mut metrics_target = crate::metrics::Target::new(OutputFormat::Plain);
        bmp_metrics.append("test", &mut metrics_target);
        eprintln!("{}", metrics_target.into_string());

        // TODO
        // assert_eq!(
        //     bmp_metrics
        //         .num_receive_io_errors
        //         .iter()
        //         .fold(0, |acc, v| acc + v.value()),
        //     0
        // );
    }
}

#[derive(Debug, Default)]
pub struct PeerStates(HashMap<PerPeerHeader<Bytes>, PeerState>);

impl PeerStates {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PeerAware for PeerStates {
    fn add_peer_config(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        session_config: SessionConfig,
        eor_capable: bool,
    ) -> bool {
        let mut added = false;
        let _ = self.0.entry(pph).or_insert_with(|| {
            added = true;
            PeerState {
                session_config,
                eor_capable,
                pending_eors: HashSet::with_capacity(0),
                announced_prefixes: HashSet::with_capacity(0),
            }
        });
        added
    }

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState> {
        self.0.keys()
    }

    fn update_peer_config(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        new_config: SessionConfig,
    ) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state.session_config = new_config;
            true
        } else {
            false
        }
    }

    fn get_peer_config(&self, pph: &PerPeerHeader<Bytes>) -> Option<&SessionConfig> {
        self.0.get(pph).map(|peer_state| &peer_state.session_config)
    }

    fn remove_peer_config(&mut self, pph: &PerPeerHeader<Bytes>) -> bool {
        self.0.remove(pph).is_some()
    }

    fn num_peer_configs(&self) -> usize {
        self.0.len()
    }

    fn is_peer_eor_capable(&self, pph: &PerPeerHeader<Bytes>) -> Option<bool> {
        self.0.get(pph).map(|peer_state| peer_state.eor_capable)
    }

    fn add_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI) {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state
                .pending_eors
                .insert(EoRProperties::new(pph, afi, safi));
        }
    }

    fn remove_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state
                .pending_eors
                .remove(&EoRProperties::new(pph, afi, safi));
        }

        // indicate if all pending EORs have been removed, i.e. this is
        // the end of the initial table dump
        self.0
            .values()
            .all(|peer_state| peer_state.pending_eors.is_empty())
    }

    fn num_pending_eors(&self) -> usize {
        self.0
            .values()
            .fold(0, |acc, peer_state| acc + peer_state.pending_eors.len())
    }

    fn add_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: Prefix) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state.announced_prefixes.insert(prefix)
        } else {
            false
        }
    }

    fn remove_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: &Prefix) {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state.announced_prefixes.remove(prefix);
        }
    }

    fn get_announced_prefixes(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<std::collections::hash_set::Iter<Prefix>> {
        self.0
            .get(pph)
            .map(|peer_state| peer_state.announced_prefixes.iter())
    }
}

// --------- BEGIN TEMPORARY CODE TO BE REPLACED BY ROUTECORE WHEN READY ----------------------------------------------

// based on code in tests/util.rs:
#[allow(clippy::vec_init_then_push)]
fn mk_bgp_update<'a, W>(withdrawals: W) -> Result<Bytes, String>
where
    W: IntoIterator<Item = &'a Prefix>,
{
    // Based on `div_ceil()` from Rust nightly.
    const fn div_ceil(lhs: u8, rhs: u8) -> u8 {
        let d = lhs / rhs;
        let r = lhs % rhs;
        if r > 0 && rhs > 0 {
            d + 1
        } else {
            d
        }
    }

    fn finalize_bgp_msg_len(buf: &mut BytesMut) -> Result<(), &'static str> {
        if buf.len() < 19 {
            Err("Cannot finalize BGP message: message would be too short")
        } else if buf.len() > 65535 {
            // TOOD: If we can support RFC 8654 we can increase this to 65,535
            Err("Cannot finalize BGP message: message would be too long")
        } else {
            let len_bytes: [u8; 2] = (buf.len() as u16).to_be_bytes();
            buf[16] = len_bytes[0];
            buf[17] = len_bytes[1];
            Ok(())
        }
    }

    // 4.3. UPDATE Message Format
    //
    // "The UPDATE message always includes the fixed-size BGP
    //  header, and also includes the other fields, as shown below (note,
    //  some of the shown fields may not be present in every UPDATE message):"
    //
    //      +-----------------------------------------------------+
    //      |   Withdrawn Routes Length (2 octets)                |
    //      +-----------------------------------------------------+
    //      |   Withdrawn Routes (variable)                       |
    //      +-----------------------------------------------------+
    //      |   Total Path Attribute Length (2 octets)            |
    //      +-----------------------------------------------------+
    //      |   Path Attributes (variable)                        |
    //      +-----------------------------------------------------+
    //      |   Network Layer Reachability Information (variable) |
    //      +-----------------------------------------------------+
    //
    // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3

    let mut buf = BytesMut::new();

    // Fixed size BGP header
    buf.resize(buf.len() + 16, 0xFFu8);
    // marker
    buf.resize(buf.len() + 2, 0);
    // placeholder length, to be replaced later
    buf.extend_from_slice(&2u8.to_be_bytes());
    // 2 - UPDATE

    // Other fields
    // Route withdrawals
    // "Withdrawn Routes Length:
    //
    //  This 2-octets unsigned integer indicates the total length of
    //  the Withdrawn Routes field in octets.  Its value allows the
    //  length of the Network Layer Reachability Information field to
    //  be determined, as specified below.
    //
    //  A value of 0 indicates that no routes are being withdrawn from
    //  service, and that the WITHDRAWN ROUTES field is not present in
    //  this UPDATE message.
    //
    //  Withdrawn Routes:
    //
    //  This is a variable-length field that contains a list of IP
    //  address prefixes for the routes that are being withdrawn from
    //  service.  Each IP address prefix is encoded as a 2-tuple of the
    //  form <length, prefix>, whose fields are described below:
    //
    //           +---------------------------+
    //           |   Length (1 octet)        |
    //           +---------------------------+
    //           |   Prefix (variable)       |
    //           +---------------------------+
    //
    //  The use and the meaning of these fields are as follows:
    //
    //  a) Length:
    //
    //     The Length field indicates the length in bits of the IP
    //     address prefix.  A length of zero indicates a prefix that
    //     matches all IP addresses (with prefix, itself, of zero
    //     octets).
    //
    //  b) Prefix:
    //
    //     The Prefix field contains an IP address prefix, followed by
    //     the minimum number of trailing bits needed to make the end
    //     of the field fall on an octet boundary.  Note that the value
    //     of trailing bits is irrelevant."
    //
    // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
    let mut withdrawn_routes = BytesMut::new();
    let mut mp_unreach_nlri = BytesMut::new();

    for prefix in withdrawals.into_iter() {
        let (addr, len) = prefix.addr_and_len();
        match addr {
            IpAddr::V4(addr) => {
                withdrawn_routes.extend_from_slice(&[len]);
                if len > 0 {
                    let min_bytes = div_ceil(len, 8) as usize;
                    withdrawn_routes.extend_from_slice(&addr.octets()[..min_bytes]);
                }
            }
            IpAddr::V6(addr) => {
                // https://datatracker.ietf.org/doc/html/rfc4760#section-4
                if mp_unreach_nlri.is_empty() {
                    mp_unreach_nlri.put_u16(AFI::Ipv6.into());
                    mp_unreach_nlri.put_u8(u8::from(SAFI::Unicast));
                }
                mp_unreach_nlri.extend_from_slice(&[len]);
                if len > 0 {
                    let min_bytes = div_ceil(len, 8) as usize;
                    mp_unreach_nlri.extend_from_slice(&addr.octets()[..min_bytes]);
                }
            }
        }
    }

    let num_withdrawn_route_bytes =
        u16::try_from(withdrawn_routes.len()).map_err(|err| format!("{err}"))?;
    buf.extend_from_slice(&num_withdrawn_route_bytes.to_be_bytes());
    // N withdrawn route bytes
    if num_withdrawn_route_bytes > 0 {
        buf.extend(&withdrawn_routes); // the withdrawn routes
    }

    if mp_unreach_nlri.is_empty() {
        buf.put_u16(0u16); // no path attributes
    } else {
        if mp_unreach_nlri.len() > u8::MAX.into() {
            buf.put_u16(
                4u16 + u16::try_from(mp_unreach_nlri.len()).map_err(|err| format!("{err}"))?,
            ); // num path attribute bytes
            buf.put_u8(0b1001_0000); // optional (1), non-transitive (0), complete (0), extended (1)
            buf.put_u8(u8::from(PathAttributeType::MpUnreachNlri)); // attr. type
            buf.put_u16(
                mp_unreach_nlri
                    .len()
                    .try_into()
                    .map_err(|err| format!("{err}"))?,
            );
        } else {
            buf.put_u16(
                3u16 + u16::try_from(mp_unreach_nlri.len()).map_err(|err| format!("{err}"))?,
            ); // num path attribute bytes
            buf.put_u8(0b1000_0000); // optional (1), non-transitive (0), complete (0), non-extended (0)
            buf.put_u8(15); // MP_UNREACH_NLRI attribute type code per RFC 4760
            buf.put_u8(
                mp_unreach_nlri
                    .len()
                    .try_into()
                    .map_err(|err| format!("{err}"))?,
            );
        }
        buf.extend(&mp_unreach_nlri); // the withdrawn routes
    }

    // Finalize BGP message
    finalize_bgp_msg_len(&mut buf)?;
    Ok(buf.freeze())
}

// --------- END TEMPORARY CODE TO BE REPLACED BY ROUTECORE WHEN READY ------------------------------------------------
