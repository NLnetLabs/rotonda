use atomic_enum::atomic_enum;
use bytes::Bytes;
use roto::types::builtin::RouteStatus;

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
            nlri::Nlri,
            open::CapabilityType,
            update::{AddPath, FourOctetAsn},
            SessionConfig, UpdateMessage,
        },
        types::{AFI, SAFI},
    },
    bmp::message::{
        InformationTlvType, InitiationMessage, Message as BmpMsg,
        PeerDownNotification, PeerUpNotification, PerPeerHeader, RibType,
        RouteMonitoring, TerminationMessage,
    },
};
use smallvec::SmallVec;

use std::{
    collections::{hash_map::Keys, HashMap, HashSet},
    ops::ControlFlow,
    sync::Arc,
};

use crate::{
    common::{
        routecore_extra::{
            generate_alternate_config, mk_route_for_prefix,
            mk_withdrawals_for_peers_announced_prefixes,
        },
        status_reporter::AnyStatusReporter,
    },
    payload::{Payload, RouterId, SourceId, Update},
};

use super::{
    metrics::BmpMetrics,
    processing::{MessageType, ProcessingResult},
    states::{
        dumping::Dumping, initiating::Initiating, terminated::Terminated,
        updating::Updating,
    },
    status_reporter::BmpTcpInStatusReporter,
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
    pub fn new<T: AsRef<[u8]>>(
        pph: &PerPeerHeader<T>,
        afi: AFI,
        safi: SAFI,
    ) -> Self {
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
    ///     > A Peer Down message implicitly withdraws all routes that were
    ///     > associated with the peer in question.  A BMP implementation MAY
    ///     > omit sending explicit withdraws for such routes."
    ///
    /// RFC 4271 section "4.3. UPDATE Message Format" states:
    ///     > An UPDATE message can list multiple routes that are to be withdrawn
    ///     > from service.  Each such route is identified by its destination
    ///     > (expressed as an IP prefix), which unambiguously identifies the route
    ///     > in the context of the BGP speaker - BGP speaker connection to which
    ///     > it has been previously advertised.
    ///     >      
    ///     > An UPDATE message might advertise only routes that are to be
    ///     > withdrawn from service, in which case the message will not include
    ///     > path attributes or Network Layer Reachability Information."
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
    Aborted(SourceId, Arc<RouterId>),
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
    pub source_id: SourceId,
    pub router_id: Arc<String>,
    pub status_reporter: Arc<BmpTcpInStatusReporter>,
    pub details: T,
}

impl BmpState {
    pub fn source_id(&self) -> SourceId {
        match self {
            BmpState::Initiating(v) => v.source_id.clone(),
            BmpState::Dumping(v) => v.source_id.clone(),
            BmpState::Updating(v) => v.source_id.clone(),
            BmpState::Terminated(v) => v.source_id.clone(),
            BmpState::Aborted(source_id, _) => source_id.clone(),
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

    pub fn status_reporter(&self) -> Option<Arc<BmpTcpInStatusReporter>> {
        match self {
            BmpState::Initiating(v) => Some(v.status_reporter.clone()),
            BmpState::Dumping(v) => Some(v.status_reporter.clone()),
            BmpState::Updating(v) => Some(v.status_reporter.clone()),
            BmpState::Terminated(v) => Some(v.status_reporter.clone()),
            BmpState::Aborted(_, _) => None,
        }
    }

    #[rustfmt::skip]
    pub fn terminate(self) -> ProcessingResult {
        match self {
            BmpState::Initiating(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None),
            BmpState::Dumping(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None),
            BmpState::Updating(v) => v.terminate(Option::<TerminationMessage<Bytes>>::None),
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

    pub fn mk_routing_update_result(
        self,
        update: Update,
    ) -> ProcessingResult {
        ProcessingResult::new(
            MessageType::RoutingUpdate { update },
            self.into(),
        )
    }

    pub fn mk_final_routing_update_result(
        next_state: BmpState,
        update: Update,
    ) -> ProcessingResult {
        ProcessingResult::new(
            MessageType::RoutingUpdate { update },
            next_state,
        )
    }

    pub fn mk_state_transition_result(
        next_state: BmpState,
    ) -> ProcessingResult {
        ProcessingResult::new(MessageType::StateTransition, next_state)
    }
}

pub trait Initiable {
    /// Set the initiating's sys name.
    fn set_information_tlvs(
        &mut self,
        sys_name: String,
        sys_desc: String,
        sys_extra: Vec<String>,
    );

    fn sys_name(&self) -> Option<&str>;
}

impl<T> BmpStateDetails<T>
where
    T: Initiable,
    BmpState: From<BmpStateDetails<T>>,
{
    pub fn initiate<Octs: Octets>(
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

    /// Remove all details about a peer.
    ///
    /// Returns true if the peer details (config, pending EoRs, announced routes, etc) were removed, false if the peer
    /// is not known.
    fn remove_peer(&mut self, pph: &PerPeerHeader<Bytes>) -> bool;

    fn update_peer_config(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        config: SessionConfig,
    ) -> bool;

    /// Get a reference to a previously inserted configuration.
    fn get_peer_config(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<&SessionConfig>;

    fn num_peer_configs(&self) -> usize;

    fn is_peer_eor_capable(&self, pph: &PerPeerHeader<Bytes>)
        -> Option<bool>;

    fn add_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi: AFI,
        safi: SAFI,
    );

    /// Remove previously recorded pending End-of-RIB note for a peer.
    ///
    /// Returns true if the configuration removed was the last one, i.e. this
    /// is the end of the initial table dump, false otherwise.
    fn remove_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi: AFI,
        safi: SAFI,
    ) -> bool;

    fn num_pending_eors(&self) -> usize;

    fn add_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: Prefix,
    ) -> bool;

    fn remove_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: &Prefix,
    );

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
    pub fn peer_up(
        mut self,
        msg: PeerUpNotification<Bytes>,
    ) -> ProcessingResult {
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

    pub fn peer_down(
        mut self,
        msg: PeerDownNotification<Bytes>,
    ) -> ProcessingResult {
        // Compatibility Note: RFC-7854 doesn't seem to indicate that a Peer
        // Down Notification has a Per Peer Header, but if it doesn't how can
        // we know which peer of the remote has gone down? Also, we see the
        // Per Peer Header attached to Peer Down Notification BMP messages in
        // packet captures so it seems that it is indeed sent.
        let pph = msg.per_peer_header();

        let eor_capable = self.details.is_peer_eor_capable(&pph);

        self.status_reporter
            .peer_down(self.router_id.clone(), eor_capable);

        let withdrawals = self.mk_withdrawals_for_peers_routes(&pph);

        if !self.details.remove_peer(&pph) {
            // This is unexpected, we should have had configuration
            // stored for this peer but apparently don't. Did we
            // receive a Peer Down Notification without a
            // corresponding prior Peer Up Notification for the same
            // peer?
            return self.mk_invalid_message_result(
                "PeerDownNotification received for peer that was not 'up'",
                Some(false),
                // TODO: Silly to_copy the bytes, but PDN won't give us the octets back..
                Some(Bytes::copy_from_slice(msg.as_ref())),
            );
        } else if withdrawals.is_empty() {
            self.mk_other_result()
        } else {
            self.mk_routing_update_result(Update::Bulk(withdrawals))
        }
    }

    pub fn mk_withdrawals_for_peers_routes(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
    ) -> SmallVec<[Payload; 8]> {
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

        // Loop over announced prefixes constructing BGP UPDATE PDUs with as many prefixes as can fit in one PDU at a
        // time until withdrawals have been generated for all announced prefixes.
        if let Some(prefixes) = self.details.get_announced_prefixes(pph) {
            mk_withdrawals_for_peers_announced_prefixes(
                prefixes,
                self.router_id.clone(),
                pph.address(),
                pph.asn(),
                self.source_id.clone(),
            )
        } else {
            SmallVec::new()
        }
    }

    /// `filter` should return `None` if the BGP message should be ignored, i.e. be filtered out, otherwise `Some(msg)`
    /// where `msg` is either the original unmodified `msg` or a modified or completely new message.
    pub fn route_monitoring<CB>(
        mut self,
        msg: RouteMonitoring<Bytes>,
        route_status: RouteStatus,
        do_state_specific_pre_processing: CB,
    ) -> ProcessingResult
    where
        CB: Fn(
            BmpStateDetails<T>,
            &PerPeerHeader<Bytes>,
            &UpdateMessage<Bytes>,
        ) -> ControlFlow<ProcessingResult, Self>,
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
                        self.details
                            .update_peer_config(&pph, chosen_peer_config);
                    }

                    let mut saved_self =
                        match do_state_specific_pre_processing(
                            self, &pph, &update,
                        ) {
                            ControlFlow::Break(res) => return res,
                            ControlFlow::Continue(saved_self) => saved_self,
                        };

                    let (
                        payloads,
                        n_new_prefixes,
                        n_announcements,
                        n_withdrawals,
                    ) = saved_self.extract_route_monitoring_routes(
                        pph.clone(),
                        &update,
                        route_status,
                    );

                    if n_announcements > 0
                        && saved_self.details.is_peer_eor_capable(&pph)
                            == Some(true)
                    {
                        // We are only looking at the (AFI,SAFI) of the first
                        // NLRI we can find!
                        if let Ok(mut nlris) = update.announcements() {
                            if let Some(Ok(afi_safi)) = nlris.next().map(|n| n.map(|n| n.afi_safi())) {
                                saved_self.details.add_pending_eor(
                                    &pph,
                                    afi_safi.0,
                                    afi_safi.1,
                                );
                            };
                        }
                    }

                    saved_self.status_reporter.routing_update(
                        saved_self.router_id.clone(),
                        n_new_prefixes,
                        n_announcements,
                        n_withdrawals,
                        0, //self.details.get_stored_routes().prefixes_len(), // TODO
                    );

                    saved_self
                        .mk_routing_update_result(Update::Bulk(payloads))
                }

                Err(err) => {
                    tried_peer_configs.push(chosen_peer_config);
                    if let Some(alt_config) =
                        generate_alternate_config(&chosen_peer_config)
                    {
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

    pub fn extract_route_monitoring_routes(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        update: &UpdateMessage<Bytes>,
        route_status: RouteStatus,
    ) -> (SmallVec<[Payload; 8]>, usize, usize, usize) {
        let mut num_new_prefixes = 0;
        let num_announcements: usize = update.announcements().map(|a| a.flat_map(|nlri| nlri.ok()).count()).unwrap_or(0);
        let mut num_withdrawals: usize = update.withdrawn_routes_len();
        let mut payloads =
            SmallVec::with_capacity(num_announcements + num_withdrawals);

        let nlris: Vec<Nlri<Bytes>> = if let Ok(nlris) = update.unicast_announcements() { nlris.filter_map(|nlri| nlri.ok()).collect() } else { vec![] };

        for nlri in &nlris {
            // If we want to process multicast here, use a `match nlri { }`:
            //      match nlri {
            //          Nlri::Unicast(b) | Nlri::Multicast(b) => {
            //          }
            //          _ => unimplemented!()
            //
            // For unicast only, the `if let` suffices, at the cost of hiding
            // any possibly unprocessed non-unicast NLRI.
            if let Nlri::Unicast(nlri) = nlri {
                let prefix = nlri.prefix;
                if self.details.add_announced_prefix(&pph, prefix) {
                    num_new_prefixes += 1;
                }

                // clone is cheap due to use of Bytes
                let route = mk_route_for_prefix(
                    self.router_id.clone(),
                    update.clone(),
                    pph.address(),
                    pph.asn(),
                    prefix,
                    route_status,
                );

                payloads.push(Payload::new(self.source_id.clone(), route));
            }
        }

        for nlri in update.withdrawals().unwrap().flatten() {
            if let Nlri::Unicast(b) = nlri {
                let prefix = b.prefix();

                // RFC 4271 section "4.3 UPDATE Message Format" states:
                //
                // "An UPDATE message SHOULD NOT include the same address prefix in the
                //  WITHDRAWN ROUTES and Network Layer Reachability Information fields.
                //  However, a BGP speaker MUST be able to process UPDATE messages in
                //  this form.  A BGP speaker SHOULD treat an UPDATE message of this form
                //  as though the WITHDRAWN ROUTES do not contain the address prefix.

                if nlris.iter().all(|nlri| {
                    if let Nlri::Unicast(b) = nlri {
                        b.prefix() != prefix
                    } else {
                        true
                    }
                }) {
                    // clone is cheap due to use of Bytes
                    let route = mk_route_for_prefix(
                        self.router_id.clone(),
                        update.clone(),
                        pph.address(),
                        pph.asn(),
                        prefix,
                        RouteStatus::Withdrawn,
                    );

                    payloads
                        .push(Payload::new(self.source_id.clone(), route));

                    self.details.remove_announced_prefix(&pph, &prefix);
                } else {
                    num_withdrawals -= 1;
                }
            }
        }

        (
            payloads,
            num_new_prefixes,
            num_announcements,
            num_withdrawals,
        )
    }
}

impl BmpState {
    pub fn new<T: AnyStatusReporter>(
        source_id: SourceId,
        router_id: Arc<RouterId>,
        parent_status_reporter: Arc<T>,
        metrics: Arc<BmpMetrics>,
    ) -> Self {
        let child_name = parent_status_reporter.link_names("bmp_state");
        let status_reporter =
            Arc::new(BmpTcpInStatusReporter::new(child_name, metrics));

        BmpState::Initiating(BmpStateDetails::<Initiating>::new(
            source_id,
            router_id,
            status_reporter,
        ))
    }

    #[allow(dead_code)]
    pub fn process_msg(self, bmp_msg: BmpMsg<Bytes>) -> ProcessingResult {
        let saved_addr = self.source_id();
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
        let may_panic_res = std::panic::catch_unwind(|| match self {
            BmpState::Initiating(inner) => inner.process_msg(bmp_msg),
            BmpState::Dumping(inner) => inner.process_msg(bmp_msg),
            BmpState::Updating(inner) => inner.process_msg(bmp_msg),
            BmpState::Terminated(inner) => inner.process_msg(bmp_msg),
            BmpState::Aborted(source_id, router_id) => ProcessingResult::new(
                MessageType::Aborted,
                BmpState::Aborted(source_id, router_id),
            ),
        });

        match may_panic_res {
            Ok(process_res) => {
                if process_res.next_state.state_idx() != saved_state_idx {
                    if let Some(reporter) =
                        process_res.next_state.status_reporter()
                    {
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
                let err = format!(
                    "Internal error while processing BMP message: {:?}",
                    err
                );
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
    use std::{
        ffi::OsStr, fs::File, io::Read, net::SocketAddr, time::Instant,
    };

    use bytes::BytesMut;
    // use chrono::{DateTime, NaiveDateTime};
    use futures::future::join_all;

    use crate::{
        metrics::{OutputFormat, Source},
        units::bmp_tcp_in::state_machine::metrics::BmpMetrics,
    };

    use super::*;

    /// This test replays data captured by BmpRawDumper.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn replay_test() {
        const USIZE_BYTES: usize = (usize::BITS as usize) >> 3;
        let bmp_metrics = Arc::new(BmpMetrics::default());
        let status_reporter = Arc::new(BmpTcpInStatusReporter::new(
            "some reporter",
            bmp_metrics.clone(),
        ));

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
                let client_addr = SocketAddr::new(
                    ip.parse().unwrap(),
                    port.parse().unwrap(),
                );
                let router_id = Arc::new(format!(
                    "{}-{}",
                    client_addr.ip(),
                    client_addr.port()
                ));
                let status_reporter = status_reporter.clone();
                let mut bmp_state = BmpState::new(
                    SourceId::SocketAddr(client_addr),
                    router_id.clone(),
                    status_reporter.clone(),
                    bmp_metrics.clone(),
                );

                let handle = tokio::task::spawn(async move {
                    let mut file = File::open(path).unwrap();
                    let mut ts_bytes: [u8; 16] = [0; 16];
                    let mut num_bmp_bytes: [u8; USIZE_BYTES] =
                        [0; USIZE_BYTES];
                    let mut last_ts = None;
                    let mut last_push = Instant::now();

                    loop {
                        let mut bmp_bytes =
                            BytesMut::with_capacity(64 * 1024);

                        // Each line has the form <timestamp:u128><num bytes:usize><bmp bytes>
                        file.read_exact(&mut ts_bytes).unwrap();
                        let ts = u128::from_be_bytes(ts_bytes);

                        file.read_exact(&mut num_bmp_bytes).unwrap();
                        let num_bytes_to_read =
                            usize::from_be_bytes(num_bmp_bytes);
                        bmp_bytes.resize(num_bytes_to_read, 0);

                        file.read_exact(&mut bmp_bytes).unwrap();

                        // Did the original stream contain the message at this
                        // point in time or do we need to wait a bit so as to
                        // more closely emulate the original rate at which the
                        // data was seen?
                        if let Some(last_ts) = last_ts {
                            let millis_between_messages = ts - last_ts;
                            let millis_since_last_push =
                                last_push.elapsed().as_millis();
                            if millis_since_last_push
                                < millis_between_messages
                            {
                                let _millis_to_sleep = millis_between_messages
                                    - millis_since_last_push;
                                //sleep(Duration::from_millis(_millis_to_sleep.try_into().unwrap()));
                            }
                        }

                        let bmp_msg =
                            BmpMsg::from_octets(bmp_bytes.freeze()).unwrap();
                        let mut res = bmp_state.process_msg(bmp_msg);

                        match res.processing_result {
                            MessageType::InvalidMessage { err, .. } => {
                                log::warn!(
                                    "{}: Invalid message: {}",
                                    res.next_state.router_id(),
                                    err
                                );
                            }

                            MessageType::StateTransition => {
                                if let BmpState::Dumping(next_state) =
                                    &mut res.next_state
                                {
                                    next_state.router_id = Arc::new(
                                        next_state.details.sys_name.clone(),
                                    );

                                    // This will enable metrics to be stored for the router id.
                                    // status_reporter.router_id_changed(next_state.router_id.clone());
                                }
                            }

                            MessageType::Aborted => {
                                log::warn!(
                                    "{}: Aborted",
                                    res.next_state.router_id()
                                );
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

        let mut metrics_target =
            crate::metrics::Target::new(OutputFormat::Plain);
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
    #[allow(dead_code)]
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

    fn get_peer_config(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<&SessionConfig> {
        self.0.get(pph).map(|peer_state| &peer_state.session_config)
    }

    fn remove_peer(&mut self, pph: &PerPeerHeader<Bytes>) -> bool {
        self.0.remove(pph).is_some()
    }

    fn num_peer_configs(&self) -> usize {
        self.0.len()
    }

    fn is_peer_eor_capable(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<bool> {
        self.0.get(pph).map(|peer_state| peer_state.eor_capable)
    }

    fn add_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi: AFI,
        safi: SAFI,
    ) {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state
                .pending_eors
                .insert(EoRProperties::new(pph, afi, safi));
        }
    }

    fn remove_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi: AFI,
        safi: SAFI,
    ) -> bool {
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

    fn add_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: Prefix,
    ) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state.announced_prefixes.insert(prefix)
        } else {
            false
        }
    }

    fn remove_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: &Prefix,
    ) {
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
