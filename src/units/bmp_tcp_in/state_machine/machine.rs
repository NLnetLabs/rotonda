use atomic_enum::atomic_enum;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, warn};
//use roto::types::{builtin::{explode_announcements, explode_withdrawals, BytesRecord, FreshRouteContext, NlriStatus, PeerId, PeerRibType, Provenance, RouteContext}, lazyrecord_types::BgpUpdateMessage};

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
use inetnum::addr::Prefix;
use rotonda_store::prefix_record::RouteStatus;
use routecore::bgp::fsm::session;
use routecore::bmp::message::InformationTlvIter;
use routecore::{
    bgp::nlri::afisafi::IsPrefix,
    bgp::nlri::afisafi::Nlri,
    bgp::{
        message::{open::CapabilityType, SessionConfig, UpdateMessage},
        types::AfiSafiType,
        workshop::route::RouteWorkshop,
    },
    bmp::message::{
        InformationTlvType, InitiationMessage, Message as BmpMsg,
        PeerDownNotification, PeerUpNotification, PerPeerHeader, RibType,
        RouteMonitoring,
    },
};
//use roto::types::builtin::ingress::IngressId;

use smallvec::SmallVec;

use std::{
    collections::{
        hash_map::{DefaultHasher, Keys},
        BTreeSet, HashMap, HashSet,
    },
    hash::{Hash, Hasher},
    io::Read,
    ops::ControlFlow,
    sync::Arc,
};

use crate::ingress::IngressId;
use crate::{
    common::{
        routecore_extra::generate_alternate_config,
        status_reporter::AnyStatusReporter,
    },
    ingress,
    payload::{Payload, RouterId, Update},
    roto_runtime::types::{
        explode_announcements, explode_withdrawals, 
        PeerId, PeerRibType,
    },
};

use super::{
    metrics::BmpStateMachineMetrics,
    processing::{MessageType, ProcessingResult},
    states::{
        dumping::Dumping, initiating::Initiating, terminated::Terminated,
        updating::Updating,
    },
    status_reporter::{BmpStateMachineStatusReporter, UpdateReportMessage},
};

//use octseq::Octets;
use routecore::Octets;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EoRProperties {
    pub afi_safi: AfiSafiType,
    pub post_policy: bool, // post-policy if 1, or pre-policy if 0
    pub adj_rib_out: bool, // rfc8671: adj-rib-out if 1, adj-rib-in if 0
}

impl EoRProperties {
    pub fn new<T: AsRef<[u8]>>(
        pph: &PerPeerHeader<T>,
        afi_safi: AfiSafiType,
    ) -> Self {
        EoRProperties {
            afi_safi,
            post_policy: pph.is_post_policy(),
            adj_rib_out: pph.adj_rib_type() == RibType::AdjRibOut,
        }
    }
}

// TODO Remove
#[allow(dead_code)]
#[derive(Clone)]
pub struct PeerDetails {
    peer_bgp_id: [u8; 4],
    peer_distinguisher: [u8; 8],
    peer_rib_type: RibType,
    peer_id: PeerId,
}

#[derive(Clone)]
pub struct PeerState {
    /// The settings needed to correctly parse BMP UPDATE messages sent
    /// for this peer.
    pub session_config: SessionConfig,

    /// Did the peer advertise the GracefulRestart capability in its BGP OPEN message?
    // Luuk: I don't think GR and EoR are related in this way here.
    pub eor_capable: bool,

    /// The set of End-of-RIB markers that we expect to see for this peer,
    /// based on received Peer Up Notifications.
    pub pending_eors: HashSet<EoRProperties>,

    pub peer_details: PeerDetails,

    pub ingress_id: ingress::IngressId,
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
    _Aborted(ingress::IngressId, Arc<RouterId>),
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
    pub ingress_id: ingress::IngressId,
    pub router_id: Arc<String>,
    pub status_reporter: Arc<BmpStateMachineStatusReporter>,
    pub ingress_register: Arc<ingress::Register>,
    pub details: T,
}

impl BmpState {
    pub fn _ingress_id(&self) -> ingress::IngressId {
        match self {
            BmpState::Initiating(v) => v.ingress_id,
            BmpState::Dumping(v) => v.ingress_id,
            BmpState::Updating(v) => v.ingress_id,
            BmpState::Terminated(v) => v.ingress_id,
            BmpState::_Aborted(ingress_id, _) => *ingress_id,
        }
    }

    pub fn router_id(&self) -> Arc<String> {
        match self {
            BmpState::Initiating(v) => v.router_id.clone(),
            BmpState::Dumping(v) => v.router_id.clone(),
            BmpState::Updating(v) => v.router_id.clone(),
            BmpState::Terminated(v) => v.router_id.clone(),
            BmpState::_Aborted(_, router_id) => router_id.clone(),
        }
    }

    pub fn state_idx(&self) -> BmpStateIdx {
        match self {
            BmpState::Initiating(_) => BmpStateIdx::Initiating,
            BmpState::Dumping(_) => BmpStateIdx::Dumping,
            BmpState::Updating(_) => BmpStateIdx::Updating,
            BmpState::Terminated(_) => BmpStateIdx::Terminated,
            BmpState::_Aborted(_, _) => BmpStateIdx::Aborted,
        }
    }

    pub fn status_reporter(
        &self,
    ) -> Option<Arc<BmpStateMachineStatusReporter>> {
        match self {
            BmpState::Initiating(v) => Some(v.status_reporter.clone()),
            BmpState::Dumping(v) => Some(v.status_reporter.clone()),
            BmpState::Updating(v) => Some(v.status_reporter.clone()),
            BmpState::Terminated(v) => Some(v.status_reporter.clone()),
            BmpState::_Aborted(_, _) => None,
        }
    }
}

impl<T> std::hash::Hash for BmpStateDetails<T>
where
    BmpState: From<BmpStateDetails<T>>,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ingress_id.hash(state);
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
        prev_state: BmpStateIdx,
        next_state: BmpState,
    ) -> ProcessingResult {
        if let Some(status_reporter) = next_state.status_reporter() {
            status_reporter.change_state(
                next_state.router_id(),
                prev_state,
                next_state.state_idx(),
            );
        }

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
            warn!(
                "Invalid BMP InitiationMessage: \
                Missing or empty sysName Information TLV"
            );
        }
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

pub trait PeerAware {
    /// Remember this peer and the configuration we will need to use later to
    /// correctly parse and interpret subsequent messages for this peer. EOR
    /// is an abbreviation of End-of-RIB [1].
    ///
    /// Returns a tuple of
    ///   * a boolean which if true signals the configuration was recorded, false if configuration
    ///     for the peer already exists.
    ///   * optionally an `IngressId`, if a peer was found in the Ingress registry (i.e. this is a
    ///     reconnecting peer).
    ///
    /// [1]: https://datatracker.ietf.org/doc/html/rfc4724#section-2
    fn add_peer_config(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        config: SessionConfig,
        eor_capable: bool,
        ingress_register: Arc<ingress::Register>,
        bmp_ingress_id: ingress::IngressId,
        tlv_iter: InformationTlvIter,
    ) -> (bool, Option<IngressId>);

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState>;

    /// Remove all details about a peer.
    ///
    /// Returns true if the peer details (config, pending EoRs, announced routes, etc) were removed, false if the peer
    /// is not known.
    fn remove_peer(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<PeerState>;

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


    /// Copy over all we know from one (existing) entry to a new one, for a given ingress_id.
    ///
    /// This is used when get_peer_config returns no known config for a RouteMon message and we
    /// retry with the Peer Flags field set to all zeroes.
    fn add_cloned_peer_config(
        &mut self,
        source_pph: &PerPeerHeader<Bytes>,
        dst_pph: &PerPeerHeader<Bytes>,
        ingress_id: IngressId,
    ) -> bool;

    fn get_peer_ingress_id(
        &self,
        _pph: &PerPeerHeader<Bytes>,
    ) -> Option<ingress::IngressId>;

    fn is_peer_eor_capable(&self, pph: &PerPeerHeader<Bytes>)
        -> Option<bool>;

    fn add_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi_safi: AfiSafiType,
    ) -> usize;

    /// Remove previously recorded pending End-of-RIB note for a peer.
    ///
    /// Returns true if the configuration removed was the last one, i.e. this
    /// is the end of the initial table dump, false otherwise.
    fn remove_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi_safi: AfiSafiType,
    ) -> bool;

    fn num_pending_eors(&self) -> usize;

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
        // LH: as mentioned elsewhere, I do not think the GR capability
        // for this _peer_ relates to whether or not the _BMP process on the
        // router_ will send an EoR after dumping.
        let eor_capable = msg
            .bgp_open_rcvd()
            .capabilities()
            .any(|cap| cap.typ() == CapabilityType::GracefulRestart);

        let tlv_iter = msg.information_tlvs();

        let (peer_added, existing_peer_ingress_id) =  self.details.add_peer_config(
            pph,
            config,
            eor_capable,
            self.ingress_register.clone(),
            self.ingress_id,
            tlv_iter,
        );
        if !peer_added {
            // This is unexpected. How can we already have an entry in
            // the map for a peer which is currently up (i.e. we have
            // already seen a PeerUpNotification for the peer but have
            // not seen a PeerDownNotification for the same peer)?
            debug!("peer is already up! returning from peer_up() in BMP fsm");
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

        if let Some(existing_peer_ingress_id) = existing_peer_ingress_id {
            self.mk_routing_update_result(Update::IngressReappeared(existing_peer_ingress_id))
        } else {
            self.mk_other_result()
        }
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

        // We have to grab these before we remove the peer.
        //let withdrawals = self.mk_withdrawals_for_peers_routes(&pph);
        //let withdrawals = vec![];

        if let Some(removed_peer) = self.details.remove_peer(&pph) {
            self.status_reporter.routing_update(UpdateReportMessage {
                router_id: self.router_id.clone(),
                n_new_prefixes: 0,        // no new prefixes
                n_valid_announcements: 0, // no new announcements
                n_valid_withdrawals: 0,   // no new withdrawals
                n_stored_prefixes: 0, // zero because we just removed all stored prefixes for this peer
                n_invalid_announcements: 0,
                n_invalid_withdrawals: 0,
                last_invalid_announcement: None,
                last_invalid_withdrawal: None,
            });

            let eor_capable = self.details.is_peer_eor_capable(&pph);

            // Don't announce this above as it will cause metric
            // underflow from 0 to MAX if there were no peers
            // currently up.
            self.status_reporter
                .peer_down(self.router_id.clone(), eor_capable);

            //if withdrawals.is_empty() {
            //    self.mk_other_result()
            //} else {
            //self.mk_routing_update_result(Update::Bulk(withdrawals))
            self.mk_routing_update_result(Update::Withdraw(
                removed_peer.ingress_id,
                None,
            ))

            /*
            ProcessingResult::new(
                MessageType::RoutingUpdate{
                    update: Update::Withdraw(removed_peer.ingress_id, None),
                },
                self.into()
            )
                */
            //}
        } else {
            //if !self.details.remove_peer(&pph) {
            // This is unexpected, we should have had configuration
            // stored for this peer but apparently don't. Did we
            // receive a Peer Down Notification without a
            // corresponding prior Peer Up Notification for the same
            // peer?
            self.mk_invalid_message_result(
                "PeerDownNotification received for peer that was not 'up'",
                Some(false),
                // TODO: Silly to_copy the bytes, but PDN won't give us the octets back..
                Some(Bytes::copy_from_slice(msg.as_ref())),
            )
        }
    }

    /*
    pub fn mk_withdrawals_for_peers_routes(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
    ) -> SmallVec<[Payload; 8]> {
        todo!()

        //LH: guess we need to get the correct ingress_ids for the withdrawals
        //here, or in the caller of this function.


        /*
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

        // Loop over announced prefixes constructing BGP UPDATE messages with
        // as many prefixes as can fit in one message at a time until
        // withdrawals have been generated for all announced prefixes.

        self.details
            .get_announced_prefixes(pph)
            .and_then(|nlri| {
                    match nlri {
                        Nlri::Ipv4Unicast(nlri) => {
                            mk_withdrawals_for_peers_announced_prefixes(
                                nlri,
                                provenance,
                                session_config
                                // self.router_id.clone(),
                                // pph.address(),
                                // pph.asn(),
                                // self.source_id.clone()
                            ).ok()
                        }
                    }
            }
            ).unwrap_or_default()

            //     mk_withdrawals_for_peers_announced_prefixes(
            //             nlri,
            //             provenance,
            //             session_config
            //             // self.router_id.clone(),
            //             // pph.address(),
            //             // pph.asn(),
            //             // self.source_id.clone()
            //         ).ok())
            // .unwrap_or_default()
        */
    }
    */

    /// `filter` should return `None` if the BGP message should be ignored,
    /// i.e. be filtered out, otherwise `Some(msg)` where `msg` is either the
    /// original unmodified `msg` or a modified or completely new message.
    pub fn route_monitoring<CB>(
        mut self,
        received: std::time::Instant,
        msg: RouteMonitoring<Bytes>,
        //route_status: NlriStatus,
        trace_id: Option<u8>,
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

        let pph = msg.per_peer_header();

        let peer_config = match self.details.get_peer_config(&pph) {
            Some(peer_config) => peer_config,
            None => {
                // No config found means we did not observe a PeerUp with the exact same PPH as
                // this RouteMonitoring message. There might have been a PeerUp with no flags set,
                // so we zero out the flags byte and try again.
                let mut raw = pph.as_ref().to_owned();
                //raw[1] = 0;
                //instead of zeroing everything, only zero the rib type and policy flags
                raw[1] &= 0b1010_1111;
                let pph_nulled_flags = PerPeerHeader::for_slice(Bytes::from(raw));

                if let Some(_nulled_peer_config) = self.details.get_peer_config(&pph_nulled_flags) {
                    // So there was a PeerUp for a similar PPH as this RouteMon message, though
                    // without any flags set. We take the info we have for that one, register a new
                    // ingress in the ingress::Register, and copy all the IngressInfo. Because the
                    // initial PPH had no flags set, we have to explicitly set the RibType based on
                    // the flags in the PPH of this RouteMon message.

                    warn!("RouteMonitoring message received for which no PeerUp has been observed, \
                            but there was a matching PeerUp with all peer flags 0 (Adj-RIB-In Pre-policy)");
                    let new_ingress_id = self.ingress_register.register();
                    let existing_ingress_id = self.details.get_peer_ingress_id(&pph_nulled_flags).unwrap();
                    let mut adapted_ingress_info = self.ingress_register.get(existing_ingress_id).unwrap();
                    adapted_ingress_info.peer_rib_type = Some((pph.is_post_policy(), pph.rib_type()).into());
                    warn!("Registered a new ingress_id {} based on PeerUp with ingress_id {}, info {:?}",
                        new_ingress_id, existing_ingress_id, adapted_ingress_info
                    );
                    self.ingress_register.update_info(
                        new_ingress_id,
                        adapted_ingress_info
                    );

                    // We also update the PeerState in the BMP FSM, which is a clone of what we
                    // have seen before except for the (just generated) ingress_id.
                    if !self.details.add_cloned_peer_config(&pph_nulled_flags, &pph, new_ingress_id) {
                        // something went wrong, abort after all

                        return self.mk_invalid_message_result(
                            format!(
                                "Could not synthesize PPH/PeerState for RouteMonitoring message lacking PeerUpNotification: {}",
                                msg.per_peer_header()
                            ),
                            Some(false),
                            Some(Bytes::copy_from_slice(msg.as_ref())),
                        );
                    }
                    // We just successfully added this, so the unwrap is safe
                    self.details.get_peer_config(&pph).unwrap()

                } else {
                    warn!("RouteMonitoring message received for which no (null-flagged) PeerUp has been observed.");
                    self.status_reporter.peer_unknown(self.router_id.clone());

                    return self.mk_invalid_message_result(
                        format!(
                            "RouteMonitoring message received for peer that is not 'up': {}",
                            msg.per_peer_header()
                        ),
                        Some(false),
                        Some(Bytes::copy_from_slice(msg.as_ref())),
                    );
                }
            }
        };

        let mut peer_config = peer_config.clone();

        let mut retry_due_to_err: Option<String> = None;
        loop {
            let res = match msg.bgp_update(&peer_config) {
                Ok(update) => {
                    if let Some(err_str) = retry_due_to_err {
                        self.status_reporter.bgp_update_parse_soft_fail(
                            self.router_id.clone(),
                            err_str,
                            Some(Bytes::copy_from_slice(msg.as_ref())),
                        );

                        // use this config from now on
                        self.details
                            .update_peer_config(&pph, peer_config.clone());
                    }

                    let mut saved_self =
                        match do_state_specific_pre_processing(
                            self, &pph, &update,
                        ) {
                            ControlFlow::Break(res) => return res,
                            ControlFlow::Continue(saved_self) => saved_self,
                        };

                    if let Ok((payloads, update_report_msg)) = saved_self
                        .extract_route_monitoring_routes(
                            received,
                            pph.clone(),
                            &update,
                            //route_status,
                            trace_id,
                        )
                    {
                        match update.announcements_vec() {
                            // For now we are completely erroring out when a part
                            // of the announcement cannot be parsed by routecore.
                            // In the future we should handover more control
                            // around processing partial errors to the roto user.
                            Err(err) => {
                                return saved_self.mk_invalid_message_result(
                                    format!(
                                        "Invalid BMP RouteMonitoring BGP \
                                UPDATE message. One or more elements in the \
                                NLRI(s) cannot be parsed: ({:?}) {:?}",
                                        &peer_config,
                                        err.to_string()
                                    ),
                                    Some(true),
                                    Some(Bytes::copy_from_slice(
                                        msg.as_ref(),
                                    )),
                                );
                            }
                            Ok(announcements) => {
                                if update_report_msg.n_valid_announcements > 0
                                    && saved_self
                                        .details
                                        .is_peer_eor_capable(&pph)
                                        == Some(true)
                                {
                                    let afi_safi: AfiSafiType = announcements
                                        .first()
                                        .unwrap()
                                        .afi_safi();

                                    let num_pending_eors = saved_self
                                        .details
                                        .add_pending_eor(&pph, afi_safi);

                                    saved_self
                                        .status_reporter
                                        .pending_eors_update(
                                            saved_self.router_id.clone(),
                                            num_pending_eors,
                                        );
                                }
                            }
                        }

                        saved_self
                            .status_reporter
                            .routing_update(update_report_msg);

                        saved_self
                            .mk_routing_update_result(Update::Bulk(payloads))
                    } else {
                        return saved_self.mk_invalid_message_result(
                            "Invalid BMP RouteMonitoring BGP UPDATE message. The message cannot be parsed.",
                            Some(true),
                            Some(Bytes::copy_from_slice(msg.as_ref())),
                        );
                    }
                }

                Err(err) => {
                    tried_peer_configs.push(peer_config.clone());
                    if let Some(alt_config) =
                        generate_alternate_config(&peer_config)
                    {
                        if !tried_peer_configs.contains(&alt_config) {
                            peer_config = alt_config;
                            if retry_due_to_err.is_none() {
                                retry_due_to_err = Some(err.to_string());
                            }
                            continue;
                        }
                    }

                    self.mk_invalid_message_result(
                        format!(
                            "Invalid BMP RouteMonitoring BGP UPDATE message: ({:?}) {}",
                            &peer_config, err
                        ),
                        Some(true),
                        Some(Bytes::copy_from_slice(msg.as_ref())),
                    )
                }
            };

            break res;
        }
    }

    // This is the method that explodes the RoutingMonitoringMessage into
    // multiple routes.
    pub fn extract_route_monitoring_routes(
        &mut self,
        received: std::time::Instant,
        pph: PerPeerHeader<Bytes>,
        bgp_msg: &UpdateMessage<Bytes>,
        //route_status: NlriStatus,
        _trace_id: Option<u8>,
    ) -> Result<(SmallVec<[Payload; 8]>, UpdateReportMessage), session::Error>
    {
        let rr_reach = explode_announcements(bgp_msg)?;
        let rr_unreach = explode_withdrawals(bgp_msg)?;

        let ingress_id = if let Some(ingress_id) =
            self.details.get_peer_ingress_id(&pph)
        {
            ingress_id
        } else {
            error!("no ingress_id for {:?}", &pph);
            return Err(session::Error::for_str("missing ingress_id"));
        };

        let mut payloads = SmallVec::new();
        let mut update_report_msg =
            UpdateReportMessage::new(self.router_id.clone());


        if !rr_reach.is_empty() {
            //update_report_msg.inc_valid_announcements();
            update_report_msg.n_new_prefixes = rr_reach.len();
        }
        //if rr_unreach.len() > 0 {
        //    update_report_msg.inc_valid_withdrawals();
        //}

        payloads.extend(
            //rws.into_iter().map(|rws| mk_payload(rws, received, context.clone()))
            rr_reach.into_iter().map(|rr| {
                update_report_msg.inc_valid_announcements();
                Payload::with_received(
                    rr,
                    None,
                    received,
                    ingress_id,
                    RouteStatus::Active,
                )
            }),
        );

        payloads.extend(rr_unreach.into_iter().map(|rr| {
            update_report_msg.inc_valid_withdrawals();
            Payload::with_received(
                rr,
                None,
                received,
                ingress_id,
                RouteStatus::Withdrawn
            )
        }));

        Ok((payloads, update_report_msg))

        // we need to turn the encapsulated BGP UPDATE into rotonda Payloads
        //
        // - similar to bgp explode_announcements/withdrawals
        // - find and attach correct ingress_id
        // - construct provenance

        /*
        let mut payloads: SmallVec<[Payload; 8]> = SmallVec::new();
        let mut update_report_msg =
            UpdateReportMessage::new(self.router_id.clone());

        let target = bytes::BytesMut::new();

        let path_attributes = routecore::bgp::message::update_builder::UpdateBuilder::from_update_message(
                bgp_msg,
                &SessionConfig::modern(),
                target
            ).map_err(|_| session::Error::for_str("Cannot parse BGP message"))?;

        let mut router_id = DefaultHasher::new();
        self.router_id.hash(&mut router_id);
        // router_id.finish();

        // let mut source_id = DefaultHasher::new();
        // self.hash(&mut source_id);
        // self.source_id.hash(&mut source_id);

        let provenance = Provenance {
            timestamp: pph.timestamp(),
            // router_id: router_id.finish() as u32,
            peer_id: PeerId::new(pph.address(), pph.asn()),
            peer_bgp_id: pph.bgp_id().into(),
            peer_distuingisher: <[u8; 8]>::try_from(pph.distinguisher()).unwrap(),
            peer_rib_type: PeerRibType::from((pph.is_post_policy(), pph.adj_rib_type())),
            connection_id: self.source_id.socket_addr(),
        };

        for a in bgp_msg.typed_announcements()?.unwrap() {
            a.unwrap().is_prefix();
            match a {
                Ok(a) => {
                    match a {
                        Nlri::Unicast(nlri) | Nlri::Multicast(nlri) => {
                            let prefix = nlri.prefix;
                            if self.details.add_announced_prefix(&pph, prefix)
                            {
                                update_report_msg.inc_new_prefixes();
                            }

                            // clone is cheap due to use of Bytes
                            let route = RouteWorkshop::<BasicNlri>::new(
                                BasicNlri::new(prefix)
                                // None,
                                // a.afi_safi(),
                                // path_attributes.attributes().clone(),
                                // route_status,
                            );

                            payloads.push(Payload::with_received(
                                // self.source_id.clone(),
                                route,
                                Some(provenance),
                                // Some(bgp_msg.clone()),
                                trace_id,
                                received,
                            ));
                            update_report_msg.inc_valid_announcements();
                        }
                        _ => {
                            // We'll count 'em, but we don't do anything with 'em.
                            update_report_msg.inc_valid_announcements();
                        }
                    }
                }
                Err(err) => {
                    update_report_msg.inc_invalid_announcements();
                    update_report_msg.set_invalid_announcement(err);
                }
            }
        }

        for nlri in bgp_msg.withdrawals()? {
            match nlri {
                Ok(nlri) => {
                    if let Nlri::Unicast(wd) = nlri {
                        let prefix = wd.prefix();

                        // RFC 4271 section "4.3 UPDATE Message Format" states:
                        //
                        // "An UPDATE message SHOULD NOT include the same address prefix in the
                        //  WITHDRAWN ROUTES and Network Layer Reachability Information fields.
                        //  However, a BGP speaker MUST be able to process UPDATE messages in
                        //  this form.  A BGP speaker SHOULD treat an UPDATE message of this form
                        //  as though the WITHDRAWN ROUTES do not contain the address prefix.

                        // RFC7606 though? What a can of worms this is.

                        if bgp_msg
                            .unicast_announcements_vec()
                            .unwrap()
                            .iter()
                            .all(|nlri| nlri.prefix != prefix)
                        {

                            let route = RouteWorkshop::<BasicNlri>::new(
                                    BasicNlri { prefix,
                                    path_id: wd.path_id(), }
                                    // nlri.afi_safi(),
                                    // path_attributes.attributes().clone(),
                                    // NlriStatus::Withdrawn,
                            );

                            payloads.push(Payload::with_received(
                                // self.source_id.clone(),
                                route,
                                Some(provenance),
                                // Some(bgp_msg.clone()),
                                trace_id,
                                received,
                            ));

                            self.details
                                .remove_announced_prefix(&pph, &prefix);
                            update_report_msg.inc_valid_withdrawals();
                        }
                    }
                }
                Err(err) => {
                    update_report_msg.inc_invalid_withdrawals();
                    update_report_msg.set_invalid_withdrawal(err);
                }
            }
        }

        Ok((payloads, update_report_msg))
            */
    }
}

impl BmpState {
    pub fn new<T: AnyStatusReporter>(
        source_id: ingress::IngressId,
        router_id: Arc<RouterId>,
        parent_status_reporter: Arc<T>,
        metrics: Arc<BmpStateMachineMetrics>,
        ingress_register: Arc<ingress::Register>,
    ) -> Self {
        let child_name = parent_status_reporter.link_names("bmp_state");
        let status_reporter =
            Arc::new(BmpStateMachineStatusReporter::new(child_name, metrics));

        BmpState::Initiating(BmpStateDetails::<Initiating>::new(
            source_id,
            router_id,
            status_reporter,
            ingress_register.clone(),
        ))
    }

    #[allow(dead_code)]
    pub fn process_msg(
        self,
        received: std::time::Instant,
        bmp_msg: BmpMsg<Bytes>,
        trace_id: Option<u8>,
    ) -> ProcessingResult {
        let res = match self {
            BmpState::Initiating(inner) => {
                inner.process_msg(bmp_msg, trace_id)
            }
            BmpState::Dumping(inner) => {
                inner.process_msg(received, bmp_msg, trace_id)
            }
            BmpState::Updating(inner) => {
                inner.process_msg(received, bmp_msg, trace_id)
            }
            BmpState::Terminated(inner) => {
                inner.process_msg(bmp_msg, trace_id)
            }
            BmpState::_Aborted(source_id, router_id) => {
                ProcessingResult::new(
                    MessageType::Aborted,
                    BmpState::_Aborted(source_id, router_id),
                )
            }
        };

        if let ProcessingResult {
            message_type:
                MessageType::InvalidMessage {
                    known_peer: _known_peer,
                    msg_bytes,
                    err,
                },
            next_state,
        } = res
        {
            if let Some(reporter) = next_state.status_reporter() {
                reporter.bgp_update_parse_hard_fail(
                    next_state.router_id(),
                    err.clone(),
                    msg_bytes,
                );
            }

            ProcessingResult::new(
                MessageType::InvalidMessage {
                    known_peer: None,
                    msg_bytes: None,
                    err,
                },
                next_state,
            )
        } else {
            res
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
        ingress_register: Arc<ingress::Register>,
        bmp_ingress_id: ingress::IngressId,
        mut tlv_iter: InformationTlvIter,
    ) -> (bool, Option<IngressId>) {
        let mut added = false;

        let mut query_ingress = ingress::IngressInfo::new()
            .with_ingress_type(ingress::IngressType::BgpViaBmp)
            .with_parent_ingress(bmp_ingress_id)
            .with_remote_addr(pph.address())
            .with_remote_asn(pph.asn())
            .with_rib_type(pph.rib_type())
            .with_peer_rib_type((pph.is_post_policy(), pph.rib_type()))
            .with_peer_type(pph.peer_type())
        ;
        use routecore::bmp::message::PeerType;
        match pph.peer_type() {
            PeerType::GlobalInstance => { /* no Peer Distinguisher to set */ },
            PeerType::RdInstance |
            PeerType::LocalInstance |
            PeerType::LocalRibInstance =>  {
                query_ingress = query_ingress
                    .with_distinguisher(TryInto::<[u8; 8]>::try_into(&pph.distinguisher()[..8]).unwrap());
            },
            PeerType::Reserved |
            PeerType::Unassigned(_) |
            PeerType::Experimental(_) |
            PeerType::Unimplemented(_) => { 
                warn!("Reserved/Unassigned Peer Type");
            },
        }

        if let Some(vrf_name) = tlv_iter
            .find(|t| t.typ() == InformationTlvType::VrfTableName) {
                query_ingress = query_ingress.with_vrf_name(
                    String::from_utf8_lossy(vrf_name.value())
                );
        }

        let peer_ingress_id;
        let existing_peer_ingress_id;
        if let Some((ingress_id, _ingress_info)) =
            ingress_register.find_existing_peer(&query_ingress)
        {
            //debug!("got existing ingress_id for BGP in BMP peer {}", ingress_id); 
            peer_ingress_id = ingress_id;
            existing_peer_ingress_id = Some(ingress_id);
        } else {
            //debug!("no existing ingress_id for BGP in BMP");
            peer_ingress_id = ingress_register.register();
            existing_peer_ingress_id = None;
            ingress_register.update_info(peer_ingress_id, query_ingress);
        }

        let _ = self.0.entry(pph.clone()).or_insert_with(|| {
            added = true;
            PeerState {
                session_config,
                eor_capable,
                pending_eors: HashSet::with_capacity(0),
                peer_details: PeerDetails {
                    peer_bgp_id: pph.bgp_id(),
                    peer_distinguisher: pph
                        .distinguisher()
                        .try_into()
                        .unwrap(),
                    peer_rib_type: pph.rib_type(),
                    peer_id: PeerId::new(pph.address(), pph.asn()),
                },
                ingress_id: peer_ingress_id,
            }
        });
        (added, existing_peer_ingress_id)
    }

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState> {
        self.0.keys()
    }

    fn get_peer_ingress_id(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<ingress::IngressId> {
        self.0.get(pph).map(|e| e.ingress_id)
    }

    fn update_peer_config(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        new_config: SessionConfig,
    ) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state.session_config = new_config;
            peer_state.peer_details = PeerDetails {
                peer_bgp_id: pph.bgp_id(),
                peer_distinguisher: pph.distinguisher().try_into().unwrap(),
                peer_rib_type: pph.rib_type(),
                peer_id: PeerId::new(pph.address(), pph.asn()),
            };
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

    fn add_cloned_peer_config(
        &mut self,
        source_pph: &PerPeerHeader<Bytes>,
        dst_pph: &PerPeerHeader<Bytes>,
        ingress_id: IngressId,
    ) -> bool {
        let mut peer_state = self.0.get(source_pph).unwrap().clone();
        peer_state.ingress_id = ingress_id;
        if let Some(_existing) = self.0.insert(dst_pph.clone(), peer_state) {
            warn!("Unexpected existing PeerState while trying to add_cloned_peer_config");
            false
        } else {
            true
        }
    }



    //fn remove_peer(&mut self, pph: &PerPeerHeader<Bytes>) -> bool {
    fn remove_peer(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<PeerState> {
        self.0.remove(pph) //.is_some()
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
        afi_safi: AfiSafiType,
    ) -> usize {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state
                .pending_eors
                .insert(EoRProperties::new(pph, afi_safi));

            peer_state.pending_eors.len()
        } else {
            0
        }
    }

    fn remove_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi_safi: AfiSafiType,
    ) -> bool {
        if let Some(peer_state) = self.0.get_mut(pph) {
            peer_state
                .pending_eors
                .remove(&EoRProperties::new(pph, afi_safi));
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
}
