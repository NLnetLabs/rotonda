use std::{
    collections::hash_map::Keys, fmt::Debug, ops::ControlFlow, sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use inetnum::addr::Prefix;
use log::debug;
use routecore::bgp::nlri::afisafi::Nlri;
use routecore::bgp::{
    message::{SessionConfig, UpdateMessage},
    types::AfiSafiType,
};
use routecore::bmp::message::{
    InformationTlvIter, Message as BmpMsg, PerPeerHeader, TerminationMessage
};
use smallvec::SmallVec;

use crate::ingress::IngressId;
use crate::{
    ingress,
    payload::{Payload, Update},
    units::bmp_tcp_in::state_machine::machine::{
        BmpStateIdx, PeerState, PeerStates,
    },
};

use super::initiating::Initiating;

use super::super::{
    machine::{BmpState, BmpStateDetails, Initiable, PeerAware},
    processing::ProcessingResult,
};

/// BmpState machine state 'Dumping'.
///
/// Expecting Peer Up messages followed by Route Monitoring messages.
///
/// The initial table dump is considered finished when all "up" peers (peers
/// for which we see a PeerUpNotification during the dumping phase and for
/// which no corresponding PeerDownNotification is seen) that are able to send
/// End-of-RIB have done so. The next phase will then be the "Updating" phase.
///
/// If at least one Peer Up Notification is received for a peer that does not
/// advertise support for Graceful Restart (and thus sending of the End-of-RIB
/// marker) we will never progress to the "Updating" phase.
///
/// The "Dumping" and "Updating" phases are almost equivalent, the reason to
/// treat them separately is to be able to detect and report End-of-RIB
/// events.
///
/// > **3.3  Lifecycle of a BMP session**
/// >
/// > ...
/// >
/// > It subsequently sends a Peer Up message over the BMP session for each of
/// > its monitored BGP peers that is in the Established state.
/// >
/// > -- <https://datatracker.ietf.org/doc/html/rfc7854#section-3.3>
///
/// So we should expect to see Peer Up messages.
///
/// > It follows by sending the contents of its Adj-RIBs-In (pre-policy,
/// > post-policy, or both, see Section 5) encapsulated in Route Monitoring
/// > messages.
///
/// So we should expect to see Route Monitoring messages.
///
/// > Once it has sent all the routes for a given peer, it MUST send an
/// > End-of-RIB message for that peer; when End-of-RIB has been sent for each
/// > monitored peer, the initial table dump has completed."
///
/// ... hence the name 'Dumping'.
///
/// End-of-RIB is a Route Monitoring message with a particular value that when
/// seen should be interpreted as meaning that the peer has finished the
/// initial table dump. However, this interpretation was not part of the
/// original BGP specification and so a peer may never send it, or send it
/// without meaning it to mean End-of-RIB. End-of-RIB was defined by RFC 4724
/// "Graceful Restart Mechanism for BGP" in section "2. Marker for End-of-RIB"
/// [^1]. Peers that know that they will send End-of-RIB should indicate this
/// according to RFC 4724 by indicating support for the "Graceful Restart
/// Capability" (capability code 64). The mechanism for a peer to indicate its
/// capabilities is defined by RFC 3392 [^2] later obsoleted by RFC 5492 [^3].
/// RFCs 3392 and 5492 define that a peer can indicate support for one or more
/// capabilities by including an "Optional Parameter" called "Capabilities" in
/// its BGP OPEN message. From a BMP perspective, the BGP OPEN message is
/// visible to us via the BMP Peer Up Notification message.
///
/// > (A monitoring station that only wants to gather a table dump could close
/// > the connection once it has gathered an End-of-RIB or Peer Down message
/// > corresponding to each Peer Up message.)
///
/// And:
///
/// > Following the initial table dump, the router sends incremental updates
/// > encapsulated in Route Monitoring messages.  It MAY periodically send
/// > Stats Reports or even new Initiation messages, according to
/// > configuration.  If any new monitored BGP peer becomes Established, a
/// > corresponding Peer Up message is sent.  If any BGP peers for which Peer
/// > Up messages were sent transition out of the Established state,
/// > corresponding Peer Down messages are sent.
///
/// So we should expect to see Peer Down messages. The above text would seem
/// to indicate that we should not expect to see Stats Reports or Initiation
/// Messages during the dumping phase. However, the following section of the
/// RFC seems to say we should support seeing an Initiation Message at any
/// time, and section 4.8 seems to indicate that we should also expect to see
/// Stats Reports messages at any time too.
///
/// > **4.3.  Initiation Message**
/// >
/// > The initiation message provides a means for the monitored router to
/// > inform the monitoring station of its vendor, software version, and so
/// > on.  An initiation message MUST be sent as the first message after the
/// > TCP session comes up.  An initiation message MAY be sent at any point
/// > thereafter, if warranted by a change on the monitored router.
///
/// So we should expect to see Initiation messages.
///
/// > **4.7.  Route Mirroring**
/// >
/// > ...
/// >
/// > A Route Mirroring message may be sent any time it would be legal to send
/// > a Route Monitoring message.
///
/// So we should expect to see Route Mirroring messages.
///
/// > **4.8.  Stats Reports**
/// >
/// > These messages contain information that could be used by the monitoring
/// > station to observe interesting events that occur on the router.
/// >
/// > Transmission of SR messages could be timer triggered or event driven
/// > (for example, when a significant event occurs or a threshold is
/// > reached).  This specification does not impose any timing restrictions on
/// > when and on what event these reports have to be transmitted.
///
/// So we should expect to see Stats Reports messages.
///
/// Also, some of the counters and gauges included in Stats Reports messages
/// might be useful as a way to sanity check things like the number of routes
/// seen so far, but it's not clear to me if one can assume that the point at
/// which the Stats Report message is seen is the point at which the counters
/// should be equivalent to the count of counted messages of the corresponding
/// type seen until this point.
///
/// > **9.  Using BMP**
/// >
/// > Once the BMP session is established, route monitoring starts dumping the
/// > current snapshot as well as incremental changes simultaneously.
///
/// This would seem to indicate that changes to the routing table may be seen
/// while receiving the initial table dump. We separate our processing into
/// "Dumping" and "Updating" phases but in reality there may be no such hard
/// separation between the phases and thus we must share behaviour with the
/// "Updating" phase.
///
/// [^1]: [RFC 4724: Graceful Restart Mechanism for BGP, section 2: Marker for
///     End-of-RIB](https://datatracker.ietf.org/doc/html/rfc4724#section-2)
///
/// [^2]: [RFC 3392: Capabilities Advertisement with
///     BGP-4](https://datatracker.ietf.org/doc/html/rfc3392)
///
/// [^3]: [RFC 5492: Capabilities Advertisement with
///     BGP-4](https://datatracker.ietf.org/doc/html/rfc5492)
#[derive(Debug, Default)]
pub struct Dumping {
    /// The name given by the Initiation Message for the router.
    pub sys_name: String,

    /// Additional description required to be provided by the Initiation
    /// Message.
    pub sys_desc: String,

    /// Optional additional strings provided by the Initiation Message.
    pub sys_extra: Vec<String>,

    /// State per peer for which a Peer Up Notification was received.
    pub peer_states: PeerStates,
}

impl BmpStateDetails<Dumping> {
    #[allow(dead_code)]
    pub fn process_msg(
        self,
        received: std::time::Instant,
        bmp_msg: BmpMsg<Bytes>,
        trace_id: Option<u8>,
    ) -> ProcessingResult {
        match bmp_msg {
            // already verified upstream
            BmpMsg::InitiationMessage(msg) => self.initiate(msg),

            BmpMsg::PeerUpNotification(msg) => {
                let res = self.peer_up(msg);
                if let BmpState::Dumping(state) = &res.next_state {
                    let num_pending_eors =
                        state.details.peer_states.num_pending_eors();
                    state.status_reporter.pending_eors_update(
                        state.router_id.clone(),
                        num_pending_eors,
                    );
                }
                res
            }

            BmpMsg::PeerDownNotification(msg) => self.peer_down(msg),

            BmpMsg::RouteMonitoring(msg) => self.route_monitoring(
                received,
                msg,
                //NlriStatus::InConvergence,
                trace_id,
                |s, pph, update| {
                    s.route_monitoring_preprocessing(pph, update)
                },
            ),

            BmpMsg::TerminationMessage(msg) => self.terminate(Some(msg)),

            _ => {
                // We ignore these. TODO: Should we count them?
                self.mk_other_result()
            }
        }
    }
}

impl BmpStateDetails<Dumping> {
    pub fn route_monitoring_preprocessing(
        mut self,
        pph: &PerPeerHeader<Bytes>,
        update: &UpdateMessage<Bytes>,
    ) -> ControlFlow<ProcessingResult, Self> {
        if let Ok(Some(afi_safi)) = update.is_eor() {
            if self
                .details
                .remove_pending_eor(pph, afi_safi)
            {
                // The last pending EOR has been removed and so this signifies
                // the end of the initial table dump, if we're in the Dumping
                // state, otherwise in the Updating state it signifies only
                // that a late Peer Up (that happened during the Updating
                // state) has finished dumping. Unfortunately EoR is not a
                // clearly defined concept in BMP (e.g. see [1]), is it peer
                // peer, per (S)AFI, per rib? Does advertising support for the
                // Graceful Restart BGP capability have relevance for a BGP
                // client, e.g. can its presence be used to assume that EoRs
                // will be present for that peer in the BMP feed? Etc. There
                // also seems to be varying implementations in deployed
                // routers, whether because of deliberate behavioural
                // differences, differing understanding or misunderstanding of
                // RFCs, implementation bugs, etc.
                //
                // TODO: We may need some sort of heuristic to ascertain the
                // likelihood that the initial table dump has actually
                // completed.
                //
                // [1]: https://www.rfc-editor.org/errata/eid7133
                let num_pending_eors = self.details.num_pending_eors();
                self.status_reporter.pending_eors_update(
                    self.router_id.clone(),
                    num_pending_eors,
                );

                return ControlFlow::Break(Self::mk_state_transition_result(
                    BmpStateIdx::Dumping,
                    BmpState::Updating(self.into()),
                ));
            }
        }

        ControlFlow::Continue(self)
    }

    pub fn terminate<Octets: AsRef<[u8]>>(
        self,
        _msg: Option<TerminationMessage<Octets>>,
    ) -> ProcessingResult {
        debug!("dumping terminate()");

        //LH: with the new store, we only need the ingress_id/MUI for all
        //sessions monitored by this BMP connection. The store will then take
        //care of marking all previously learned routes as withdrawn.

        let mut ids = SmallVec::<[ingress::IngressId; 8]>::new();
        for pph in self.details.get_peers() {
            if let Some(id) = self.details.get_peer_ingress_id(pph) {
                ids.push(id);
            }
        }

        let next_state = BmpState::Terminated(self.into());

        if ids.is_empty() {
            Self::mk_state_transition_result(BmpStateIdx::Dumping, next_state)
        } else {
            let update = Update::WithdrawBulk(ids);

            Self::mk_final_routing_update_result(next_state, update)
        }

        /*
        // let reason = msg
        //     .information()
        //     .map(|tlv| tlv.to_string())
        //     .collect::<Vec<_>>()
        //     .join("|");
        let peers = self.details.get_peers().cloned().collect::<Vec<_>>();
        debug!("peers: {:?}", &peers);
        let routes: SmallVec<[Payload; 8]> = peers
            .iter()
            .flat_map(|pph| self.mk_withdrawals_for_peers_routes(pph))
            .collect();
        let next_state = BmpState::Terminated(self.into());
        if routes.is_empty() {
            Self::mk_state_transition_result(BmpStateIdx::Dumping, next_state)
        } else {
            Self::mk_final_routing_update_result(
                next_state,
                Update::Bulk(routes),
            )
        }
        */
    }
}

impl From<Initiating> for Dumping {
    fn from(v: Initiating) -> Self {
        // A missing sysName should have been handled before now so it should
        // be safe to unwrap it, and we need a name for the router to aid in
        // diagnosing and labeling the data we produce.
        Self {
            sys_name: v.sys_name.unwrap(),
            sys_desc: v.sys_desc.unwrap(),
            sys_extra: v.sys_extra,
            ..Default::default()
        }
    }
}

/// Support transitioning from the Initiating state to the Dumping state. This
/// is the only way to create an instance of the BmpStateDetails<Dumping> type.
impl From<BmpStateDetails<Initiating>> for BmpStateDetails<Dumping> {
    fn from(v: BmpStateDetails<Initiating>) -> Self {
        let details: Dumping = v.details.into();
        Self {
            ingress_id: v.ingress_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            ingress_register: v.ingress_register,
            details,
        }
    }
}

impl Initiable for Dumping {
    fn set_information_tlvs(
        &mut self,
        sys_name: String,
        sys_desc: String,
        sys_extra: Vec<String>,
    ) {
        self.sys_name = sys_name;
        self.sys_desc = sys_desc;
        self.sys_extra = sys_extra;
    }

}

impl PeerAware for Dumping {
    fn add_peer_config(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        session_config: SessionConfig,
        eor_capable: bool,
        ingress_register: Arc<ingress::Register>,
        bmp_ingress_id: ingress::IngressId,
        tlv_iter: InformationTlvIter,
    ) -> (bool, Option<IngressId>) {
        self.peer_states.add_peer_config(
            pph,
            session_config,
            eor_capable,
            ingress_register,
            bmp_ingress_id,
            tlv_iter,
        )
    }

    fn add_cloned_peer_config(
        &mut self,
        source_pph: &PerPeerHeader<Bytes>,
        dst_pph: &PerPeerHeader<Bytes>,
        ingress_id: IngressId,
    ) -> bool {
        self.peer_states.add_cloned_peer_config(source_pph, dst_pph, ingress_id)
    }

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState> {
        self.peer_states.get_peers()
    }

    fn get_peer_ingress_id(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<ingress::IngressId> {
        self.peer_states.get_peer_ingress_id(pph)
    }

    fn update_peer_config(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        config: SessionConfig,
    ) -> bool {
        self.peer_states.update_peer_config(pph, config)
    }

    fn get_peer_config(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<&SessionConfig> {
        self.peer_states.get_peer_config(pph)
    }

    fn remove_peer(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<PeerState> {
        self.peer_states.remove_peer(pph)
    }

    fn is_peer_eor_capable(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<bool> {
        self.peer_states.is_peer_eor_capable(pph)
    }

    fn add_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi_safi: AfiSafiType,
    ) -> usize {
        self.peer_states.add_pending_eor(pph, afi_safi)
    }

    fn remove_pending_eor(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        afi_safi: AfiSafiType,
    ) -> bool {
        self.peer_states.remove_pending_eor(pph, afi_safi)
    }

    fn num_pending_eors(&self) -> usize {
        self.peer_states.num_pending_eors()
    }

}
