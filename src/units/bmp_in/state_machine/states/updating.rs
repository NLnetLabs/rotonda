use std::{ops::ControlFlow, collections::hash_map::Keys};

use bytes::Bytes;
use roto::types::builtin::{BgpUpdateMessage, RouteStatus};
use routecore::{
    addr::Prefix,
    bgp::{
        message::{SessionConfig, UpdateMessage},
        types::{AFI, SAFI},
    },
    bmp::message::{Message as BmpMsg, PerPeerHeader, TerminationMessage},
};
use smallvec::SmallVec;

use crate::{units::bmp_in::state_machine::machine::{PeerStates, PeerState}, payload::{Payload, Update}};

use super::super::{
    machine::{BmpState, BmpStateDetails, Initiable, PeerAware},
    processing::ProcessingResult,
};

use super::dumping::Dumping;

/// BmpState machine state 'Updating'.
///
/// Expecting incremental updates (messages that change the initial state).
///
/// > Following the initial table dump, the router sends incremental
/// > updates encapsulated in Route Monitoring messages.  It MAY
/// > periodically send Stats Reports or even new Initiation messages,
/// > according to configuration.  If any new monitored BGP peer becomes
/// > Established, a corresponding Peer Up message is sent.  If any BGP
/// > peers for which Peer Up messages were sent transition out of the
/// > Established state, corresponding Peer Down messages are sent.
/// >
/// > A BMP session ends when the TCP session that carries it is closed
/// > for any reason.  The router MAY send a Termination message prior to
/// > closing the session.
/// >
/// > -- <https://datatracker.ietf.org/doc/html/rfc7854#section-3.3>
#[derive(Debug, Default)]
pub struct Updating {
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

impl BmpStateDetails<Updating> {
    pub async fn process_msg(self, msg_buf: Bytes) -> ProcessingResult {
        self.process_msg_with_filter(msg_buf, None::<()>, |msg, _| Ok(ControlFlow::Continue(msg))).await
    }

    /// `filter` should return true if the BGP message should be ignored, i.e. be filtered out.
    pub async fn process_msg_with_filter<F, D>(
        self,
        msg_buf: Bytes,
        filter_data: Option<D>,
        filter: F,
    ) -> ProcessingResult
    where
        F: Fn(BgpUpdateMessage, Option<D>) -> Result<ControlFlow<(), BgpUpdateMessage>, String>
    {
        match BmpMsg::from_octets(msg_buf).unwrap() {
            // already verified upstream
            BmpMsg::InitiationMessage(msg) => self.initiate(msg).await,

            BmpMsg::PeerUpNotification(msg) => self.peer_up(msg).await,

            BmpMsg::PeerDownNotification(msg) => self.peer_down(msg).await,

            BmpMsg::RouteMonitoring(msg) => {
                self.route_monitoring(
                    msg,
                    RouteStatus::UpToDate,
                    filter_data,
                    filter,
                    |s, pph, update| s.route_monitoring_preprocessing(pph, update),
                )
                .await
            }

            BmpMsg::TerminationMessage(msg) => self.terminate(Some(msg)).await,

            _ => {
                // We ignore these. TODO: Should we count them?
                self.mk_other_result()
            }
        }
    }
}

impl BmpStateDetails<Updating> {
    pub fn route_monitoring_preprocessing(
        mut self,
        pph: &PerPeerHeader<Bytes>,
        update: &UpdateMessage<Bytes>,
    ) -> ControlFlow<ProcessingResult, Self> {
        if let Some((afi, safi)) = update.is_eor() {
            if self.details.remove_pending_eor(pph, afi, safi) {
                let num_pending_eors = self.details.num_pending_eors();
                self.status_reporter
                    .pending_eors_update(self.router_id.clone(), num_pending_eors);
            }
        }

        ControlFlow::Continue(self)
    }

    pub async fn terminate(mut self, _msg: Option<TerminationMessage<Bytes>>) -> ProcessingResult {
        // let reason = msg
        //     .information()
        //     .map(|tlv| tlv.to_string())
        //     .collect::<Vec<_>>()
        //     .join("|");
        let peers = self.details.get_peers().cloned().collect::<Vec<_>>();
        let routes: SmallVec<[Payload; 8]> = peers.iter().flat_map(|pph| self.do_peer_down(pph)).collect();
        let next_state = BmpState::Terminated(self.into());
        if routes.is_empty() {
            Self::mk_state_transition_result(next_state)
        } else {
            Self::mk_final_routing_update_result(next_state, Update::Bulk(routes.into()))
        }
    }
}

impl From<Dumping> for Updating {
    fn from(v: Dumping) -> Self {
        Self {
            sys_name: v.sys_name,
            sys_desc: v.sys_desc,
            sys_extra: v.sys_extra,
            peer_states: v.peer_states,
        }
    }
}

impl From<BmpStateDetails<Dumping>> for BmpStateDetails<Updating> {
    fn from(v: BmpStateDetails<Dumping>) -> Self {
        let details: Updating = v.details.into();
        Self {
            addr: v.addr,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details,
        }
    }
}

impl Initiable for Updating {
    fn set_information_tlvs(&mut self, sys_name: String, sys_desc: String, sys_extra: Vec<String>) {
        self.sys_name = sys_name;
        self.sys_desc = sys_desc;
        self.sys_extra = sys_extra;
    }

    fn sys_name(&self) -> Option<&str> {
        Some(self.sys_name.as_str())
    }
}

impl PeerAware for Updating {
    fn add_peer_config(
        &mut self,
        pph: PerPeerHeader<Bytes>,
        session_config: SessionConfig,
        eor_capable: bool,
    ) -> bool {
        self.peer_states
            .add_peer_config(pph, session_config, eor_capable)
    }

    fn get_peers(&self) -> Keys<'_, PerPeerHeader<Bytes>, PeerState> {
        self.peer_states.get_peers()
    }

    fn update_peer_config(&mut self, pph: &PerPeerHeader<Bytes>, config: SessionConfig) -> bool {
        self.peer_states.update_peer_config(pph, config)
    }

    fn get_peer_config(&self, pph: &PerPeerHeader<Bytes>) -> Option<&SessionConfig> {
        self.peer_states.get_peer_config(pph)
    }

    fn remove_peer_config(&mut self, pph: &PerPeerHeader<Bytes>) -> bool {
        self.peer_states.remove_peer_config(pph)
    }

    fn num_peer_configs(&self) -> usize {
        self.peer_states.num_peer_configs()
    }

    fn is_peer_eor_capable(&self, pph: &PerPeerHeader<Bytes>) -> Option<bool> {
        self.peer_states.is_peer_eor_capable(pph)
    }

    fn add_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI) {
        self.peer_states.add_pending_eor(pph, afi, safi)
    }

    fn remove_pending_eor(&mut self, pph: &PerPeerHeader<Bytes>, afi: AFI, safi: SAFI) -> bool {
        self.peer_states.remove_pending_eor(pph, afi, safi)
    }

    fn num_pending_eors(&self) -> usize {
        self.peer_states.num_pending_eors()
    }

    fn add_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: routecore::addr::Prefix,
    ) -> bool {
        self.peer_states.add_announced_prefix(pph, prefix)
    }

    fn remove_announced_prefix(
        &mut self,
        pph: &PerPeerHeader<Bytes>,
        prefix: &routecore::addr::Prefix,
    ) {
        self.peer_states.remove_announced_prefix(pph, prefix)
    }

    fn get_announced_prefixes(&self, pph: &PerPeerHeader<Bytes>) -> Option<std::collections::hash_set::Iter<Prefix>> {
        self.peer_states.get_announced_prefixes(pph)
    }
}
