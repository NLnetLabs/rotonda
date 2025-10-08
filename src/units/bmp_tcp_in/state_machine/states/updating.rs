use std::{collections::hash_map::Keys, ops::ControlFlow, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::debug;
//use roto::types::builtin::NlriStatus;
use inetnum::addr::Prefix;
use routecore::{
    bgp::{
        message::{SessionConfig, UpdateMessage},
        nlri::afisafi::Nlri,
        types::AfiSafiType,
    },
    bmp::message::{InformationTlvIter, Message as BmpMsg, PerPeerHeader, TerminationMessage},
};
use smallvec::SmallVec;

use crate::{
    ingress::{self, IngressId},
    payload::{Payload, Update},
    units::bmp_tcp_in::state_machine::machine::{
        BmpStateIdx, PeerState, PeerStates,
    },
};

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

            BmpMsg::PeerUpNotification(msg) => self.peer_up(msg),

            BmpMsg::PeerDownNotification(msg) => self.peer_down(msg),

            BmpMsg::RouteMonitoring(msg) => self.route_monitoring(
                received,
                msg,
                //NlriStatus::UpToDate,
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

impl BmpStateDetails<Updating> {
    pub fn route_monitoring_preprocessing(
        mut self,
        pph: &PerPeerHeader<Bytes>,
        update: &UpdateMessage<Bytes>,
    ) -> ControlFlow<ProcessingResult, Self> {
        if let Ok(Some(afi_safi)) = update.is_eor() {
            if self.details.remove_pending_eor(pph, afi_safi) {
                let num_pending_eors = self.details.num_pending_eors();
                self.status_reporter.pending_eors_update(
                    self.router_id.clone(),
                    num_pending_eors,
                );
            }
        }

        ControlFlow::Continue(self)
    }

    pub fn terminate(
        self,
        _msg: Option<TerminationMessage<Bytes>>,
    ) -> ProcessingResult {
        debug!("updating terminate");
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
        let routes: SmallVec<[Payload; 8]> = peers
            .iter()
            .flat_map(|pph| self.mk_withdrawals_for_peers_routes(pph))
            .collect();
        let next_state = BmpState::Terminated(self.into());
        if routes.is_empty() {
            Self::mk_state_transition_result(
                BmpStateIdx::Updating,
                next_state,
            )
        } else {
            Self::mk_final_routing_update_result(
                next_state,
                Update::Bulk(routes),
            )
        }
        */
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
            ingress_id: v.ingress_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            ingress_register: v.ingress_register,
            details,
        }
    }
}

impl Initiable for Updating {
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

impl PeerAware for Updating {
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
