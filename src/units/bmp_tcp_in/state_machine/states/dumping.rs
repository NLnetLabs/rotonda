use std::{collections::hash_map::Keys, fmt::Debug, ops::ControlFlow};

use bytes::Bytes;
use roto::types::builtin::RouteStatus;
use routecore::{
    addr::Prefix,
    bgp::{
        message::{SessionConfig, UpdateMessage},
        types::{AFI, SAFI},
    },
    bmp::message::{Message as BmpMsg, PerPeerHeader, TerminationMessage},
};
use smallvec::SmallVec;

use crate::{
    payload::{Payload, Update},
    units::bmp_tcp_in::state_machine::machine::{PeerState, PeerStates},
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
/// > It subsequently sends a Peer Up message over the BMP session for
/// > each of its monitored BGP peers that is in the Established state.
/// >
/// > -- <https://datatracker.ietf.org/doc/html/rfc7854#section-3.3>
///
/// So we should expect to see Peer Up messages.
///
/// > It follows by sending the contents of its Adj-RIBs-In (pre-policy,
/// > post-policy, or both, see Section 5) encapsulated in Route
/// > Monitoring messages.
///
/// So we should expect to see Route Monitoring messages.
///
/// > Once it has sent all the routes for a given peer, it MUST send an
/// > End-of-RIB message for that peer; when End-of-RIB has been sent for
/// > each monitored peer, the initial table dump has completed."
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
/// > on.  An initiation message MUST be sent as the first message after
/// > the TCP session comes up.  An initiation message MAY be sent at any
/// > point thereafter, if warranted by a change on the monitored router.
///
/// So we should expect to see Initiation messages.
///
/// > **4.7.  Route Mirroring**
/// >
/// > ...
/// >
/// > A Route Mirroring message may be sent any time it would be legal to
/// > send a Route Monitoring message.
///
/// So we should expect to see Route Mirroring messages.
///
/// > **4.8.  Stats Reports**
/// >
/// > These messages contain information that could be used by the
/// > monitoring station to observe interesting events that occur on the
/// > router.
/// >
/// > Transmission of SR messages could be timer triggered or event driven
/// > (for example, when a significant event occurs or a threshold is
/// > reached).  This specification does not impose any timing restrictions
/// > on when and on what event these reports have to be transmitted.
///
/// So we should expect to see Stats Reports messages.
///
/// Also, some of the counters and gauges included in Stats Reports messages
/// might be useful as a way to sanity check things like the number of routes
/// seen so far, but it's not clear to me if one can assume that the point at
/// which the Stats Report message is seen is the point at which the counters
/// should be equivalent to the count of counted messages of the
/// corresponding type seen until this point.
///
/// > **9.  Using BMP**
/// >
/// > Once the BMP session is established, route monitoring starts dumping
/// > the current snapshot as well as incremental changes simultaneously.
///
/// This would seem to indicate that changes to the routing table may be
/// seen while receiving the initial table dump. We separate our processing
/// into "Dumping" and "Updating" phases but in reality there may be no such
/// hard separation between the phases and thus we must share behaviour with
/// the "Updating" phase.
///
/// [^1]: [RFC 4724: Graceful Restart Mechanism for BGP, section 2: Marker for End-of-RIB](https://datatracker.ietf.org/doc/html/rfc4724#section-2)
///
/// [^2]: [RFC 3392: Capabilities Advertisement with BGP-4](https://datatracker.ietf.org/doc/html/rfc3392)
///
/// [^3]: [RFC 5492: Capabilities Advertisement with BGP-4](https://datatracker.ietf.org/doc/html/rfc5492)
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
    pub fn process_msg(self, bmp_msg: BmpMsg<Bytes>) -> ProcessingResult {
        match bmp_msg {
            // already verified upstream
            BmpMsg::InitiationMessage(msg) => self.initiate(msg),

            BmpMsg::PeerUpNotification(msg) => {
                let res = self.peer_up(msg);
                if let BmpState::Dumping(state) = &res.next_state {
                    let num_pending_eors = state.details.peer_states.num_pending_eors();
                    state
                        .status_reporter
                        .pending_eors_update(state.router_id.clone(), num_pending_eors);
                }
                res
            }

            BmpMsg::PeerDownNotification(msg) => self.peer_down(msg),

            BmpMsg::RouteMonitoring(msg) => {
                self.route_monitoring(msg, RouteStatus::InConvergence, |s, pph, update| {
                    s.route_monitoring_preprocessing(pph, update)
                })
            }

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
        if let Some((afi, safi)) = update.is_eor() {
            if self.details.remove_pending_eor(pph, afi, safi) {
                // The last pending EOR has been removed and so this signifies the end of the initial table dump, if
                // we're in the Dumping state, otherwise in the Updating state it signifies only that a late Peer Up
                // (that happened during the Updating state) has finished dumping. Unfortunately EoR is not a clearly
                // defined concept in BMP (e.g. see [1]), is it peer peer, per (S)AFI, per rib? Does advertising
                // support for the Graceful Restart BGP capability have relevance for a BGP client, e.g. can its
                // presence be used to assume that EoRs will be present for that peer in the BMP feed? Etc. There also
                // seems to be varying implementations in deployed routers, whether because of deliberate behavioural
                // differences, differing understanding or misunderstanding of RFCs, implementation bugs, etc.
                //
                // TODO: We may need some sort of heuristic to ascertain the likelihood that the initial table dump
                // has actually completed.
                //
                // [1]: https://www.rfc-editor.org/errata/eid7133
                let num_pending_eors = self.details.num_pending_eors();
                self.status_reporter
                    .pending_eors_update(self.router_id.clone(), num_pending_eors);

                let next_state = BmpState::Updating(self.into());
                return ControlFlow::Break(Self::mk_state_transition_result(next_state));
            }
        }

        ControlFlow::Continue(self)
    }

    pub fn terminate<Octets: AsRef<[u8]>>(
        mut self,
        _msg: Option<TerminationMessage<Octets>>,
    ) -> ProcessingResult {
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
            Self::mk_state_transition_result(next_state)
        } else {
            Self::mk_final_routing_update_result(next_state, Update::Bulk(routes))
        }
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
            source_id: v.source_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details,
        }
    }
}

impl Initiable for Dumping {
    fn set_information_tlvs(&mut self, sys_name: String, sys_desc: String, sys_extra: Vec<String>) {
        self.sys_name = sys_name;
        self.sys_desc = sys_desc;
        self.sys_extra = sys_extra;
    }

    fn sys_name(&self) -> Option<&str> {
        Some(self.sys_name.as_str())
    }
}

impl PeerAware for Dumping {
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

    fn remove_peer(&mut self, pph: &PerPeerHeader<Bytes>) -> bool {
        self.peer_states.remove_peer(pph)
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

    fn add_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: Prefix) -> bool {
        self.peer_states.add_announced_prefix(pph, prefix)
    }

    fn remove_announced_prefix(&mut self, pph: &PerPeerHeader<Bytes>, prefix: &Prefix) {
        self.peer_states.remove_announced_prefix(pph, prefix)
    }

    fn get_announced_prefixes(
        &self,
        pph: &PerPeerHeader<Bytes>,
    ) -> Option<std::collections::hash_set::Iter<Prefix>> {
        self.peer_states.get_announced_prefixes(pph)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, str::FromStr, sync::Arc};

    use roto::types::{
        builtin::{BuiltinTypeValue, MaterializedRoute},
        typevalue::TypeValue,
    };
    use routecore::{asn::Asn, bmp::message::PeerType};

    use crate::{
        bgp::encode::{Announcements, Prefixes},
        payload::{Payload, SourceId, Update},
        units::bmp_tcp_in::state_machine::{processing::MessageType, states::updating::Updating},
    };

    use super::*;

    const TEST_ROUTER_SYS_NAME: &str = "test-router";
    const TEST_ROUTER_SYS_DESC: &str = "test-desc";

    #[test]
    fn initiate() {
        // Given
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        assert!(processor.details.sys_name.is_empty());

        // When
        let res = processor.process_msg(initiation_msg_buf);

        // Then
        assert!(matches!(res.processing_result, MessageType::Other));
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let BmpState::Dumping(next_state) = res.next_state {
            let Dumping {
                sys_name,
                sys_desc,
                peer_states,
                ..
            } = next_state.details;
            assert_eq!(&sys_name, TEST_ROUTER_SYS_NAME);
            assert_eq!(&sys_desc, TEST_ROUTER_SYS_DESC);
            assert!(peer_states.is_empty());
        }
    }

    #[test]
    fn terminate() {
        // Given
        let processor = mk_test_processor();
        let termination_msg_buf = mk_termination_msg();

        // When
        let res = processor.process_msg(termination_msg_buf);

        // Then
        assert!(matches!(
            res.processing_result,
            MessageType::StateTransition
        ));
        assert!(matches!(res.next_state, BmpState::Terminated(_)));
    }

    #[test]
    fn statistics_report() {
        // Given
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph, peer_up_msg_buf, _real_pph) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);
        let stats_report_msg_buf = mk_statistics_report_msg(&pph);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let processor = processor.process_msg(peer_up_msg_buf).next_state;
        let res = processor.process_msg(stats_report_msg_buf);

        // Then
        assert!(matches!(res.processing_result, MessageType::Other));
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
    }

    #[test]
    fn peer_up_with_eor_capable_peer() {
        // Given
        let processor = mk_test_processor();
        assert!(processor.details.peer_states.is_empty());

        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (_, peer_up_msg_buf) = mk_eor_capable_peer_up_notification_msg("127.0.0.1", 12345);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let res = processor.process_msg(peer_up_msg_buf);

        // Then
        assert!(matches!(res.processing_result, MessageType::Other));
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let BmpState::Dumping(next_state) = res.next_state {
            let Dumping { peer_states, .. } = next_state.details;
            assert_eq!(peer_states.num_peer_configs(), 1);
            let pph = peer_states.get_peers().next().unwrap();
            assert_eq!(peer_states.is_peer_eor_capable(pph), Some(true));
            assert_eq!(peer_states.num_pending_eors(), 0); // zero because we have to see a route announcement to know which (S)AFI an EoR is expected for
        }
    }

    #[test]
    fn duplicate_peer_up() {
        // Given
        let processor = mk_test_processor();
        assert!(processor.details.peer_states.is_empty());

        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (_, peer_up_msg_1_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);
        let (_, peer_up_msg_2_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let processor = processor.process_msg(peer_up_msg_1_buf).next_state;
        let res = processor.process_msg(peer_up_msg_2_buf);

        // Then
        assert!(matches!(
            res.processing_result,
            MessageType::InvalidMessage { .. }
        ));
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let MessageType::InvalidMessage { err, .. } = res.processing_result {
            assert!(err.starts_with("PeerUpNotification received for peer that is already 'up'"));
        }
    }

    #[test]
    fn peer_up_peer_down() {
        // Given a BMP state machine in the Dumping state with no known peers
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph_1, peer_up_msg_1_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);
        let (pph_2, peer_up_msg_2_buf, real_pph_2) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.2", 12345);
        let route_mon_msg_buf = mk_route_monitoring_msg(&pph_2);
        let ipv6_route_mon_msg_buf = mk_ipv6_route_monitoring_msg(&pph_2);
        let route_withdraw_msg_buf = mk_route_monitoring_withdrawal_msg(&pph_2);
        let peer_down_msg_1_buf = mk_peer_down_notification_msg(&pph_1);
        let peer_down_msg_2_buf = mk_peer_down_notification_msg(&pph_2);
        assert!(processor.details.peer_states.is_empty());

        // When the state machine processes the initiate and peer up notifications
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let processor = processor.process_msg(peer_up_msg_1_buf).next_state;
        let res = processor.process_msg(peer_up_msg_2_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert!(matches!(res.processing_result, MessageType::Other));
        let processor = res.next_state;

        // And there should now be two known peers
        assert_eq!(get_unique_peer_up_count(&processor), 2);

        // When the state machine processes a peer down notification for a peer that announced no routes
        let res = processor.process_msg(peer_down_msg_1_buf);

        // Then the state should remain unchanged
        // And there should not be a routing update
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert!(matches!(res.processing_result, MessageType::Other));
        let processor = res.next_state;

        // When the state machine processes a couple of route announcements
        let processor = processor.process_msg(route_mon_msg_buf).next_state;
        let res = processor.process_msg(ipv6_route_mon_msg_buf);

        // Then the state should remain unchanged
        // And the number of announced prefixes should increase by 2
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert_eq!(get_announced_prefix_count(&res.next_state, &real_pph_2), 2);
        let processor = res.next_state;

        // And when one of the routes is withdrawn
        let res = processor.process_msg(route_withdraw_msg_buf.clone());

        // Then the state should remain unchanged
        // And the number of announced prefixes should decrease by 1
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert_eq!(get_announced_prefix_count(&res.next_state, &real_pph_2), 1);
        let processor = res.next_state;

        // Unless it is a not-before-announced/already-withdrawn route
        let res = processor.process_msg(route_withdraw_msg_buf);

        // Then the number of announced prefixes should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert_eq!(get_announced_prefix_count(&res.next_state, &real_pph_2), 1);
        let processor = res.next_state;

        // And when a peer down notification is received
        let res = processor.process_msg(peer_down_msg_2_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));

        // And a routing update to withdraw the remaining announced routes for the downed peer should be issued
        assert!(matches!(
            res.processing_result,
            MessageType::RoutingUpdate { .. }
        ));
        if let MessageType::RoutingUpdate { update } = res.processing_result {
            assert!(matches!(update, Update::Bulk(_)));
            if let Update::Bulk(mut bulk) = update {
                assert_eq!(bulk.len(), 1);

                // Verify that the update fits inline into the SmallVec without spilling on to the heap.
                assert!(!bulk.spilled());

                let pfx = Prefix::from_str("2001:2000:3080:e9c::2/128").unwrap();
                let mut expected_roto_prefixes: Vec<TypeValue> = vec![pfx.into()];
                for Payload { source_id, value } in bulk.drain(..) {
                    if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = value {
                        let materialized_route = MaterializedRoute::from(route);
                        let found_pfx = materialized_route.route.prefix.as_ref().unwrap();
                        let position = expected_roto_prefixes
                            .iter()
                            .position(|pfx| pfx == found_pfx)
                            .unwrap();
                        expected_roto_prefixes.remove(position);
                        assert_eq!(materialized_route.status, RouteStatus::Withdrawn);
                    } else {
                        panic!("Expected TypeValue::Builtin(BuiltinTypeValue::Route(_)");
                    }
                }
                assert!(expected_roto_prefixes.is_empty());
            }
        } else {
            unreachable!();
        }

        // And there should no longer be any known peers
        let processor = res.next_state;
        if let BmpState::Dumping(processor) = &processor {
            assert!(processor.details.peer_states.is_empty());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn peer_down_without_peer_up() {
        // Given
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        assert!(processor.details.peer_states.is_empty());

        let (pph, _) = mk_eor_capable_peer_up_notification_msg("127.0.0.1", 12345);
        let peer_down_msg_buf = mk_peer_down_notification_msg(&pph);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let res = processor.process_msg(peer_down_msg_buf);

        // Then
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let MessageType::InvalidMessage { err, .. } = res.processing_result {
            assert!(err.starts_with("PeerDownNotification received for peer that was not 'up'"));
        }
    }

    #[test]
    fn peer_up_different_peer_down() {
        // Given a BMP state machine in the Dumping state with no known peers
        let processor = mk_test_processor();
        assert!(processor.details.peer_states.is_empty());

        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);

        let (pph_up, peer_up_msg_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);
        let (pph_down, _, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.2", 54321);
        let peer_down_msg_buf = mk_peer_down_notification_msg(&pph_down);

        assert_ne!(&pph_up, &pph_down);

        // When the state machine processes a peer up notification
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let res = processor.process_msg(peer_up_msg_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert!(matches!(res.processing_result, MessageType::Other));

        // And there should now be one known peer
        let processor = res.next_state;
        assert_eq!(get_unique_peer_up_count(&processor), 1);

        // When the state machine processes a peer down notification for a different peer
        let res = processor.process_msg(peer_down_msg_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));

        // And there should still be one known peer
        let processor = res.next_state;
        assert_eq!(get_unique_peer_up_count(&processor), 1);

        // And the message should be considered invalid
        assert!(matches!(
            res.processing_result,
            MessageType::InvalidMessage { .. }
        ));
        if let MessageType::InvalidMessage { err, .. } = res.processing_result {
            assert!(err.starts_with("PeerDownNotification received for peer that was not 'up'"));
        }
    }

    #[test]
    fn peer_down_spreads_withdrawals_across_multiple_bgp_updates_if_needed() {
        // Given a BMP state machine in the Dumping state with no known peers
        let processor = mk_test_processor();

        // And some simulated BMP messages
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph, peer_up_msg_buf, real_pph) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);

        // Including a large number of prefix announcements
        const NUM_PREFIXES: usize = 256 * 10;
        let mut route_mon_msg_bufs = Vec::with_capacity(NUM_PREFIXES);
        'outer: for b in 0..256 {
            for c in 0..256 {
                for d in 0..256 {
                    if route_mon_msg_bufs.len() == NUM_PREFIXES {
                        break 'outer;
                    } else {
                        let announcements = Announcements::from_str(&format!(
                            "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.{b}.{c}.{d}/32"
                        ))
                        .unwrap();
                        route_mon_msg_bufs.push(mk_route_monitoring_msg_with_details(
                            &pph,
                            &Prefixes::default(),
                            &announcements,
                            &[],
                        ));
                    }
                }
            }
        }

        let peer_down_msg_buf = mk_peer_down_notification_msg(&pph);

        // When the state machine processes the initiate and peer up notifications
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let res = processor.process_msg(peer_up_msg_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert!(matches!(res.processing_result, MessageType::Other));
        let mut processor = res.next_state;

        // And there should now be one known peer
        assert_eq!(get_unique_peer_up_count(&processor), 1);

        // When the state machine processes the route announcements
        for (i, route_mon_msg_buf) in route_mon_msg_bufs.into_iter().enumerate() {
            let res = processor.process_msg(route_mon_msg_buf);

            // Then the state should remain unchanged
            // And the number of announced prefixes should increase
            assert!(matches!(res.next_state, BmpState::Dumping(_)));
            assert_eq!(
                get_announced_prefix_count(&res.next_state, &real_pph),
                i + 1
            );
            processor = res.next_state;
        }

        // And when a peer down notification is received
        let res = processor.process_msg(peer_down_msg_buf);

        // Then the state should remain unchanged
        assert!(matches!(res.next_state, BmpState::Dumping(_)));

        // And a routing update to withdraw the remaining announced routes for the downed peer should be issued
        assert!(matches!(
            res.processing_result,
            MessageType::RoutingUpdate { .. }
        ));
        if let MessageType::RoutingUpdate { update } = res.processing_result {
            assert!(matches!(update, Update::Bulk(_)));
            if let Update::Bulk(mut bulk) = update {
                // Verify that the update had too many payload items to fit inline into the SmallVec and so it had to
                // spill over on to the heap.
                assert!(bulk.spilled());

                let mut expected_roto_prefixes = Vec::<TypeValue>::with_capacity(NUM_PREFIXES);
                'outer: for b in 0..256 {
                    for c in 0..256 {
                        for d in 0..256 {
                            if expected_roto_prefixes.len() == NUM_PREFIXES {
                                break 'outer;
                            } else {
                                expected_roto_prefixes.push(
                                    Prefix::from_str(&format!("127.{b}.{c}.{d}/32"))
                                        .unwrap()
                                        .into(),
                                );
                            }
                        }
                    }
                }

                #[allow(clippy::mutable_key_type)]
                let mut distinct_bgp_updates_seen = std::collections::HashSet::new();
                let mut num_withdrawals_seen = 0;

                for Payload { source_id, value } in bulk.drain(..) {
                    if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = value {
                        if !distinct_bgp_updates_seen.contains(&route.raw_message) {
                            num_withdrawals_seen += route
                                .raw_message
                                .raw_message()
                                .0
                                .withdrawals()
                                .iter()
                                .count();
                            distinct_bgp_updates_seen.insert(route.raw_message.clone());
                        }
                        let materialized_route = MaterializedRoute::from(route);
                        let found_pfx = materialized_route.route.prefix.as_ref().unwrap();
                        let position = expected_roto_prefixes
                            .iter()
                            .position(|pfx| pfx == found_pfx)
                            .unwrap();
                        expected_roto_prefixes.remove(position);
                        assert_eq!(materialized_route.status, RouteStatus::Withdrawn);
                    } else {
                        panic!("Expected TypeValue::Builtin(BuiltinTypeValue::Route(_)");
                    }
                }

                // All prefixes should have been seen
                assert!(expected_roto_prefixes.is_empty());

                // More than one BGP UPDATE is expected as NUM_PREFIXES don't fit in 4096 bytes
                assert!(distinct_bgp_updates_seen.len() > 1);

                // The sum of prefixes withdrawn across the set of distinct BGP UPDATE messages
                // should be the same as the number of prefixes that were expected to be withdrawn.
                assert_eq!(NUM_PREFIXES, num_withdrawals_seen);
            }
        } else {
            unreachable!();
        }

        // And there should no longer be any known peers
        let processor = res.next_state;
        if let BmpState::Dumping(processor) = &processor {
            assert!(processor.details.peer_states.is_empty());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn end_of_rib_ipv4_for_a_single_peer() {
        // Given
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph, peer_up_msg_buf) = mk_eor_capable_peer_up_notification_msg("127.0.0.1", 12345);
        let route_mon_msg_buf = mk_route_monitoring_msg(&pph);
        let eor_msg_buf = mk_route_monitoring_end_of_rib_msg(&pph);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let res = processor.process_msg(peer_up_msg_buf);

        // Then there should be one up peer but no pending EoRs
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let BmpState::Updating(next_state) = &res.next_state {
            let Updating { peer_states, .. } = &next_state.details;
            assert_eq!(peer_states.num_peer_configs(), 1);
            assert_eq!(peer_states.num_pending_eors(), 0);
        }

        // And when a route announcement is received
        let processor = res.next_state;
        let res = processor.process_msg(route_mon_msg_buf);

        // Then there should be one up peer and one pending EoR
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        if let BmpState::Dumping(next_state) = &res.next_state {
            let Dumping { peer_states, .. } = &next_state.details;
            assert_eq!(peer_states.num_peer_configs(), 1);
            assert_eq!(peer_states.num_pending_eors(), 1);
        } else {
            panic!("Expected to be in the DUMPING state");
        }

        // And when an EoR is received
        let processor = res.next_state;
        let res = processor.process_msg(eor_msg_buf);

        // Then
        assert!(matches!(
            res.processing_result,
            MessageType::StateTransition
        ));
        assert!(matches!(res.next_state, BmpState::Updating(_)));
        if let BmpState::Updating(next_state) = res.next_state {
            let Updating { peer_states, .. } = next_state.details;
            assert_eq!(peer_states.num_peer_configs(), 1);
            assert_eq!(peer_states.num_pending_eors(), 0);
        } else {
            panic!("Expected to be in the UPDATING state");
        }
    }

    #[test]
    #[ignore = "to do"]
    fn end_of_rib_ipv6_for_a_single_peer() {}

    #[test]
    #[ignore = "to do"]
    fn end_of_rib_for_all_pending_peers() {}

    #[test]
    fn route_monitoring_invalid_message() {
        // Given
        let processor = mk_test_processor();

        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph, peer_up_msg_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);

        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();

        // The following hex bytes represent the MP_REACH_NLRI attribute, with an invalid AFI.
        //
        // 0xFFFF is a reserved AFI number according to the IANA registry and so causes an unknown (S)AFI error from
        // the routecore BGP parsing code.
        //
        // See:
        //   - https://datatracker.ietf.org/doc/html/rfc2858#section-2
        //   - https://www.iana.org/assignments/address-family-numbers/address-family-numbers.xhtml#address-family-numbers-2
        let invalid_mp_reach_nlri_attr = hex::decode(
            "900e0024FFFF4604C0A800010001190001C0A8000100070003030303030303030100000000000000",
        )
        .unwrap();

        let route_mon_msg_buf = mk_route_monitoring_msg_with_details(
            &pph,
            &Prefixes::default(),
            &announcements,
            &invalid_mp_reach_nlri_attr,
        );

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let processor = processor.process_msg(peer_up_msg_buf).next_state;
        let res = processor.process_msg(route_mon_msg_buf);

        // Then
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        let expected_err =
            "Invalid BMP RouteMonitoring BGP UPDATE message: unimplemented AFI/SAFI".to_string();
        assert!(matches!(
            res.processing_result,
            MessageType::InvalidMessage { err, .. } if err == expected_err
        ));
    }

    #[test]
    #[ignore = "to do"]
    fn route_monitoring_announce_route() {
        // Given
        let processor = mk_test_processor();
        let initiation_msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
        let (pph, peer_up_msg_buf, _) =
            mk_peer_up_notification_msg_without_rfc4724_support("127.0.0.1", 12345);
        let route_mon_msg_buf = mk_route_monitoring_msg(&pph);

        // When
        let processor = processor.process_msg(initiation_msg_buf).next_state;
        let processor = processor.process_msg(peer_up_msg_buf).next_state;
        let res = processor.process_msg(route_mon_msg_buf);

        // Then
        assert!(matches!(res.next_state, BmpState::Dumping(_)));
        assert!(matches!(
            res.processing_result,
            MessageType::RoutingUpdate { .. }
        ));
        if let MessageType::RoutingUpdate { update } = res.processing_result {
            assert!(matches!(update, Update::Bulk(_)));
            if let Update::Bulk(updates) = &update {
                assert_eq!(updates.len(), 1);
                if let Payload {
                    source_id,
                    value: TypeValue::Builtin(BuiltinTypeValue::Route(route)),
                } = &updates[0]
                {
                    assert_eq!(
                        route.peer_ip().unwrap(),
                        IpAddr::from_str("127.0.0.1").unwrap()
                    );
                    assert_eq!(route.peer_asn().unwrap(), Asn::from_u32(12345));
                    assert_eq!(route.router_id().unwrap().as_str(), TEST_ROUTER_SYS_NAME);
                } else {
                    panic!("Expected a route");
                }
            } else {
                panic!("Expected a bulk update");
            }
        }
    }

    #[test]
    #[ignore = "to do"]
    fn route_monitoring_withdraw_route() {}

    #[test]
    #[ignore = "to do"]
    fn ignore_asns() {}

    #[test]
    #[ignore = "to do"]
    fn full_lifecycle_happy_flow() {
        // Single peer that supports End-of-RIB for which we receive peer up,
        // then the initial dump including End-of-RIB, then a route monitoring
        // announce, a withdraw, followed by peer down and then terminate.
    }

    #[test]
    #[ignore = "to do"]
    fn full_lifecycle_multiple_peers_no_interleaved_peer_up_and_route_monitoring() {}

    #[test]
    #[ignore = "to do"]
    fn full_lifecycle_multiple_peers_interleaved_peer_up_and_route_monitoring() {
        // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-9
        //
        //   "9.  Using BMP
        //
        //    Once the BMP session is established, route monitoring starts
        //    dumping the current snapshot as well as incremental changes
        //    simultaneously.
        //
        //    It is fine to have these operations occur concurrently.  If the
        //    initial dump visits a route and subsequently a withdraw is
        //    received, this will be forwarded to the monitoring station that
        //    would have to correlate and reflect the deletion of that route
        //    in its internal state.  This is an operation that a monitoring
        //    station would need to support, regardless.
        //
        //    If the router receives a withdraw for a prefix even before the
        //    peer dump procedure visits that prefix, then the router would
        //    clean up that route from its internal state and will not forward
        //    it to the monitoring station.  In this case, the monitoring
        //    station may receive a bogus withdraw it can safely ignore."
    }

    #[test]
    #[ignore = "to do"]
    fn full_lifecycle_incremental_update_before_end_of_rib() {}

    #[test]
    #[ignore = "to do"]
    fn route_attribute_changes() {
        // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-5
        //
        //   "5.  Route Monitoring
        //
        //    ...
        //
        //    When a change occurs to a route, such as an attribute change,
        //    the router must update the monitoring station with the new
        //    attribute.  As discussed above, it MAY generate either an update
        //    with the L flag clear, with it set, or two updates, one with the
        //    L flag clear and the other with the L flag set.  When a route is
        //    withdrawn by a peer, a corresponding withdraw is sent to the
        //    monitoring station.  The withdraw MUST have its L flag set to
        //    correspond to that of any previous announcement; if the route in
        //    question was previously announced with L flag both clear and
        //    set, the withdraw MUST similarly be sent twice, with L flag
        //    clear and set. Multiple changed routes MAY be grouped into a
        //    single BGP UPDATE PDU when feasible, exactly as in the standard
        //    BGP protocol."
    }

    // --- Test helpers -----------------------------------------------------------------------------------------------

    fn mk_per_peer_header(peer_ip: &str, peer_as: u32) -> crate::bgp::encode::PerPeerHeader {
        crate::bgp::encode::PerPeerHeader {
            peer_type: PeerType::GlobalInstance.into(),
            peer_flags: 0,
            peer_distinguisher: [0u8; 8],
            peer_address: peer_ip.parse().unwrap(),
            peer_as: Asn::from_u32(peer_as),
            peer_bgp_id: [1u8, 2u8, 3u8, 4u8],
        }
    }

    // RFC 4724 Graceful Restart Mechanism for BGP
    // BMP uses the End-of-RIB feature of RFC 4724.
    fn mk_peer_up_notification_msg_without_rfc4724_support(
        peer_ip: &str,
        peer_as: u32,
    ) -> (
        crate::bgp::encode::PerPeerHeader,
        BmpMsg<Bytes>,
        PerPeerHeader<Bytes>,
    ) {
        let pph = mk_per_peer_header(peer_ip, peer_as);
        let (bmp_msg, real_pph) = mk_peer_up_notification_msg(&pph, false);
        (pph, bmp_msg, real_pph)
    }

    fn mk_eor_capable_peer_up_notification_msg(
        peer_ip: &str,
        peer_as: u32,
    ) -> (crate::bgp::encode::PerPeerHeader, BmpMsg<Bytes>) {
        let pph = mk_per_peer_header(peer_ip, peer_as);
        let (bmp_msg, _) = mk_peer_up_notification_msg(&pph, true);
        (pph, bmp_msg)
    }

    fn mk_peer_up_notification_msg(
        pph: &crate::bgp::encode::PerPeerHeader,
        eor_capable: bool,
    ) -> (BmpMsg<Bytes>, PerPeerHeader<Bytes>) {
        let bytes = crate::bgp::encode::mk_peer_up_notification_msg(
            pph,
            "10.0.0.1".parse().unwrap(),
            11019,
            4567,
            111,
            222,
            0,
            0,
            vec![],
            eor_capable,
        );

        let bmp_msg = BmpMsg::from_octets(bytes).unwrap();
        let real_pph = match &bmp_msg {
            BmpMsg::PeerUpNotification(msg) => msg.per_peer_header(),
            _ => unreachable!(),
        };

        (bmp_msg, real_pph)
    }

    fn mk_peer_down_notification_msg(pph: &crate::bgp::encode::PerPeerHeader) -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_peer_down_notification_msg(pph)).unwrap()
    }

    fn mk_route_monitoring_msg(pph: &crate::bgp::encode::PerPeerHeader) -> BmpMsg<Bytes> {
        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();
        BmpMsg::from_octets(crate::bgp::encode::mk_route_monitoring_msg(
            pph,
            &Prefixes::default(),
            &announcements,
            &[],
        ))
        .unwrap()
    }

    fn mk_ipv6_route_monitoring_msg(pph: &crate::bgp::encode::PerPeerHeader) -> BmpMsg<Bytes> {
        let announcements = Announcements::from_str(
            "e [123,456,789] 2001:2000:3080:e9c::1 BLACKHOLE,123:44 2001:2000:3080:e9c::2/128",
        )
        .unwrap();
        BmpMsg::from_octets(crate::bgp::encode::mk_route_monitoring_msg(
            pph,
            &Prefixes::default(),
            &announcements,
            &[],
        ))
        .unwrap()
    }

    fn mk_route_monitoring_end_of_rib_msg(
        pph: &crate::bgp::encode::PerPeerHeader,
    ) -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_route_monitoring_msg(
            pph,
            &Prefixes::default(),
            &Announcements::default(),
            &[],
        ))
        .unwrap()
    }

    fn mk_route_monitoring_withdrawal_msg(
        pph: &crate::bgp::encode::PerPeerHeader,
    ) -> BmpMsg<Bytes> {
        let prefixes = Prefixes::from_str("127.0.0.1/32").unwrap();
        BmpMsg::from_octets(crate::bgp::encode::mk_route_monitoring_msg(
            pph,
            &prefixes,
            &Announcements::None,
            &[],
        ))
        .unwrap()
    }

    fn mk_ipv6_route_monitoring_withdrawal_msg(pph: &crate::bgp::encode::PerPeerHeader) -> Bytes {
        let prefixes = Prefixes::from_str("2001:2000:3080:e9c::2/128").unwrap();
        crate::bgp::encode::mk_route_monitoring_msg(pph, &prefixes, &Announcements::None, &[])
    }

    fn mk_statistics_report_msg(pph: &crate::bgp::encode::PerPeerHeader) -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_statistics_report_msg(pph)).unwrap()
    }

    fn mk_test_processor() -> BmpStateDetails<Dumping> {
        let addr = "127.0.0.1:1818".parse().unwrap();
        BmpStateDetails::<Dumping> {
            source_id: SourceId::SocketAddr(addr),
            router_id: Arc::new("test-router".to_string()),
            status_reporter: Arc::default(),
            details: Dumping::default(),
        }
    }

    fn get_unique_peer_up_count(processor: &BmpState) -> usize {
        get_peer_states(processor).num_peer_configs()
    }

    fn get_announced_prefix_count(processor: &BmpState, pph: &PerPeerHeader<Bytes>) -> usize {
        get_peer_states(processor)
            .get_announced_prefixes(pph)
            .unwrap()
            .len()
    }

    fn get_peer_states(processor: &BmpState) -> &PeerStates {
        match processor {
            BmpState::Dumping(v) => &v.details.peer_states,
            BmpState::Updating(v) => &v.details.peer_states,
            _ => unreachable!(),
        }
    }

    fn mk_initiation_msg(sys_name: &str, sys_descr: &str) -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_initiation_msg(sys_name, sys_descr)).unwrap()
    }

    fn mk_termination_msg() -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_termination_msg()).unwrap()
    }

    #[allow(clippy::vec_init_then_push)]
    fn mk_route_monitoring_msg_with_details(
        pph: &crate::bgp::encode::PerPeerHeader,
        withdrawals: &Prefixes,
        announcements: &Announcements,
        extra_path_attributes: &[u8],
    ) -> BmpMsg<Bytes> {
        BmpMsg::from_octets(crate::bgp::encode::mk_route_monitoring_msg(
            pph,
            withdrawals,
            announcements,
            extra_path_attributes,
        ))
        .unwrap()
    }
}
