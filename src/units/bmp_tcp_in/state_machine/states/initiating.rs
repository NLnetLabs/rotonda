use std::sync::Arc;

use bytes::Bytes;
use routecore::bmp::message::{Message as BmpMsg, TerminationMessage};

use crate::payload::{RouterId, SourceId};

use super::super::{
    machine::{BmpState, BmpStateDetails, Initiable},
    processing::{MessageType, ProcessingResult},
    status_reporter::BmpTcpInStatusReporter,
};

/// BmpState machine state 'Initiating'.
///
/// Expecting an Initiation message.
///
/// The initiating phase is:
///
/// > **3.3.  Lifecycle of a BMP Session**
/// >
/// > ...
/// >
/// > Once the session is up, the router begins to send BMP messages.
/// > It MUST begin by sending an Initiation message.
/// >
/// > -- <https://datatracker.ietf.org/doc/html/rfc7854#section-3.3>
///
/// This seems to clearly prohibit sending of any other type of message, thus
/// we apparently also therefore shouldn't see Termination or Stats Reports
/// messages.
#[derive(Default, Debug)]
pub struct Initiating {
    /// The name given by the Initiation Message for the router.
    pub sys_name: Option<String>,

    /// Additional description required to be provided by the Initiation
    /// Message.
    pub sys_desc: Option<String>,

    /// Optional additional strings provided by the Initiation Message.
    pub sys_extra: Vec<String>,
}

impl BmpStateDetails<Initiating> {
    pub fn new(
        source_id: SourceId,
        router_id: Arc<RouterId>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
    ) -> Self {
        BmpStateDetails {
            source_id,
            router_id,
            status_reporter,
            details: Initiating::default(),
        }
    }

    #[allow(dead_code)]
    pub fn process_msg(self, bmp_msg: BmpMsg<Bytes>, _trace_id: Option<u8>) -> ProcessingResult {
        match bmp_msg {
            // already verified upstream
            BmpMsg::InitiationMessage(msg) => {
                let res = self.initiate(msg);

                match res.processing_result {
                    MessageType::InvalidMessage { .. } => res,

                    _ => {
                        // A newly connected router, once initiated, should then
                        // start its initial table dump.
                        if let BmpState::Initiating(state) = res.next_state {
                            let next_state = BmpState::Dumping(state.into());
                            Self::mk_state_transition_result(next_state)
                        } else {
                            unreachable!("We should still be in state Initiating")
                        }
                    }
                }
            }

            other_msg_type => {
                // https://datatracker.ietf.org/doc/html/rfc7854#section-4.3
                //    "An initiation message MUST be sent as the first message
                //     after the TCP session comes up."
                self.mk_invalid_message_result(
                    format!(
                        "RFC 7854 4.3 violation: Expected BMP Initiation Message but received: {}",
                        other_msg_type
                    ),
                    None,
                    Some(Bytes::copy_from_slice(other_msg_type.as_ref())),
                )
            }
        }
    }

    pub fn terminate(self, _msg: Option<TerminationMessage<Bytes>>) -> ProcessingResult {
        let next_state = BmpState::Terminated(self.into());
        Self::mk_state_transition_result(next_state)
    }
}

impl Initiable for Initiating {
    fn set_information_tlvs(&mut self, sys_name: String, sys_desc: String, sys_extra: Vec<String>) {
        self.sys_name = Some(sys_name);
        self.sys_desc = Some(sys_desc);
        self.sys_extra = sys_extra;
    }

    fn sys_name(&self) -> Option<&str> {
        self.sys_name.as_deref()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::ops::Deref;

//     use routecore::asn::Asn;
//     use routecore::bmp::message::PeerType;

//     use crate::{
//         bgp::encode::{
//             mk_initiation_msg, mk_invalid_initiation_message_that_lacks_information_tlvs,
//         },
//         units::bmp_tcp_in::state_machine::states::dumping::Dumping,
//     };

//     use super::*;

//     const TEST_ROUTER_ID: &str = "test router id";
//     const TEST_ROUTER_SYS_NAME: &str = "test-router";
//     const TEST_ROUTER_SYS_DESC: &str = "test-desc";
//     const TEST_PEER_ASN: u32 = 12345;

//     fn mk_per_peer_header() -> crate::bgp::encode::PerPeerHeader {
//         crate::bgp::encode::PerPeerHeader {
//             peer_type: PeerType::GlobalInstance.into(),
//             peer_flags: 0,
//             peer_distinguisher: [0u8; 8],
//             peer_address: "127.0.0.1".parse().unwrap(),
//             peer_as: Asn::from_u32(TEST_PEER_ASN),
//             peer_bgp_id: [1u8, 2u8, 3u8, 4u8],
//         }
//     }

//     fn mk_peer_up_notification_msg() -> Bytes {
//         crate::bgp::encode::mk_peer_up_notification_msg(
//             &mk_per_peer_header(),
//             "10.0.0.1".parse().unwrap(),
//             11019,
//             4567,
//             111,
//             222,
//             0,
//             0,
//             vec![],
//             false,
//         )
//     }

//     #[test]
//     fn sysname_should_be_correctly_extracted() {
//         // Given
//         let processor = mk_test_processor();
//         let msg_buf = mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC);
//         let bmp_msg = BmpMsg::from_octets(msg_buf).unwrap();

//         // When
//         let res = processor.process_msg(bmp_msg);

//         // Then
//         assert!(matches!(
//             res.processing_result,
//             MessageType::StateTransition
//         ));
//         assert!(matches!(res.next_state, BmpState::Dumping(_)));
//         if let BmpState::Dumping(next_state) = res.next_state {
//             assert_eq!(next_state.router_id.deref(), TEST_ROUTER_ID);
//             assert!(matches!(next_state.details, Dumping { .. }));
//             let Dumping {
//                 sys_name,
//                 sys_desc,
//                 peer_states,
//                 ..
//             } = next_state.details;
//             assert_eq!(&sys_name, TEST_ROUTER_SYS_NAME);
//             assert_eq!(&sys_desc, TEST_ROUTER_SYS_DESC);
//             assert!(peer_states.is_empty());
//         }
//     }

//     #[test]
//     fn missing_sysname_should_result_in_invalid_message() {
//         // Given
//         let processor = mk_test_processor();
//         let msg_buf = mk_invalid_initiation_message_that_lacks_information_tlvs();
//         let bmp_msg = BmpMsg::from_octets(msg_buf).unwrap();

//         // When
//         let res = processor.process_msg(bmp_msg);

//         // Then
//         assert!(matches!(
//             res.processing_result,
//             MessageType::InvalidMessage { .. }
//         ));
//         assert!(matches!(res.next_state, BmpState::Initiating(_)));
//         if let MessageType::InvalidMessage { err, .. } = res.processing_result {
//             assert_eq!(
//                 err,
//                 "Invalid BMP InitiationMessage: Missing or empty sysName Information TLV"
//             );
//         }
//     }

//     #[test]
//     fn wrong_message_type_should_result_in_invalid_message() {
//         // Given
//         let processor = mk_test_processor();
//         let msg_buf = mk_peer_up_notification_msg();
//         let bmp_msg = BmpMsg::from_octets(msg_buf).unwrap();

//         // When
//         let res = processor.process_msg(bmp_msg);

//         // Then
//         assert!(matches!(
//             res.processing_result,
//             MessageType::InvalidMessage { .. }
//         ));
//         assert!(matches!(res.next_state, BmpState::Initiating(_)));
//         if let MessageType::InvalidMessage { err, .. } = res.processing_result {
//             assert_eq!(err, "RFC 7854 4.3 violation: Expected BMP Initiation Message but received: PeerUpNotification");
//         }
//     }

//     fn mk_test_processor() -> BmpStateDetails<Initiating> {
//         let router_addr = "127.0.0.1:1818".parse().unwrap();
//         let source_id = SourceId::SocketAddr(router_addr);
//         let router_id = Arc::new(TEST_ROUTER_ID.to_string());
//         BmpStateDetails::<Initiating>::new(source_id, router_id, Arc::default())
//     }
// }
