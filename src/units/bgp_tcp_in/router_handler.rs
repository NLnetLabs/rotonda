use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use log::{debug, error};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use roto::types::typevalue::TypeValue;
use roto::types::builtin::{Asn as RotoAsn, BuiltinTypeValue, Prefix as RotoPrefix, RawRouteWithDeltas};
use roto::types::builtin::{IpAddress, RouteStatus, RotondaId, BgpUpdateMessage, UpdateMessage};
use routecore::asn::Asn;
use routecore::bgp::message::UpdateMessage as UpdatePdu;
use routecore::addr::Prefix;

use rotonda_fsm::bgp::session::{
    BgpConfig, // trait
    Command,
    DisconnectReason,
    Message,
    Session as BgpSession
};


use crate::comms::{Gate, GateStatus, Terminated};
use crate::payload::{Payload, Update};
use crate::units::bgp_tcp_in::status_reporter::BgpTcpInStatusReporter;
use crate::units::Unit;

use super::unit::BgpTcpIn;
use super::peer_config::{CombinedConfig, ConfigExt};


struct Processor {
    gate: Gate,
    unit_cfg: BgpTcpIn,
    bgp_ltime: u64, // XXX or should this be on Unit level?
    tx: mpsc::Sender<Command>,
    status_reporter: Arc<BgpTcpInStatusReporter>,

    observed_prefixes: BTreeSet<Prefix>
}

impl Processor {
    fn new(
        gate: Gate,
        unit_cfg: BgpTcpIn,
        //rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<Command>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
    ) -> Self {
        Processor {
            gate,
            unit_cfg,
            bgp_ltime: 0,
            tx,
            status_reporter,
            observed_prefixes: BTreeSet::new()
        }
    }


    async fn process<C: BgpConfig + ConfigExt>(
        &mut self,
        mut session: BgpSession<C>,
        mut rx_sess: mpsc::Receiver<Message>,
        live_sessions: Arc<Mutex<super::unit::LiveSessions>>,
    ) -> (BgpSession<C>, mpsc::Receiver<Message>) {
        let peer_addr_cfg = session.config().remote_prefix_or_exact();

        let mut rejected = false;

        // XXX is this all OK cancel-safety-wise? 
        loop {
            tokio::select! {
                fsm_res = session.tick() => {
                    match fsm_res {
                        Ok(()) => { },
                        Err(e) => {
                            error!("error from fsm: {e}");
                            break;
                        }
                    }
                }
                res = self.gate.process() => {
                    match res {
                        Err(Terminated) => {
                            debug!("Terminated: {:?}", session.negotiated());
                            if let Some(remote_addr) = session.connected_addr() {
                                self.status_reporter.disconnect(remote_addr.ip());
                            }
                            let _ = self.tx.send(Command::Disconnect(
                                    DisconnectReason::Shutdown
                            )).await;
                            debug!("TODO send Payload::bgp_eof");
                            //break;
                        }
                        Ok(status) => match status {
                            GateStatus::Reconfiguring {
                                new_config: Unit::BgpTcpIn(new_unit),
                            } => {
                                // Checking whether we need to reconnect is a
                                // two-stage thing:
                                // if the 'main' config, i.e. my_asn or
                                // my_bgp_id or the address:port to listen on
                                // is changed, reconnect all peers.
                                // If those are unchanged, check the
                                // PeerConfig only for the connection this
                                // Processor handles

                                // Changes in main config?
                                // Based on own impl of PartialEq
                                if new_unit != self.unit_cfg {
                                    debug!("GateStatus::Reconfiguring, \
                                           change in unit config, break.");
                                    // XXX 
                                    // unsure what the correct subcode is:
                                    // 4 Administrative Reset, or
                                    // 6 'Other Configuration Change'?
                                    // and perhaps, eventually, we'll need:
                                    // 3 'Peer deconfigured' when applicable

                                    let _ = self.tx.send(
                                        Command::Disconnect(
                                            DisconnectReason::Reconfiguration
                                        )).await;
                                    break;
                                } else {
                                    // Main unit has not changed, check for
                                    // this specific peer.
                                    // A peer might have been removed from the
                                    // config, which results in a specific
                                    // NOTIFICATION.
                                    if let Some(new_peer_config) = new_unit.peer_configs.get_exact(&peer_addr_cfg) {
                                        let current = self.unit_cfg.peer_configs.get_exact(&peer_addr_cfg).expect("must exist");
                                        if *new_peer_config != *current {
                                            let _ = self.tx.send(
                                                Command::Disconnect(
                                                    DisconnectReason::Reconfiguration
                                                    )
                                                ).await;
                                        } else {
                                            debug!("GateStatus::Reconfiguring, noop");
                                        }
                                    } else {
                                        // disconnect, de-configured
                                        if let Some(remote_addr) = session.connected_addr() {
                                            self.status_reporter.disconnect(remote_addr.ip());
                                        }
                                        debug!(
                                            "GateStatus::Reconfiguring, deconfigured {:?}",
                                            peer_addr_cfg
                                        );
                                        let _ = self.tx.send(
                                            Command::Disconnect(
                                                DisconnectReason::Deconfigured
                                                )).await;
                                        break;

                                    }

                                }

                            },
                            GateStatus::ReportLinks { report } => {
                                report.declare_source();
                            }
                            _ => { /* Nothing to do */ }
                        }
                    }
                }
                res = rx_sess.recv() => {
                    match res {
                        None => { break; } 
                        Some(Message::UpdateMessage(pdu)) => {
                            // We can only receive UPDATE messages over an
                            // established session, so not having a
                            // NegotiatedConfig should never happen.
                            if let Some(negotiated) = session.negotiated() {
                                self.process_update(
                                    pdu,
                                    //negotiated.remote_addr(),
                                    //negotiated.remote_asn()
                                ).await;
                            } else {
                                error!("unexpected state: no NegotiatedConfig for session");
                            }
                        }
                        Some(Message::NotificationMessage(pdu)) => {
                            debug!(
                                "received NOTIFICATION: {:?}",
                                pdu.details()
                            );
                        }
                        Some(Message::ConnectionLost(socket)) => {
                            //TODO clean up RIB etc?
                            self.status_reporter
                                .peer_connection_lost(socket);
                            debug!(
                                "Connection lost: {}@{}",
                                session.negotiated()
                                    .map(|n| n.remote_asn())
                                    .unwrap_or(Asn::MIN),
                                socket
                                );
                            break;
                        }
                        Some(Message::SessionNegotiated(negotiated)) => {
                            let key = (negotiated.remote_addr(), negotiated.remote_asn());
                            if live_sessions.lock().unwrap().contains_key(&key) {
                                error!("Already got a session for {:?}", key);
                                let _ = self.tx.send(Command::Disconnect(
                                        DisconnectReason::ConnectionRejected
                                )).await;
                                rejected = true;
                                break;
                            }
                            let mut live_sessions = live_sessions.lock().unwrap();
                            live_sessions.insert(
                                (negotiated.remote_addr(), negotiated.remote_asn()),
                                self.tx.clone()
                            );
                            debug!(
                                "inserted into live_sessions (current count: {})",
                                live_sessions.len()
                            );
                        }
                        Some(Message::Attributes(_)) => unimplemented!(),
                    }
                }
            }

        }
        // Done, for whatever reason. Remove ourselves form the live sessions.
        // But only if this was not an 'early reject' case, because we would
        // wrongfully remove the firstly inserted (IpAddr, Asn) (i.e., an
        // earlier session, not the currently rejected one) from the
        // live_sessions set.
        if !rejected {
            if let Some(negotiated) = session.negotiated() {
                live_sessions.lock().unwrap().remove(
                    &(negotiated.remote_addr(), negotiated.remote_asn()),
                );
                debug!(
                    "removed {}@{} from live_sessions (current count: {})",
                    negotiated.remote_asn(), negotiated.remote_addr(),
                    live_sessions.lock().unwrap().len()
               );
            }
        } 

        if rejected {
            assert!(self.observed_prefixes.is_empty());
        }

        // TODO
        // And, withdraw all prefixes we observed.
        /*
        for prefix in self.observed_prefixes {
            let rot_id = RotondaId(0_usize);
            let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
            let rrwd = RawRouteWithDeltas::new_with_message(
                (rot_id, ltime),
                RotoPrefix::new(prefix),
                Some(IpAddress::new(peer_ip)),
                Some(RotoAsn::new(peer_asn)),
                UpdateMessage(pdu.clone()),
                RouteStatus::Withdrawn
                );
            let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
            let payload = Payload::TypeValue(typval);
            self.gate.update_data(Update::Single(payload)).await;
        }
        */

        (session, rx_sess)
    }

    // For every NLRI and every withdrawal, send out a Single Payload to the
    // next unit.
    // TODO: properly batch these in an Update::Bulk instead of Update::Single
    async fn process_update(
        &mut self,
        pdu: UpdatePdu<Bytes>,
        //peer_ip: IpAddr,
        //peer_asn: Asn
    ) {
        let rot_id = RotondaId(0_usize);
        let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
        let msg = Arc::new(BgpUpdateMessage::new((rot_id, ltime), UpdateMessage(pdu.clone())));
        for n in pdu.nlris().iter() {
            let prefix = if let Some(prefix) = n.prefix() {
                prefix
            } else {
                debug!("NLRI without actual prefix");
                continue
            };

            self.observed_prefixes.insert(prefix);

            let rrwd = RawRouteWithDeltas::new_with_message_ref(
                (rot_id, ltime),
                RotoPrefix::new(prefix),
                &msg,
                RouteStatus::InConvergence
                );
            let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
            let payload = Payload::TypeValue(typval);
            self.gate.update_data(Update::Single(payload)).await;
        }
        for w in pdu.withdrawals().iter() {
            let prefix = if let Some(prefix) = w.prefix() {
                prefix
            } else {
                debug!("Withdrawal without actual prefix");
                continue
            };

            self.observed_prefixes.remove(&prefix);

            let rrwd = RawRouteWithDeltas::new_with_message_ref(
                (rot_id, ltime),
                RotoPrefix::new(prefix),
                &msg,
                RouteStatus::Withdrawn
                );
            let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
            let payload = Payload::TypeValue(typval);
            self.gate.update_data(Update::Single(payload)).await;
        }
    }

}

pub async fn handle_connection(
    gate: Gate,
    unit_config: BgpTcpIn,
    tcp_stream: TcpStream,
    candidate_config: CombinedConfig,
    cmds_tx: mpsc::Sender<Command>,
    cmds_rx: mpsc::Receiver<Command>,
    status_reporter: Arc<BgpTcpInStatusReporter>,
    live_sessions: Arc<Mutex<super::unit::LiveSessions>>,
) {
    // NB: when testing with an FRR instance configured with
    //  "neighbor 1.2.3.4 timers delayopen 15"
    // the socket is not readable until their delayopen has passed.
    // Not sure whether this is correct/intentional.
    // 
    // To work around that, instead of checking for both writability and
    // readability, we do not wait for the latter.
    // So instead of:
    //      let socket_status = tokio::join!(
    //          tcp_stream.writable(),
    //          tcp_stream.readable()
    //      );
    // we do:
    let _ = tcp_stream.writable().await;
    
    let (sess_tx, sess_rx) = mpsc::channel::<Message>(100);

    /*
    //  - depending on candidate_config, with or without DelayOpen
    //  Ugly use of temp bool here, because candidate_config is moved.
    //  We do not want to put this logic in BgpSession itself, because this
    //  all looks a bit to rotonda-unit specific.
    */
    let delay_open = !candidate_config.is_exact();
    debug!("delay_open for {}: {}", candidate_config.peer_config().name(), delay_open);

    let mut session = BgpSession::new(
        candidate_config,
        tcp_stream,
        sess_tx,
        cmds_rx
    );

    if delay_open {
        session.enable_delay_open();
    }
    session.manual_start().await;
    session.connection_established().await;

    let mut p = Processor::new(gate, unit_config, cmds_tx, status_reporter);
    p.process(session, sess_rx, live_sessions).await;
}
