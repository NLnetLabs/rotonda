use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use log::{debug, error, warn};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use roto::types::typevalue::TypeValue;
use roto::types::builtin::{Asn as RotoAsn, BuiltinTypeValue, Prefix, RawRouteWithDeltas};
use roto::types::builtin::{IpAddress, RouteStatus, RotondaId, UpdateMessage};
use routecore::asn::Asn;
use routecore::bgp::message::UpdateMessage as UpdatePdu;

use rotonda_fsm::bgp::session::{
    Command,
    DisconnectReason,
    LocalConfig,
    Message,
    Session as BgpSession
};

use crate::comms::{Gate, GateStatus, Terminated};
use crate::payload::{Payload, Update};
use crate::units::bgp_tcp_in::status_reporter::BgpTcpInStatusReporter;
use crate::units::Unit;

use super::unit::BgpTcpIn;


struct Processor {
    gate: Gate,
    unit_cfg: BgpTcpIn,
    bgp_ltime: u64, // XXX or should this be on Unit level?
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Command>,
    status_reporter: Arc<BgpTcpInStatusReporter>,
}

impl Processor {
    fn new(
        gate: Gate,
        unit_cfg: BgpTcpIn,
        rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<Command>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
    ) -> Self {
        Processor { gate, unit_cfg, bgp_ltime: 0, rx, tx, status_reporter }
    }


    async fn process(&mut self, mut session: BgpSession) {
        let (peer_addr, peer_asn) = session.details();
        debug!("peer_addr peer_asn {:?} {}", peer_addr, peer_asn);
        let peer_addr_cfg = session.config.remote_addr;
        let current_config = session.config.clone();

        // attempt to select! on session.tick() instead of passing the
        // ownership into session.process()
        /*
        tokio::spawn(async {
            session.process().await;
        });
        */

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
                            self.status_reporter.disconnect(peer_addr_cfg);
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
                                    if let Some(new_peer_config) = new_unit.peers.get(&peer_addr_cfg) {
                                        if new_peer_config.remote_asn != current_config.remote_asn ||
                                            new_peer_config.hold_time != current_config.hold_time {
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
                                        self.status_reporter.disconnect(peer_addr_cfg);
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
                res = self.rx.recv() => {
                    match res {
                        None => { break; } 
                        Some(Message::UpdateMessage(pdu)) => {
                            self.process_update(
                                pdu,
                                session.config.remote_addr,
                                session.config.remote_asn,
                            ).await;
                        }
                        Some(Message::NotificationMessage(pdu)) => {
                            debug!(
                                "received NOTIFICATION: {:?}",
                                pdu.details()
                            );
                        }
                        Some(Message::ConnectionLost) => {
                            //TODO clean up RIB etc?
                            if let Some(peer_addr) = peer_addr {
                                self.status_reporter
                                    .peer_connection_lost(peer_addr);
                                debug!(
                                    "Connection lost: {}@{}",
                                    peer_asn,
                                    peer_addr
                                );
                            } else {
                                debug!(
                                    "Connection lost but \
                                       not even established, \
                                       configured remote {}",
                                       peer_addr_cfg
                                );
                            }
                            break;
                        }
                        _ => unimplemented!()
                    }
                }
            }

        }
    }

    // For every NLRI and every withdrawal, send out a Single Payload to the
    // next unit.
    // TODO properly batch these per Action (one batch for announcements one
    // batch for withdrawals). Should we switch to
    // RawRouteWithDeltas::new_with_message_ref for that?
    // TODO store all announced prefixes so we can easily withdraw them when
    // the BGP session goes down.
    async fn process_update(
        &self,
        pdu: UpdatePdu<Bytes>,
        peer_ip: IpAddr,
        peer_asn: Asn
    ) {
        for n in pdu.nlris().iter() {
            let prefix = if let Some(prefix) = n.prefix() {
                prefix
            } else {
                debug!("NLRI without actual prefix");
                continue
            };

            let rot_id = RotondaId(0_usize);
            let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
            let rrwd = RawRouteWithDeltas::new_with_message(
                (rot_id, ltime),
                Prefix::new(prefix),
                Some(IpAddress::new(peer_ip)),
                Some(RotoAsn::new(peer_asn)),
                UpdateMessage(pdu.clone()),
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

            let rot_id = RotondaId(0_usize);
            let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
            let rrwd = RawRouteWithDeltas::new_with_message(
                (rot_id, ltime),
                Prefix::new(prefix),
                Some(IpAddress::new(peer_ip)),
                Some(RotoAsn::new(peer_asn)),
                UpdateMessage(pdu.clone()),
                RouteStatus::Withdrawn
                );
            let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
            let payload = Payload::TypeValue(typval);
            self.gate.update_data(Update::Single(payload)).await;
        }
    }

}

pub async fn handle_router(
    gate: Gate,
    tcp_stream: TcpStream,
    peer_addr: SocketAddr,
    unit_cfg: BgpTcpIn,
    status_reporter: Arc<BgpTcpInStatusReporter>,
    ) {

    // peer_addr is sort of redundant here, we have tcp_stream
    let peer = if let Some(peer) = unit_cfg.peers.get(&peer_addr.ip()) {
        peer
    } else {
        warn!("No peer configuration for {}, dropping connection", peer_addr);
        return;
    };
    // fsm session::LocalConfig:
    // note there is significant overlap with BgpTcpIn here
    let config = LocalConfig::new(
        unit_cfg.my_asn,
        unit_cfg.my_bgp_id,
        peer_addr.ip(),
        peer.remote_asn,
        peer.hold_time,
    );

    let (tx_sess, rx_sess) = mpsc::channel::<Message>(100);

    let socket_status = tokio::join!(
        tcp_stream.writable(),
        tcp_stream.readable()
    );

    match BgpSession::try_for_connection(
        config, tcp_stream, tx_sess
    ).await {
        Ok((mut session, tx_cmds)) => {
            debug!("session with {}", peer_addr);
            let mut p = Processor::new(gate, unit_cfg, rx_sess, tx_cmds, status_reporter);
            p.process(session).await;
        }
        Err(e) => {
            debug!("error {}", e);
        }
    }
}
