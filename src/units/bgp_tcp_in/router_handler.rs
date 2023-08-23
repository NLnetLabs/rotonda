use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use log::{debug, error, trace};
use roto::types::builtin::{
    BgpUpdateMessage, /*IpAddress,*/
    RotondaId, RouteStatus, UpdateMessage,
};
use roto::types::builtin::{
    /*Asn as RotoAsn,*/
    BuiltinTypeValue, Prefix as RotoPrefix, RawRouteWithDeltas,
};
use roto::types::typevalue::TypeValue;
use routecore::addr::Prefix;
use routecore::asn::Asn;
use routecore::bgp::message::{
    nlri::Nlri,
    update::{ComposeError, UpdateBuilder}, UpdateMessage as UpdatePdu,
};
use smallvec::SmallVec;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use rotonda_fsm::bgp::session::{
    BgpConfig, // trait
    Command,
    DisconnectReason,
    Message,
    Session as BgpSession,
};

use crate::comms::{Gate, GateStatus, Terminated};
use crate::payload::{Payload, Update};
use crate::units::bgp_tcp_in::status_reporter::BgpTcpInStatusReporter;
use crate::units::Unit;

use super::peer_config::{CombinedConfig, ConfigExt};
use super::unit::BgpTcpIn;

struct Processor {
    gate: Gate,
    unit_cfg: BgpTcpIn,
    bgp_ltime: u64, // XXX or should this be on Unit level?
    tx: mpsc::Sender<Command>,
    status_reporter: Arc<BgpTcpInStatusReporter>,

    observed_prefixes: BTreeSet<Prefix>,
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
            observed_prefixes: BTreeSet::new(),
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
                            if let Some(_negotiated) = session.negotiated() {
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
                            {
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
                live_sessions
                    .lock()
                    .unwrap()
                    .remove(&(negotiated.remote_addr(), negotiated.remote_asn()));
                debug!(
                    "removed {}@{} from live_sessions (current count: {})",
                    negotiated.remote_asn(),
                    negotiated.remote_addr(),
                    live_sessions.lock().unwrap().len()
                );
            }
        }

        if rejected {
            assert!(self.observed_prefixes.is_empty());
        }

        // attempt using new withdrawals_from_iter methods in routecore
        // XXX see https://github.com/rust-lang/rust/issues/102211
        // we can not do observed_prefixes.iter().map(|p| ...).peekable()
        // because that results in an error
        //  "higher-ranked lifetime error"
        //
        // For now, we create a separate vec and push Nlri in there.

        let mut nlris = vec![];
        self.observed_prefixes.iter().for_each(|p| {
            nlris.push(Nlri::Unicast((*p).into()));
        });
        let mut withdrawals_iter = nlris.into_iter().peekable();
        let mut observed_prefixes = self.observed_prefixes.iter();

        while withdrawals_iter.peek().is_some() {
            let mut builder = UpdateBuilder::new_bytes();
            let old_len = withdrawals_iter.len();
            match builder.withdrawals_from_iter(&mut withdrawals_iter) {
                Ok(()) => {
                    // Everything fit into this PDU, done after this one.

                    debug!("Ok(()) from builder, last PDU");
                }
                Err(ComposeError::PduTooLarge(_)) => {
                    // There is more in prefix_iter to process in a next PDU.

                    debug!(
                        "PduTooLarge from builder, remaining: {}",
                        withdrawals_iter.len()
                    );
                }
                Err(e) => {
                    error!("error while building withdrawal PDUs: {}", e);
                    break;
                }
            }
            let pdu = match builder.into_message() {
                Ok(pdu) => pdu,
                Err(e) => {
                    error!("error constructing withdrawal PDU: {e}");
                    break;
                }
            };

            let in_pdu = old_len - withdrawals_iter.len();
            let rot_id = RotondaId(0_usize);
            let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");

            let msg = Arc::new(BgpUpdateMessage::new((rot_id, ltime), UpdateMessage(pdu)));

            let mut sent_out = 0;

            while sent_out < in_pdu {
                let mut bulk = SmallVec::new();
                while bulk.len() < 8 {
                    trace!("bulk.len() / sent_out: {}/{}", bulk.len(), sent_out);
                    let prefix = if let Some(p) = observed_prefixes.next() {
                        p
                    } else {
                        break;
                    };

                    let rot_id = RotondaId(0_usize);
                    let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
                    let rrwd = RawRouteWithDeltas::new_with_message_ref(
                        (rot_id, ltime),
                        RotoPrefix::new(*prefix),
                        &msg,
                        RouteStatus::InConvergence,
                    );
                    let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
                    let payload = Payload::TypeValue(typval);
                    sent_out += 1;
                    bulk.push(payload);
                }
                self.gate.update_data(Update::Bulk(bulk.into())).await;
            }
        }

        assert!(observed_prefixes.len() == 0);

        (session, rx_sess)
    }

    #[allow(dead_code)]
    fn print_pcap<T: AsRef<[u8]>>(buf: T) {
        print!("000000 ");
        for b in buf.as_ref() {
            print!("{:02x} ", b);
        }
        println!();
    }

    // For every NLRI and every withdrawal, send out Bulk Payload to the next
    // unit.
    async fn process_update(
        &mut self,
        pdu: UpdatePdu<Bytes>,
        //peer_ip: IpAddr,
        //peer_asn: Asn
    ) {
        //Self::print_pcap(pdu.as_ref());

        // When sending both v4 and v6 nlri using exabgp, exa sends a v4
        // NextHop in a v6 MP_REACH_NLRI, which is invalid.
        // However, routecore logs that the nexthop looks bogus but continues
        // and happily gives us the announced prefixes from the nlri in the
        // pdu.
        // What should we do here? One option is to let NextHop::check return
        // an Error in such case, and let Nlris::parse use NextHop::check
        // instead of NextHop::skip.
        // (For now, returning an Error from NextHop::check would perhaps be
        // sufficient, but in the near future we want to make all the parsing
        // even lazier.)
        //
        // ------
        //
        // Local copy of routecore now throws the Error, that seems to work.
        // The session is reset:
        //
        // [2023-08-02 21:13:18] WARN  routecore::bgp::message::nlri: Unimplemented NextHop AFI/SAFI Ipv6/Unicast len 4
        // [2023-08-02 21:13:18] ERROR rotonda_fsm::bgp::session: error: parse error
        // [2023-08-02 21:13:18] DEBUG rotonda_fsm::bgp::fsm: FSM Established -> Connect
        // [2023-08-02 21:13:18] ERROR rotonda::units::bgp_tcp_in::router_handler: error from fsm: error: error from read_frame
        // [2023-08-02 21:13:18] DEBUG rotonda::units::bgp_tcp_in::router_handler: removed AS200@10.1.0.2 from live_sessions (current count: 0)
        //
        // And all previously announced correct prefixes are withdrawn from
        // rib-unit. Perhaps this can serve when further developing the part
        // where such an invalid PDU results in a specific NOTIFICATION that
        // needs to go out. Also, check whether 7606 comes into play here.

        let rot_id = RotondaId(0_usize);
        let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
        let msg = Arc::new(BgpUpdateMessage::new(
            (rot_id, ltime),
            UpdateMessage(pdu.clone()),
        ));
        for chunk in pdu.nlris().iter().collect::<Vec<_>>().chunks(8) {
            let mut bulk = SmallVec::new();
            for nlri in chunk {
                let prefix = if let Nlri::Unicast(b) = nlri {
                    b.prefix()
                } else {
                    debug!("non unicast NLRI, skipping");
                    continue;
                };

                self.observed_prefixes.insert(prefix);

                let rrwd = RawRouteWithDeltas::new_with_message_ref(
                    (rot_id, ltime),
                    RotoPrefix::new(prefix),
                    &msg,
                    RouteStatus::InConvergence,
                );
                let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
                let payload = Payload::TypeValue(typval);
                //self.gate.update_data(Update::Single(payload)).await;
                bulk.push(payload);
            }
            self.gate.update_data(Update::Bulk(bulk.into())).await;
        }

        for chunk in pdu.withdrawals().iter().collect::<Vec<_>>().chunks(8) {
            let mut bulk = SmallVec::new();
            for withdrawal in chunk {
                let prefix = if let Nlri::Unicast(b) = withdrawal {
                    b.prefix()
                } else {
                    debug!("non unicast Withdrawal, skipping");
                    continue;
                };

                self.observed_prefixes.remove(&prefix);

                let rrwd = RawRouteWithDeltas::new_with_message_ref(
                    (rot_id, ltime),
                    RotoPrefix::new(prefix),
                    &msg,
                    RouteStatus::Withdrawn,
                );
                let typval = TypeValue::Builtin(BuiltinTypeValue::Route(rrwd));
                let payload = Payload::TypeValue(typval);
                //self.gate.update_data(Update::Single(payload)).await;
                bulk.push(payload);
            }
            self.gate.update_data(Update::Bulk(bulk.into())).await;
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
    debug!(
        "delay_open for {}: {}",
        candidate_config.peer_config().name(),
        delay_open
    );

    let mut session = BgpSession::new(candidate_config, tcp_stream, sess_tx, cmds_rx);

    if delay_open {
        session.enable_delay_open();
    }
    session.manual_start().await;
    session.connection_established().await;

    let mut p = Processor::new(gate, unit_config, cmds_tx, status_reporter);
    p.process(session, sess_rx, live_sessions).await;
}
