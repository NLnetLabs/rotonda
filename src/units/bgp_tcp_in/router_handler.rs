use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, warn};
// use roto::types::builtin::{
//     BgpUpdateMessage, /*IpAddress,*/
//     RotondaId, RouteStatus, UpdateMessage,
// };
use roto::types::builtin::{
    /*Asn as RotoAsn,*/
    explode_announcements, explode_withdrawals, BuiltinTypeValue, Nlri, NlriStatus, PeerId, PeerRibType, Provenance, RouteContext
};
use roto::types::collections::BytesRecord;
use roto::types::lazyrecord_types::BgpUpdateMessage;
// use roto::types::lazyrecord_types::BgpUpdateMessage;
use roto::types::typevalue::TypeValue;
use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use routecore::bgp::message::update_builder::ComposeError;
// use routecore::bgp::message::UpdateMessage as UpdatePdu;
use routecore::bgp::message::{SessionConfig, UpdateMessage, Message as BgpMsg};
use routecore::bgp::nlri::afisafi::{AfiSafiNlri, Ipv4UnicastNlri};
use routecore::bgp::path_attributes::PaMap;
use routecore::bgp::types::AfiSafi;
use routecore::bgp::workshop::route::RouteWorkshop;
use smallvec::SmallVec;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use routecore::bgp::fsm::session::{
    self,
    BgpConfig, // trait
    Command,
    DisconnectReason,
    Message,
    NegotiatedConfig,
    Session,
};

use roto::types::builtin::basic_route::SourceId;

//use crate::bgp::encode::Announcements;
use crate::common::roto::{FilterOutput, RotoScripts, ThreadLocalVM};
use crate::common::routecore_extra::mk_withdrawals_for_peers_announced_prefixes;
use crate::comms::{Gate, GateStatus, Terminated};
use crate::payload::{Payload, Update};
use crate::units::bgp_tcp_in::status_reporter::BgpTcpInStatusReporter;
use crate::units::Unit;

use super::peer_config::{CombinedConfig, ConfigExt};
use super::unit::BgpTcpIn;

#[async_trait::async_trait]
trait BgpSession<C: BgpConfig + ConfigExt> {
    fn config(&self) -> &C;

    fn connected_addr(&self) -> Option<SocketAddr>;

    fn negotiated(&self) -> Option<&NegotiatedConfig>;

    async fn tick(&mut self) -> Result<(), session::Error>;
}

#[async_trait::async_trait]
impl BgpSession<CombinedConfig> for Session<CombinedConfig> {
    fn config(&self) -> &CombinedConfig {
        self.config()
    }

    fn connected_addr(&self) -> Option<SocketAddr> {
        self.connected_addr()
    }

    fn negotiated(&self) -> Option<&NegotiatedConfig> {
        self.negotiated()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn tick(&mut self) -> Result<(), session::Error> {
        self.tick().await
    }
}

struct Processor {
    roto_scripts: RotoScripts,
    gate: Gate,
    unit_cfg: BgpTcpIn,
    bgp_ltime: u64, // XXX or should this be on Unit level?
    tx: mpsc::Sender<Command>,
    pdu_out_tx: mpsc::Sender<BgpMsg<Bytes>>,
    status_reporter: Arc<BgpTcpInStatusReporter>,
    observed_nlri: BTreeSet<Nlri>,
}

impl Processor {
    thread_local!(
        static VM: ThreadLocalVM = RefCell::new(None);
    );

    fn new(
        roto_scripts: RotoScripts,
        gate: Gate,
        unit_cfg: BgpTcpIn,
        //rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<Command>,
        pdu_out_tx: mpsc::Sender<BgpMsg<Bytes>>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
    ) -> Self {
        Processor {
            roto_scripts,
            gate,
            unit_cfg,
            bgp_ltime: 0,
            tx,
            pdu_out_tx,
            status_reporter,
            observed_nlri: BTreeSet::new(),
        }
    }

    #[cfg(test)]
    fn mock(unit_cfg: BgpTcpIn) -> (Self, crate::comms::GateAgent) {
        let (gate, gate_agent) = Gate::new(0);

        let (cmds_tx, _) = mpsc::channel(16);
        let (pdu_out_tx, _) = mpsc::channel(16);

        let processor = Self {
            roto_scripts: Default::default(),
            gate,
            unit_cfg,
            bgp_ltime: 0,
            tx: cmds_tx,
            pdu_out_tx,
            status_reporter: Default::default(),
            observed_nlri: BTreeSet::new(),
        };

        (processor, gate_agent)
    }

    async fn process<C: BgpConfig + ConfigExt, T: BgpSession<C>>(
        &mut self,
        mut session: T,
        mut rx_sess: mpsc::Receiver<Message>,
        //mut pdu_out_rx: mpsc::Receiver<BgpMsg<Bytes>>,
        live_sessions: Arc<Mutex<super::unit::LiveSessions>>,
    ) -> (T, mpsc::Receiver<Message>) {
        let peer_addr_cfg = session.config().remote_prefix_or_exact();

        let mut rejected = false;
        let mut connection_id = DefaultHasher::new();
        session.connected_addr().hash(&mut connection_id);

        let mut provenance = Provenance {
            timestamp: Utc::now(),
            // connection_id: connection_id.finish() as u32,
            // Provisional PeerId!
            connection_id: session.connected_addr().unwrap(),
            peer_id: PeerId { addr: "0.0.0.0".parse().unwrap(), asn: 0.into() }, 
            peer_bgp_id: session.config().bgp_id().into(),
            peer_distuingisher: [0,0,0,0,0,0,0,0],
            peer_rib_type: PeerRibType::OutPost,
        };

        if let Some(negotiated) = session.negotiated() {
            let mut connection_id = DefaultHasher::new();
            session.connected_addr().hash(&mut connection_id);

            provenance = Provenance {
                timestamp: Utc::now(),
                // router_id: 0,
                connection_id: session.connected_addr().unwrap(),
                peer_id: PeerId { addr: negotiated.remote_addr(), asn: negotiated.remote_asn() },
                peer_bgp_id: session.config().bgp_id().into(),
                peer_distuingisher: [0,0,0,0,0,0,0,0],
                peer_rib_type: PeerRibType::OutPost,
            };
        }

            //capabilities: Vec<Capability<Vec<u8>>,
    // hold_time: u16, // smaller of the two OPENs, 0 or >= 3
    // remote_bgp_id: [u8; 4],
    // remote_asn: Asn,
    // remote_addr: IpAddr,
    // addpath: Vec<AddpathFamDir>,

        // fn local_asn(&self) -> Asn;
        // fn bgp_id(&self) -> [u8; 4];
        // fn remote_addr_allowed(&self, remote_addr: IpAddr) -> bool;
        // fn remote_asn_allowed(&self, remote_asn: Asn) -> bool;
        // fn hold_time(&self) -> Option<u16>;
        // fn is_exact(&self) -> bool;
    
        // fn protocols(&self) -> Vec<AfiSafi>;
        // fn addpath(&self) -> Vec<AfiSafi>;
        // let mut ticker = session.tick();

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
                        Some(Message::UpdateMessage(bgp_msg)) => {
                            // We can only receive UPDATE messages over an
                            // established session, so not having a
                            // NegotiatedConfig should never happen.
                            if let Some(_negotiated) = session.negotiated() {
                                if let Ok(ControlFlow::Continue(FilterOutput { south, east, received })) = Self::VM.with(|vm| {
                                    // let delta_id = (RotondaId(0), 0); // TODO
                                    // let msg: BytesRecord<BgpUpdateMessage> = bgp_msg.into();
                                    // let msg = BgpUpdateMessage::new(delta_id, roto_update_msg);
                                    // let msg = Arc::new(msg);
                                    let msg = BytesRecord::<BgpUpdateMessage>::from(bgp_msg);
                                    let context = RouteContext::new(Some(msg.clone()), NlriStatus::InConvergence, provenance);
                                    //if vm.borrow().is_none() {
                                    //    self.roto_scripts.init_vm(vm, &self.unit_cfg.filter_name).unwrap();
                                    //}
                                    //if let Some(vm) = vm.borrow_mut().as_mut() {
                                    //    vm.update_context(Arc::new(context));
                                    //} else {
                                    //    // LH: This shouldn't happen I suppose?
                                    //    debug!("no vm, attempt to init");
                                    //    //self.roto_scripts.init_vm(vm, &self.unit_cfg.filter_name);
                                    //    //vm.update_context(Arc::new(context));
                                    //}
                                    //vm.borrow_mut().as_mut().map(|vm| vm.update_context(Arc::new(context)));
                                    self.roto_scripts.exec(vm, &self.unit_cfg.filter_name, msg.into(), Utc::now(), context)
                                }) {
                                    if !south.is_empty() {
                                        let context = RouteContext::new(
                                            None,
                                            NlriStatus::Empty,
                                            provenance,
                                        );
                                        let update = Payload::from_output_stream_queue(south, context, None).into();
                                        self.gate.update_data(update).await;
                                    }
                                    if let TypeValue::Builtin(BuiltinTypeValue::BgpUpdateMessage(pdu)) = east {
                                        // let pdu = Arc::into_inner(pdu).unwrap(); // This should succeed
                                        let pdu = pdu.bytes_parser().clone(); // Bytes is cheap to clone
                                        let context = RouteContext::new(
                                            Some(pdu.into()),
                                            NlriStatus::InConvergence,
                                            provenance,
                                        );
                                        let update = self.process_update(
                                            received,
                                            // pdu,
                                            context,
                                            //negotiated.remote_addr(),
                                            //negotiated.remote_asn()
                                        ).await;
                                        match update {
                                            Ok(update) => { self.gate.update_data(update).await; },
                                            Err(e) => { error!("unexpected state: {e}"); },
                                        };
                                    }
                                }
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
                            if let Some(socket) = socket {
                                debug!(
                                    "Connection lost: {}@{}",
                                    session.negotiated()
                                        .map(|n| n.remote_asn())
                                        .unwrap_or(Asn::MIN),
                                    socket
                                    );
                                } else {
                                    debug!(
                                        "Connection lost: {}@UNKOWN",
                                        session.negotiated()
                                            .map(|n| n.remote_asn())
                                            .unwrap_or(Asn::MIN),
                                        );
                                }
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
                                (self.tx.clone(), self.pdu_out_tx.clone())
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

        if rejected {
            assert!(self.observed_nlri.is_empty());
        }

        // Done, for whatever reason. Remove ourselves form the live sessions.
        // But only if this was not an 'early reject' case, because we would
        // wrongfully remove the firstly inserted (IpAddr, Asn) (i.e., an
        // earlier session, not the currently rejected one) from the
        // live_sessions set.
        if !rejected {
            if let Some(negotiated) = session.negotiated() {
                live_sessions.lock().unwrap().remove(&(
                    negotiated.remote_addr(),
                    negotiated.remote_asn(),
                ));
                debug!(
                    "removed {}@{} from live_sessions (current count: {})",
                    negotiated.remote_asn(),
                    negotiated.remote_addr(),
                    live_sessions.lock().unwrap().len()
                );

                let prefixes = self.observed_nlri.iter();
                // let router_id = Arc::new("TODO".into());
                let source_id: SourceId = "TODO".into();
                let peer_address = negotiated.remote_addr();
                let peer_asn = negotiated.remote_asn();

                let ipv4_unicast_nlri = prefixes.filter_map(|n| if let Nlri::Ipv4Unicast(p) = n { Some(p) } else { None });

                // XXX mk_withdrawals_ is a todo!() now, so this will panic
                let payloads = mk_withdrawals_for_peers_announced_prefixes::<'_, Ipv4UnicastNlri, _>(
                    ipv4_unicast_nlri,
                    provenance,
                    SessionConfig::modern()
                );

                if let Ok(payloads) = payloads {
                    self.gate.update_data(Update::Bulk(payloads)).await;
                }
            }
        }

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
        received: DateTime<Utc>,
        // bgp_msg: BgpUpdateMessage,
        context: RouteContext,
        //peer_ip: IpAddr,
        //peer_asn: Asn
    ) -> Result<Update, session::Error> {
        fn mk_payload(
            tv: TypeValue,
            received: DateTime<Utc>,
            //provenance: Option<Provenance>,
            context: RouteContext,
        ) -> Payload {
            

            // let tv;
            // if let Ok(route) = route.try_into() {
            //     tv = TypeValue::Builtin(
            //         BuiltinTypeValue::Route(route)
            //     );
            // } else {
            //     return Err(session::Error::for_str("Cannot parse route"));
            // }
            
            Payload::with_received(
                tv,
                context,
                None,
                received
            )
            
        }

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
        // rib. Perhaps this can serve when further developing the part
        // where such an invalid PDU results in a specific NOTIFICATION that
        // needs to go out. Also, check whether 7606 comes into play here.
        let mut payloads = SmallVec::new();

        // let source_id: SourceId = "unknown".into(); // TODO
        // // let rot_id = RotondaId(0_usize);
        // let ltime = self.bgp_ltime.checked_add(1).expect(">u64 ltime?");
        // let target = bytes::BytesMut::new();

        let bgp_msg = context.message().clone().unwrap().into_inner();
    
        let rws = explode_announcements(&bgp_msg, &mut self.observed_nlri)?;

        payloads.extend(
            rws.into_iter().map(|rws| mk_payload(rws, received, context.clone()))
        );

        let wds = explode_withdrawals(&bgp_msg, &mut self.observed_nlri)?;

        payloads.extend(wds.into_iter().map(|wds|
                mk_payload(wds, received, context.clone())
        ));

        Ok(payloads.into())
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_connection(
    roto_scripts: RotoScripts,
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

    let (tcp_in, tcp_out) = tcp_stream.into_split();
    let (sess_tx, sess_rx) = mpsc::channel::<Message>(100);

    let (pdu_out_tx, mut pdu_out_rx) = mpsc::channel(10);

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

    let cmds_tx2 = cmds_tx.clone();
    let sess_tx2 = sess_tx.clone();


    let mut session =
        Session::new(
            candidate_config,
            tcp_in,
            sess_tx,
            cmds_rx,
            pdu_out_tx.clone(),
        );

    if delay_open {
        session.enable_delay_open();
    }
    session.manual_start().await;
    session.connection_established().await;

    let mut p = Processor::new(
        roto_scripts,
        gate,
        unit_config,
        cmds_tx,
        pdu_out_tx,
        status_reporter,
    );

    tokio::spawn(async move {
        while let Some(pdu) = pdu_out_rx.recv().await {
            /*
            if let Err(e) = tcp_out.writable().await.and({
                tcp_out.try_write(pdu.as_ref())
            }) {
                warn!("error sending pdu ({:?}): {}", tcp_out.peer_addr(), e);
            }
            */
            if let Err(e) = tcp_out.writable().await {
                warn!("error while awaiting tcp_out.writable(): {}", e);
            }
            //if let Err(e) = tcp_out.try_write(pdu.as_ref()) {
            //    warn!("error sending pdu ({:?}): {}", tcp_out.peer_addr(), e);
            //}
            match tcp_out.try_write(pdu.as_ref()) {
                Ok(_) => { },
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                    debug!("WouldBlock after writable().await");
                }
                Err(e) => {
                    warn!(
                        "error sending pdu ({:?}): {}",
                        tcp_out.peer_addr(), e
                    );
                    break;
                }
            }
        }
        // Make sure we get rid of the other half of the TcpStream:
        let _ = cmds_tx2.send(Command::Disconnect(DisconnectReason::Other)).await;
        let _ = sess_tx2.send(Message::ConnectionLost(None)).await;
        debug!("pre tcp_out.forget()");
        tcp_out.forget();
        debug!("post tcp_out.forget()");
    });

    p.process(session, sess_rx, live_sessions).await;
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use routecore::bgp::fsm::session::{self, Message, NegotiatedConfig};
    use inetnum::asn::Asn;
    use tokio::{sync::mpsc, task::JoinHandle};

    use crate::{
        common::status_reporter::AnyStatusReporter,
        comms::GateAgent,
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
        units::bgp_tcp_in::{
            peer_config::{CombinedConfig, PeerConfig, PrefixOrExact},
            router_handler::Processor,
            status_reporter::BgpTcpInStatusReporter,
            unit::BgpTcpIn,
        },
    };

    use super::BgpSession;

    #[tokio::test(flavor = "multi_thread")]
    async fn processor_should_abort_on_unit_termination() {
        enable_logging("trace");
        let (join_handle, status_reporter, gate_agent, sess_tx) =
            setup_test();

        gate_agent.terminate().await;

        // We should be able to just wait for the processor to abort but it
        // doesn't... because the mock session doesn't respond to the
        // Disconnect command that gets sent to which it would in turn send
        // a ConnectionLost message. However we still want to test unit
        // termination because it should also increment the disconnect metric
        // while sending only the ConnectionLost message does not.
        // join_handle.await.unwrap();

        // Note: the disconnect metric is only incremented if both
        // session.negotiated() and session.connected_addr() return Some.

        // Wait for the termination command to be handled:
        let mut count = 0;
        while count < 1 {
            let metrics = get_testable_metrics_snapshot(
                &status_reporter.metrics().unwrap(),
            );
            count = metrics.with_name::<usize>("bgp_tcp_in_disconnect_count");
        }

        // Emulate the real session behaviour of sending a ConnectionLost
        // message.
        let msg = Message::ConnectionLost(Some("10.0.0.2:12345".parse().unwrap()));
        let _ = sess_tx.send(msg).await;

        // Now it's safe to wait for the processor to abort.
        join_handle.await.unwrap();

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bgp_tcp_in_connection_lost_count"),
            1
        );
        assert_eq!(
            metrics.with_name::<usize>("bgp_tcp_in_disconnect_count"),
            1
        );
    }

    #[tokio::test]
    async fn processor_should_abort_on_connection_lost() {
        let (join_handle, status_reporter, _gate_agent, sess_tx) =
            setup_test();

        let msg = Message::ConnectionLost("10.0.0.2:12345".parse().ok());
        let _ = sess_tx.send(msg).await;

        join_handle.await.unwrap();

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bgp_tcp_in_connection_lost_count"),
            1
        );
    }

    //-------- Test helpers --------------------------------------------------

    #[allow(clippy::type_complexity)]
    fn setup_test() -> (
        JoinHandle<(MockBgpSession, mpsc::Receiver<Message>)>,
        Arc<BgpTcpInStatusReporter>,
        GateAgent,
        mpsc::Sender<Message>,
    ) {
        let unit_settings =
            BgpTcpIn::mock("dummy-listen-address", Asn::from_u32(12345));
        let peer_config = PeerConfig::mock();
        let remote_net = PrefixOrExact::Exact("10.0.0.1".parse().unwrap());
        let config = CombinedConfig::new(
            unit_settings.clone(),
            peer_config,
            remote_net,
        );
        let session = MockBgpSession(config, NegotiatedConfig::dummy());
        let (mut p, gate_agent) = Processor::mock(unit_settings);
        let (sess_tx, sess_rx) = mpsc::channel::<Message>(100);
        let live_sessions = Arc::new(Mutex::new(HashMap::new()));
        let status_reporter = p.status_reporter.clone();

        let join_handle =
            crate::tokio::spawn("mock_bgp_tcp_in_processor", async move {
                p.process(session, sess_rx, live_sessions).await
            });

        (join_handle, status_reporter, gate_agent, sess_tx)
    }

    struct MockBgpSession(CombinedConfig, NegotiatedConfig);

    #[async_trait::async_trait]
    impl BgpSession<CombinedConfig> for MockBgpSession {
        fn config(&self) -> &CombinedConfig {
            &self.0
        }

        fn connected_addr(&self) -> Option<SocketAddr> {
            Some("1.2.3.4:12345".parse().unwrap())
        }

        fn negotiated(&self) -> Option<&NegotiatedConfig> {
            Some(&self.1)
        }

        async fn tick(&mut self) -> Result<(), session::Error> {
            // Don't tick too fast otherwise process() spends all its time
            // handling ticks and won't do anything else.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(())
        }
    }
}
