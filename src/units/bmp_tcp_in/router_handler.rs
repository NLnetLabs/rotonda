//! BMP message stream handler for a single connected BMP publishing client.
use std::cell::RefCell;
use std::hash::{self, DefaultHasher, Hash};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::{net::SocketAddr, ops::ControlFlow};

use arc_swap::ArcSwap;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hash32::Hasher;
use inetnum::asn::Asn;
use log::{debug, error, info};
use routecore::bmp::message::Message;

use smallvec::smallvec;
use tokio::sync::Mutex;
use tokio::{io::AsyncRead, net::TcpStream};

use crate::roto_runtime::types::{
    FilterName, Output, OutputStreamMessage, PeerRibType, Provenance, RotoOutputStream, RotoScripts, RouteContext
};

use crate::ingress::{self, IngressId};
use crate::payload::RouterId;
use crate::tracing::Tracer;
use crate::{
    comms::{Gate, GateStatus},
    payload::{Payload, Update, UpstreamStatus},
    units::{
        bmp_tcp_in::{
            io::BmpStream, status_reporter::BmpTcpInStatusReporter,
        },
        Unit,
    },
};

use super::io::FatalError;
use super::state_machine::{BmpState, BmpStateMachineMetrics, MessageType};
use super::unit::{RotoFunc, TracingMode};
use super::util::format_source_id;

pub struct RouterHandler {
    gate: Gate,
    roto_function: Option<RotoFunc>,
    router_id_template: Arc<ArcSwap<String>>,
    filter_name: Arc<ArcSwap<FilterName>>,
    status_reporter: Arc<BmpTcpInStatusReporter>,
    state_machine: Arc<Mutex<Option<BmpState>>>,
    tracer: Arc<Tracer>,
    tracing_mode: Arc<ArcSwap<TracingMode>>,
    last_msg_at: Option<Arc<RwLock<DateTime<Utc>>>>,
    bmp_metrics: Arc<BmpStateMachineMetrics>,
}

impl RouterHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gate: Gate,
        roto_function: Option<RotoFunc>,
        router_id_template: Arc<ArcSwap<String>>,
        filter_name: Arc<ArcSwap<FilterName>>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
        state_machine: Arc<Mutex<Option<BmpState>>>,
        tracer: Arc<Tracer>,
        tracing_mode: Arc<ArcSwap<TracingMode>>,
        last_msg_at: Option<Arc<RwLock<DateTime<Utc>>>>,
        bmp_metrics: Arc<BmpStateMachineMetrics>,
    ) -> Self {
        Self {
            gate,
            roto_function,
            router_id_template,
            filter_name,
            status_reporter,
            state_machine,
            tracer,
            tracing_mode,
            last_msg_at,
            bmp_metrics,
        }
    }

    #[cfg(test)]
    pub fn mock() -> (Self, crate::comms::GateAgent, Gate) {
        use crate::units::bmp_tcp_in::unit::BmpTcpIn;

        use super::metrics::BmpTcpInMetrics;

        let (parent_gate, gate_agent) = Gate::new(0);

        let source_id = 0;
        let router_id = Arc::new("unknown".into());
        let bmp_in_metrics = Arc::new(BmpTcpInMetrics::default());
        let bmp_metrics = Arc::new(BmpStateMachineMetrics::default());
        let parent_status_reporter = Arc::new(BmpTcpInStatusReporter::new(
            "dummy",
            bmp_in_metrics.clone(),
        ));

        let state_machine = BmpState::new(
            source_id,
            router_id,
            parent_status_reporter.clone(),
            bmp_metrics.clone(),
            Arc::new(ingress::Register::new()),
        );

        let state_machine = Arc::new(Mutex::new(Some(state_machine)));

        let mock = Self {
            gate: parent_gate.clone(),
            // roto_scripts: Default::default(),
            router_id_template: Arc::new(ArcSwap::from_pointee(
                BmpTcpIn::default_router_id_template(),
            )),
            filter_name: Default::default(),
            status_reporter: parent_status_reporter,
            state_machine,
            tracer: Default::default(),
            tracing_mode: Default::default(),
            last_msg_at: None,
            bmp_metrics,
            roto_function: todo!(),
        };

        (mock, gate_agent, parent_gate)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        mut tcp_stream: TcpStream,
        router_addr: SocketAddr,
        //source_id: SourceId,
        ingress_id: IngressId,
        ingress_register: Arc<ingress::Register>,
        // we need access to the ingress Register to register new IDs, for
        // every peer / session in the BMP connection
    ) {
        // Discard the write half of the TCP stream as we are a "monitoring
        // station" and per the BMP RFC 7584 specification _"No BMP message is
        // ever sent from the monitoring station to the monitored router"_.
        // See: https://datatracker.ietf.org/doc/html/rfc7854#section-3.2
        let (rx, _tx) = tcp_stream.split();
        self.read_from_router(rx, router_addr, ingress_id, ingress_register)
            .await;
    }

    async fn read_from_router<T: AsyncRead + Unpin>(
        &self,
        rx: T,
        router_addr: SocketAddr,
        //source_id: SourceId,
        ingress_id: IngressId,
        ingress_register: Arc<ingress::Register>,
    ) {
        // Setup BMP streaming
        let mut stream =
            BmpStream::new(rx, self.gate.clone(), self.tracing_mode.clone());

        let mut router_id = hash32::FnvHasher::default();
        router_addr.hash(&mut router_id);

        //let mut connection_id =  hash32::FnvHasher::default();
        //source_id.hash(&mut connection_id);

        let router_ingress_id = ingress_register.register();

        let provenance = Provenance::for_bmp(
            router_ingress_id,
            // peer_ip: we are not diving into the BMP message to see if this
            // is a RouteMonitoring msg, just set to BMP IP and overwrite
            // later if necessary
            router_addr.ip(),
            // peer_asn, set to Asn(0) for now, overwrite later
            Asn::from_u32(0),
            router_addr.ip(),
            [0; 9],
            PeerRibType::InPre,
        );

        // Ensure that on first use the metrics for the "unknown" router are
        // correctly initialised.
        // self.status_reporter.router_id_changed(router_addr);

        loop {
            // Read the incoming TCP stream, extracting BMP messages.
            match stream.next().await {
                Err(err) => {
                    // There was a problem reading from the BMP stream.
                    let bmp_state_lock = self.state_machine.lock().await;

                    // SAFETY: Each connection should always have a state machine.
                    self.status_reporter.receive_io_error(
                        bmp_state_lock.as_ref().unwrap().router_id(),
                        &err,
                    );

                    if err.is_fatal() {
                        // Break to close our side of the connection and stop
                        // processing this BMP stream.
                        break;
                    }
                }

                Ok((None, _, _)) => {
                    // The stream consumer exited in response to a Gate
                    // termination message. Break to close our side of the
                    // connection and stop processing this BMP stream.
                    break;
                }

                Ok((Some(msg_buf), status, mut trace_id)) => {
                    let received = std::time::Instant::now();

                    // We want the stream reading to abort as soon as the Gate
                    // is terminated so we handle status updates to the Gate in
                    // the stream reader. The last non-terminal status updates is
                    // then passed to us here along with the next message that was
                    // read.
                    //
                    // TODO: Should we require that we receive all non-terminal
                    // gate status updates here so that none are missed?

                    // Update our behaviour to follow setting changes, if any.
                    // Note that this is delayed until a BMP message was received,
                    // but as we only read BMP messages it shouldn't matter that
                    // we can't react to setting changes while waiting for the
                    // next message but can then handle it at that point.
                    match status {
                        Some(GateStatus::Reconfiguring {
                            new_config: Unit::BmpTcpIn(_unit),
                        }) => {
                            // We don't have any settings to reconfigure.
                        }

                        Some(GateStatus::ReportLinks { report }) => {
                            report.declare_source();
                        }

                        _ => { /* Nothing to do */ }
                    }

                    let tracing_mode = **self.tracing_mode.load();

                    if trace_id == 0 && tracing_mode == TracingMode::On {
                        trace_id = self.tracer.next_tracing_id();
                    }

                    if trace_id > 0 || tracing_mode == TracingMode::On {
                        self.tracer.clear_trace_id(trace_id);
                    }

                    if let Ok(bmp_msg) = Message::from_octets(msg_buf) {
                        let trace_id = if trace_id > 0
                            || tracing_mode == TracingMode::On
                        {
                            self.tracer.note_component_event(
                                trace_id,
                                self.gate.id(),
                                format!("Started tracing BMP message {bmp_msg:#?}"),
                            );
                            Some(trace_id)
                        } else {
                            None
                        };
                        if let Err((router_id, err)) = self
                            .process_msg(
                                received,
                                router_addr,
                                //source_id.clone(),
                                ingress_id,
                                bmp_msg,
                                //None,
                                provenance,
                                trace_id,
                            )
                            .await
                        {
                            self.status_reporter
                                .router_connection_aborted(&router_id, err);
                            self.bmp_metrics
                                .remove_router_metrics(&router_id);
                            break;
                        }
                    }
                }
            }
        }

        let bmp_state_lock = self.state_machine.lock().await;

        self.status_reporter.router_connection_lost(
            &bmp_state_lock.as_ref().unwrap().router_id(),
        );

        // Signal withdrawal of all bgp sessions monitored via this BMP
        // session:
        let session_ids = ingress_register.ids_for_parent(ingress_id);
        self.gate
            .update_data(Update::WithdrawBulk(session_ids.into()))
            .await;

        // Signal withdrawal of all address families for this ingress_id.
        // XXX if ingress ids are assigned properly, i.e. on the BGP level
        // within this BMP stream, there should be no RIB entries for the
        // ingress id of the BMP connector.
        //self.gate
        //    .update_data(Update::Withdraw(ingress_id, None))
        //    .await;

        // Notify downstream units that the data stream for this
        // particular monitored router has ended.
        let new_status = UpstreamStatus::EndOfStream { ingress_id };

        self.gate
            .update_data(Update::UpstreamStatusChange(new_status))
            .await;
    }

    async fn process_msg(
        &self,
        received: std::time::Instant,
        addr: SocketAddr,
        //source_id: SourceId,
        ingress_id: IngressId,
        msg: Message<Bytes>,
        provenance: Provenance,
        trace_id: Option<u8>,
    ) -> Result<(), (Arc<RouterId>, String)> {
        let mut bmp_state_lock = self.state_machine.lock().await;

        // SAFETY: Each connection should always have a state machine.
        let bmp_state = bmp_state_lock.take().unwrap();

        if let Some(last_msg_at) = &self.last_msg_at {
            if let Ok(mut guard) = last_msg_at.write() {
                *guard = Utc::now();
            }
        }

        let _bound_tracer = self.tracer.bind(self.gate.id());

        self.status_reporter.message_received(
            bmp_state.router_id(),
            msg.common_header().msg_type().into(),
        );

        let mut output_stream = RotoOutputStream::new();
        let verdict = self.roto_function.as_ref().map(|roto_function| {
            roto_function.call(
                roto::Val(&mut output_stream),
                roto::Val(msg.clone()),
                roto::Val(provenance),
            )
        });
        if !output_stream.is_empty() {
            let mut osms = smallvec![];
            for entry in output_stream.drain() {
                let osm = match entry {
                    Output::Prefix(_prefix) => {
                        OutputStreamMessage::prefix(None, Some(ingress_id))
                    }
                    Output::Community(_u32) => {
                        OutputStreamMessage::community(None, Some(ingress_id))
                    }
                    Output::Asn(_u32) => {
                        OutputStreamMessage::asn(None, Some(ingress_id))
                    }
                    Output::Origin(_u32) => {
                        OutputStreamMessage::origin(None, Some(ingress_id))
                    }
                    Output::PeerDown => {
                        if let Message::PeerDownNotification(ref pdn) = msg {
                            let pph = pdn.per_peer_header();
                            OutputStreamMessage::peer_down(
                                "mqtt".into(),
                                "peerdown".into(),
                                pph.address(),
                                pph.asn(),
                                Some(ingress_id),
                            )
                        } else {
                            error!(
                                "log_peer_down on a non-peerdownnotification"
                            );
                            continue;
                        }
                    }
                    Output::Custom((id, local)) => {
                        OutputStreamMessage::custom(
                            id,
                            local,
                            Some(ingress_id),
                        )
                    }
                };
                osms.push(osm);
            }
            self.gate.update_data(Update::OutputStream(osms)).await;
        }
        let next_state = match verdict {
            // Default action when no roto script is used
            // is Accept (i.e. None here).
            Some(roto::Verdict::Accept(_)) | None => {
                self.status_reporter
                    .message_processed(bmp_state.router_id());

                let mut res = bmp_state.process_msg(received, msg, trace_id);

                match res.message_type {
                    MessageType::InvalidMessage { .. } => {
                        self.status_reporter.message_processing_failure(
                            res.next_state.router_id(),
                        );
                    }

                    MessageType::StateTransition => {
                        // If we have transitioned to the Dumping state that
                        // means we just processed an Initiation message and
                        // MUST have captured a sysName Information TLV
                        // string. Use the captured value to make the router
                        // ID more meaningful, instead of the
                        // UNKNOWN_ROUTER_SYSNAME sysName value we used until
                        // now.
                        self.check_update_router_id(
                            addr,
                            ingress_id, //&source_id,
                            &mut res.next_state,
                        );
                    }

                    MessageType::RoutingUpdate { update } => {
                        // Pass the routing update on to downstream units
                        // and/or targets. This is where we send an update
                        // down the pipeline.
                        self.gate.update_data(update).await;
                    }

                    MessageType::Other => {
                        // A BMP initiation message received after the
                        // initiation phase will result in this type of
                        // message.
                        self.check_update_router_id(
                            addr,
                            ingress_id, //&source_id,
                            &mut res.next_state,
                        );
                    }

                    MessageType::Aborted => {
                        // Something went fatally wrong and we've lost the BMP
                        // state machine. The issue should already have been
                        // logged so there's nothing more we can do here
                        // except stop processing this BMP stream.
                        return Err((
                            res.next_state.router_id(),
                            "Aborted".to_string(),
                        ));
                    }
                }
                res.next_state
            }
            Some(roto::Verdict::Reject(_)) => {
                // increase metrics and continue
                debug!("bmp-in roto Reject");
                bmp_state
            }
        };

        *bmp_state_lock = Some(next_state);
        Ok(())

        //LH: we'll need roughly the same as in the BGP handler here:
        //- execute the BMP pre-explosion filter
        //- based on the verdict, call process_msg or not
        //- handle any items in the outputstream queue

        /*

        let next_state = if let ControlFlow::Continue(FilterOutput {
            south,
            east,
            received,
        }) = Self::VM
            .with(|vm| {
                let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(
                    msg.into(),
                ));

                // We will only create a context if we have a provenance, i.e.
                // we have already processed a BMP PeerUpNotification (the
                // first BMP message with a per-peer-header that contains some
                // of the Provenance content).
                //
                // LH: but, we must have a RouteContext for the VM to build
                // successfully...
                // Currently, prior to the very first run of the VM,
                // vm.borrow_mut().as_mut() will return None and thus the
                // map() below has no effect.
                let ctx;
                if let Some(prov) = provenance {
                    ctx = RouteContext::new(None, NlriStatus::InConvergence, prov);
                    vm.borrow_mut().as_mut().map(|vm| vm.update_context(Arc::new(ctx.clone())));
                } else {
                    ctx = RouteContext::new(None, NlriStatus::InConvergence, Provenance::mock());
                }

                self.roto_scripts.exec_with_tracer(
                    vm,
                    &self.filter_name.load(),
                    value,
                    received,
                    bound_tracer,
                    trace_id,
                    ctx,
                )
            })
            .map_err(|err| {
                self.status_reporter.message_filtering_failure(&err);
                (bmp_state.router_id(), err.to_string())
            })? {
            if !south.is_empty() {
                let context = RouteContext::new(
                    None,
                    NlriStatus::Empty,
                    provenance.unwrap_or_else(||Provenance::mock())
                );
                let payload = Payload::from_output_stream_queue(
                    south,
                    context,
                    trace_id
                ).into();
                self.gate.update_data(payload).await;
            }

            if let TypeValue::Builtin(BuiltinTypeValue::BmpMessage(msg)) =
                east
            {
                self.status_reporter
                    .message_processed(bmp_state.router_id());

                let mut res = bmp_state.process_msg(received, msg.into_inner(), trace_id);

                match res.message_type {
                    MessageType::InvalidMessage { .. } => {
                        self.status_reporter.message_processing_failure(
                            res.next_state.router_id(),
                        );
                    }

                    MessageType::StateTransition => {
                        // If we have transitioned to the Dumping state that means we
                        // just processed an Initiation message and MUST have captured
                        // a sysName Information TLV string. Use the captured value to
                        // make the router ID more meaningful, instead of the
                        // UNKNOWN_ROUTER_SYSNAME sysName value we used until now.
                        self.check_update_router_id(
                            addr,
                            &source_id,
                            &mut res.next_state,
                        );
                    }

                    MessageType::RoutingUpdate { update } => {
                        // Pass the routing update on to downstream units and/or targets.
                        // This is where we send an update down the pipeline.
                        self.gate.update_data(update).await;
                    }

                    MessageType::Other => {
                        // A BMP initiation message received after the initiation
                        // phase will result in this type of message.
                        self.check_update_router_id(
                            addr,
                            &source_id,
                            &mut res.next_state,
                        );
                    }

                    MessageType::Aborted => {
                        // Something went fatally wrong and we've lost the BMP
                        // state machine. The issue should already have been
                        // logged so there's nothing more we can do here
                        // except stop processing this BMP stream.
                        return Err((
                            res.next_state.router_id(),
                            "Aborted".to_string(),
                        ));
                    }
                }

                res.next_state
            } else {
                bmp_state
            }
        } else {
            bmp_state
        };

        *bmp_state_lock = Some(next_state);
        Ok(())
        */
    }

    fn check_update_router_id(
        &self,
        _addr: SocketAddr, // TODO: Why both socket address AND source id?
        //source_id: &SourceId,
        ingress_id: IngressId,
        next_state: &mut BmpState,
    ) {
        let new_sys_name = match next_state {
            BmpState::Dumping(v) => &v.details.sys_name,
            BmpState::Updating(v) => &v.details.sys_name,
            _ => {
                // Other states don't carry the sys name
                return;
            }
        };

        let new_router_id = Arc::new(format_source_id(
            &self.router_id_template.load(),
            new_sys_name,
            //sources_id,
            ingress_id,
        ));

        let old_router_id = next_state.router_id();
        if new_router_id != old_router_id {
            // Ensure that on first use the metrics for this
            // new router ID are correctly initialised.
            self.status_reporter
                .router_id_changed(old_router_id, new_router_id.clone());

            match next_state {
                BmpState::Dumping(v) => v.router_id = new_router_id.clone(),
                BmpState::Updating(v) => v.router_id = new_router_id.clone(),
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{io::ReadBuf, time::timeout};

    use crate::{
        bgp::encode::{
            mk_initiation_msg,
            mk_invalid_initiation_message_that_lacks_information_tlvs,
            mk_peer_down_notification_msg, mk_per_peer_header,
        },
        common::status_reporter::AnyStatusReporter,
        metrics::Target,
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
    };

    use super::*;

    const SYS_NAME: &str = "some-sys-name";
    const SYS_DESCR: &str = "some-sys-desc";
    const OTHER_SYS_NAME: &str = "other-sys-name";

    #[tokio::test(flavor = "multi_thread")]
    async fn terminate_on_loss_of_parent_gate() {
        let (runner, _gate_agent, parent_gate) = RouterHandler::mock();

        struct MockRouterStream;

        impl AsyncRead for MockRouterStream {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<tokio::io::Result<()>> {
                Poll::Pending
            }
        }

        let rx = MockRouterStream;

        eprintln!("STARTING ROUTER READER");
        let router_addr = "1.2.3.4:12345".parse().unwrap();
        let source_id = 0_u32.into();
        let join_handle = runner.read_from_router(
            rx,
            router_addr,
            source_id,
            Arc::new(ingress::Register::default()),
        );

        // Simulate the unit terminating. Without this the reader continues
        // forever.
        eprintln!("DROPPING PARENT GATE");
        drop(parent_gate);

        eprintln!("WAITING FOR ROUTER READER TO EXIT");
        timeout(Duration::from_secs(5), join_handle).await.unwrap();

        eprintln!("DONE");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_count_io_errors() {
        enable_logging("trace");
        let (runner, _, _parent_gate) = RouterHandler::mock();

        struct MockRouterStream {
            interrupted_already: bool,
            status_reporter: Arc<BmpTcpInStatusReporter>,
        }

        impl AsyncRead for MockRouterStream {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<tokio::io::Result<()>> {
                // Fail with a non-fatal error so that reading from the router
                // continues giving us a chance to check the router specific
                // metrics rather than returning a fatal error which would
                // cause the simulated router to be disconnected and its
                // associated metrics to be removed.
                if !self.interrupted_already {
                    self.get_mut().interrupted_already = true;
                    Poll::Ready(Err(std::io::ErrorKind::Interrupted.into()))
                } else {
                    let metrics = get_testable_metrics_snapshot(
                        &self.status_reporter.metrics().unwrap(),
                    );
                    // Unknown because no BMP Initiation message with a
                    // sysName was processed
                    let label = ("router", "unknown");
                    assert_eq!(
                        metrics.with_label::<usize>(
                            "bmp_tcp_in_num_bmp_messages_received",
                            label
                        ),
                        0
                    );
                    assert_eq!(
                        metrics.with_label::<usize>(
                            "bmp_tcp_in_num_receive_io_errors",
                            label
                        ),
                        1
                    );

                    // Fail with a fatal error to stop the reader polling for
                    // more data.
                    Poll::Ready(Err(std::io::ErrorKind::Other.into()))
                }
            }
        }

        let router_addr = "1.2.3.4:12345".parse().unwrap();
        let source_id = 0_u32.into();

        let rx = MockRouterStream {
            interrupted_already: false,
            status_reporter: runner.status_reporter.clone(),
        };

        let metrics = get_testable_metrics_snapshot(
            &runner.status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            0
        );

        runner
            .read_from_router(
                rx,
                router_addr,
                source_id,
                Arc::new(ingress::Register::default()),
            )
            .await;

        let metrics = get_testable_metrics_snapshot(
            &runner.status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            1
        );
    }

    // Note: The tests below assume that the default router id template
    // includes the BMP sysName.

    #[tokio::test(flavor = "multi_thread")]
    async fn num_invalid_bmp_messages_counter_should_increase() {
        let (runner, _, _) = RouterHandler::mock();

        // A BMP Initiation message that lacks required fields
        let bad_initiation_msg = Message::from_octets(
            mk_invalid_initiation_message_that_lacks_information_tlvs(),
        )
        .unwrap();

        // A BMP Peer Down Notification message without a corresponding Peer
        // Up Notification message.
        let pph = mk_per_peer_header("10.0.0.1", 12345);
        let bad_peer_down_msg =
            Message::from_octets(mk_peer_down_notification_msg(&pph))
                .unwrap();

        process_msg(&runner, bad_initiation_msg, None)
            .await
            .unwrap();
        process_msg(&runner, bad_peer_down_msg, None).await.unwrap();

        let metrics = get_testable_metrics_snapshot(
            &runner.status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_label::<usize>(
                "bmp_in_num_invalid_bmp_messages",
                ("router", "unknown")
            ),
            2,
        );
    }

    #[rustfmt::skip]
    #[tokio::test(flavor = "multi_thread")]
    async fn new_counters_should_be_started_if_the_router_id_changes() {
        let (runner, ..) = RouterHandler::mock();
        let initiation_msg =
            Message::from_octets(mk_initiation_msg(SYS_NAME, SYS_DESCR))
                .unwrap();
        let pph = mk_per_peer_header("10.0.0.1", 12345);
        let bad_peer_down_msg =
            Message::from_octets(mk_peer_down_notification_msg(&pph))
                .unwrap();
        let reinitiation_msg = Message::from_octets(mk_initiation_msg(
            OTHER_SYS_NAME,
            SYS_DESCR,
        ))
        .unwrap();

        // router id is "unknown" at this point
        process_msg(&runner, bad_peer_down_msg.clone(), None).await.unwrap(); // 1
        process_msg(&runner, initiation_msg, None).await.unwrap(); // 2

        // messages after this point are counted under router id SYS_NAME
        process_msg(&runner, bad_peer_down_msg.clone(), None).await.unwrap(); // 3
        process_msg(&runner, reinitiation_msg, None).await.unwrap(); // 4

        // messages after this point are counted under router id OTHER_SYS_NAME
        process_msg(&runner, bad_peer_down_msg, None).await.unwrap(); // 5

        let metrics = get_testable_metrics_snapshot(
            &runner.status_reporter.metrics().unwrap(),
        );

        // router id is only determined AFTER the message has been processed
        // by the BMP state machine, but messages are counted by type as soon
        // as they are received i.e. under the last known router id. Thus the
        // sysName value carried by a BMP Initiation Message does NOT
        // influence the router id of the metric counter which the receipt of
        // the BMP Initiation Message causes us to increment.

        assert_metric_label_value(
            &metrics, "unknown", "bmp_tcp_in_num_bmp_messages_received",
            "msg_type", "Peer Down Notification", 1
        ); // from 1
        assert_metric_value(
            &metrics, "unknown", "bmp_in_num_invalid_bmp_messages", 1
        ); // from 1
        assert_metric_label_value(
            &metrics, "unknown", "bmp_tcp_in_num_bmp_messages_received",
            "msg_type", "Initiation Message", 1
        ); // from 2
        assert_metric_label_value(
            &metrics, SYS_NAME, "bmp_tcp_in_num_bmp_messages_received",
            "msg_type", "Peer Down Notification", 1
        ); // from 3
        assert_metric_value(
            &metrics, SYS_NAME, "bmp_in_num_invalid_bmp_messages", 1
        ); // from 3
        assert_metric_label_value(
            &metrics, SYS_NAME, "bmp_tcp_in_num_bmp_messages_received",
            "msg_type", "Initiation Message", 1
        ); // from 4
        assert_metric_value(
            &metrics, OTHER_SYS_NAME, "bmp_in_num_invalid_bmp_messages", 1
        ); // from 5
        assert_metric_label_value(
            &metrics, OTHER_SYS_NAME, "bmp_tcp_in_num_bmp_messages_received",
            "msg_type", "Peer Down Notification", 1
        ); // from 5
    }

    // --- Test helpers ------------------------------------------------------

    async fn process_msg(
        router_handler: &RouterHandler,
        msg: Message<bytes::Bytes>,
        provenance: Option<Provenance>,
    ) -> Result<(), (Arc<String>, String)> {
        router_handler
            .process_msg(
                std::time::Instant::now(),
                "1.2.3.4:12345".parse().unwrap(),
                0_u32.into(),
                msg,
                provenance.unwrap(),
                None,
            )
            .await
    }

    fn assert_metric_value(
        metrics: &Target,
        router: &str,
        metric_name: &str,
        expected_value: usize,
    ) {
        assert_eq!(
            metrics.with_label::<usize>(metric_name, ("router", router)),
            expected_value,
        );
    }

    fn assert_metric_label_value(
        metrics: &Target,
        router: &str,
        metric_name: &str,
        label_name: &str,
        label_value: &str,
        expected_value: usize,
    ) {
        assert_eq!(
            metrics.with_labels::<usize>(
                metric_name,
                &[("router", router), (label_name, label_value)],
            ),
            expected_value,
        );
    }
}
