//! BMP message stream handler for a single connected BMP publishing client.
use std::cell::RefCell;
use std::sync::{Arc, RwLock};
use std::{net::SocketAddr, ops::ControlFlow};

use arc_swap::ArcSwap;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use roto::types::{
    builtin::BuiltinTypeValue, collections::BytesRecord,
    lazyrecord_types::BmpMessage, typevalue::TypeValue,
};
use routecore::bmp::message::Message;
use tokio::sync::Mutex;
use tokio::{io::AsyncRead, net::TcpStream};

use crate::common::roto::{
    FilterName, FilterOutput, RotoScripts, ThreadLocalVM,
};
use crate::payload::{RouterId, SourceId};
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
use super::state_machine::machine::BmpState;
use super::state_machine::metrics::BmpMetrics;
use super::state_machine::processing::MessageType;
use super::unit::TracingMode;
use super::util::format_source_id;

pub struct RouterHandler {
    gate: Gate,
    roto_scripts: RotoScripts,
    router_id_template: Arc<ArcSwap<String>>,
    filter_name: Arc<ArcSwap<FilterName>>,
    status_reporter: Arc<BmpTcpInStatusReporter>,
    state_machine: Arc<Mutex<Option<BmpState>>>,
    tracer: Arc<Tracer>,
    tracing_mode: Arc<ArcSwap<TracingMode>>,
    last_msg_at: Option<Arc<RwLock<DateTime<Utc>>>>,
    bmp_metrics: Arc<BmpMetrics>,
}

impl RouterHandler {
    thread_local!(
        static VM: ThreadLocalVM = RefCell::new(None);
    );

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gate: Gate,
        roto_scripts: RotoScripts,
        router_id_template: Arc<ArcSwap<String>>,
        filter_name: Arc<ArcSwap<FilterName>>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
        state_machine: Arc<Mutex<Option<BmpState>>>,
        tracer: Arc<Tracer>,
        tracing_mode: Arc<ArcSwap<TracingMode>>,
        last_msg_at: Option<Arc<RwLock<DateTime<Utc>>>>,
        bmp_metrics: Arc<BmpMetrics>,
    ) -> Self {
        Self {
            gate,
            roto_scripts,
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
        use super::metrics::BmpTcpInMetrics;

        let (parent_gate, gate_agent) = Gate::new(0);

        let source_id =
            SourceId::SocketAddr("1.2.3.4:12345".parse().unwrap());
        let router_id = Arc::new("unknown".into());
        let bmp_in_metrics = Arc::new(BmpTcpInMetrics::default());
        let bmp_metrics = Arc::new(BmpMetrics::default());
        let parent_status_reporter = Arc::new(BmpTcpInStatusReporter::new(
            "dummy",
            bmp_in_metrics.clone(),
        ));

        let state_machine = BmpState::new(
            source_id,
            router_id,
            parent_status_reporter.clone(),
            bmp_metrics.clone(),
        );

        let state_machine = Arc::new(Mutex::new(Some(state_machine)));

        let mock = Self {
            gate: parent_gate.clone(),
            roto_scripts: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
            status_reporter: parent_status_reporter,
            state_machine,
            tracer: Default::default(),
            tracing_mode: Default::default(),
            last_msg_at: None,
            bmp_metrics,
        };

        (mock, gate_agent, parent_gate)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        mut tcp_stream: TcpStream,
        router_addr: SocketAddr,
        source_id: SourceId,
    ) {
        // Discard the write half of the TCP stream as we are a "monitoring
        // station" and per the BMP RFC 7584 specification _"No BMP message is
        // ever sent from the monitoring station to the monitored router"_.
        // See: https://datatracker.ietf.org/doc/html/rfc7854#section-3.2
        let (rx, _tx) = tcp_stream.split();
        self.read_from_router(rx, router_addr, source_id).await;
    }

    async fn read_from_router<T: AsyncRead + Unpin>(
        &self,
        rx: T,
        router_addr: SocketAddr,
        source_id: SourceId,
    ) {
        // Setup BMP streaming
        let mut stream =
            BmpStream::new(rx, self.gate.clone(), self.tracing_mode.clone());

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
                    let received = Utc::now();

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

                    if let Ok(bmp_msg) = BmpMessage::from_octets(msg_buf) {
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
                                source_id.clone(),
                                bmp_msg,
                                trace_id,
                            )
                            .await
                        {
                            self.status_reporter
                                .router_connection_aborted(&router_id, err);
                            self.bmp_metrics.remove_router_metrics(&router_id);
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

        // Notify downstream units that the data stream for this
        // particular monitored router has ended.
        let new_status = UpstreamStatus::EndOfStream {
            source_id: router_addr.into(),
        };
        self.gate
            .update_data(Update::UpstreamStatusChange(new_status))
            .await;
    }

    async fn process_msg(
        &self,
        received: DateTime<Utc>,
        addr: SocketAddr,
        source_id: SourceId,
        msg: Message<Bytes>,
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

        let bound_tracer = self.tracer.bind(self.gate.id());

        self.status_reporter
            .bmp_message_received(bmp_state.router_id());

        let next_state = if let ControlFlow::Continue(FilterOutput {
            south,
            east,
            received,
        }) = Self::VM
            .with(|vm| {
                let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(
                    Arc::new(BytesRecord(msg)),
                ));
                self.roto_scripts.exec_with_tracer(
                    vm,
                    &self.filter_name.load(),
                    value,
                    received,
                    bound_tracer,
                    trace_id,
                )
            })
            .map_err(|err| {
                self.status_reporter.message_filtering_failure(&err);
                (bmp_state.router_id(), err.to_string())
            })? {
            if !south.is_empty() {
                let payload =
                    Payload::from_output_stream_queue(south, trace_id).into();
                self.gate.update_data(payload).await;
            }

            if let TypeValue::Builtin(BuiltinTypeValue::BmpMessage(msg)) =
                east
            {
                let msg = Arc::into_inner(msg).unwrap(); // This should succeed
                let msg = msg.0;

                self.status_reporter
                    .bmp_message_processed(bmp_state.router_id());

                let mut res = bmp_state.process_msg(received, msg, trace_id);

                match res.processing_result {
                    MessageType::InvalidMessage {
                        err,
                        known_peer,
                        msg_bytes,
                    } => {
                        self.status_reporter.invalid_bmp_message_received(
                            res.next_state.router_id(),
                        );
                        if let Some(reporter) =
                            res.next_state.status_reporter()
                        {
                            reporter.bgp_update_parse_hard_fail(
                                res.next_state.router_id(),
                                known_peer,
                                err,
                                msg_bytes,
                            );
                        }
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
    }

    fn check_update_router_id(
        &self,
        _addr: SocketAddr, // TODO: Why both socket address AND source id?
        source_id: &SourceId,
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
            source_id,
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
        common::status_reporter::AnyStatusReporter,
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
    };

    use super::*;

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
        let source_id = "dummy".into();
        let join_handle = runner.read_from_router(rx, router_addr, source_id);

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
                    let label = ("router", "unknown"); // Unknown because no BMP Initiation message with a sysName was processed
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
        let source_id = "dummy".into();

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

        runner.read_from_router(rx, router_addr, source_id).await;

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

    // #[tokio::test(flavor = "multi_thread")]
    // #[should_panic]
    // async fn counters_for_expected_sys_name_should_not_exist() {
    //     let (runner, _, _) = RouterHandler::mock();
    //     let initiation_msg =
    //         BmpMessage::from_octets(mk_initiation_msg(SYS_NAME, SYS_DESCR))
    //             .unwrap();

    //     runner
    //         .process_msg(
    //             "127.0.0.1".parse().unwrap(),
    //             "unknown".into(),
    //             initiation_msg,
    //             None,
    //         )
    //         .await;

    // // let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
    // // assert_eq!(
    // //     metrics.with_label::<usize>("bmp_in_num_invalid_bmp_messages", ("router", SYS_NAME)),
    // //     0,
    // // );
    // }

    // #[tokio::test(flavor = "multi_thread")]
    // #[should_panic]
    // async fn counters_for_other_sys_name_should_not_exist() {
    //     let (runner, _) = BmpInRunner::mock();
    //     let initiation_msg = mk_update(mk_initiation_msg(SYS_NAME, SYS_DESCR));

    //     runner.process_update(initiation_msg).await;

    //     let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
    //     assert_eq!(
    //         metrics.with_label::<usize>(
    //             "bmp_in_num_invalid_bmp_messages",
    //             ("router", OTHER_SYS_NAME)
    //         ),
    //         0,
    //     );
    // }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn num_invalid_bmp_messages_counter_should_increase() {
    //     let (runner, _) = BmpInRunner::mock();

    //     // A BMP Initiation message that lacks required fields
    //     let bad_initiation_msg =
    //         mk_update(mk_invalid_initiation_message_that_lacks_information_tlvs());

    //     // A BMP Peer Down Notification message without a corresponding Peer
    //     // Up Notification message.
    //     let pph = mk_per_peer_header("10.0.0.1", 12345);
    //     let bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));

    //     runner.process_update(bad_initiation_msg).await;
    //     runner.process_update(bad_peer_down_msg).await;

    //     let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
    //     assert_eq!(
    //         metrics.with_label::<usize>(
    //             "bmp_in_num_invalid_bmp_messages",
    //             ("router", UNKNOWN_ROUTER_SYSNAME)
    //         ),
    //         2,
    //     );
    // }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn new_counters_should_be_started_if_the_router_id_changes() {
    //     let (runner, _) = BmpInRunner::mock();
    //     let initiation_msg = mk_update(mk_initiation_msg(SYS_NAME, SYS_DESCR));
    //     let pph = mk_per_peer_header("10.0.0.1", 12345);
    //     let bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));
    //     let reinitiation_msg = mk_update(mk_initiation_msg(OTHER_SYS_NAME, SYS_DESCR));
    //     let another_bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));

    //     runner.process_update(initiation_msg).await;
    //     runner.process_update(bad_peer_down_msg).await;
    //     runner.process_update(reinitiation_msg).await;
    //     runner.process_update(another_bad_peer_down_msg).await;

    //     let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
    //     assert_eq!(
    //         metrics.with_label::<usize>("bmp_in_num_invalid_bmp_messages", ("router", SYS_NAME)),
    //         1,
    //     );
    //     assert_eq!(
    //         metrics.with_label::<usize>(
    //             "bmp_in_num_invalid_bmp_messages",
    //             ("router", OTHER_SYS_NAME)
    //         ),
    //         1,
    //     );
    // }

    // --- Test helpers ------------------------------------------------------

    // fn mk_update(msg_buf: Bytes) -> Update {
    //     let source_id =
    //         SourceId::SocketAddr("127.0.0.1:8080".parse().unwrap());
    //     let bmp_msg =
    //         Arc::new(BytesRecord(BmpMessage::from_octets(msg_buf).unwrap()));
    //     let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg));
    //     Update::Single(Payload::new(source_id, value))
    // }
}
