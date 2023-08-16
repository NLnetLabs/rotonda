use std::{
    cell::RefCell,
    net::SocketAddr,
    ops::ControlFlow,
    path::PathBuf,
    sync::{Arc, Weak},
    time::Instant,
};

use super::state_machine::processing::ProcessingResult;
use crate::{
    common::{
        file_io::{FileIo, TheFileIo},
        roto::is_filtered_in_vm,
    },
    tokio::TokioTaskMetrics,
    units::bmp_in::{
        metrics::BmpInMetrics,
        state_machine::{machine::BmpState, processing::MessageType},
        status_reporter::BmpInStatusReporter,
    },
};
use crate::{
    common::{
        frim::FrimMap,
        roto::ThreadLocalVM,
        status_reporter::{AnyStatusReporter, Chainable, UnitStatusReporter},
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate},
    manager::{Component, WaitPoint},
    payload::RawBmpPayload,
};
use crate::{
    comms::{Gate, GateStatus, Terminated},
    payload::{Payload, Update},
    units::Unit,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use non_empty_vec::NonEmpty;
use roto::{
    traits::RotoType,
    types::{builtin::BuiltinTypeValue, typevalue::TypeValue},
};
use serde::Deserialize;
use tokio::{runtime::Handle, sync::Mutex};

#[cfg(feature = "router-list")]
use {
    super::{
        http::{RouterInfoApi, RouterListApi},
        types::RouterInfo,
    },
    crate::http::ProcessRequest,
    chrono::Utc,
    tokio::sync::RwLock,
};

use super::state_machine::metrics::BmpMetrics;
use super::util::format_router_id;

#[derive(Clone, Debug, Deserialize)]
pub struct BmpIn {
    /// The set of units to receive updates from.
    sources: NonEmpty<DirectLink>,

    /// The relative path at which we should listen for HTTP query API requests
    #[cfg(feature = "router-list")]
    #[serde(default = "BmpIn::default_http_api_path")]
    http_api_path: Arc<String>,

    #[serde(default = "BmpIn::default_router_id_template")]
    pub router_id_template: Arc<String>,

    /// Path to roto script to use
    roto_path: Option<PathBuf>,
}

impl BmpIn {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        BmpInRunner::new(
            component,
            gate,
            self.router_id_template,
            #[cfg(feature = "router-list")]
            self.http_api_path,
            self.roto_path,
            TheFileIo::default(),
        )
        .run(self.sources, waitpoint)
        .await
    }

    #[cfg(feature = "router-list")]
    fn default_http_api_path() -> Arc<String> {
        Arc::new("/routers/".to_string())
    }

    pub fn default_router_id_template() -> Arc<String> {
        Arc::new("{sys_name}".to_string())
    }
}

struct BmpInRunner {
    gate: Arc<Gate>,
    #[cfg(feature = "router-list")]
    component: Arc<RwLock<Component>>,
    router_id_template: Arc<String>,
    router_states: Arc<FrimMap<SocketAddr, Arc<tokio::sync::Mutex<Option<BmpState>>>>>, // Option is never None, instead Some is take()'n and replace()'d.
    #[cfg(feature = "router-list")]
    router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>,
    router_metrics: Arc<BmpMetrics>,
    status_reporter: Arc<BmpInStatusReporter>,
    #[cfg(feature = "router-list")]
    http_api_path: Arc<String>,
    #[cfg(feature = "router-list")]
    _api_processor: Arc<dyn ProcessRequest>,
    roto_source: Arc<ArcSwap<(std::time::Instant, String)>>,
    state_machine_metrics: Arc<TokioTaskMetrics>,
    file_io: TheFileIo,
}

impl BmpInRunner {
    thread_local!(
        #[allow(clippy::type_complexity)]
        static VM: ThreadLocalVM = RefCell::new(None);
    );

    fn new(
        mut component: Component,
        gate: Gate,
        router_id_template: Arc<String>,
        #[cfg(feature = "router-list")] http_api_path: Arc<String>,
        roto_path: Option<PathBuf>,
        file_io: TheFileIo,
    ) -> Self {
        let unit_name = component.name().clone();

        // Setup our metrics
        let router_metrics = Arc::new(BmpInMetrics::new(&gate));
        component.register_metrics(router_metrics.clone());

        // Setup our status reporting
        let status_reporter =
            Arc::new(BmpInStatusReporter::new(&unit_name, router_metrics.clone()));

        // Setup metrics to be updated by the BMP state machines that we use
        // to make sense of the BMP data per router that supplies it.
        let bmp_metrics = Arc::new(BmpMetrics::new());
        component.register_metrics(bmp_metrics.clone());

        let state_machine_metrics = Arc::new(TokioTaskMetrics::new());
        component.register_metrics(state_machine_metrics.clone());

        // Setup storage for tracking multiple connected router BMP states at
        // once.
        let router_states = Arc::new(FrimMap::default());

        // Setup REST API endpoint
        #[cfg(feature = "router-list")]
        let (_api_processor, router_info) = {
            let router_info = Arc::new(FrimMap::default());

            let processor = Arc::new(RouterListApi::new(
                http_api_path.clone(),
                router_info.clone(),
                router_metrics,
                bmp_metrics.clone(),
                router_id_template.clone(),
                router_states.clone(),
            ));

            component.register_http_resource(processor.clone());

            (processor, router_info)
        };

        #[cfg(feature = "router-list")]
        let component = Arc::new(RwLock::new(component));

        let roto_source_code = roto_path
            .map(|v| file_io.read_to_string(v).unwrap())
            .unwrap_or_default();
        let roto_source = (Instant::now(), roto_source_code);
        let roto_source = Arc::new(ArcSwap::from_pointee(roto_source));

        Self {
            gate: Arc::new(gate),
            #[cfg(feature = "router-list")]
            component,
            router_id_template,
            router_states,
            #[cfg(feature = "router-list")]
            router_info,
            router_metrics: bmp_metrics,
            status_reporter,
            #[cfg(feature = "router-list")]
            http_api_path,
            #[cfg(feature = "router-list")]
            _api_processor,
            roto_source,
            state_machine_metrics,
            file_io,
        }
    }

    pub async fn run(
        self,
        mut sources: NonEmpty<DirectLink>,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        arc_self.gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone waiting to send important gate status
        // updates won't send them while we are in process_until() which will just eat them without handling them.
        waitpoint.running().await;

        loop {
            match arc_self.gate.process().await {
                Ok(status) => {
                    arc_self.status_reporter.gate_status_announced(&status);
                    match status {
                        GateStatus::Reconfiguring {
                            new_config: Unit::BmpIn(BmpIn {
                                sources: new_sources,
                                roto_path: new_roto_path,
                                .. /*http_api_path, router_id_template, filters*/ }),
                        } => {
                            // Replace the roto script with the new one
                            let roto_source_code = if let Some(new_roto_path) = new_roto_path {
                                info!("Using roto script at path '{}'", new_roto_path.to_string_lossy());
                                arc_self.file_io.read_to_string(new_roto_path).unwrap()
                            } else {
                                Default::default()
                            };
                            let roto_source = (Instant::now(), roto_source_code);
                            let roto_source = Arc::new(roto_source);
                            arc_self.roto_source.store(roto_source);

                            arc_self.status_reporter.reconfigured();

                            // Register as a direct update receiver with the new
                            // set of linked gates.
                            arc_self
                                .status_reporter
                                .upstream_sources_changed(sources.len(), new_sources.len());
                            sources = new_sources;
                            for link in sources.iter_mut() {
                                link.connect(arc_self.clone(), false).await.unwrap();
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.set_sources(&sources);
                            report.set_graph_status(arc_self.gate.metrics());
                        }

                        _ => { /* Nothing to do */ }
                    }
                }

                Err(Terminated) => {
                    arc_self.status_reporter.terminated();
                    return Err(Terminated);
                }
            }
        }
    }

    fn router_connected(&self, router_addr: &SocketAddr) -> BmpState {
        let router_id = Arc::new(format_router_id(
            self.router_id_template.clone(),
            "unknown",
            router_addr,
        ));

        // Choose a name to be reported in application logs.
        let child_name = format!("router[{}:{}]", router_addr.ip(), router_addr.port());

        // Create a status reporter whose name in output will be a combination
        // of ours as parent and the newly chosen child name, enabling logged
        // messages relating to this newly connected router to be
        // distinguished from logged messages relating to other connected
        // routers.
        let child_status_reporter = Arc::new(self.status_reporter.add_child(child_name));

        #[cfg(feature = "router-list")]
        {
            let this_router_info = Arc::new(RouterInfo::new());
            self.router_info.insert(*router_addr, this_router_info);
        }

        let metrics = self.router_metrics.clone();

        BmpState::new(*router_addr, router_id, child_status_reporter, metrics)
    }

    fn router_disconnected(&self, router_addr: &SocketAddr) {
        self.router_states.remove(router_addr);
        // TODO: also remove from self.router_info ?
    }

    async fn process_msg(
        &self,
        router_addr: &SocketAddr,
        bmp_state: BmpState,
        msg_buf: Bytes,
    ) -> BmpState {
        #[cfg(feature = "router-list")]
        if let Some(router_info) = self.router_info.get(router_addr) {
            let lock = router_info.last_msg_at.write();
            match lock {
                Ok(mut guard) => *guard = Utc::now(),
                Err(err) => {
                    // This should never happen.
                    self.status_reporter.internal_error(format!(
                        "Failed to acquire write lock for last_msg_at for router connection {}: {}",
                        router_addr, err,
                    ));
                }
            }
        }

        let roto_source = self.roto_source.clone();
        let res = bmp_state.process_msg_with_filter(
            msg_buf,
            Some(roto_source),
            |raw_bgp_msg, roto_source| {
                // map the result type from TypeValue to BgpUpdateMessage
                match Self::is_filtered(raw_bgp_msg, roto_source) {
                    Ok(ControlFlow::Break(())) => Ok(ControlFlow::Break(())),
                    // TODO: Modify roto to not output an Arc if the given rx wasn't an Arc, so then we don't have to do the Arc::try_unwrap() dance.
                    Ok(ControlFlow::Continue(TypeValue::Builtin(BuiltinTypeValue::BgpUpdateMessage(raw_bgp_msg)))) => Ok(ControlFlow::Continue(Arc::try_unwrap(raw_bgp_msg).unwrap())),
                    Ok(ControlFlow::Continue(some_unsupported_type)) => Err(format!("Filter result type must be BgpUpdateMessage, found: {some_unsupported_type}")),
                    Err(err) => Err(format!("Filter execution failed: {err}")),
                }
            })
            .await;

        self.process_result(res, router_addr).await
    }

    pub async fn process_result(
        &self,
        mut res: ProcessingResult,
        router_addr: &SocketAddr,
    ) -> BmpState {
        match res.processing_result {
            MessageType::InvalidMessage {
                err,
                known_peer,
                msg_bytes,
            } => {
                self.status_reporter
                    .invalid_bmp_message_received(res.next_state.router_id());
                if let Some(reporter) = res.next_state.status_reporter() {
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
                // "unknown" sysName value we used until now.
                if let BmpState::Dumping(next_state) = &mut res.next_state {
                    next_state.router_id = Arc::new(format_router_id(
                        self.router_id_template.clone(),
                        &next_state.details.sys_name,
                        router_addr,
                    ));

                    // Ensure that on first use the metrics for this
                    // new router ID are correctly initialised.
                    self.status_reporter
                        .router_id_changed(next_state.router_id.clone());
                }
            }

            MessageType::RoutingUpdate { update } => {
                // Pass the routing update on to downstream units and/or targets.
                // This is where we send an update down the pipeline.
                self.gate.update_data(update).await;
            }

            MessageType::Other => {} // Nothing to do

            MessageType::Aborted => {
                // Something went fatally wrong, the issue should already have
                // been logged so there's nothing more we can do here.
            }
        }

        res.next_state
    }

    // TODO: Should we tear these individual API endpoints down when the
    // connection to the monitored router is lost?
    async fn setup_router_specific_api_endpoint(
        &self,
        state_machine: Weak<Mutex<Option<BmpState>>>,
        #[allow(unused_variables)] router_addr: &SocketAddr,
    ) {
        #[cfg(feature = "router-list")]
        match self.router_info.get(router_addr) {
            None => {
                // This should never happen.
                self.status_reporter.internal_error(format!(
                    "Router info for router connection {} doesn't exist",
                    router_addr,
                ));
            }

            Some(mut this_router_info) => {
                // Setup a REST API endpoint for querying information
                // about this particular monitored router.
                let processor = RouterInfoApi::new(
                    self.http_api_path.clone(),
                    *router_addr,
                    self.status_reporter.metrics(),
                    self.router_metrics.clone(),
                    this_router_info.connected_at,
                    this_router_info.last_msg_at.clone(),
                    state_machine,
                );

                let processor = Arc::new(processor);

                self.component
                    .write()
                    .await
                    .register_http_resource(processor.clone());

                let mut updatable_router_info = Arc::make_mut(&mut this_router_info);
                updatable_router_info.api_processor = Some(processor);

                self.router_info.insert(*router_addr, this_router_info);

                // TODO: unregister the processor if the router disconnects? (maybe after a delay so that we can
                // still inspect the last known state for the monitored router)
            }
        }
    }

    async fn process_update(&self, update: Update) {
        // We may be invoked concurently, potentially even in parallel if
        // different Tokio threads are running different bmp_tcp_in
        // handle_router() tasks, but we should never receive two BMP update
        // messages from the same router connection at the same time, only BMP
        // updates from different router connections can be received in
        // parallel, BMP updates from the same router connection should be
        // received serially with the next one only being received after we
        // finish processing the previous one.
        //
        // Each BMP update must be processed through the BMP state machine for
        // that router connection, which means we have to both lookup the
        // right state machine and process the message potentially modifying
        // the state machine as a result. We must not attempt to use the same
        // state machine instance concurrently, state machine updates must be
        // atomic.
        //
        // The FrimMap clones entries on updates to the map. If a new router
        // connection is received a new entry will be added to the map which
        // would cause the BMP state machines to be cloned. As we don't want
        // an in-progress BMP state machine update to be lost we have to
        // ensure that old and new state machines are in fact the same state
        // machine, or block map updates while any state machines are in use.
        // The latter could be problematic on a system receiving updates from
        // many concurrently connected routers, as it may block lots of
        // updates and/or take a while before the map can be locked for
        // updating, which contradicts our goal of being as fast as we can.
        // We don't use a DashMap as we think we hit lock contention issues
        // previously when using a DashMap.
        //
        // Instead of locking the map, we wrap each state machine in a mutex
        // as a lock/unlock per BMP message per connected router is perhaps
        // fast enough given (a) that the cost of processing the BMP message
        // is non-trivial and (b) that it wouldn't prevent concurrent
        // processing of BMP messages received from other router connections.
        match update {
            Update::Single(Payload::RawBmp {
                router_addr, msg, ..
            }) => {
                let entry = self.router_states.entry(router_addr);

                // Process the message through the BMP state machine.
                // Initialize the state machine if not already done for this
                // router.
                let entry = entry.or_insert_with(|| {
                    let new_entry = Arc::new(tokio::sync::Mutex::new(Some(
                        self.router_connected(&router_addr),
                    )));

                    let weak_ref = Arc::downgrade(&new_entry);
                    tokio::task::block_in_place(|| {
                        Handle::current().block_on(
                            self.setup_router_specific_api_endpoint(weak_ref, &router_addr),
                        );
                    });

                    new_entry
                });

                let mut locked = entry.lock().await;
                let this_state = locked.take().unwrap();

                let next_state = match msg {
                    RawBmpPayload::Eof => {
                        // The connection to the router has been lost so drop the
                        // connection state machine we have for it, if any.
                        let res = this_state.terminate().await;
                        let next_state = self.process_result(res, &router_addr).await;
                        self.router_disconnected(&router_addr);
                        next_state
                    }

                    RawBmpPayload::Msg(msg_bytes) => {
                        // Run the state machine resulting in a new state.
                        self.state_machine_metrics
                            .instrument(self.process_msg(&router_addr, this_state, msg_bytes))
                            .await
                    }
                };

                // TODO: if the next_state is Aborted, we have no way of feeding that back to the TCP connection
                // handler... we can only sit here and uselessly pump any future messages into the dead state
                // machine ... The TCP handling architecture created for the domain crate may serve us better here
                // as it has a means for feeding back down from the application specific network handling layer to
                // the TCP handling layer.

                // Store the new state machine state.
                locked.replace(next_state);
            }

            Update::Single(_) => {
                self.status_reporter
                    .input_mismatch("Update::Single(_)", "Update::Single(_)");
            }

            Update::Bulk(_) => {
                self.status_reporter
                    .input_mismatch("Update::Single(_)", "Update::Bulk(_)");
            }

            Update::QueryResult(..) => {
                self.status_reporter
                    .input_mismatch("Update::Single(_)", "Update::QueryResult(_)");
            }
        }
    }

    fn is_filtered<R: RotoType>(
        rx: R,
        roto_source: Option<Arc<ArcSwap<(Instant, String)>>>,
    ) -> Result<ControlFlow<(), TypeValue>, String> {
        match roto_source {
            None => {
                // No Roto filter defined, accept the BGP UPDATE messsage
                Ok(ControlFlow::Continue(rx.into()))
            }
            Some(roto_source) => {
                // TODO: Run the Roto VM on a dedicated thread pool, to prevent blocking the Tokio async runtime, as we
                // don't know how long we will have to wait for the VM execution to complete (as it depends on the
                // behaviour of the user provided script). Timeouts might be a good idea!
                Self::VM.with(move |vm| -> Result<ControlFlow<(), TypeValue>, String> {
                    is_filtered_in_vm(vm, roto_source, rx)
                })
            }
        }
    }
}

impl AnyDirectUpdate for BmpInRunner {}

#[async_trait]
impl DirectUpdate for BmpInRunner {
    async fn direct_update(&self, update: Update) {
        self.process_update(update).await;
    }
}

impl std::fmt::Debug for BmpInRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BmpInRunner").finish()
    }
}

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use roto::types::builtin::{BgpUpdateMessage, RotondaId};
    use routecore::bgp::message::SessionConfig;

    use crate::bgp::encode::{mk_bgp_update, Announcements, Prefixes};

    use super::*;

    fn mk_bgp_update_msg() -> Bytes {
        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();
        mk_bgp_update(&Prefixes::default(), &announcements, &[])
    }

    fn mk_filter_input(bgp_msg: Bytes) -> BgpUpdateMessage {
        let update_msg = roto::types::builtin::UpdateMessage::new(bgp_msg, SessionConfig::modern());
        let delta_id = (RotondaId(0), 0); // TODO
        BgpUpdateMessage::new(delta_id, update_msg)
    }

    #[test]
    fn sources_are_required() {
        // suppress the panic backtrace as we expect the panic
        std::panic::set_hook(Box::new(|_| {}));

        // parse and panic due to missing 'sources' field
        assert!(mk_config_from_toml("").is_err());
    }

    #[test]
    fn sources_must_be_non_empty() {
        assert!(mk_config_from_toml("sources = []").is_err());
    }

    #[test]
    fn okay_with_one_source() {
        let toml = r#"
        sources = ["some source"]
        "#;

        mk_config_from_toml(toml).unwrap();
    }

    #[tokio::test]
    async fn route_monitoring_afi_accepted() {
        const ROTO_FILTER: &str = r###"
        filter-map filter-unicast-v4-v6-only {
            define {
                rx_tx bgp_msg: BgpUpdateMessage;
            }

            term afi-safi-unicast {
                match {
                    bgp_msg.nlris.afi in [IPV4, IPV6];
                    bgp_msg.nlris.safi == UNICAST;
                }
            }
        
            apply {
                filter match afi-safi-unicast matching {
                    return accept;
                };
                reject;
            }
        }
        "###;

        // Given a Roto filter script for matching all AFIs and SAFIs
        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), ROTO_FILTER.into())));

        // And a BGP UPDATE message to pass as input to the Roto script
        let bgp_update_bytes = mk_bgp_update_msg();
        let bgp_update_msg = mk_filter_input(bgp_update_bytes.clone());

        // When the Roto VM executes the filter script with our BGP UPDATE message as input
        let res = BmpInRunner::is_filtered(bgp_update_msg, Some(roto_source));

        // Then we should be told that it is okay to proceed and the output of the script should match the BGP UPDATE
        // message that we passed in (as the script doesn't modify the output).
        let expected_raw_bgp_message = Arc::new(mk_filter_input(bgp_update_bytes));
        assert_eq!(
            res,
            Ok(ControlFlow::Continue(TypeValue::Builtin(
                BuiltinTypeValue::BgpUpdateMessage(expected_raw_bgp_message)
            )))
        );
    }

    #[tokio::test]
    async fn route_monitoring_afi_rejected_not_ipv6() {
        const ROTO_FILTER: &str = r###"
        filter-map filter-unicast-v4-v6-only {
            define {
                rx_tx bgp_msg: BgpUpdateMessage;
            }

            term afi-safi-unicast {
                match {
                    bgp_msg.nlris.afi in [IPV6];
                }
            }
        
            apply {
                filter match afi-safi-unicast matching {
                    return accept;
                };
                reject;
            }
        }
        "###;

        // Given a Roto filter script for matching only IPv6 AFI
        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), ROTO_FILTER.into())));

        // And an IPv4 BGP UPDATE message to pass as input to the Roto script
        let bgp_update_bytes = mk_bgp_update_msg();
        let bgp_update_msg = mk_filter_input(bgp_update_bytes.clone());

        // When the Roto VM executes the filter script with our BGP UPDATE message as input
        let res = BmpInRunner::is_filtered(bgp_update_msg, Some(roto_source));

        // Then we should be told to sotp as the filter has rejected the input
        assert_eq!(res, Ok(ControlFlow::Break(())));
    }

    // --- Test helpers ------------------------------------------------------

    fn mk_config_from_toml(toml: &str) -> Result<BmpIn, toml::de::Error> {
        toml::de::from_slice::<BmpIn>(toml.as_bytes())
    }
}
