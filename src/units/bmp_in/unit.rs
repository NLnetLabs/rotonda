use std::{
    cell::RefCell,
    sync::{Arc, Weak},
};

use super::state_machine::processing::ProcessingResult;
use crate::{
    common::{
        file_io::TheFileIo,
        frim::FrimMap,
        roto::ThreadLocalVM,
        status_reporter::{AnyStatusReporter, Chainable, UnitStatusReporter},
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate},
    manager::{Component, WaitPoint},
};
use crate::{
    comms::{Gate, GateStatus, Terminated},
    payload::{Payload, Update},
    units::Unit,
};
use crate::{
    payload::{SourceId, UpstreamStatus},
    tokio::TokioTaskMetrics,
    units::bmp_in::{
        metrics::BmpInMetrics,
        state_machine::{machine::BmpState, processing::MessageType},
        status_reporter::BmpInStatusReporter,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use non_empty_vec::NonEmpty;
use roto::types::{builtin::BuiltinTypeValue, typevalue::TypeValue};
use routecore::bmp::message::Message as BmpMsg;
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
use super::util::format_source_id;

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
    router_states: Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>, // Option is never None, instead Some is take()'n and replace()'d.
    #[cfg(feature = "router-list")]
    router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
    router_metrics: Arc<BmpMetrics>,
    status_reporter: Arc<BmpInStatusReporter>,
    #[cfg(feature = "router-list")]
    http_api_path: Arc<String>,
    #[cfg(feature = "router-list")]
    _api_processor: Arc<dyn ProcessRequest>,
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
                                .. /*http_api_path, router_id_template, filters*/ }),
                        } => {
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

    fn router_connected(&self, source_id: &SourceId) -> BmpState {
        let router_id = Arc::new(format_source_id(
            self.router_id_template.clone(),
            "unknown",
            source_id,
        ));

        // Choose a name to be reported in application logs.
        let child_name = format!("router[{}]", source_id);

        // Create a status reporter whose name in output will be a combination
        // of ours as parent and the newly chosen child name, enabling logged
        // messages relating to this newly connected router to be
        // distinguished from logged messages relating to other connected
        // routers.
        let child_status_reporter = Arc::new(self.status_reporter.add_child(child_name));

        #[cfg(feature = "router-list")]
        {
            let this_router_info = Arc::new(RouterInfo::new());
            self.router_info.insert(source_id.clone(), this_router_info);
        }

        let metrics = self.router_metrics.clone();

        BmpState::new(source_id.clone(), router_id, child_status_reporter, metrics)
    }

    fn router_disconnected(&self, source_id: &SourceId) {
        self.router_states.remove(source_id);
        // TODO: also remove from self.router_info ?
    }

    async fn process_msg(
        &self,
        source_id: &SourceId,
        bmp_state: BmpState,
        bmp_msg: BmpMsg<Bytes>,
    ) -> BmpState {
        #[cfg(feature = "router-list")]
        if let Some(router_info) = self.router_info.get(source_id) {
            let lock = router_info.last_msg_at.write();
            match lock {
                Ok(mut guard) => *guard = Utc::now(),
                Err(err) => {
                    // This should never happen.
                    self.status_reporter.internal_error(format!(
                        "Failed to acquire write lock for last_msg_at for source {}: {}",
                        source_id, err,
                    ));
                }
            }
        }

        let res = bmp_state.process_msg(bmp_msg).await;

        self.process_result(res, source_id).await
    }

    pub async fn process_result(
        &self,
        mut res: ProcessingResult,
        source_id: &SourceId,
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
                    next_state.router_id = Arc::new(format_source_id(
                        self.router_id_template.clone(),
                        &next_state.details.sys_name,
                        source_id,
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
        #[allow(unused_variables)] source_id: &SourceId,
    ) {
        #[cfg(feature = "router-list")]
        match self.router_info.get(source_id) {
            None => {
                // This should never happen.
                self.status_reporter.internal_error(format!(
                    "Router info for source {} doesn't exist",
                    source_id,
                ));
            }

            Some(mut this_router_info) => {
                // Setup a REST API endpoint for querying information
                // about this particular monitored router.
                let processor = RouterInfoApi::new(
                    self.http_api_path.clone(),
                    source_id.clone(),
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

                let updatable_router_info = Arc::make_mut(&mut this_router_info);
                updatable_router_info.api_processor = Some(processor);

                self.router_info.insert(source_id.clone(), this_router_info);

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
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream { ref source_id }) => {
                // The connection to the router has been lost so drop the connection state machine we have for it, if
                // any.
                if let Some(entry) = self.router_states.get(&source_id) {
                    let mut locked = entry.lock().await;
                    let this_state = locked.take().unwrap();
                    let res = this_state.terminate().await;
                    let _ = self.process_result(res, &source_id).await;
                    self.router_disconnected(&source_id);
                }

                // Pass it on
                self.gate.update_data(update).await;
            }

            Update::Single(Payload {
                source_id,
                value: TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg)),
            }) => {
                let entry = self.router_states.entry(source_id.clone());

                // Process the message through the BMP state machine.
                // Initialize the state machine if not already done for this router.
                let entry = entry.or_insert_with(|| {
                    let new_entry = Arc::new(tokio::sync::Mutex::new(Some(
                        self.router_connected(&source_id),
                    )));

                    let weak_ref = Arc::downgrade(&new_entry);
                    tokio::task::block_in_place(|| {
                        Handle::current().block_on(
                            self.setup_router_specific_api_endpoint(weak_ref, &source_id),
                        );
                    });

                    new_entry
                });

                let mut locked = entry.lock().await;
                let this_state = locked.take().unwrap();

                // Run the state machine resulting in a new state.
                let next_state = self
                    .state_machine_metrics
                    .instrument(self.process_msg(&source_id, this_state, bmp_msg.0.clone())) // Bytes::clone() is cheap
                    .await;

                // TODO: if the next_state is Aborted, we have no way of feeding that back to the TCP connection
                // handler... we can only sit here and uselessly pump any future messages into the dead state
                // machine ... The TCP handling architecture created for the domain crate may serve us better here
                // as it has a means for feeding back down from the application specific network handling layer to
                // the TCP handling layer.

                // Store the new state machine state.
                locked.replace(next_state);
            }

            _ => {
                self.status_reporter
                    .input_mismatch("Update::Single(BmpMessage)", update);
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
    use super::*;

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

    // --- Test helpers ------------------------------------------------------

    fn mk_config_from_toml(toml: &str) -> Result<BmpIn, toml::de::Error> {
        toml::de::from_slice::<BmpIn>(toml.as_bytes())
    }
}
