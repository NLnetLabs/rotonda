//-------- Constants ---------------------------------------------------------

use std::{
    net::SocketAddr,
    ops::ControlFlow,
    sync::{Arc, Weak},
    time::Duration,
};

use arc_swap::ArcSwap;
use futures::{future::select, pin_mut, Future};
use serde::Deserialize;
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
    time::sleep,
};

use crate::{
    common::{
        frim::FrimMap,
        roto::{FilterName, RotoScripts},
        status_reporter::Chainable,
        unit::UnitActivity,
    },
    comms::{Gate, GateStatus, Terminated},
    http,
    manager::{Component, WaitPoint},
    payload::SourceId,
    tokio::TokioTaskMetrics,
    units::Unit,
};

use super::{
    http::{RouterInfoApi, RouterListApi},
    metrics::BmpTcpInMetrics,
    router_handler::RouterHandler,
    state_machine::{machine::BmpState, metrics::BmpMetrics},
    status_reporter::BmpTcpInStatusReporter,
    types::RouterInfo,
    util::format_source_id,
};

pub const UNKNOWN_ROUTER_SYSNAME: &str = "unknown";

//--- TCP listener traits ----------------------------------------------------
//
// These traits enable us to swap out the real TCP listener for a mock when
// testing.

#[async_trait::async_trait]
trait TcpListenerFactory<T> {
    async fn bind(&self, addr: &str) -> std::io::Result<T>;
}

#[async_trait::async_trait]
trait TcpListener {
    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)>;
}

struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(&self, addr: &str) -> std::io::Result<StandardTcpListener> {
        let listener = ::tokio::net::TcpListener::bind(addr).await?;
        Ok(StandardTcpListener(listener))
    }
}

struct StandardTcpListener(::tokio::net::TcpListener);

#[async_trait::async_trait]
impl TcpListener for StandardTcpListener {
    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        self.0.accept().await
    }
}

//-------- BmpIn -------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct BmpTcpIn {
    /// A colon separated IP address and port number to listen on for incoming
    /// BMP over TCP connections from routers.
    ///
    /// On change: existing connections to routers will be unaffected, new
    ///            connections will only be accepted at the changed URI.
    pub listen: Arc<String>,

    /// The relative path at which we should listen for HTTP query API requests
    #[cfg(feature = "router-list")]
    #[serde(default = "BmpTcpIn::default_http_api_path")]
    http_api_path: Arc<String>,

    #[serde(default = "BmpTcpIn::default_router_id_template")]
    pub router_id_template: String,

    #[serde(default)]
    pub filter_name: FilterName,
}

impl BmpTcpIn {
    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let unit_name = component.name().clone();

        // Setup our metrics
        let bmp_in_metrics = Arc::new(BmpTcpInMetrics::new(&gate));
        component.register_metrics(bmp_in_metrics.clone());

        // Setup metrics to be updated by the BMP state machines that we use
        // to make sense of the BMP data per router that supplies it.
        let bmp_metrics = Arc::new(BmpMetrics::new());
        component.register_metrics(bmp_metrics.clone());

        let state_machine_metrics = Arc::new(TokioTaskMetrics::new());
        component.register_metrics(state_machine_metrics.clone());

        // Setup our status reporting
        let status_reporter = Arc::new(BmpTcpInStatusReporter::new(
            &unit_name,
            bmp_in_metrics.clone(),
        ));

        // Setup storage for tracking multiple connected router BMP states at
        // once.
        let router_states = Arc::new(FrimMap::default());

        let router_id_template = Arc::new(ArcSwap::from_pointee(self.router_id_template));

        let filter_name = Arc::new(ArcSwap::from_pointee(self.filter_name));

        // Setup REST API endpoint
        #[cfg(feature = "router-list")]
        let (_api_processor, router_info) = {
            let router_info = Arc::new(FrimMap::default());

            let processor = Arc::new(RouterListApi::new(
                component.http_resources().clone(),
                self.http_api_path.clone(),
                router_info.clone(),
                bmp_in_metrics.clone(),
                bmp_metrics.clone(),
                router_id_template.clone(),
                router_states.clone(),
            ));

            component.register_http_resource(processor.clone(), &self.http_api_path);

            (processor, router_info)
        };

        let roto_scripts = component.roto_scripts().clone();

        // Wait for other components to be, and signal to other components
        // that we are, ready to start. All units and targets start together,
        // otherwise data passed from one component to another may be lost if
        // the receiving component is not yet ready to accept it.
        gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone
        // waiting to send important gate status updates won't send them while
        // we are in process_until() which will just eat them without handling
        // them.
        waitpoint.running().await;

        let component = Arc::new(RwLock::new(component));

        BmpTcpInRunner::new(
            component,
            self.listen,
            self.http_api_path,
            gate,
            router_states,
            router_info,
            bmp_metrics,
            bmp_in_metrics,
            state_machine_metrics,
            status_reporter,
            roto_scripts,
            router_id_template,
            filter_name,
        )
        .run(Arc::new(StandardTcpListenerFactory))
        .await?;

        eprintln!("Finished");
        Ok(())
    }

    #[cfg(feature = "router-list")]
    fn default_http_api_path() -> Arc<String> {
        Arc::new("/routers/".to_string())
    }

    pub fn default_router_id_template() -> String {
        "{sys_name}".to_string()
    }
}

//-------- BmpTcpInRunner ----------------------------------------------------

struct BmpTcpInRunner {
    component: Arc<RwLock<Component>>,
    listen: Arc<String>,
    http_api_path: Arc<String>,
    gate: Gate,
    router_states: Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>, // Option is never None, instead Some is take()'n and replace()'d.
    router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
    bmp_metrics: Arc<BmpMetrics>,
    bmp_in_metrics: Arc<BmpTcpInMetrics>,
    state_machine_metrics: Arc<TokioTaskMetrics>,
    status_reporter: Arc<BmpTcpInStatusReporter>,
    roto_scripts: RotoScripts,
    router_id_template: Arc<ArcSwap<String>>,
    filter_name: Arc<ArcSwap<FilterName>>,
}

impl BmpTcpInRunner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        component: Arc<RwLock<Component>>,
        listen: Arc<String>,
        http_api_path: Arc<String>,
        gate: Gate,
        router_states: Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>, // Option is never None, instead Some is take()'n and replace()'d.
        router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
        bmp_metrics: Arc<BmpMetrics>,
        bmp_in_metrics: Arc<BmpTcpInMetrics>,
        state_machine_metrics: Arc<TokioTaskMetrics>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
        roto_scripts: RotoScripts,
        router_id_template: Arc<ArcSwap<String>>,
        filter_name: Arc<ArcSwap<FilterName>>,
    ) -> Self {
        Self {
            component,
            listen,
            http_api_path: http_api_path,
            gate,
            router_states,
            router_info,
            bmp_metrics: bmp_metrics,
            bmp_in_metrics: bmp_in_metrics,
            state_machine_metrics: state_machine_metrics,
            status_reporter,
            roto_scripts,
            router_id_template,
            filter_name,
        }
    }

    #[cfg(test)]
    pub(crate) fn mock() -> (Self, crate::comms::GateAgent) {
        let (gate, gate_agent) = Gate::new(0);

        let runner = Self {
            component: Default::default(),
            listen: Default::default(),
            http_api_path: BmpTcpIn::default_http_api_path().into(),
            gate,
            router_states: Default::default(),
            router_info: Default::default(),
            bmp_metrics: Default::default(),
            bmp_in_metrics: Default::default(),
            state_machine_metrics: Default::default(),
            status_reporter: Default::default(),
            roto_scripts: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
        };

        (runner, gate_agent)
    }

    async fn run<T, U>(mut self, listener_factory: Arc<T>) -> Result<(), crate::comms::Terminated>
    where
        T: TcpListenerFactory<U>,
        U: TcpListener,
    {
        // Loop until terminated, accepting TCP connections from routers and
        // spawning tasks to handle them.
        let status_reporter = self.status_reporter.clone();

        loop {
            let listen_addr = self.listen.clone();

            let bind_with_backoff = || async {
                let mut wait = 1;
                loop {
                    match listener_factory.bind(&listen_addr).await {
                        Err(_err) => {
                            // let err = format!("{err}: Will retry in {wait} seconds.");
                            // status_reporter.bind_error(&listen_addr, &err);
                            sleep(Duration::from_secs(wait)).await;
                            wait *= 2;
                        }
                        res => break res,
                    }
                }
            };

            let listener = match self.process_until(bind_with_backoff()).await {
                ControlFlow::Continue(Ok(res)) => res,
                ControlFlow::Continue(Err(_err)) => continue,
                ControlFlow::Break(Terminated) => return Err(Terminated),
            };

            status_reporter.listener_listening(&listen_addr);

            'inner: loop {
                match self.process_until(listener.accept()).await {
                    ControlFlow::Continue(Ok((tcp_stream, client_addr))) => {
                        let source_id = SourceId::from(client_addr);

                        let state_machine =
                            Arc::new(Mutex::new(Some(self.router_connected(&source_id))));

                        let weak_ref = Arc::downgrade(&state_machine);
                        self.setup_router_specific_api_endpoint(weak_ref, &source_id)
                            .await;

                        self.router_states
                            .insert(source_id.clone(), state_machine.clone());

                        status_reporter.listener_connection_accepted(client_addr);

                        // Spawn a task to handle the newly connected routers BMP
                        // message stream.

                        // Choose a name to be reported in application logs.
                        let child_name =
                            format!("router[{}:{}]", client_addr.ip(), client_addr.port());

                        // Create a status reporter whose name in output will be
                        // a combination of ours as parent and the newly chosen
                        // child name, enabling logged messages relating to this
                        // newly connected router to be distinguished from logged
                        // messages relating to other connected routers.
                        let child_status_reporter =
                            Arc::new(self.status_reporter.add_child(&child_name));

                        let router_handler = RouterHandler::new(
                            self.gate.clone(),
                            self.roto_scripts.clone(),
                            self.router_id_template.clone(),
                            self.filter_name.clone(),
                            child_status_reporter,
                            state_machine,
                        );

                        crate::tokio::spawn(&child_name, async move {
                            router_handler.run(tcp_stream, client_addr, source_id).await
                        });
                    }
                    ControlFlow::Continue(Err(_err)) => break 'inner,
                    ControlFlow::Break(Terminated) => return Err(Terminated),
                }
            }
        }

        eprintln!("Loop ending");
    }

    async fn process_until<T, U>(
        &mut self,
        until_fut: T,
    ) -> ControlFlow<Terminated, std::io::Result<U>>
    where
        T: Future<Output = std::io::Result<U>>,
    {
        let mut until_fut = Box::pin(until_fut);

        loop {
            let process_fut = self.gate.process();
            pin_mut!(process_fut);

            let res = select(process_fut, until_fut).await;

            match (&self.status_reporter, res).into() {
                UnitActivity::GateStatusChanged(status, next_fut) => {
                    match status {
                        GateStatus::Reconfiguring {
                            new_config:
                                Unit::BmpTcpIn(BmpTcpIn {
                                    listen: new_listen,
                                    http_api_path: _http_api_path,
                                    router_id_template: new_router_id_template,
                                    filter_name: new_filter_name,
                                }),
                        } => {
                            // Runtime reconfiguration of this unit has
                            // been requested. New connections will be
                            // handled using the new configuration,
                            // existing connections handled by
                            // router_handler() tasks will receive their
                            // own copy of this Reconfiguring status
                            // update and can react to it accordingly.
                            let rebind = self.listen != new_listen;

                            self.listen = new_listen;
                            self.filter_name.store(new_filter_name.into());
                            self.router_id_template.store(new_router_id_template.into());

                            if rebind {
                                // Trigger re-binding to the new listen port.
                                let err = std::io::ErrorKind::Other;
                                return ControlFlow::Continue(Err(err.into()));
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.declare_source();
                            report.set_graph_status(self.bmp_in_metrics.clone());
                        }

                        _ => { /* Nothing to do */ }
                    }

                    until_fut = next_fut;
                }

                UnitActivity::InputError(err) => {
                    // self.status_reporter.listener_io_error(&err);
                    return ControlFlow::Continue(Err(err));
                }

                UnitActivity::InputReceived(v) => {
                    return ControlFlow::Continue(Ok(v));
                }

                UnitActivity::Terminated => {
                    return ControlFlow::Break(Terminated);
                }
            }
        }
    }

    fn router_connected(&self, source_id: &SourceId) -> BmpState {
        let router_id = Arc::new(format_source_id(
            &self.router_id_template.load(),
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

        let metrics = self.bmp_metrics.clone();

        BmpState::new(source_id.clone(), router_id, child_status_reporter, metrics)
    }

    fn router_disconnected(&self, source_id: &SourceId) {
        self.router_states.remove(source_id);
        self.router_info.remove(source_id);
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
                    self.component.read().await.http_resources().clone(),
                    self.http_api_path.clone(),
                    source_id.clone(),
                    self.bmp_in_metrics.clone(),
                    self.bmp_metrics.clone(),
                    this_router_info.connected_at,
                    this_router_info.last_msg_at.clone(),
                    state_machine,
                );

                let processor = Arc::new(processor);

                self.component
                    .write()
                    .await
                    .register_sub_http_resource(processor.clone(), &self.http_api_path);

                let updatable_router_info = Arc::make_mut(&mut this_router_info);
                updatable_router_info.api_processor = Some(processor);

                self.router_info.insert(source_id.clone(), this_router_info);

                // TODO: unregister the processor if the router disconnects? (maybe after a delay so that we can
                // still inspect the last known state for the monitored router)
            }
        }
    }
}

impl std::fmt::Debug for BmpTcpInRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BmpTcpInRunner").finish()
    }
}

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use roto::types::{collections::BytesRecord, lazyrecord_types::BmpMessage};

    use crate::{
        bgp::encode::{
            mk_initiation_msg, mk_invalid_initiation_message_that_lacks_information_tlvs,
            mk_peer_down_notification_msg, mk_per_peer_header,
        },
        tests::util::internal::get_testable_metrics_snapshot,
    };

    use super::*;

    const SYS_NAME: &str = "some sys name";
    const SYS_DESCR: &str = "some sys descr";
    const OTHER_SYS_NAME: &str = "other sys name";

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

    // Note: The tests below assume that the default router id template
    // includes the BMP sysName.

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic]
    async fn counters_for_expected_sys_name_should_not_exist() {
        let (runner, _) = BmpInRunner::mock();
        let initiation_msg = mk_update(mk_initiation_msg(SYS_NAME, SYS_DESCR));

        runner.process_update(initiation_msg).await;

        let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
        assert_eq!(
            metrics.with_label::<usize>("bmp_in_num_invalid_bmp_messages", ("router", SYS_NAME)),
            0,
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic]
    async fn counters_for_other_sys_name_should_not_exist() {
        let (runner, _) = BmpInRunner::mock();
        let initiation_msg = mk_update(mk_initiation_msg(SYS_NAME, SYS_DESCR));

        runner.process_update(initiation_msg).await;

        let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
        assert_eq!(
            metrics.with_label::<usize>(
                "bmp_in_num_invalid_bmp_messages",
                ("router", OTHER_SYS_NAME)
            ),
            0,
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn num_invalid_bmp_messages_counter_should_increase() {
        let (runner, _) = BmpInRunner::mock();

        // A BMP Initiation message that lacks required fields
        let bad_initiation_msg =
            mk_update(mk_invalid_initiation_message_that_lacks_information_tlvs());

        // A BMP Peer Down Notification message without a corresponding Peer
        // Up Notification message.
        let pph = mk_per_peer_header("10.0.0.1", 12345);
        let bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));

        runner.process_update(bad_initiation_msg).await;
        runner.process_update(bad_peer_down_msg).await;

        let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
        assert_eq!(
            metrics.with_label::<usize>(
                "bmp_in_num_invalid_bmp_messages",
                ("router", UNKNOWN_ROUTER_SYSNAME)
            ),
            2,
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_counters_should_be_started_if_the_router_id_changes() {
        let (runner, _) = BmpInRunner::mock();
        let initiation_msg = mk_update(mk_initiation_msg(SYS_NAME, SYS_DESCR));
        let pph = mk_per_peer_header("10.0.0.1", 12345);
        let bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));
        let reinitiation_msg = mk_update(mk_initiation_msg(OTHER_SYS_NAME, SYS_DESCR));
        let another_bad_peer_down_msg = mk_update(mk_peer_down_notification_msg(&pph));

        runner.process_update(initiation_msg).await;
        runner.process_update(bad_peer_down_msg).await;
        runner.process_update(reinitiation_msg).await;
        runner.process_update(another_bad_peer_down_msg).await;

        let metrics = get_testable_metrics_snapshot(&runner.status_reporter.metrics().unwrap());
        assert_eq!(
            metrics.with_label::<usize>("bmp_in_num_invalid_bmp_messages", ("router", SYS_NAME)),
            1,
        );
        assert_eq!(
            metrics.with_label::<usize>(
                "bmp_in_num_invalid_bmp_messages",
                ("router", OTHER_SYS_NAME)
            ),
            1,
        );
    }

    // --- Test helpers ------------------------------------------------------

    fn mk_config_from_toml(toml: &str) -> Result<BmpTcpIn, toml::de::Error> {
        toml::from_str::<BmpTcpIn>(toml)
    }

    fn mk_update(msg_buf: Bytes) -> Update {
        let source_id = SourceId::SocketAddr("127.0.0.1:8080".parse().unwrap());
        let bmp_msg = Arc::new(BytesRecord(BmpMessage::from_octets(msg_buf).unwrap()));
        let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg));
        Update::Single(Payload::new(source_id, value))
    }
}
