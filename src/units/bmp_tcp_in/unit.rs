//-------- Constants ---------------------------------------------------------

use std::{
    net::SocketAddr,
    ops::ControlFlow,
    str::FromStr,
    sync::{Arc, Weak},
    time::Duration,
};

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use futures::{future::select, pin_mut, Future};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
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
    manager::{Component, WaitPoint},
    payload::SourceId,
    tokio::TokioTaskMetrics,
    tracing::Tracer,
    units::Unit,
};

use super::{
    http::{RouterInfoApi, RouterListApi},
    state_machine::{BmpState, BmpStateMachineMetrics},
    types::RouterInfo,
};
use super::{
    metrics::BmpTcpInMetrics, router_handler::RouterHandler,
    status_reporter::BmpTcpInStatusReporter, util::format_source_id,
};

//--- TCP listener traits ----------------------------------------------------
//
// These traits enable us to swap out the real TCP listener for a mock when
// testing.

#[async_trait::async_trait]
trait TcpListenerFactory<T> {
    async fn bind(&self, addr: SocketAddr) -> std::io::Result<T>;
}

#[async_trait::async_trait]
trait TcpListener {
    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)>;
}

struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(
        &self,
        addr: SocketAddr,
    ) -> std::io::Result<StandardTcpListener> {
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

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
pub enum TracingMode {
    /// Don't enable tracing of incoming messages through the pipeline.
    #[default]
    Off,

    /// Only enable tracing if the incoming BMP message includes a trace ID.
    IfRequested,

    /// Trace all incoming messages by automatically generating sequentially
    /// rising trace IDs in the range 0..255 then wrapping back to 0.
    On,
}

impl FromStr for TracingMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowered = s.to_lowercase();
        if lowered == "on" {
            Ok(TracingMode::On)
        } else if lowered == "ifrequested" {
            Ok(TracingMode::IfRequested)
        } else if lowered == "off" {
            Ok(TracingMode::Off)
        } else {
            Err("tracing_mode must be one of: Off, IfRequested or On")
        }
    }
}

impl std::fmt::Display for TracingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TracingMode::Off => f.write_str("Off"),
            TracingMode::IfRequested => f.write_str("IfRequested"),
            TracingMode::On => f.write_str("On"),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct BmpTcpIn {
    /// A colon separated IP address and port number to listen on for incoming
    /// BMP over TCP connections from routers.
    ///
    /// On change: existing connections to routers will be unaffected, new
    ///            connections will only be accepted at the changed URI.
    #[serde_as(as = "Arc<DisplayFromStr>")]
    pub listen: Arc<SocketAddr>,

    /// The relative path at which we should listen for HTTP query API requests
    #[serde(default = "BmpTcpIn::default_http_api_path")]
    http_api_path: Arc<String>,

    #[serde(default = "BmpTcpIn::default_router_id_template")]
    pub router_id_template: String,

    #[serde(default)]
    pub filter_name: FilterName,

    #[serde(default)]
    pub tracing_mode: TracingMode,
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

        let bmp_metrics = Arc::new(BmpStateMachineMetrics::new());
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

        let router_id_template =
            Arc::new(ArcSwap::from_pointee(self.router_id_template));

        let filter_name = Arc::new(ArcSwap::from_pointee(self.filter_name));

        // Setup REST API endpoint
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

            component.register_http_resource(
                processor.clone(),
                &self.http_api_path,
            );

            (processor, router_info)
        };

        let roto_scripts = component.roto_scripts().clone();
        let tracer = component.tracer().clone();

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

        let tracing_mode = Arc::new(ArcSwap::from_pointee(self.tracing_mode));

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
            tracer,
            tracing_mode,
        )
        .run(Arc::new(StandardTcpListenerFactory))
        .await?;

        Ok(())
    }

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
    listen: Arc<SocketAddr>,
    http_api_path: Arc<String>,
    gate: Gate,
    router_states:
        Arc<FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>>, // Option is never None, instead Some is take()'n and replace()'d.
    router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
    bmp_metrics: Arc<BmpStateMachineMetrics>,
    bmp_in_metrics: Arc<BmpTcpInMetrics>,
    state_machine_metrics: Arc<TokioTaskMetrics>,
    status_reporter: Arc<BmpTcpInStatusReporter>,
    roto_scripts: RotoScripts,
    router_id_template: Arc<ArcSwap<String>>,
    filter_name: Arc<ArcSwap<FilterName>>,
    tracer: Arc<Tracer>,
    tracing_mode: Arc<ArcSwap<TracingMode>>,
}

impl BmpTcpInRunner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        component: Arc<RwLock<Component>>,
        listen: Arc<SocketAddr>,
        http_api_path: Arc<String>,
        gate: Gate,
        router_states: Arc<
            FrimMap<SourceId, Arc<tokio::sync::Mutex<Option<BmpState>>>>,
        >, // Option is never None, instead Some is take()'n and replace()'d.
        router_info: Arc<FrimMap<SourceId, Arc<RouterInfo>>>,
        bmp_metrics: Arc<BmpStateMachineMetrics>,
        bmp_in_metrics: Arc<BmpTcpInMetrics>,
        state_machine_metrics: Arc<TokioTaskMetrics>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
        roto_scripts: RotoScripts,
        router_id_template: Arc<ArcSwap<String>>,
        filter_name: Arc<ArcSwap<FilterName>>,
        tracer: Arc<Tracer>,
        tracing_mode: Arc<ArcSwap<TracingMode>>,
    ) -> Self {
        Self {
            component,
            listen,
            http_api_path,
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
            tracer,
            tracing_mode,
        }
    }

    #[cfg(test)]
    pub(crate) fn mock() -> (Self, crate::comms::GateAgent) {
        let (gate, gate_agent) = Gate::new(0);

        let runner = Self {
            component: Default::default(),
            listen: Arc::new("127.0.0.1:12345".parse().unwrap()),
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
            tracer: Default::default(),
            tracing_mode: Default::default(),
        };

        (runner, gate_agent)
    }

    async fn run<T, U>(
        mut self,
        listener_factory: Arc<T>,
    ) -> Result<(), crate::comms::Terminated>
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
                    match listener_factory.bind(*listen_addr).await {
                        Err(err) => {
                            let err = format!(
                                "{err}: Will retry in {wait} seconds."
                            );
                            status_reporter
                                .bind_error(&listen_addr.to_string(), &err);
                            sleep(Duration::from_secs(wait)).await;
                            wait *= 2;
                        }
                        res => break res,
                    }
                }
            };

            let listener = match self.process_until(bind_with_backoff()).await
            {
                ControlFlow::Continue(Ok(res)) => res,
                ControlFlow::Continue(Err(_err)) => continue,
                ControlFlow::Break(Terminated) => return Err(Terminated),
            };

            status_reporter.listener_listening(&listen_addr.to_string());

            'inner: loop {
                match self.process_until(listener.accept()).await {
                    ControlFlow::Continue(Ok((tcp_stream, client_addr))) => {
                        let source_id = SourceId::from(client_addr);

                        let state_machine = Arc::new(Mutex::new(Some(
                            self.router_connected(&source_id),
                        )));

                        let last_msg_at = {
                            let weak_ref = Arc::downgrade(&state_machine);
                            self.setup_router_specific_api_endpoint(
                                weak_ref, &source_id,
                            )
                            .await
                        };

                        self.router_states
                            .insert(source_id.clone(), state_machine.clone());

                        status_reporter
                            .listener_connection_accepted(client_addr);

                        // Spawn a task to handle the newly connected routers BMP
                        // message stream.

                        // Choose a name to be reported in application logs.
                        let child_name = format!(
                            "router[{}:{}]",
                            client_addr.ip(),
                            client_addr.port()
                        );

                        // Create a status reporter whose name in output will be
                        // a combination of ours as parent and the newly chosen
                        // child name, enabling logged messages relating to this
                        // newly connected router to be distinguished from logged
                        // messages relating to other connected routers.
                        let child_status_reporter = Arc::new(
                            self.status_reporter.add_child(&child_name),
                        );

                        let router_handler = RouterHandler::new(
                            self.gate.clone(),
                            self.roto_scripts.clone(),
                            self.router_id_template.clone(),
                            self.filter_name.clone(),
                            child_status_reporter,
                            state_machine,
                            self.tracer.clone(),
                            self.tracing_mode.clone(),
                            last_msg_at,
                            self.bmp_metrics.clone(),
                        );

                        let router_states = self.router_states.clone();
                        let router_info = self.router_info.clone();

                        crate::tokio::spawn(&child_name, async move {
                            router_handler
                                .run(
                                    tcp_stream,
                                    client_addr,
                                    source_id.clone(),
                                )
                                .await;
                            router_states.remove(&source_id);
                            router_info.remove(&source_id);
                        });
                    }
                    ControlFlow::Continue(Err(_err)) => break 'inner,
                    ControlFlow::Break(Terminated) => return Err(Terminated),
                }
            }
        }
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
                                    tracing_mode: new_tracing_mode,
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
                            self.router_id_template
                                .store(new_router_id_template.into());
                            self.tracing_mode.store(new_tracing_mode.into());

                            if rebind {
                                // Trigger re-binding to the new listen port.
                                let err = std::io::ErrorKind::Other;
                                return ControlFlow::Continue(
                                    Err(err.into()),
                                );
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.declare_source();
                            report.set_graph_status(
                                self.bmp_in_metrics.clone(),
                            );
                        }

                        _ => { /* Nothing to do */ }
                    }

                    until_fut = next_fut;
                }

                UnitActivity::InputError(err) => {
                    self.status_reporter.listener_io_error(&err);
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
        let child_status_reporter =
            Arc::new(self.status_reporter.add_child(child_name));

        {
            let this_router_info = Arc::new(RouterInfo::new());
            self.router_info.insert(source_id.clone(), this_router_info);
        }

        let metrics = self.bmp_metrics.clone();

        BmpState::new(
            source_id.clone(),
            router_id,
            child_status_reporter,
            metrics,
        )
    }

    // TODO: Should we tear these individual API endpoints down when the
    // connection to the monitored router is lost?
    async fn setup_router_specific_api_endpoint(
        &self,
        state_machine: Weak<Mutex<Option<BmpState>>>,
        #[allow(unused_variables)] source_id: &SourceId,
    ) -> Option<Arc<std::sync::RwLock<DateTime<Utc>>>> {
        match self.router_info.get(source_id) {
            None => {
                // This should never happen.
                self.status_reporter.internal_error(format!(
                    "Router info for source {} doesn't exist",
                    source_id,
                ));

                None
            }

            Some(mut this_router_info) => {
                // Setup a REST API endpoint for querying information
                // about this particular monitored router.
                let last_msg_at = this_router_info.last_msg_at.clone();

                let processor = RouterInfoApi::new(
                    self.component.read().await.http_resources().clone(),
                    self.http_api_path.clone(),
                    source_id.clone(),
                    self.bmp_in_metrics.clone(),
                    self.bmp_metrics.clone(),
                    this_router_info.connected_at,
                    last_msg_at.clone(),
                    state_machine,
                );

                let processor = Arc::new(processor);

                self.component.write().await.register_sub_http_resource(
                    processor.clone(),
                    &self.http_api_path,
                );

                let updatable_router_info =
                    Arc::make_mut(&mut this_router_info);
                updatable_router_info.api_processor = Some(processor);

                self.router_info.insert(source_id.clone(), this_router_info);

                Some(last_msg_at)

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
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{net::TcpStream, time::timeout};

    use crate::{
        common::status_reporter::AnyStatusReporter,
        comms::{Gate, GateAgent},
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
        units::{
            bmp_tcp_in::{
                metrics::BmpTcpInMetrics,
                status_reporter::BmpTcpInStatusReporter,
                unit::{BmpTcpInRunner, TcpListener, TcpListenerFactory},
            },
            Unit,
        },
    };

    use super::BmpTcpIn;

    struct MockTcpListener;

    #[async_trait::async_trait]
    impl TcpListener for MockTcpListener {
        async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
            // block forever
            std::future::pending().await
        }
    }

    struct MockTcpListenerFactory<T>
    where
        T: Fn(SocketAddr) -> std::io::Result<()> + Sync,
    {
        pub bind_cb: T,
        pub binds: Arc<Mutex<Vec<SocketAddr>>>,
    }

    impl<T> MockTcpListenerFactory<T>
    where
        T: Fn(SocketAddr) -> std::io::Result<()> + Sync,
    {
        pub fn new(bind_cb: T) -> Self {
            Self {
                bind_cb,
                binds: Arc::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl<T> TcpListenerFactory<MockTcpListener> for MockTcpListenerFactory<T>
    where
        T: Fn(SocketAddr) -> std::io::Result<()> + Sync,
    {
        async fn bind(
            &self,
            addr: SocketAddr,
        ) -> std::io::Result<MockTcpListener> {
            (self.bind_cb)(addr.clone())?;
            self.binds.lock().unwrap().push(addr);
            Ok(MockTcpListener)
        }
    }

    #[test]
    fn listen_is_required() {
        assert!(mk_config_from_toml("").is_err());
    }

    #[test]
    fn listen_must_be_a_valid_socket_address() {
        assert!(mk_config_from_toml("listen = ''").is_err());
        assert!(mk_config_from_toml("listen = '12345'").is_err());
        assert!(mk_config_from_toml("listen = '1.2.3.4'").is_err());
    }

    #[test]
    fn listen_is_the_only_required_field() {
        assert!(mk_config_from_toml("listen = '1.2.3.4:12345'").is_ok());
    }

    // --- Test helpers ------------------------------------------------------

    fn mk_config_from_toml(toml: &str) -> Result<BmpTcpIn, toml::de::Error> {
        toml::from_str::<BmpTcpIn>(toml)
    }

    #[tokio::test(start_paused = true)]
    async fn test_reconfigured_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (runner, agent) = setup_test("1.2.3.4:12345");
        let status_reporter = runner.status_reporter.clone();
        let mock_listener_factory =
            Arc::new(MockTcpListenerFactory::new(|_| Ok(())));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // "127.0.0.1:11019":
        let (new_gate, new_agent) = Gate::new(1);
        let listen = Arc::new("127.0.0.1:11019".parse().unwrap());
        let new_config = BmpTcpIn {
            listen,
            http_api_path: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
            tracing_mode: Default::default(),
        };
        let new_config = Unit::BmpTcpIn(new_config);
        agent.reconfigure(new_config, new_gate).await.unwrap();

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Then send a termination command to the gate:
        assert!(!join_handle.is_finished());
        new_agent.terminate().await;
        let _ = timeout(Duration::from_millis(100), join_handle)
            .await
            .unwrap();

        // And verify that the unit bound first to the first given URI and
        // then to the second given URI and not to any other URI.
        let binds = mock_listener_factory.binds.lock().unwrap();
        assert_eq!(binds.len(), 2);
        assert_eq!(binds[0], "1.2.3.4:12345".parse().unwrap());
        assert_eq!(binds[1], "127.0.0.1:11019".parse().unwrap());

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_listener_bound_count"),
            2
        );
        assert_eq!(
            metrics
                .with_name::<usize>("bmp_tcp_in_connection_accepted_count"),
            0
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_unchanged_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (runner, agent) = setup_test("127.0.0.1:11019");
        let status_reporter = runner.status_reporter.clone();
        let mock_listener_factory =
            Arc::new(MockTcpListenerFactory::new(|_| Ok(())));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // an unchanged listen address:
        let (new_gate, new_agent) = Gate::new(1);
        let listen = Arc::new("127.0.0.1:11019".parse().unwrap());
        let new_config = BmpTcpIn {
            listen,
            http_api_path: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
            tracing_mode: Default::default(),
        };
        let new_config = Unit::BmpTcpIn(new_config);
        agent.reconfigure(new_config, new_gate).await.unwrap();

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Then send a termination command to the gate:
        assert!(!join_handle.is_finished());
        new_agent.terminate().await;
        let _ = timeout(Duration::from_millis(100), join_handle)
            .await
            .unwrap();

        // And verify that the unit bound only once and only to the given URI:
        let binds = mock_listener_factory.binds.lock().unwrap();
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0], "127.0.0.1:11019".parse().unwrap());

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_listener_bound_count"),
            1
        );
        assert_eq!(
            metrics
                .with_name::<usize>("bmp_tcp_in_connection_accepted_count"),
            0
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_overcoming_bind_failure() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let fail_on_bad_addr = |addr: SocketAddr| {
            // Not technically a bad address, just one we can match on for test purposes
            if addr.to_string() != "1.2.3.4:12345" {
                Ok(())
            } else {
                Err(std::io::ErrorKind::PermissionDenied.into())
            }
        };
        let (runner, agent) = setup_test("1.2.3.4:12345");
        let status_reporter = runner.status_reporter.clone();
        let mock_listener_factory =
            Arc::new(MockTcpListenerFactory::new(fail_on_bad_addr));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(10)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // an unchanged listen address:
        let (new_gate, new_agent) = Gate::new(1);
        let listen = Arc::new("127.0.0.1:11019".parse().unwrap());
        let new_config = BmpTcpIn {
            listen,
            http_api_path: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
            tracing_mode: Default::default(),
        };
        let new_config = Unit::BmpTcpIn(new_config);
        agent.reconfigure(new_config, new_gate).await.unwrap();

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Then send a termination command to the gate:
        assert!(!join_handle.is_finished());
        new_agent.terminate().await;
        let _ = timeout(Duration::from_millis(100), join_handle)
            .await
            .unwrap();

        // And verify that the unit bound only once and only to the given URI:
        let binds = mock_listener_factory.binds.lock().unwrap();
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0], "127.0.0.1:11019".parse().unwrap());

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_listener_bound_count"),
            1
        );
        assert_eq!(
            metrics
                .with_name::<usize>("bmp_tcp_in_connection_accepted_count"),
            0
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            0
        );
    }

    //-------- Test helpers --------------------------------------------------

    fn setup_test(listen: &str) -> (BmpTcpInRunner, GateAgent) {
        enable_logging("trace");

        let (gate, gate_agent) = Gate::new(0);
        let metrics = Arc::new(BmpTcpInMetrics::default());
        let status_reporter = Arc::new(BmpTcpInStatusReporter::default());
        let runner = BmpTcpInRunner {
            component: Default::default(),
            listen: Arc::new(listen.parse().unwrap()),
            http_api_path: Default::default(),
            gate,
            router_states: Default::default(),
            router_info: Default::default(),
            bmp_metrics: Default::default(),
            bmp_in_metrics: metrics,
            state_machine_metrics: Default::default(),
            status_reporter,
            roto_scripts: Default::default(),
            router_id_template: Default::default(),
            filter_name: Default::default(),
            tracing_mode: Default::default(),
            tracer: Default::default(),
        };

        (runner, gate_agent)
    }
}
