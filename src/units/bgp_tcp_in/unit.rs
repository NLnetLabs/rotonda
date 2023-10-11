use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::select;
use futures::{pin_mut, Future};
use log::debug;
use rotonda_fsm::bgp::session::Command;
use routecore::asn::Asn;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::common::roto::{FilterName, RotoScripts};
use crate::common::status_reporter::{Chainable, UnitStatusReporter};
use crate::common::unit::UnitActivity;
use crate::comms::{GateStatus, Terminated};
use crate::manager::{Component, WaitPoint};
use crate::units::{Gate, Unit};

use super::metrics::BgpTcpInMetrics;
use super::router_handler::handle_connection;
use super::status_reporter::BgpTcpInStatusReporter;

use super::peer_config::{CombinedConfig, PeerConfigs};

// XXX copied from BmpTcpIn, we probably want to separate these out

//--- TCP listener traits ----------------------------------------------------
//
// These traits enable us to swap out the real TCP listener for a mock when
// testing.

#[async_trait::async_trait]
trait TcpListenerFactory<T> {
    async fn bind(&self, addr: String) -> std::io::Result<T>;
}

#[async_trait::async_trait]
trait TcpListener<T> {
    async fn accept(&self) -> std::io::Result<(T, SocketAddr)>;
}

#[async_trait::async_trait]
trait TcpStreamWrapper {
    fn into_inner(self) -> std::io::Result<TcpStream>;
}

/// A thin wrapper around the real Tokio TcpListener.
struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(&self, addr: String) -> std::io::Result<StandardTcpListener> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(StandardTcpListener(listener))
    }
}

struct StandardTcpListener(::tokio::net::TcpListener);

/// A thin wrapper around the real Tokio TcpListener bind call.
#[async_trait::async_trait]
impl TcpListener<StandardTcpStream> for StandardTcpListener {
    async fn accept(&self) -> std::io::Result<(StandardTcpStream, SocketAddr)> {
        let (stream, addr) = self.0.accept().await?;
        Ok((StandardTcpStream(stream), addr))
    }
}

struct StandardTcpStream(::tokio::net::TcpStream);

/// A thin wrapper around the Tokio TcpListener accept() call result.
impl TcpStreamWrapper for StandardTcpStream {
    fn into_inner(self) -> std::io::Result<TcpStream> {
        Ok(self.0)
    }
}

//----------- BgpTcpIn -------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct BgpTcpIn {
    /// Address:port to listen on incoming BGP connections over TCP.
    pub listen: String,

    pub my_asn: Asn, // TODO make getters, or impl Into<-fsm::bgp::Config>

    pub my_bgp_id: [u8; 4],

    #[serde(rename = "peers", default)]
    pub peer_configs: PeerConfigs,

    #[serde(default)]
    pub filter_name: FilterName,
}

impl PartialEq for BgpTcpIn {
    fn eq(&self, other: &Self) -> bool {
        self.listen == other.listen
            && self.my_asn == other.my_asn
            && self.my_bgp_id == other.my_bgp_id
    }
}

impl BgpTcpIn {
    #[cfg(test)]
    pub fn mock(listen: &str, my_asn: Asn) -> Self {
        Self {
            listen: listen.to_string(),
            my_asn,
            my_bgp_id: Default::default(),
            peer_configs: Default::default(),
            filter_name: Default::default(),
        }
    }

    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated> {
        let unit_name = component.name().clone();

        // Setup metrics
        let metrics = Arc::new(BgpTcpInMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(BgpTcpInStatusReporter::new(&unit_name, metrics.clone()));

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

        BgpTcpInRunner::new(self, gate, metrics, status_reporter, roto_scripts)
            .run::<_, _, StandardTcpStream, BgpTcpInRunner>(Arc::new(StandardTcpListenerFactory))
            .await
    }
}

trait ConfigAcceptor {
    fn accept_config(
        child_name: String,
        roto_scripts: &RotoScripts,
        gate: &Gate,
        bgp: &BgpTcpIn,
        tcp_stream: impl TcpStreamWrapper,
        cfg: &super::peer_config::PeerConfig,
        remote_net: super::peer_config::PrefixOrExact,
        child_status_reporter: Arc<BgpTcpInStatusReporter>,
        live_sessions: Arc<Mutex<LiveSessions>>,
    );
}

pub type LiveSessions = HashMap<(IpAddr, Asn), mpsc::Sender<Command>>;

struct BgpTcpInRunner {
    // The configuration from the .conf.
    bgp: BgpTcpIn,

    gate: Gate,

    metrics: Arc<BgpTcpInMetrics>,

    status_reporter: Arc<BgpTcpInStatusReporter>,

    roto_scripts: RotoScripts,

    // To send commands to a Session based on peer IP + ASN.
    live_sessions: Arc<Mutex<LiveSessions>>,
}

impl BgpTcpInRunner {
    fn new(
        bgp: BgpTcpIn,
        gate: Gate,
        metrics: Arc<BgpTcpInMetrics>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
        roto_scripts: RotoScripts,
    ) -> Self {
        BgpTcpInRunner {
            bgp,
            gate,
            metrics,
            status_reporter,
            roto_scripts,
            live_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    fn mock(bgp: BgpTcpIn) -> (Self, crate::comms::GateAgent) {
        let (gate, gate_agent) = Gate::new(0);

        let runner = BgpTcpInRunner {
            bgp,
            gate,
            metrics: Default::default(),
            status_reporter: Default::default(),
            roto_scripts: Default::default(),
            live_sessions: Arc::new(Mutex::new(HashMap::new())),
        };

        (runner, gate_agent)
    }

    async fn run<T, U, V, F>(mut self, listener_factory: Arc<T>) -> Result<(), Terminated>
    where
        T: TcpListenerFactory<U>,
        U: TcpListener<V>,
        V: TcpStreamWrapper,
        F: ConfigAcceptor,
    {
        // Loop until terminated, accepting TCP connections from routers and
        // spawning tasks to handle them.
        let status_reporter = self.status_reporter.clone();

        loop {
            let listen_addr = self.bgp.listen.clone();

            let bind_with_backoff = || async {
                let mut wait = 1;
                loop {
                    match listener_factory.bind(listen_addr.clone()).await {
                        Err(err) => {
                            let err = format!("{err}: Will retry in {wait} seconds.");
                            status_reporter.bind_error(&listen_addr, &err);
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
                    ControlFlow::Continue(Ok((tcp_stream, peer_addr))) => {
                        status_reporter.listener_connection_accepted(peer_addr);

                        // Now:
                        // check for the peer_addr.ip() in the new PeerConfig
                        // and spawn a Session for it.
                        // We might need some new stuff:
                        // a temporary 'pending' list/map, keyed on only the peer
                        // IP, from which entries will be removed once the OPENs
                        // are exchanged and we know the remote ASN.

                        if let Some((remote_net, cfg)) = self.bgp.peer_configs.get(peer_addr.ip()) {
                            let child_name =
                                format!("bgp[{}:{}]", peer_addr.ip(), peer_addr.port());
                            let child_status_reporter =
                                Arc::new(status_reporter.add_child(&child_name));
                            debug!("[{}] config matched: {}", peer_addr.ip(), cfg.name());
                            F::accept_config(
                                child_name,
                                &self.roto_scripts,
                                &self.gate,
                                &self.bgp,
                                tcp_stream,
                                cfg,
                                remote_net,
                                child_status_reporter,
                                self.live_sessions.clone(),
                            );
                        } else {
                            debug!("No config to accept {}", peer_addr.ip());
                        }
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
                            new_config: Unit::BgpTcpIn(new_unit),
                        } => {
                            debug!(
                                "pre reconfigure, current live sessions: {:?}",
                                self.live_sessions
                            );
                            debug!("new_config: {:?}", new_unit);
                            // Runtime reconfiguration of this unit has
                            // been requested. New connections will be
                            // handled using the new configuration,
                            // existing connections handled by
                            // router_handler() tasks will receive their
                            // own copy of this Reconfiguring status
                            // update and can react to it accordingly.
                            let rebind = self.bgp.listen != new_unit.listen;

                            self.bgp = new_unit;

                            if rebind {
                                // Trigger re-binding to the new listen port.
                                let err = std::io::ErrorKind::Other;
                                return ControlFlow::Continue(Err(err.into()));
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.declare_source();
                            report.set_graph_status(self.metrics.clone());
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
                    self.status_reporter.terminated();
                    return ControlFlow::Break(Terminated);
                }
            }
        }
    }
}

impl ConfigAcceptor for BgpTcpInRunner {
    fn accept_config(
        child_name: String,
        roto_scripts: &RotoScripts,
        gate: &Gate,
        bgp: &BgpTcpIn,
        tcp_stream: impl TcpStreamWrapper,
        cfg: &super::peer_config::PeerConfig,
        remote_net: super::peer_config::PrefixOrExact,
        child_status_reporter: Arc<BgpTcpInStatusReporter>,
        live_sessions: Arc<Mutex<LiveSessions>>,
    ) {
        let (cmds_tx, cmds_rx) = mpsc::channel(16);
        let tcp_stream = tcp_stream.into_inner().unwrap(); // SAFETY: StandardTcpStream::into_inner() always returns Ok(...)
        crate::tokio::spawn(
            &child_name,
            handle_connection(
                roto_scripts.clone(),
                gate.clone(),
                bgp.clone(),
                tcp_stream,
                CombinedConfig::new(bgp.clone(), cfg.clone(), remote_net),
                cmds_tx.clone(),
                cmds_rx,
                child_status_reporter,
                live_sessions,
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use futures::Future;
    use routecore::asn::Asn;
    use tokio::net::TcpStream;

    use crate::{
        common::{roto::RotoScripts, status_reporter::AnyStatusReporter},
        comms::{Gate, GateAgent, Terminated},
        tests::util::internal::mk_testable_metrics,
        units::bgp_tcp_in::{
            peer_config::{PeerConfig, PrefixOrExact},
            status_reporter::BgpTcpInStatusReporter,
            unit::{BgpTcpIn, BgpTcpInRunner, ConfigAcceptor, LiveSessions},
        },
    };

    use super::{TcpListener, TcpListenerFactory, TcpStreamWrapper};

    #[tokio::test(flavor = "multi_thread")]
    async fn listener_bound_count_metric_should_work() {
        let mock_listener_factory_cb = || {
            Ok(MockTcpListener::new(|| {
                Err(std::io::ErrorKind::ConnectionRefused.into())
            }))
        };

        let (runner_fut, gate_agent, status_reporter) = setup_test(mock_listener_factory_cb);
        let join_handle = crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        loop {
            let metrics = mk_testable_metrics(&status_reporter.metrics().unwrap());
            let count = metrics.with_name::<usize>("bgp_tcp_in_listener_bound_count");
            if count > 0 {
                break;
            }
        }

        gate_agent.terminate().await;

        let res = join_handle.await.unwrap();
        assert_eq!(res, Err(Terminated));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "TCP accept panics and so the unit never responds to the terminate command."]
    async fn retry_with_backoff_on_accept_error() {
        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_clone = accept_count.clone();

        let mock_listener_factory_cb = move || {
            let old_count = accept_count_clone.fetch_add(1, Ordering::SeqCst);
            if old_count < 1 {
                Ok(MockTcpListener::new(|| {
                    Err(std::io::ErrorKind::ConnectionRefused.into())
                }))
            } else {
                Err(std::io::ErrorKind::PermissionDenied.into())
            }
        };

        let (runner_fut, gate_agent, status_reporter) = setup_test(mock_listener_factory_cb);
        let join_handle = crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        loop {
            let metrics = mk_testable_metrics(&status_reporter.metrics().unwrap());
            let count = metrics.with_name::<usize>("bgp_tcp_in_connection_accepted_count");
            if count > 0 {
                break;
            }
        }

        gate_agent.terminate().await;

        let res = join_handle.await.unwrap();
        assert_eq!(res, Err(Terminated));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_accepted_count_metric_should_work() {
        let conn_count = Arc::new(AtomicUsize::new(0));
        let mock_listener_factory_cb = {
            let conn_count = conn_count.clone();
            move || {
                let conn_count = conn_count.clone();
                Ok(MockTcpListener::new(move || {
                    let old_count = conn_count.fetch_add(1, Ordering::SeqCst);
                    if old_count < 1 {
                        Ok((MockTcpStreamWrapper, "1.2.3.4:5".parse().unwrap()))
                    } else {
                        Err(std::io::ErrorKind::PermissionDenied.into())
                    }
                }))
            }
        };

        let (runner_fut, gate_agent, status_reporter) = setup_test(mock_listener_factory_cb);
        let join_handle = crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        let mut count = 0;
        while count != 1 {
            let metrics = mk_testable_metrics(&status_reporter.metrics().unwrap());
            count = metrics.with_name::<usize>("bgp_tcp_in_connection_accepted_count");
        }

        gate_agent.terminate().await;

        let res = join_handle.await.unwrap();
        assert_eq!(res, Err(Terminated));
    }

    //-------- Test helpers --------------------------------------------------

    fn setup_test<T, U>(
        mock_listener_factory_cb: T,
    ) -> (
        impl Future<Output = Result<(), Terminated>>,
        GateAgent,
        Arc<BgpTcpInStatusReporter>,
    )
    where
        T: Fn() -> std::io::Result<MockTcpListener<U>> + std::marker::Sync,
        U: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)> + std::marker::Sync,
    {
        let mock_listener_factory = MockTcpListenerFactory::new(mock_listener_factory_cb);

        let unit_settings = BgpTcpIn::mock("dummy-listen-address", Asn::from_u32(12345));

        let (runner, gate_agent) = BgpTcpInRunner::mock(unit_settings);

        let status_reporter = runner.status_reporter.clone();

        let runner_fut = runner.run::<_, _, _, NoOpConfigAcceptor>(mock_listener_factory.into());

        (runner_fut, gate_agent, status_reporter)
    }

    /// A config acceptor that does nothing, i.e. it does not spawn a child
    /// task to handle the connection as would normally happen.
    struct NoOpConfigAcceptor;

    impl ConfigAcceptor for NoOpConfigAcceptor {
        fn accept_config(
            _child_name: String,
            _roto_scripts: &RotoScripts,
            _gate: &Gate,
            _bgp: &BgpTcpIn,
            _tcp_stream: impl TcpStreamWrapper,
            _cfg: &PeerConfig,
            _remote_net: PrefixOrExact,
            _child_status_reporter: Arc<BgpTcpInStatusReporter>,
            _live_sessions: Arc<std::sync::Mutex<LiveSessions>>,
        ) {
        }
    }

    /// A mock TcpListenerFactory that stores a callback supplied by the
    /// unit test thereby allowing the unit test to determine if binding to
    /// the given address should succeed or not, and on success delegates to
    /// MockTcpListener.
    struct MockTcpListenerFactory<T, U>(T)
    where
        U: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        T: Fn() -> std::io::Result<MockTcpListener<U>>;

    impl<T, U> MockTcpListenerFactory<T, U>
    where
        U: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        T: Fn() -> std::io::Result<MockTcpListener<U>>,
    {
        pub fn new(cb: T) -> Self {
            Self(cb)
        }
    }

    #[async_trait::async_trait]
    impl<T, U> TcpListenerFactory<MockTcpListener<U>> for MockTcpListenerFactory<T, U>
    where
        T: Fn() -> std::io::Result<MockTcpListener<U>> + std::marker::Sync,
        U: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
    {
        async fn bind(&self, _addr: String) -> std::io::Result<MockTcpListener<U>> {
            self.0()
        }
    }

    /// A mock TcpListener that stores a callback supplied by the unit test
    /// thereby allowing the unit test to determine if accepting incoming
    /// connections should appear to succeed or fail, and on success delegates
    /// to MockTcpStreamWrapper.
    struct MockTcpListener<T>(T)
    where
        T: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)>;

    impl<T> MockTcpListener<T>
    where
        T: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
    {
        pub fn new(cb: T) -> Self {
            Self(cb)
        }
    }

    #[async_trait::async_trait]
    impl<T> TcpListener<MockTcpStreamWrapper> for MockTcpListener<T>
    where
        T: Fn() -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)> + std::marker::Sync,
    {
        async fn accept(&self) -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)> {
            self.0()
        }
    }

    /// A mock TcpStreamWraper that is not actually usable, but can be passed
    /// in place of a StandardTcpStream in order to avoid needing to create a
    /// real TcpStream which would interact with the actual operating system
    /// network stack.
    struct MockTcpStreamWrapper;

    impl TcpStreamWrapper for MockTcpStreamWrapper {
        fn into_inner(self) -> std::io::Result<TcpStream> {
            Err(std::io::ErrorKind::Unsupported.into())
        }
    }
}
