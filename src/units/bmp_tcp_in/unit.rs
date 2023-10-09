//! A unit that acts as a BMP "monitoring station".
//!
//! According to [RFC #7854] _"The BGP Monitoring Protocol (BMP) \[sic\] can
//! be used to monitor BGP sessions. BMP& is intended to provide a convenient
//! interface for obtaining route views"_.
//!
//! This unit accepts BMP data over incoming TCP connections from one or more
//! routers to a specified TCP IP address and port number on which we listen.
//!
//! [RFC #7854]: https://tools.ietf.org/html/rfc7854

use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::Arc;

use super::{
    metrics::BmpTcpInMetrics, router_handler::handle_router,
    status_reporter::BmpTcpInStatusReporter,
};
use crate::common::status_reporter::UnitStatusReporter;
use crate::common::{status_reporter::Chainable, unit::UnitActivity};
use crate::manager::{Component, WaitPoint};
use crate::{
    comms::{Gate, GateStatus, Terminated},
    units::Unit,
};

use futures::future::select;
use futures::{pin_mut, Future};

use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

//--- TCP listener traits ----------------------------------------------------
//
// These traits enable us to swap out the real TCP listener for a mock when
// testing.

#[async_trait::async_trait]
trait TcpListenerFactory<T> {
    async fn bind(&self, addr: String) -> std::io::Result<T>;
}

#[async_trait::async_trait]
trait TcpListener {
    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)>;
}

struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(&self, addr: String) -> std::io::Result<StandardTcpListener> {
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

//--- BmpTcpIn ---------------------------------------------------------------

/// An unit that receives incoming BMP data. For testing purposes you can
/// stream from a captured BMP stream in PCAP format using a command like so:
///     tshark -r <CAPTURED.pcap> -q -z follow,tcp,raw,0 | \
///         grep -E '^[0-9a-z]+' | \
///         xxd -r -p | \
///         socat - TCP4:127.0.0.1:11019
///
/// On change: if in the config file the unit is renamed, commented out,
///            deleted or no longer referenced it, AND any established
///            connections with routers, will be terminated.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct BmpTcpIn {
    /// A colon separated IP address and port number to listen on for incoming
    /// BMP over TCP connections from routers.
    ///
    /// On change: existing connections to routers will be unaffected, new
    ///            connections will only be accepted at the changed URI.
    pub listen: String,
}

impl BmpTcpIn {
    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated> {
        let unit_name = component.name().clone();

        // Setup metrics
        let metrics = Arc::new(BmpTcpInMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(BmpTcpInStatusReporter::new(&unit_name, metrics.clone()));

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone waiting to send important gate status
        // updates won't send them while we are in process_until() which will just eat them without handling them.
        waitpoint.running().await;

        BmpTcpInRunner::new(self, gate, metrics, status_reporter)
            .run(Arc::new(StandardTcpListenerFactory))
            .await
    }
}

struct BmpTcpInRunner {
    bmp: BmpTcpIn,
    gate: Gate,
    metrics: Arc<BmpTcpInMetrics>,
    status_reporter: Arc<BmpTcpInStatusReporter>,
}

impl BmpTcpInRunner {
    fn new(
        bmp: BmpTcpIn,
        gate: Gate,
        metrics: Arc<BmpTcpInMetrics>,
        status_reporter: Arc<BmpTcpInStatusReporter>,
    ) -> Self {
        BmpTcpInRunner {
            bmp,
            gate,
            metrics,
            status_reporter,
        }
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
            let listen_addr = self.bmp.listen.clone();

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
                    ControlFlow::Continue(Ok((tcp_stream, client_addr))) => {
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

                        crate::tokio::spawn(
                            &child_name,
                            handle_router(
                                self.gate.clone(),
                                tcp_stream,
                                client_addr,
                                child_status_reporter,
                            ),
                        );
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
                            new_config: Unit::BmpTcpIn(new_unit),
                        } => {
                            // Runtime reconfiguration of this unit has
                            // been requested. New connections will be
                            // handled using the new configuration,
                            // existing connections handled by
                            // router_handler() tasks will receive their
                            // own copy of this Reconfiguring status
                            // update and can react to it accordingly.
                            let rebind = self.bmp.listen != new_unit.listen;

                            self.bmp = new_unit;

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

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{net::TcpStream, time::timeout};

    use crate::{
        comms::{Gate, GateAgent},
        tests::util::internal::enable_logging,
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

    struct MockTcpListenerFactory<T>
    where
        T: Fn(String) -> std::io::Result<()> + Sync,
    {
        pub bind_cb: T,
        pub binds: Arc<Mutex<Vec<String>>>,
    }

    struct MockTcpListener;

    impl<T> MockTcpListenerFactory<T>
    where
        T: Fn(String) -> std::io::Result<()> + Sync,
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
        T: Fn(String) -> std::io::Result<()> + Sync,
    {
        async fn bind(&self, addr: String) -> std::io::Result<MockTcpListener> {
            (self.bind_cb)(addr.clone())?;
            self.binds.lock().unwrap().push(addr);
            Ok(MockTcpListener)
        }
    }

    #[async_trait::async_trait]
    impl TcpListener for MockTcpListener {
        async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
            // block forever
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_reconfigured_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (runner, agent) = setup_test("localhost:8080");
        let mock_listener_factory = Arc::new(MockTcpListenerFactory::new(|_| Ok(())));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // "otherhost:8081":
        let (new_gate, new_agent) = Gate::new(1);
        let new_config = BmpTcpIn {
            listen: "otherhost:8081".to_string(),
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
        assert_eq!(binds[0], "localhost:8080");
        assert_eq!(binds[1], "otherhost:8081");
    }

    #[tokio::test(start_paused = true)]
    async fn test_unchanged_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (runner, agent) = setup_test("localhost:8080");
        let mock_listener_factory = Arc::new(MockTcpListenerFactory::new(|_| Ok(())));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(1)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // an unchanged listen address:
        let (new_gate, new_agent) = Gate::new(1);
        let new_config = BmpTcpIn {
            listen: "localhost:8080".to_string(),
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
        assert_eq!(binds[0], "localhost:8080");
    }

    #[tokio::test(start_paused = true)]
    async fn test_overcoming_bind_failure() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let fail_on_bad_addr = |addr| {
            if addr != "idontexist:-1" {
                Ok(())
            } else {
                Err(std::io::ErrorKind::PermissionDenied.into())
            }
        };
        let (runner, agent) = setup_test("idontexist:-1");
        let mock_listener_factory = Arc::new(MockTcpListenerFactory::new(fail_on_bad_addr));
        let task = runner.run(mock_listener_factory.clone());
        let join_handle = tokio::task::spawn(task);

        // Allow time for bind attempts to occur
        tokio::time::sleep(Duration::from_secs(10)).await;

        // When the unit is reconfigured to listen for incoming connections on
        // an unchanged listen address:
        let (new_gate, new_agent) = Gate::new(1);
        let new_config = BmpTcpIn {
            listen: "localhost:8080".to_string(),
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
        assert_eq!(binds[0], "localhost:8080");
    }

    //-------- Test helpers --------------------------------------------------

    fn setup_test(listen: &str) -> (BmpTcpInRunner, GateAgent) {
        enable_logging("trace");

        let (gate, gate_agent) = Gate::new(0);
        let bmp = BmpTcpIn {
            listen: listen.to_string(),
        };
        let metrics = Arc::new(BmpTcpInMetrics::default());
        let status_reporter = Arc::new(BmpTcpInStatusReporter::default());
        let runner = BmpTcpInRunner {
            bmp,
            gate,
            metrics,
            status_reporter,
        };

        (runner, gate_agent)
    }
}
