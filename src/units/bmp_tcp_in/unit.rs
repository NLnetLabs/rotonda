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
use std::sync::Arc;

use super::{
    metrics::BmpTcpInMetrics, router_handler::handle_router,
    status_reporter::BmpTcpInStatusReporter,
};
use crate::common::status_reporter::UnitStatusReporter;
use crate::common::{status_reporter::Chainable, unit::UnitActivity};
use crate::manager::{Component, WaitPoint};
use crate::tokio::TokioTaskMetrics;
use crate::{
    comms::{Gate, GateStatus, Terminated},
    units::Unit,
};

use futures::future::select;
use futures::pin_mut;

use serde::Deserialize;
use tokio::net::TcpStream;

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
        component: Component,
        gate: Gate,
        waipoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated> {
        BmpTcpInRunner::new(self)
            .run(
                component,
                gate,
                Arc::new(StandardTcpListenerFactory),
                waipoint,
            )
            .await
    }
}

struct BmpTcpInRunner {
    bmp: BmpTcpIn,
}

impl BmpTcpInRunner {
    fn new(bmp: BmpTcpIn) -> Self {
        BmpTcpInRunner { bmp }
    }

    async fn run<T, U>(
        mut self,
        mut component: Component,
        gate: Gate,
        listener_factory: Arc<T>,
        mut waitpoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated>
    where
        T: TcpListenerFactory<U>,
        U: TcpListener,
    {
        let unit_name = component.name().clone();

        // Setup metrics
        let metrics = Arc::new(BmpTcpInMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        let accept_metrics = Arc::new(TokioTaskMetrics::new());
        component.register_metrics(accept_metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(BmpTcpInStatusReporter::new(&unit_name, metrics.clone()));

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone waiting to send important gate status
        // updates won't send them while we are in process_until() which will just eat them without handling them.
        waitpoint.running().await;

        // Loop until terminated, accepting TCP connections from routers and
        // spawning tasks to handle them.
        loop {
            status_reporter.listener_listening(&self.bmp.listen);
            let listener = listener_factory
                .bind(self.bmp.listen.clone())
                .await
                .unwrap_or_else(|err| {
                    status_reporter.listener_io_error(&err);
                    panic!(
                        "Listening for connections on {} failed: {}",
                        self.bmp.listen, err
                    );
                });

            let mut accept_fut = Box::pin(accept_metrics.instrument(listener.accept()));
            loop {
                let res = {
                    // wait for a status change or an incoming connection from
                    // a router.
                    let process_fut = gate.process();
                    pin_mut!(process_fut);

                    let res = select(process_fut, accept_fut).await;

                    match (&status_reporter, res).into() {
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
                                        // Break to trigger re-binding to the
                                        // new listen port.
                                        break;
                                    }
                                }

                                GateStatus::ReportLinks { report } => {
                                    report.declare_source();
                                    report.set_graph_status(metrics.clone());
                                }

                                _ => { /* Nothing to do */ }
                            }

                            accept_fut = next_fut;
                            None
                        }

                        UnitActivity::InputError(err) => {
                            // Error while listening for router connections.
                            // Break to trigger re-binding to the listen port.
                            status_reporter.listener_io_error(&err);
                            break;
                        }

                        UnitActivity::InputReceived((tcp_stream, router_addr)) => {
                            // Incoming router connection.
                            status_reporter.listener_connection_accepted(router_addr);
                            accept_fut = Box::pin(accept_metrics.instrument(listener.accept()));
                            Some((tcp_stream, router_addr))
                        }

                        UnitActivity::Terminated => {
                            status_reporter.terminated();

                            return Err(Terminated);
                        }
                    }
                };

                if let Some((tcp_stream, client_addr)) = res {
                    // Spawn a task to handle the newly connected routers BMP
                    // message stream.

                    // Choose a name to be reported in application logs.
                    let child_name = format!("router[{}:{}]", client_addr.ip(), client_addr.port());

                    // Create a status reporter whose name in output will be
                    // a combination of ours as parent and the newly chosen
                    // child name, enabling logged messages relating to this
                    // newly connected router to be distinguished from logged
                    // messages relating to other connected routers.
                    let child_status_reporter = Arc::new(status_reporter.add_child(&child_name));

                    crate::tokio::spawn(
                        &child_name,
                        handle_router(gate.clone(), tcp_stream, client_addr, child_status_reporter),
                    );
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
        comms::Gate,
        manager::{Component, Coordinator},
        units::{
            bmp_tcp_in::unit::{BmpTcpInRunner, TcpListener, TcpListenerFactory},
            Unit,
        },
    };

    use super::BmpTcpIn;

    struct MockTcpListenerFactory {
        pub binds: Arc<Mutex<Vec<String>>>,
    }

    struct MockTcpListener;

    impl MockTcpListenerFactory {
        pub fn new() -> Self {
            Self {
                binds: Arc::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl TcpListenerFactory<MockTcpListener> for MockTcpListenerFactory {
        async fn bind(&self, addr: String) -> std::io::Result<MockTcpListener> {
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconfigured_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (gate, agent) = Gate::new(1);
        let component = Component::default();
        let bmp = BmpTcpIn {
            listen: "localhost:8080".to_string(),
        };
        let bmp_tcp_in = BmpTcpInRunner { bmp };
        let mock_listener_factory = Arc::new(MockTcpListenerFactory::new());
        let coordinator = Coordinator::new(1);
        let task = bmp_tcp_in.run(
            component,
            gate,
            mock_listener_factory.clone(),
            coordinator.clone().track("mock".to_string()),
        );
        let join_handle = tokio::task::spawn(task);

        // Wait until the unit is ready
        timeout(
            Duration::from_secs(10),
            coordinator.wait(|_, _| unreachable!()),
        )
        .await
        .unwrap();

        // When the unit is reconfigured to listen for incoming connections on
        // "otherhost:8081":
        let (new_gate, new_agent) = Gate::new(1);
        let new_config = BmpTcpIn {
            listen: "otherhost:8081".to_string(),
        };
        let new_config = Unit::BmpTcpIn(new_config);
        agent.reconfigure(new_config, new_gate).await.unwrap();

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unchanged_bind_address() {
        // Given an instance of the BMP TCP input unit that is configured to
        // listen for incoming connections on "localhost:8080":
        let (gate, agent) = Gate::new(1);
        let component = Component::default();
        let bmp = BmpTcpIn {
            listen: "localhost:8080".to_string(),
        };
        let bmp_tcp_in = BmpTcpInRunner { bmp };
        let mock_listener_factory = Arc::new(MockTcpListenerFactory::new());
        let coordinator = Coordinator::new(1);
        let task = bmp_tcp_in.run(
            component,
            gate,
            mock_listener_factory.clone(),
            coordinator.clone().track("mock".to_string()),
        );
        let join_handle = tokio::task::spawn(task);

        // Wait until the unit is ready
        timeout(
            Duration::from_secs(10),
            coordinator.wait(|_, _| unreachable!()),
        )
        .await
        .expect("Unit took too long to become ready");

        // When the unit is reconfigured to listen for incoming connections on
        // an unchanged listen address:
        let (new_gate, new_agent) = Gate::new(1);
        let new_config = BmpTcpIn {
            listen: "localhost:8080".to_string(),
        };
        let new_config = Unit::BmpTcpIn(new_config);
        agent.reconfigure(new_config, new_gate).await.unwrap();

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
}
