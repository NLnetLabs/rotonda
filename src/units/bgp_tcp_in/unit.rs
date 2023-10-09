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
trait TcpListener {
    async fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)>;
}

struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(&self, addr: String) -> std::io::Result<StandardTcpListener> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
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

//----------- BgpTcpIn -------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct BgpTcpIn {
    /// Address:port to listen on incoming BGP connections over TCP.
    listen: String,
    pub my_asn: Asn, // TODO make getters, or impl Into<-fsm::bgp::Config>
    pub my_bgp_id: [u8; 4],
    #[serde(rename = "peers", default)]
    pub peer_configs: PeerConfigs,
}

impl PartialEq for BgpTcpIn {
    fn eq(&self, other: &Self) -> bool {
        self.listen == other.listen
            && self.my_asn == other.my_asn
            && self.my_bgp_id == other.my_bgp_id
    }
}

impl BgpTcpIn {
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

        BgpTcpInRunner::new(self, gate, metrics, status_reporter)
            .run(Arc::new(StandardTcpListenerFactory))
            .await
    }
}

pub type LiveSessions = HashMap<(IpAddr, Asn), mpsc::Sender<Command>>;

struct BgpTcpInRunner {
    // The configuration from the .conf.
    bgp: BgpTcpIn,

    gate: Gate,

    metrics: Arc<BgpTcpInMetrics>,

    status_reporter: Arc<BgpTcpInStatusReporter>,

    // To send commands to a Session based on peer IP + ASN.
    live_sessions: Arc<Mutex<LiveSessions>>,
}

impl BgpTcpInRunner {
    fn new(
        bgp: BgpTcpIn,
        gate: Gate,
        metrics: Arc<BgpTcpInMetrics>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
    ) -> Self {
        BgpTcpInRunner {
            bgp,
            gate,
            metrics,
            status_reporter,
            live_sessions: Arc::new(Mutex::new(HashMap::new())),
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
                            let (cmds_tx, cmds_rx) = mpsc::channel(16);
                            crate::tokio::spawn(
                                &child_name,
                                handle_connection(
                                    self.gate.clone(),
                                    self.bgp.clone(),
                                    tcp_stream,
                                    //cfg.clone(),
                                    CombinedConfig::new(self.bgp.clone(), cfg.clone(), remote_net),
                                    cmds_tx.clone(),
                                    cmds_rx,
                                    child_status_reporter,
                                    self.live_sessions.clone(),
                                ),
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
