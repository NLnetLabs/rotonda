use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::future::select;
use futures::{pin_mut, Future};
use log::{debug, error, warn};
use non_empty_vec::NonEmpty;
//use roto::types::{builtin::BuiltinTypeValue, typevalue::TypeValue};
use inetnum::asn::Asn;
use routecore::bgp::fsm::session::{Command, DisconnectReason};
use routecore::bgp::message::UpdateMessage;
use routecore::bgp::message::{
    update_builder::UpdateBuilder, Message as BgpMsg, SessionConfig,
};

use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::common::net::{
    StandardTcpListenerFactory, StandardTcpStream, TcpListener,
    TcpListenerFactory, TcpStreamWrapper,
};
use crate::roto_runtime::metrics::RotoMetricsWrapper;
use crate::roto_runtime::types::{
    RotoPackage, FilterName, RotoOutputStream, RotoScripts
};
//use crate::common::roto::{FilterName, RotoScripts};
use crate::common::status_reporter::{Chainable, UnitStatusReporter};
use crate::common::unit::UnitActivity;
use crate::comms::{
    AnyDirectUpdate, DirectLink, DirectUpdate, GateStatus, Terminated,
};
use crate::ingress;
use crate::manager::{Component, WaitPoint};
use crate::payload::Update;
use crate::roto_runtime::{Ctx, MutIngressInfoCache};
use crate::units::rib_unit::rpki::RtrCache;
use crate::units::{Gate, Unit};

use super::metrics::BgpTcpInMetrics;
use super::router_handler::handle_connection;
use super::status_reporter::BgpTcpInStatusReporter;

use super::peer_config::{CombinedConfig, PeerConfigs};

//----------- BgpTcpIn -------------------------------------------------------

pub(crate) type RotoFunc = roto::TypedFunc<
    crate::roto_runtime::Ctx,
    fn(
        roto::Val<UpdateMessage<Bytes>>,
        roto::Val<MutIngressInfoCache>,
    ) ->
    roto::Verdict<(), ()>,
>;

pub const ROTO_FUNC_FILTER_NAME: &str = "bgp_in";

#[derive(Clone, Debug, Deserialize)]
pub struct BgpTcpIn {
    /// Address:port to listen on incoming BGP connections over TCP.
    pub listen: String,

    // TODO make getters, or impl Into<-fsm::bgp::Config>
    pub my_asn: Asn,

    pub my_bgp_id: [u8; 4],

    #[serde(rename = "peers", default)]
    pub peer_configs: PeerConfigs,

    #[serde(default)]
    pub filter_name: FilterName,
    ///// Outgoing BGP UPDATEs can come from these sources.
    //pub sources: Vec<DirectLink>
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
            //sources: Vec::new(),
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

        let ingresses = component.ingresses();
        let roto_metrics = component.roto_metrics().clone();

        // Setup status reporting
        let status_reporter = Arc::new(BgpTcpInStatusReporter::new(
            &unit_name,
            metrics.clone(),
        ));

        let roto_compiled = component.roto_package().clone();

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

        // XXX refactor BgpTcpInRunner so it does not take the mut BgpTcpIn
        // so that we can pass self.sources into .run below
        // That way this unit is a bit more consistent with the RibUnit.
        //let sources = self.sources.clone();
        BgpTcpInRunner::new(
            self,
            gate,
            metrics,
            status_reporter,
            roto_compiled,
            roto_metrics,
            ingresses,
        )
        .run::<_, _, StandardTcpStream, BgpTcpInRunner>(
            //sources,
            Vec::new(),
            Arc::new(StandardTcpListenerFactory),
        )
        .await
    }
}

trait ConfigAcceptor {
    #[allow(clippy::too_many_arguments)]
    fn accept_config(
        child_name: String,
        roto_function: Option<RotoFunc>,
        roto_context: Arc<Mutex<Ctx>>,
        gate: &Gate,
        bgp: &BgpTcpIn,
        tcp_stream: impl TcpStreamWrapper,
        cfg: &super::peer_config::PeerConfig,
        remote_net: super::peer_config::PrefixOrExact,
        child_status_reporter: Arc<BgpTcpInStatusReporter>,
        live_sessions: Arc<Mutex<LiveSessions>>,
        ingresses: Arc<ingress::Register>,
        connector_ingress_id: ingress::IngressId,
    );
}

pub type LiveSessions = HashMap<
    (IpAddr, Asn),
    (mpsc::Sender<Command>, mpsc::Sender<BgpMsg<Bytes>>),
>;

//#[derive(Debug)]
struct BgpTcpInRunner {
    // The configuration from the .conf.
    bgp: Arc<ArcSwap<BgpTcpIn>>,

    gate: Gate,

    metrics: Arc<BgpTcpInMetrics>,

    status_reporter: Arc<BgpTcpInStatusReporter>,

    roto_compiled: Option<Arc<RotoPackage>>,

    roto_metrics: Option<Arc<RotoMetricsWrapper>>,

    // To send commands to a Session based on peer IP + ASN.
    live_sessions: Arc<Mutex<LiveSessions>>,

    ingresses: Arc<ingress::Register>,
}

// As long as roto::Compiled has no Debug impl, we cook up something simple
// ourselves here. We need it because AnyDirectUpdate requires Debug.
impl fmt::Debug for BgpTcpInRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BgpTcpInRunner: {{ bgp: {:?}  }}", self.bgp)
    }
}

impl BgpTcpInRunner {
    fn new(
        bgp: BgpTcpIn,
        gate: Gate,
        metrics: Arc<BgpTcpInMetrics>,
        status_reporter: Arc<BgpTcpInStatusReporter>,
        roto_compiled: Option<Arc<RotoPackage>>,
        roto_metrics: Option<Arc<RotoMetricsWrapper>>,
        ingresses: Arc<ingress::Register>,
    ) -> Self {
        BgpTcpInRunner {
            bgp: Arc::new(ArcSwap::from_pointee(bgp)),
            gate,
            metrics,
            status_reporter,
            roto_compiled,
            roto_metrics,
            live_sessions: Arc::new(Mutex::new(HashMap::new())),
            ingresses,
        }
    }

    #[cfg(test)]
    fn mock(bgp: BgpTcpIn) -> (Self, crate::comms::GateAgent) {
        let (gate, gate_agent) = Gate::new(0);

        let runner = BgpTcpInRunner {
            bgp: Arc::new(ArcSwap::from_pointee(bgp)),
            gate,
            metrics: Default::default(),
            status_reporter: Default::default(),
            live_sessions: Arc::new(Mutex::new(HashMap::new())),
            ingresses: Arc::new(ingress::Register::default()),
            roto_compiled: None,
            roto_metrics: Default::default(),
        };

        (runner, gate_agent)
    }

    async fn run<T, U, V, F>(
        self,
        mut sources: Vec<DirectLink>,
        listener_factory: Arc<T>,
    ) -> Result<(), Terminated>
    where
        T: TcpListenerFactory<U>,
        U: TcpListener<V>,
        V: TcpStreamWrapper,
        F: ConfigAcceptor,
    {
        // Loop until terminated, accepting TCP connections from routers and
        // spawning tasks to handle them.
        let status_reporter = self.status_reporter.clone();

        let arc_self = Arc::new(self);
        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        let roto_function: Option<RotoFunc> =
            arc_self.roto_compiled.clone().and_then(|c| {
                let mut c = c.lock().unwrap();
                c.get_function(ROTO_FUNC_FILTER_NAME)
                .inspect_err(|_|
                    warn!("Loaded Roto script has no filter for bgp-in")
                )
                .ok()
            });

        let mut roto_context = Ctx::empty();

        if let Some(roto_metrics) = arc_self.roto_metrics.as_ref() {
            roto_context.set_metrics(roto_metrics.metrics.clone());
        }

        if let Some(c) = arc_self.roto_compiled.clone() {
            roto_context.prepare(&mut c.lock().unwrap());
        }

        let roto_context = Arc::new(Mutex::new(roto_context));

        loop {
            let listen_addr = arc_self.bgp.load().listen.clone();

            let bind_with_backoff = || async {
                let mut wait = 1;
                loop {
                    match listener_factory.bind(listen_addr.clone()).await {
                        Err(err) => {
                            let err = format!(
                                "{err}: Will retry in {wait} seconds."
                            );
                            status_reporter.bind_error(&listen_addr, &err);
                            sleep(Duration::from_secs(wait)).await;
                            wait *= 2;
                        }
                        res => break res,
                    }
                }
            };

            let listener =
                match arc_self.process_until(bind_with_backoff()).await {
                    ControlFlow::Continue(Ok(res)) => res,
                    ControlFlow::Continue(Err(_err)) => continue,
                    ControlFlow::Break(Terminated) => return Err(Terminated),
                };

            status_reporter.listener_listening(&listen_addr);

            'inner: loop {
                match arc_self.process_until(listener.accept()).await {
                    ControlFlow::Continue(Ok((tcp_stream, peer_addr))) => {
                        status_reporter
                            .listener_connection_accepted(peer_addr);

                        // Now: check for the peer_addr.ip() in the new
                        // PeerConfig and spawn a Session for it. We might
                        // need some new stuff: a temporary 'pending'
                        // list/map, keyed on only the peer IP, from which
                        // entries will be removed once the OPENs are
                        // exchanged and we know the remote ASN.

                        if let Some((remote_net, cfg)) = arc_self
                            .bgp
                            .load()
                            .peer_configs
                            .get(peer_addr.ip())
                        {
                            let child_name = format!(
                                "bgp[{}:{}]",
                                peer_addr.ip(),
                                peer_addr.port()
                            );
                            let child_status_reporter = Arc::new(
                                status_reporter.add_child(&child_name),
                            );
                            debug!(
                                "[{}] config matched: {}",
                                peer_addr.ip(),
                                cfg.name()
                            );
                            F::accept_config(
                                child_name,
                                roto_function.clone(),
                                roto_context.clone(),
                                &arc_self.gate,
                                &arc_self.bgp.load().clone(),
                                tcp_stream,
                                cfg,
                                remote_net,
                                child_status_reporter,
                                arc_self.live_sessions.clone(),
                                arc_self.ingresses.clone(),
                                // XXX we need to do a find_existing_peer here instead of blindly
                                // doing a .register().
                                arc_self.ingresses.register(),
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
        &self,
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
                            let rebind =
                                self.bgp.load().listen != new_unit.listen;

                            self.bgp.store(new_unit.into());

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

#[async_trait]
impl DirectUpdate for BgpTcpInRunner {
    async fn direct_update(&self, _update: Update) {
        error!("Using a Bgp-in unit as target is currently not supported");
        return;
        /*
        let process_update = |value: TypeValue| async {
            if let TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(pfr)) = value {
                //debug!("got {} for prefix {} from {:?}@{:?}",
                //    rrwd.status(),
                //    rrwd.prefix,
                //    rrwd.peer_asn(),
                //    rrwd.peer_ip(),
                //);
                use roto::types::builtin::PrefixRouteWs;
                let pdus: Vec<_> = match pfr.0 {
                    PrefixRouteWs::Ipv4Unicast(rws)=> {
                        let b = UpdateBuilder::<BytesMut, _>::from_workshop(rws);
                        b.into_pdu_iter(&SessionConfig::modern()).collect()
                        }
                    PrefixRouteWs::Ipv4UnicastAddpath(rws)=> {
                        let b = UpdateBuilder::<BytesMut, _>::from_workshop(rws);
                        b.into_pdu_iter(&SessionConfig::modern()).collect()
                    }
                    PrefixRouteWs::Ipv6Unicast(rws) => {
                        let b = UpdateBuilder::<BytesMut, _>::from_workshop(rws);
                        b.into_pdu_iter(&SessionConfig::modern()).collect()
                    }
                    PrefixRouteWs::Ipv6UnicastAddpath(_) => todo!(),
                    PrefixRouteWs::Ipv4Multicast(_) => todo!(),
                    PrefixRouteWs::Ipv4MulticastAddpath(_) => todo!(),
                    PrefixRouteWs::Ipv6Multicast(_) => todo!(),
                    PrefixRouteWs::Ipv6MulticastAddpath(_) => todo!(),
                };

                /*
                let mut builder = match UpdateBuilder::from_update_message(
                    &rrwd.raw_message.raw_message().0,
                    &SessionConfig::modern(), // currenlty unused
                    buffer
                ) {
                    Ok(builder) => builder,
                    Err(_e) => {
                        warn!("got invalid PDU from RIB");
                        return;
                    }
                };
                match rrwd.afi_safi {
                    AfiSafi::Ipv4Unicast | AfiSafi::Ipv6Unicast => {
                        if let Err(e) = builder.add_announcement(
                            &Nlri::<&[u8]>::Unicast(BasicNlri::from(rrwd.prefix))
                        ) {
                            warn!("failed to add announcement for {}: {}",
                                rrwd.prefix,
                                e
                            );
                        }
                    }
                    _ => { warn!("got non unicast PDU from RIB"); return }
                }
                if let Ok(Some(nh)) = rrwd.raw_message.raw_message().0.mp_next_hop() {
                    if let Err(e) = builder.set_mp_nexthop(nh) {
                        warn!("invalid MP nexthop {:?} in PDU leaving RIB: {}", nh, e);
                    }
                }
                let pdu_out = builder.into_message().unwrap();
                */
                let chans = {
                    self.live_sessions.lock().unwrap().clone().into_values()
                };
                for (cmd_tx, pdu_out_tx) in chans {
                    //debug!(
                    //    "emitting Command::RawUpdate, chan cap {}",
                    //    chan.capacity()
                    //);
                    for pdu_out in &pdus {
                        let pdu_out = match pdu_out {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Failed to construct PDU: {e}");
                                continue;
                            }
                        };
                        if let Err(e) = pdu_out_tx.send(
                            //Command::RawUpdate(pdu_out.clone())
                            BgpMsg::Update(pdu_out.clone())
                        ).await {
                            warn!("failed to send to pdu_out_tx: {e}");
                            let _ = cmd_tx.send(Command::Disconnect(DisconnectReason::Other)).await;
                        }
                    }
                }
            }
        };

        match update {
            Update::Single(update) => {
                process_update(update.rx_value).await;
            },
            Update::Bulk(bulk) => {
                for u in bulk {
                    process_update(u.rx_value).await;
                }
            },
            _ => todo!(),
        }
        */
    }
}

impl AnyDirectUpdate for BgpTcpInRunner {}

impl ConfigAcceptor for BgpTcpInRunner {
    fn accept_config(
        child_name: String,
        roto_function: Option<RotoFunc>,
        roto_context: Arc<Mutex<Ctx>>,
        gate: &Gate,
        bgp: &BgpTcpIn,
        tcp_stream: impl TcpStreamWrapper,
        cfg: &super::peer_config::PeerConfig,
        remote_net: super::peer_config::PrefixOrExact,
        child_status_reporter: Arc<BgpTcpInStatusReporter>,
        live_sessions: Arc<Mutex<LiveSessions>>,
        ingresses: Arc<ingress::Register>,
        connector_ingress_id: ingress::IngressId,
    ) {
        let (cmds_tx, cmds_rx) = mpsc::channel(10 * 10); //XXX this is limiting and
                                                         //causes loss
        let tcp_stream = tcp_stream.into_inner().unwrap(); // SAFETY: StandardTcpStream::into_inner() always returns Ok(...)
        crate::tokio::spawn(
            &child_name,
            handle_connection(
                roto_function,
                roto_context,
                gate.clone(),
                bgp.clone(),
                tcp_stream,
                CombinedConfig::new(bgp.clone(), cfg.clone(), remote_net),
                cmds_tx.clone(),
                cmds_rx,
                child_status_reporter,
                live_sessions,
                ingresses,
                connector_ingress_id,
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
    use inetnum::asn::Asn;

    use crate::{
        common::{
            net::TcpStreamWrapper,
            status_reporter::AnyStatusReporter,
        }, comms::{Gate, GateAgent, Terminated}, ingress, roto_runtime::types::RotoScripts, tests::util::{
            internal::get_testable_metrics_snapshot,
            net::{
                MockTcpListener, MockTcpListenerFactory, MockTcpStreamWrapper,
            },
        }, units::bgp_tcp_in::{
            peer_config::{PeerConfig, PrefixOrExact},
            status_reporter::BgpTcpInStatusReporter,
            unit::{BgpTcpIn, BgpTcpInRunner, ConfigAcceptor, LiveSessions},
        }
    };

    use super::RotoFunc;

    #[tokio::test(flavor = "multi_thread")]
    async fn listener_bound_count_metric_should_work() {
        let mock_listener_factory_cb = |_addr| {
            Ok(MockTcpListener::new(|| async {
                Err(std::io::ErrorKind::ConnectionRefused.into())
            }))
        };

        let (runner_fut, gate_agent, status_reporter) =
            setup_test(mock_listener_factory_cb);
        let join_handle =
            crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        loop {
            let metrics = get_testable_metrics_snapshot(
                &status_reporter.metrics().unwrap(),
            );
            let count =
                metrics.with_name::<usize>("bgp_tcp_in_listener_bound_count");
            if count > 0 {
                break;
            }
        }

        gate_agent.terminate().await;

        let res = join_handle.await.unwrap();
        assert_eq!(res, Err(Terminated));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_with_backoff_on_accept_error() {
        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_clone = accept_count.clone();

        let mock_listener_factory_cb = move |_addr| {
            let old_count = accept_count_clone.fetch_add(1, Ordering::SeqCst);
            if old_count < 1 {
                Err(std::io::ErrorKind::PermissionDenied.into())
            } else {
                Ok(MockTcpListener::new(|| {
                    std::future::ready(Ok((
                        MockTcpStreamWrapper,
                        "1.2.3.4:12345".parse().unwrap(),
                    )))
                }))
            }
        };

        let (runner_fut, gate_agent, status_reporter) =
            setup_test(mock_listener_factory_cb);
        let join_handle =
            crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        loop {
            let metrics = get_testable_metrics_snapshot(
                &status_reporter.metrics().unwrap(),
            );
            let count = metrics
                .with_name::<usize>("bgp_tcp_in_connection_accepted_count");
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
            move |_addr| {
                let conn_count = conn_count.clone();
                Ok(MockTcpListener::new(move || {
                    std::future::ready({
                        let old_count =
                            conn_count.fetch_add(1, Ordering::SeqCst);
                        if old_count < 1 {
                            Ok((
                                MockTcpStreamWrapper,
                                "1.2.3.4:5".parse().unwrap(),
                            ))
                        } else {
                            Err(std::io::ErrorKind::PermissionDenied.into())
                        }
                    })
                }))
            }
        };

        let (runner_fut, gate_agent, status_reporter) =
            setup_test(mock_listener_factory_cb);
        let join_handle =
            crate::tokio::spawn("mock_bgp_tcp_in_runner", runner_fut);

        let mut count = 0;
        while count != 1 {
            let metrics = get_testable_metrics_snapshot(
                &status_reporter.metrics().unwrap(),
            );
            count = metrics
                .with_name::<usize>("bgp_tcp_in_connection_accepted_count");
        }

        gate_agent.terminate().await;

        let res = join_handle.await.unwrap();
        assert_eq!(res, Err(Terminated));
    }

    //-------- Test helpers --------------------------------------------------

    fn setup_test<T, U, Fut>(
        mock_listener_factory_cb: T,
    ) -> (
        impl Future<Output = Result<(), Terminated>>,
        GateAgent,
        Arc<BgpTcpInStatusReporter>,
    )
    where
        T: Fn(String) -> std::io::Result<MockTcpListener<U, Fut>> + Sync,
        U: Fn() -> Fut + Send + Sync,
        Fut: Future<
                Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
            > + Send,
    {
        let mock_listener_factory =
            MockTcpListenerFactory::new(mock_listener_factory_cb);

        let unit_settings =
            BgpTcpIn::mock("dummy-listen-address", Asn::from_u32(12345));

        let (runner, gate_agent) = BgpTcpInRunner::mock(unit_settings);

        let status_reporter = runner.status_reporter.clone();

        let runner_fut = runner.run::<_, _, _, NoOpConfigAcceptor>(
            vec![],
            mock_listener_factory.into(),
        );

        (runner_fut, gate_agent, status_reporter)
    }

    /// A config acceptor that does nothing, i.e. it does not spawn a child
    /// task to handle the connection as would normally happen.
    struct NoOpConfigAcceptor;

    impl ConfigAcceptor for NoOpConfigAcceptor {
        fn accept_config(
            _child_name: String,
            _roto_function: Option<RotoFunc>,
            _roto_context: Arc<std::sync::Mutex<crate::roto_runtime::Ctx>>,
            _gate: &Gate,
            _bgp: &BgpTcpIn,
            _tcp_stream: impl TcpStreamWrapper,
            _cfg: &PeerConfig,
            _remote_net: PrefixOrExact,
            _child_status_reporter: Arc<BgpTcpInStatusReporter>,
            _live_sessions: Arc<std::sync::Mutex<LiveSessions>>,
            _ingressess: Arc<ingress::Register>,
            _connector_ingress_id: ingress::IngressId,
        ) {
        }
    }
}
