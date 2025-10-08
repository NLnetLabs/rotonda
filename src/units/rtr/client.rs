//! RTR client units.
//!
//! There are two units in this module that act as an RTR client but use
//! different transport protocols: [`Tcp`] uses plain, unencrypted TCP while
//! [`Tls`] uses TLS.

use std::error::Error;
use std::fmt::Display;
use std::io;
use std::fs::File;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64};
use std::task::{Context, Poll};
use std::time::Duration;
use chrono::{TimeZone, Utc};
//use daemonbase::config::ConfigPath;
use futures_util::pin_mut;
use futures_util::future::{select, Either};
use log::info;
use log::{debug, error, warn};
use pin_project_lite::pin_project;
use rpki::rtr::client::{Client, PayloadError, PayloadTarget, PayloadUpdate};
use rpki::rtr::payload::RouteOrigin;
use rpki::rtr::payload::{Action, Payload, Timing};
use rpki::rtr::state::State;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{timeout_at, Instant};
use crate::manager::WaitPoint;
//use tokio_rustls::TlsConnector;
//use tokio_rustls::client::TlsStream;
//use tokio_rustls::rustls::{ClientConfig, RootCertStore};
//use tokio_rustls::rustls::pki_types::ServerName;
use crate::metrics;
use crate::comms::{Gate, GateMetrics, GateStatus, Terminated};
use crate::manager::Component;
use crate::metrics::{Metric, MetricType, MetricUnit};
use crate::payload;
use crate::payload::Update;

//------------ Tcp -----------------------------------------------------------

/// An RTR client using an unencrypted plain TCP socket.
#[derive(Clone, Debug, Deserialize)]
pub struct Tcp {
    /// The remote address to connect to.
    remote: String,

    /// How long to wait before connecting again if the connection is closed.
    #[serde(default = "Tcp::default_retry")]
    retry: u64,

    /// Start the RTR version negotiation with this version.
    #[serde(default = "Tcp::default_version")]
    initial_version: u8
}

impl Tcp {
    /// The default re-connect timeout in seconds.
    fn default_retry() -> u64 {
        60
    }

    /// The default version to start the RTR version negotiation with.
    fn default_version() -> u8 {
        2
    }

    /// Runs the unit.
    ///
    /// This method will only ever return if the RTR client encounters a fatal
    /// error.
    pub async fn run(
        self, component: Component, gate: Gate, mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let metrics = Arc::new(RtrMetrics::new(&gate));

        gate.process_until(waitpoint.ready()).await?;
        tokio::spawn(async  {
            debug!("waiting 5 secs to give rtr-in a headstart");
            tokio::time::sleep(Duration::from_secs(5)).await;
            debug!("waiting done, calling waitpoint.running()");
            waitpoint.running().await;
        });

        RtrRunner::new(
            component, gate, self, metrics
        ).run().await?;
        Ok(())
    }
}

struct RtrRunner {
    component: Component,
    gate: Gate,
    tcp: Tcp,
    metrics: Arc<RtrMetrics>,
}

impl RtrRunner {
    pub fn new(
        component: Component,
        gate: Gate,
        tcp: Tcp,
        metrics: Arc<RtrMetrics>
    ) -> Self {
        Self {
            component, gate, tcp, metrics
        }
    }

    pub async fn run(mut self) -> Result<(), Terminated>{
        self.component.register_metrics(self.metrics.clone());
        let remote = self.tcp.remote.clone();
        let metrics = self.metrics.clone();
        let fut = RtrClient::run(
            self.component.name().clone(), self.gate.clone(), self.tcp.initial_version, self.tcp.retry, self.metrics.clone(),
            || async {
                Ok(RtrTcpStream {
                    sock: TcpStream::connect(remote.clone()).await?,
                    metrics: metrics.clone(),
                })
            }
        );

        // We don't have to loop like in other (ingress) units, as the RtrClient itself loops and
        // only returns in case of a terminal failure.
        match self.process_until(fut).await {
            ControlFlow::Continue(Ok(res)) => res,
            ControlFlow::Continue(Err(_)) | 
                ControlFlow::Break(Terminated) => {
                    error!("Terminated");
                    return Err(Terminated)
                }
        };
        Ok(())
    }


    async fn process_until<T, U>(
        &mut self,
        until_fut: T,
        ) -> ControlFlow<Terminated, Result<U, Terminated>> 
    where
        T: Future<Output = Result<U, Terminated>>
    {
        let mut until_fut = Box::pin(until_fut);

        loop {
            let process_fut = self.gate.process();
            pin_mut!(process_fut);

            let res = select(process_fut, until_fut).await;

            match res {
                Either::Left((Ok(process_fut), next_fut)) => {
                    match process_fut {
                        GateStatus::Active => { },
                        GateStatus::Dormant => { },
                        GateStatus::Reconfiguring { new_config: _ } => {
                            warn!("Reconfiguration for RTR ingress not yet supported, \
                                old config remains active");
                        }
                        GateStatus::ReportLinks { report } => {
                            report.declare_source();
                        }

                        GateStatus::Triggered { data: _ } => {
                            warn!("GateStatus::Triggered in RTR ingress not supported");
                        }
                    }
                    until_fut = next_fut;
                }
                Either::Left((Err(_r), _other_fut)) => {
                    return ControlFlow::Break(Terminated)
                }
                Either::Right((Result::Ok(r), _other_fut)) => {
                    return ControlFlow::Continue(Result::Ok(r))
                }
                Either::Right((Result::Err(_r), _other_fut)) => {
                    return ControlFlow::Break(Terminated)
                }

            }

        }
    }


}

/*
//------------ Tls -----------------------------------------------------------

/// An RTR client using a TLS encrypted TCP socket.
#[derive(Debug, Deserialize)]
pub struct Tls {
    /// The remote address to connect to.
    remote: String,

    /// How long to wait before connecting again if the connection is closed.
    #[serde(default = "Tcp::default_retry")]
    retry: u64,

    /// Paths to root certficates.
    ///
    /// The files should contain one or more PEM-encoded certificates.
    #[serde(default)]
    cacerts: Vec<ConfigPath>,
}

/// Run-time information of the TLS unit.
struct TlsState {
    /// The unit configuration.
    tls: Tls,

    /// The name of the server.
    domain: ServerName<'static>,

    /// The TLS configuration for connecting to the server.
    connector: TlsConnector,

    /// The unit’s metrics.
    metrics: Arc<RtrMetrics>,
}

impl Tls {
    /// Runs the unit.
    ///
    /// This method will only ever return if the RTR client encounters a fatal
    /// error.
    pub async fn run(
        self, component: Component, gate: Gate
    ) -> Result<(), Terminated> {
        let domain = self.get_domain_name(component.name())?;
        let connector = self.build_connector(component.name())?;
        let retry = self.retry;
        let metrics = Arc::new(RtrMetrics::new(&gate));
        let state = Arc::new(TlsState {
            tls: self, domain, connector, metrics: metrics.clone(), 
        });
        RtrClient::run(
            component, gate, retry, metrics,
            move || {
                Self::connect(state.clone())
            }
        ).await
    }

    /// Converts the server address into the name for certificate validation.
    fn get_domain_name(
        &self, unit_name: &str
    ) -> Result<ServerName<'static>, Terminated> {
        let host = if let Some((host, port)) = self.remote.rsplit_once(':') {
            if port.parse::<u16>().is_ok() {
                host
            }
            else {
                self.remote.as_ref()
            }
        }
        else {
            self.remote.as_ref()
        };
        ServerName::try_from(host).map(|res| res.to_owned()).map_err(|err| {
            error!(
                "Unit {}: Invalid remote name '{}': {}'",
                unit_name, host, err
            );
            Terminated
        })
    }

    /// Prepares the TLS configuration for connecting to the server.
    fn build_connector(
        &self, unit_name: &str
    ) -> Result<TlsConnector, Terminated> {
        let mut root_certs = RootCertStore {
            roots: Vec::from(webpki_roots::TLS_SERVER_ROOTS)
        };
        for path in &self.cacerts {
            let mut file = io::BufReader::new(
                File::open(path).map_err(|err| {
                    error!(
                        "Unit {}: failed to open cacert file '{}': {}",
                        unit_name, path.display(), err
                    );
                    Terminated
                })?
            );
            for cert in rustls_pemfile::certs(&mut file) {
                let cert = match cert {
                    Ok(cert) => cert,
                    Err(err) => {
                        error!(
                            "Unit {}: failed to read certificate file '{}': \
                             {}",
                            unit_name, path.display(), err
                        );
                        return Err(Terminated)
                    }
                };
                if let Err(err) = root_certs.add(cert) {
                    error!(
                        "Unit {}: failed to add TLS certificate \
                         from file '{}': {}",
                        unit_name, path.display(), err
                    );
                    return Err(Terminated)
                }
            }
        }

        Ok(TlsConnector::from(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(root_certs)
                .with_no_client_auth()
        )))
    }

    /// Connects to the server.
    async fn connect(
        state: Arc<TlsState>
    ) -> Result<TlsStream<RtrTcpStream>, io::Error> {
        let stream = TcpStream::connect(&state.tls.remote).await?;
        state.connector.connect(
            state.domain.clone(),
            RtrTcpStream {
                sock: stream,
                metrics: state.metrics.clone(),
            }
        ).await
    }
}
*/


//------------ RtrClient -----------------------------------------------------

/// The transport-agnostic parts of a running RTR client.
#[derive(Debug)]
struct RtrClient<Connect> {
    /// The connect closure.
    connect: Connect,

    /// How long to wait before connecting again if the connection is closed.
    retry: u64,

    /// Our gate status.
    status: GateStatus,

    /// The unit’s metrics.
    metrics: Arc<RtrMetrics>,
}

impl<Connect> RtrClient<Connect> {
    /// Creates a new client from the connect closure and retry timeout.
    fn new(connect: Connect, retry: u64, metrics: Arc<RtrMetrics>) -> Self {
        RtrClient {
            connect,
            retry,
            status: Default::default(),
            metrics,
        }
    }
}

impl<Connect, ConnectFut, Socket> RtrClient<Connect>
where
    Connect: FnMut() -> ConnectFut,
    ConnectFut: Future<Output = Result<Socket, io::Error>>,
    Socket: AsyncRead + AsyncWrite + Unpin,
    {
        /// Runs the client.
        ///
        /// This method will only ever return if the RTR client encounters a fatal
        /// error.
        async fn run(
            name: Arc<str>,
            mut gate: Gate,
            initial_version: u8, 
            retry: u64,
            metrics: Arc<RtrMetrics>,
            connect: Connect,
        ) -> Result<(), Terminated> {
            let mut rtr_target = RtrTarget::new(name.clone());
            let mut this = Self::new(connect, retry, metrics);

            // If the rtr-in connector is configured, we want to give it a
            // couple of seconds to retrieve RPKI data from the connected RP
            // software. Ideally we signal the other components immediatly
            // after a RTR Reset (i.e., the initial sync), but that requires
            // some refactoring to not stall Rotonda entirely when the
            // connection to the RP software is never successful. For now,
            // simply wait 5 seconds.

            loop {
                let mut client = match this.connect(rtr_target, initial_version, &mut gate).await {
                    Ok(client) => client,
                    Err(res) => {
                        info!(
                            "Unit {}: Connection failed, retrying in {retry}s",
                            res.name
                        );
                        this.retry_wait(&mut gate).await?;
                        rtr_target = res;
                        continue;
                    }
                };

                loop {
                    let update = match this.update(&mut client, &mut gate).await {
                        Ok(Ok(update)) => {
                            update
                        }
                        Ok(Err(err)) => {
                            warn!(
                                "Unit {}: RTR client disconnected: {}",
                                client.target().name, err,
                            );
                            debug!(
                                "Unit {}: awaiting reconnect.",
                                client.target().name,
                            );
                            break;
                        }
                        Err(_) => {
                            debug!(
                                "Unit {}: RTR client terminated.",
                                client.target().name
                            );
                           return Err(Terminated);
                        }
                    };
                    if let Some(update) = update {
                        gate.update_data(update).await;
                    }
                }

                rtr_target = client.into_target();
                this.retry_wait(&mut gate).await?;
            }
        }

        /// Connects to the server.
        ///
        /// Upon succes, returns an RTR client that wraps the provided target.
        /// Upon failure to connect, logs the reason and returns the target for
        /// later retry.
        async fn connect(
            &mut self, target: RtrTarget, initial_version: u8, gate: &mut Gate,
        ) -> Result<Client<Socket, RtrTarget>, RtrTarget> {
            let sock = {
                let connect = (self.connect)();
                pin_mut!(connect);

                loop {
                    let process = gate.process();
                    pin_mut!(process);
                    match select(process, connect).await {
                        Either::Left((Err(_), _)) => {
                            return Err(target)
                        }
                        Either::Left((Ok(status), next_fut)) => {
                            self.status = status;
                            connect = next_fut;
                        }
                        Either::Right((res, _)) => break res
                    }
                }
            };

            let sock = match sock {
                Ok(sock) => sock,
                Err(err) => {
                    warn!(
                        "Unit {}: failed to connect to server: {}",
                        target.name, err
                    );
                    return Err(target)
                }
            };

            //let state = target.state;
            Ok(Client::with_initial_version(initial_version, sock, target, None))
        }

        /// Updates the data set from upstream.
        ///
        /// Waits until it is time to ask for an update or the server sends a
        /// notification and then asks for an update.
        ///
        /// This can fail fatally, in which case `Err(Terminated)` is returned and
        /// the client should shut down. This can also fail normally, i.e., the
        /// connections with the server fails or the server misbehaves. In this
        /// case, `Ok(Err(_))` is returned and the client should wait and try
        /// again.
        ///
        /// A successful update results in a (slightly passive-agressive)
        /// `Ok(Ok(_))`. If the client’s data set has changed, this change is
        /// returned, otherwise the fact that there are no changes is indicated
        /// via `None`.
        #[allow(clippy::needless_pass_by_ref_mut)] // false positive
        async fn update(
            &mut self, client: &mut Client<Socket, RtrTarget>, gate: &mut Gate
        ) -> Result<Result<Option<payload::Update>, io::Error>, Terminated> {
            let update_fut = async {
                let update = client.update().await?;
                let state = client.state();
                //if update.is_definitely_empty() {
                //    return Ok((state, None))
                //}
                Ok((state, Some(update)))
            };
            pin_mut!(update_fut);

            loop {
                let process = gate.process();
                pin_mut!(process);
                match select(process, update_fut).await {
                    Either::Left((Err(_), _)) => {
                        return Err(Terminated)
                    }
                    Either::Left((Ok(status), next_fut)) => {
                        self.status = status;
                        update_fut = next_fut;
                    }
                    Either::Right((res, _)) => {
                        let res = match res {
                            Ok((state, res)) => {
                                if let Some(state) = state {
                                    self.metrics.session.store(
                                        state.session().into(),
                                        atomic::Ordering::Relaxed
                                    );
                                    self.metrics.serial.store(
                                        state.serial().into(),
                                        atomic::Ordering::Relaxed
                                    );
                                    self.metrics.updated.store(
                                        Utc::now().timestamp(),
                                        atomic::Ordering::Relaxed
                                    );
                                }
                                Ok(res.map(payload::Update::Rtr))
                            }
                            Err(err) => Err(err)
                        };
                        return Ok(res)
                    }
                }
            }
        }

    /// Waits until we should retry connecting to the server.
    async fn retry_wait(
        &mut self, gate: &mut Gate
    ) -> Result<(), Terminated> {
        debug!("in retry_wait");
        let end = Instant::now() + Duration::from_secs(self.retry);

        while end > Instant::now() {
            match timeout_at(end, gate.process()).await {
                Ok(Ok(status)) => {
                    self.status = status
                }
                Ok(Err(_)) => return Err(Terminated),
                Err(_) => return Ok(()),
            }
        }

        Ok(())
    }
}


struct RtrTarget {
    cache: RtrVerbs,
    pub name: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct VrpUpdate {
    pub action: Action,
    pub vrp: RouteOrigin
}

impl Display for VrpUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.action {
            Action::Announce => { write!(f, "announce ")? }
            Action::Withdraw =>  { write!(f, "withdraw ")? }
        }
        write!(f, "VRP {} from {}", self.vrp.prefix, self.vrp.asn)
    }
}

//impl Display for RtrUpdate {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        match self.action {
//            Action::Announce => { write!(f, "announce ")? }
//            Action::Withdraw =>  { write!(f, "withdraw ")? }
//        }
//        match self.payload {
//            Payload::Origin(ref vrp) => {
//                write!(f, "VRP {} from {}", vrp.prefix, vrp.asn)
//            }
//            Payload::RouterKey(ref key) => {
//                write!(f, "BGPSec router for {}", key.asn)
//            }
//            Payload::Aspa(ref aspa) => {
//                write!(f, "ASPA for {}, providers: {:?}",
//                    aspa.customer,
//                    aspa.providers,
//                )
//            }
//        }
//        
//    }
//}

#[derive(Clone, Debug, Default)]
pub struct RtrVerbs {
    verbs: Vec<(Action, Payload)>,
}
impl IntoIterator for RtrVerbs {
    type Item = (Action, Payload);

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.verbs.into_iter()
    }
}

#[derive(Clone, Debug)]
pub enum RtrUpdate {
    Full(RtrVerbs),
    Delta(RtrVerbs),
}

impl RtrTarget {
    pub fn new(name: Arc<str>) -> Self {
        Self {
            cache: RtrVerbs::default(),
            name,
        }
    }

}

impl PayloadTarget for RtrTarget {
    type Update = RtrUpdate;

    fn start(&mut self, reset: bool) -> Self::Update {
        if reset {
            debug!("RTR reset/ full dump");
            //RtrCache::default()
            RtrUpdate::Full(Default::default())
        } else {
            // XXX this is unnecessarily expensive
            debug!("RTR delta");
            //self.cache.clone()
            RtrUpdate::Delta(self.cache.clone())
        }
    }

    fn apply(
        &mut self, _update: Self::Update, _timing: Timing
    ) -> Result<(), PayloadError> {
        todo!()
    }
}

impl PayloadUpdate for RtrUpdate {
    fn push_update(
        &mut self, action: Action, payload: Payload
    ) -> Result<(), PayloadError> {
        match self {
            RtrUpdate::Full(rtr_cache) => {
                if action == Action::Withdraw {
                    return Err(PayloadError::Corrupt);
                }
                rtr_cache.verbs.push((Action::Announce, payload));
            }
            RtrUpdate::Delta(rtr_cache) => {
                rtr_cache.verbs.push((action, payload));
            }
        }
        Ok(())
    }
}

/*
//------------ Target --------------------------------------------------------

/// The RPKI data target for the RTR client.
struct Target {
    /// The current payload set.
    current: payload::Set,

    /// The RTR client state.
    state: Option<State>,

    /// The component name.
    name: Arc<str>,
}

impl Target {
    /// Creates a new RTR target for the component with the given name.
    pub fn new(name: Arc<str>) -> Self {
        Target {
            current: Default::default(),
            state: None,
            name
        }
    }
}

impl PayloadTarget for Target {
    type Update = TargetUpdate;

    fn start(&mut self, reset: bool) -> Self::Update {
        debug!("Unit {}: starting update (reset={})", self.name, reset);
        if reset {
            TargetUpdate::Reset(payload::PackBuilder::empty())
        }
        else {
            TargetUpdate::Serial {
                set: self.current.clone(),
                diff: payload::DiffBuilder::empty(),
            }
        }
    }

    fn apply(
        &mut self,
        _update: Self::Update,
        _timing: Timing
    ) -> Result<(), PayloadError> {
        // This method is not used by the way we use the RTR client.
        unreachable!()
    }
}


//------------ TargetUpdate --------------------------------------------------

/// An update of the RPKI data set being assembled by the RTR client.
enum TargetUpdate {
    /// This is a reset query producing the complete data set.
    Reset(payload::PackBuilder),

    /// This is a serial query producing the difference to an earlier set.
    Serial {
        /// The current data set the differences are to be applied to.
        set: payload::Set,

        /// The differences as sent by the server.
        diff: payload::DiffBuilder,
    }
}

impl TargetUpdate {
    /// Returns whether there are definitely no changes in the update.
    fn is_definitely_empty(&self) -> bool {
        match *self {
            TargetUpdate::Reset(_) => false,
            TargetUpdate::Serial { ref diff, .. } => diff.is_empty()
        }
    }

    /// Converts the target update into a payload update.
    ///
    /// This will fail if the diff of a serial update doesn’t apply cleanly.
    fn into_update(self) -> Result<payload::Update, PayloadError> {
        match self {
            TargetUpdate::Reset(pack) => {
                Ok(payload::Update::new(pack.finalize().into()))
            }
            TargetUpdate::Serial { set, diff } => {
                let diff = diff.finalize();
                let set = diff.apply(&set)?;
                Ok(payload::Update::new(set))
            }
        }
    }
}

impl PayloadUpdate for TargetUpdate {
    fn push_update(
        &mut self,
        action: Action,
        payload: Payload
    ) -> Result<(), PayloadError> {
        match *self {
            TargetUpdate::Reset(ref mut pack) => {
                if action == Action::Withdraw {
                    Err(PayloadError::Corrupt)
                }
                else {
                    pack.insert(payload)
                }
            }
            TargetUpdate::Serial { ref mut diff, .. } => {
                diff.push(payload, action)
            }
        }
    }
}

*/

//------------ RtrMetrics ----------------------------------------------------

/// The metrics for an RTR client.
#[derive(Debug, Default)]
struct RtrMetrics {
    /// The gate metrics.
    gate: Arc<GateMetrics>,

    /// The session ID of the last successful update.
    ///
    /// This is actually an `Option<u16>` with the value of `u32::MAX`
    /// serving as `None`.
    session: AtomicU32,

    /// The serial number of the last successful update.
    ///
    /// This is actually an option with the value of `u32::MAX` serving as
    /// `None`.
    serial: AtomicU32,

    /// The time the last successful update finished.
    ///
    /// This is an option of the unix timestamp. The value of `i64::MIN`
    /// serves as a `None`.
    updated: AtomicI64,

    /// The number of bytes read.
    bytes_read: AtomicU64,

    /// The number of bytes written.
    bytes_written: AtomicU64,
}

impl RtrMetrics {
    fn new(gate: &Gate) -> Self {
        RtrMetrics {
            gate: gate.metrics(),
            session: u32::MAX.into(),
            serial: u32::MAX.into(),
            updated: i64::MIN.into(),
            bytes_read: 0.into(),
            bytes_written: 0.into(),
        }
    }

    fn inc_bytes_read(&self, count: u64) {
        self.bytes_read.fetch_add(count, atomic::Ordering::Relaxed);
    }

    fn inc_bytes_written(&self, count: u64) {
        self.bytes_written.fetch_add(count, atomic::Ordering::Relaxed);
    }
}

impl RtrMetrics {
    const SESSION_METRIC: Metric = Metric::new(
        "session_id", "the session ID of the last successful update",
        MetricType::Text, MetricUnit::Info
    );
    const SERIAL_METRIC: Metric = Metric::new(
        "serial", "the serial number of the last successful update",
        MetricType::Counter, MetricUnit::Total
    );
    const UPDATED_AGO_METRIC: Metric = Metric::new(
        "since_last_rtr_update",
        "the number of seconds since last successful update",
        MetricType::Counter, MetricUnit::Total
    );
    const UPDATED_METRIC: Metric = Metric::new(
        "rtr_updated", "the time of the last successful update",
        MetricType::Text, MetricUnit::Info
    );
    const BYTES_READ_METRIC: Metric = Metric::new(
        "bytes_read", "the number of bytes read",
        MetricType::Counter, MetricUnit::Total,
    );
    const BYTES_WRITTEN_METRIC: Metric = Metric::new(
        "bytes_written", "the number of bytes written",
        MetricType::Counter, MetricUnit::Total,
    );

    const ISO_DATE: &'static [chrono::format::Item<'static>] = &[
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Year, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal("-"),
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Month, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal("-"),
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Day, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal("T"),
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Hour, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal(":"),
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Minute, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal(":"),
        chrono::format::Item::Numeric(
            chrono::format::Numeric::Second, chrono::format::Pad::Zero
        ),
        chrono::format::Item::Literal("Z"),
    ];
}

impl metrics::Source for RtrMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target)  {
        self.gate.append(unit_name, target);

        let session = self.session.load(atomic::Ordering::Relaxed);
        if session != u32::MAX {
            target.append_simple(
                &Self::SESSION_METRIC, Some(unit_name), session
            );
        }

        let serial = self.serial.load(atomic::Ordering::Relaxed);
        if serial != u32::MAX {
            target.append_simple(
                &Self::SERIAL_METRIC, Some(unit_name), serial
            )
        }

        let updated = self.updated.load(atomic::Ordering::Relaxed);
        if updated != i64::MIN {
            if let Some(updated) = Utc.timestamp_opt(updated, 0).single() {
                let ago = Utc::now().signed_duration_since(updated);
                target.append_simple(
                    &Self::UPDATED_AGO_METRIC, Some(unit_name),
                    ago.num_seconds()
                );
                target.append_simple(
                    &Self::UPDATED_METRIC, Some(unit_name),
                    updated.format_with_items(Self::ISO_DATE.iter())
                );
            }
        }

        target.append_simple(
            &Self::BYTES_READ_METRIC, Some(unit_name),
            self.bytes_read.load(atomic::Ordering::Relaxed)
        );
        target.append_simple(
            &Self::BYTES_WRITTEN_METRIC, Some(unit_name),
            self.bytes_written.load(atomic::Ordering::Relaxed)
        );
    }
}


//------------ RtrTcpStream --------------------------------------------------

pin_project! {
    /// A wrapper around a TCP socket producing metrics.
    struct RtrTcpStream {
        #[pin] sock: TcpStream,

        metrics: Arc<RtrMetrics>,
    }
}

impl AsyncRead for RtrTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<Result<(), io::Error>> {
        let len = buf.filled().len();
        let res = self.as_mut().project().sock.poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = res {
            self.metrics.inc_bytes_read(
                (buf.filled().len().saturating_sub(len)) as u64
            )    
        }
        res
    }
}

impl AsyncWrite for RtrTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8]
    ) -> Poll<Result<usize, io::Error>> {
        let res = self.as_mut().project().sock.poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = res {
            self.metrics.inc_bytes_written(n as u64)
        }
        res
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), io::Error>> {
        self.as_mut().project().sock.poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), io::Error>> {
        self.as_mut().project().sock.poll_shutdown(cx)
    }
}

