use std::{
    collections::{HashMap, HashSet}, future::Future, net::{IpAddr, SocketAddr}, ops::ControlFlow, path::PathBuf, sync::{Arc, Mutex}, time::Duration
};

use axum::extract::State;
use futures::{
    future::{select, Either},
    pin_mut,
};
use log::{debug, error, info, warn};
use routecore::bmp::message_ng::statistics_report::{StatType, StatValue};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use tokio::fs::File;
use tokio::net::{TcpListener, TcpStream};

use crate::{
    comms::{Gate, GateStatus, Terminated}, http_ng::ApiState, ingress::{self, IngressId, IngressInfo}, manager::{Component, WaitPoint}, roto_runtime::{MutIngressInfoCache, RotondaCtx}, units::bmp_tcp_in_ng::{error::BmpNgError, router_handler::RouterHandler}
};

use super::router_handler;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct BmpTcpIn {
    /// A colon separated IP address and port number to listen on for incoming
    /// BMP over TCP connections from routers.
    #[serde_as(as = "DisplayFromStr")]
    pub listen: SocketAddr,

    /// Roto filter name to use.
    ///
    /// If set, the filter must be found in the configured roto script.
    pub roto_filter: Option<String>,

    /// Read stream from binary file.
    pub read_from_file: Option<PathBuf>,

    /// (Convert to and) write BMPv4 messages to file as a binary stream.
    pub write_v4_to_file_bin: Option<PathBuf>,

    ///// (Convert to and) write BMPv4 messages to a .pcapng file.
    pub write_v4_to_file_pcapng: Option<PathBuf>,

    /// (Convert to and) write BMPv4 messages to an .mrt file.
    pub write_v4_to_file_mrt: Option<PathBuf>,

    /// Tmp dev: When writing to .bin/.mrt/.pcap, make it a BMP Snapshot
    pub write_snapshot: Option<bool>,

    pub write_bgp_as_mrt_in_pcapng: Option<PathBuf>,
}

impl BmpTcpIn {
    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        // TODO hook up metrics
        // do we need a metrics-ng as well?

        let roto_package = component.roto_package().clone();

        // XXX checking for the roto_filter can not happen here:
        // returning Terminated does not stop Rotonda
        // These checks need to happen earlier, somewhere in the Manager
        // supposedly.

        //let roto_filter = if let Some(ref filter_name) = self.roto_filter {
        //    let Some(package) = roto_package else {
        //        error!(
        //            "roto filter '{filter_name}' configured for unit '{}' \
        //            but 'roto_script' not set.",
        //            component.name()
        //        );
        //        return Err(Terminated);
        //    };
        //    let mut package = package.lock().unwrap();
        //    let Ok(roto_filter) = package.get_function(filter_name) else {
        //        error!("filter {filter_name} not found in roto script");
        //        return Err(Terminated);
        //    };
        //    Some(roto_filter)
        //} else {
        //    None
        //};

        let roto_filter = if let Some(ref filter_name) = self.roto_filter {
            if let Some(package) = roto_package {
                let mut package = package.lock().unwrap();
                if let Ok(roto_filter) = package.get_function(filter_name) {
                    Some(roto_filter)
                } else {
                    error!("filter {filter_name} not found in roto script");
                    None
                }
            } else {
                error!(
                    "roto filter '{filter_name}' configured for unit '{}' \
                    but 'roto_script' not set.",
                    component.name()
                );
                //return Err(Terminated);
                None
            }
        } else {
            None
        };

        let ingress_register = component.ingresses();

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

        let stats_db;

        if let Ok(mut api) = component.http_ng_api_arc().lock() {
            api.add_get("/bmp_stats_reports_metrics", serve_stats_as_metrics);
            stats_db = api.get_stats_store();

        } else {
            debug!("could not get lock on HTTP API");
            stats_db = StatsStore::default();
        }

        let _ = Runner::new(self.clone(), gate, roto_filter, ingress_register, stats_db)
            .run()
            .await;

        Ok(())
    }
}

type RotoFilter = roto::TypedFunc<
    roto::Ctx<RotondaCtx>,
    fn(
        roto::Val<routecore::bmp::message::Message<bytes::Bytes>>, // TODO this will be the new BmpMessage
        roto::Val<MutIngressInfoCache>,
    ) -> roto::Verdict<(), ()>,
>;


async fn serve_stats_as_metrics(state: State<ApiState> ) -> Result<String, crate::http_ng::ApiError> {
    use routecore::bmp::message_ng::statistics_report::Stat::{CounterStat, GaugeStat, AfiSafiGaugeStat};
    use std::fmt::Write;

    const METRIC_PREFIX: &str = "bmp_stats_report_";

    let mut res = String::new();
    let mut type_lines_printed = HashSet::<routecore::bmp::message_ng::statistics_report::StatType>::new();

    macro_rules! write_type_help_once(
        (&mut $res:ident, $stat_type:expr, $metric_type:literal) => {
            if !type_lines_printed.contains(&$stat_type) {
                let suffix = if $metric_type == "counter" {
                    "_total"
                } else {
                    ""
                };
                let _ = writeln!(&mut $res, "# HELP {METRIC_PREFIX}{}{suffix} BMP Statistics Report type {}", $stat_type, u16::from($stat_type));
                let _ = writeln!(&mut $res, "# TYPE {METRIC_PREFIX}{}{suffix} {}", $stat_type, $metric_type);
                type_lines_printed.insert($stat_type);
            }
        }

    );

    let db = state.stats_db.lock().unwrap();
    for (session, StatsValue { timestamp, raw_stats: blob}) in &*db {
        let timestamp = timestamp.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis();
        let iter = routecore::bmp::message_ng::statistics_report::StatIterator::for_slice(blob);

        // labels lacks the closing curly brace
        // we add this at 'print' time, so we can still add afisafi= for
        // the AfiSafiGaugeStat.
        // TODO include bmp-ingress-unit-name
        let labels = format_args!("{{\
                bmp_router_name=\"{}\",\
                bmp_router_addr=\"{}\",\
                asn=\"{}\",\
                address=\"{}\",\
                ribview=\"{}\",\
                distinguisher=\"{:?}\"\
                ", // no closing curly brace!
                session.bmp_router_name,
                session.bmp_router_addr,
                session.asn,
                session.address,
                session.rib_view,
                session.distinguisher,
        );

        for stat in iter {
            let Ok(stat) = stat.into_specific() else {
                debug!("unknown stat type");
                continue
            };
            match stat {
                CounterStat(counter_stat) => {
                    write_type_help_once!(&mut res, counter_stat.stat_type, "counter");
                    let _ = writeln!(&mut res, "{METRIC_PREFIX}{}_total{labels}}} {} {}", counter_stat.stat_type, StatValue::value(counter_stat), timestamp);
                }
                GaugeStat(gauge_stat) => {
                    write_type_help_once!(&mut res, gauge_stat.stat_type, "gauge");
                    let _ = writeln!(&mut res, "{METRIC_PREFIX}{}{labels}}} {} {}", gauge_stat.stat_type, StatValue::value(gauge_stat), timestamp);
                }
                AfiSafiGaugeStat(afi_safi_gauge_stat) => {
                    write_type_help_once!(&mut res, afi_safi_gauge_stat.stat_type, "gauge");
                    let (afisafi, gauge) = StatValue::value(afi_safi_gauge_stat);
                    let _ = writeln!(&mut res, "{METRIC_PREFIX}{}{labels},afisafi=\"{afisafi}\"}} {} {}", afi_safi_gauge_stat.stat_type, gauge, timestamp);
                }
            }
        }
    }


    Ok(res)

}

// Types to store BMP Stats Reports.
// This should move into routedb eventually.
#[derive(Eq, Hash, PartialEq)]
pub struct StatsKey {
    pub bmp_router_name: String,
    pub bmp_router_addr: IpAddr,
    pub rib_view: routecore::bmp::message_ng::common::RibViewType,
    pub distinguisher: [u8; 8],
    pub address: IpAddr,
    pub asn: routecore::bmp::message_ng::common::Asn,
    pub bgp_id: [u8; 4],
}

pub struct StatsValue {
    pub timestamp: std::time::SystemTime,
    pub raw_stats: Vec<u8>,
}

pub(crate) type StatsStore = Arc<Mutex<HashMap::<StatsKey, StatsValue>>>;


struct Runner {
    config: BmpTcpIn,
    gate: Gate,
    roto_filter: Option<RotoFilter>,
    ingress_register: Arc<ingress::Register>,
    unit_ingress_id: IngressId,
    stats_db: StatsStore,
}

impl Runner {
    fn new(
        config: BmpTcpIn,
        gate: Gate,
        roto_filter: Option<RotoFilter>,
        ingress_register: Arc<ingress::Register>,
        stats_db: StatsStore,
    ) -> Self {
        let unit_ingress_id = ingress_register.register();
        debug!("Runner registered {unit_ingress_id}");

        Self {
            config,
            gate,
            roto_filter,
            ingress_register,
            unit_ingress_id,
            stats_db,
        }
    }

    async fn run(mut self) -> Result<(), Terminated> {
        // depending on whether Config.read_from_file is_some, read from that
        // file or spawn a socket

        if let Some(filename) = self.config.read_from_file.clone() {
            self.spawn_file_handler(filename);
        }

        loop {
            let f = bind_with_backoff(self.config.listen);
            let listener = match self.process_until(f).await {
                ControlFlow::Continue(Ok(res)) => res,
                ControlFlow::Continue(Err(_err)) => continue,
                ControlFlow::Break(Terminated) => return Err(Terminated),
            };

            loop {
                let connection = self.process_until(listener.accept()).await;
                match connection {
                    ControlFlow::Continue(Ok((stream, socket))) => {
                        self.spawn_stream_handler(stream, socket);
                    }
                    ControlFlow::Continue(Err(e)) => {
                        error!("BmpTcpIngNg Runner inner: {e}");
                        //break; // XXX should we break?
                    }
                    ControlFlow::Break(Terminated) => return Err(Terminated),
                }
            }
        }
    }

    fn spawn_stream_handler(&mut self, stream: TcpStream, socket: SocketAddr) {
        let gate = self.gate.clone();
        let ingress_register = self.ingress_register.clone();
        let unit_ingress_id = self.unit_ingress_id;
        let config = self.config.clone();
        let partial_ingress_info =
            IngressInfo::new().with_remote_addr(socket.ip());

        let stats_db = self.stats_db.clone();
        
        tokio::spawn(async move {
            info!(
                "spawning handler for tcp stream from {:?}",
                stream.peer_addr()
            );
            let handler = RouterHandler::new(
                stream,
                gate,
                ingress_register,
                unit_ingress_id,
                config,
                stats_db,
            );
            let _ = Box::pin(handler.run(partial_ingress_info)).await;
        });
    }

    fn spawn_file_handler(&mut self, filename: PathBuf) {
        let gate = self.gate.clone();
        let ingress_register = self.ingress_register.clone();
        let unit_ingress_id = self.unit_ingress_id;
        let config = self.config.clone();
        let partial_ingress_info =
            IngressInfo::new().with_filename(filename.clone());

        let stats_db = self.stats_db.clone();

        tokio::spawn(async move {
            let name = filename.to_string_lossy().to_string();
            info!("spawning handler for file {name}");
            let t0 = std::time::Instant::now();
            let Ok(handle) = File::open(filename).await else {
                error!("can't open file");
                return;
            };
            let filesize = handle.metadata().await.unwrap().len();
            let handler = RouterHandler::new(
                handle,
                gate,
                ingress_register,
                unit_ingress_id,
                config,
                stats_db,
            );
            if let Err(e) = Box::pin(handler.run(partial_ingress_info)).await {
                error!("error while handling {name}: {e}");
            }
            eprintln!(
                "processed {name} in {}ms, {:.1}MB/s",
                //filename.to_string_lossy(),
                t0.elapsed().as_millis(),
                (filesize >> 20) as f64 / t0.elapsed().as_secs_f64()
            );
        });
    }

    fn process_gate_status(&self, gate_status: GateStatus) {
        match gate_status {
            GateStatus::Reconfiguring { new_config: _ } => {
                todo!()
            }
            GateStatus::ReportLinks { report } => {
                report.declare_source();
            }
            GateStatus::Active
            | GateStatus::Dormant
            | GateStatus::Triggered { data: _ } => {}
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

            match res {
                Either::Left((p, next_fut)) => {
                    match p {
                        Ok(gate_status) => {
                            self.process_gate_status(gate_status);
                        }
                        Err(Terminated) => {
                            return ControlFlow::Break(Terminated)
                        }
                    }
                    until_fut = next_fut;
                }
                Either::Right((o, _next_fut)) => match o {
                    Ok(r) => return ControlFlow::Continue(Ok(r)),
                    Err(e) => return ControlFlow::Continue(Err(e)),
                },
            }
        }
    }
}

async fn bind_with_backoff<E>(listen: SocketAddr) -> Result<TcpListener, E> {
    let mut wait = 1;
    loop {
        match TcpListener::bind(listen).await {
            Ok(listener) => return Ok(listener),
            Err(e) => error!("{e}: retry in {wait} seconds"),
        }
        tokio::time::sleep(Duration::from_secs(wait)).await;
        if wait < 16 {
            wait *= 2;
        }
    }
}
