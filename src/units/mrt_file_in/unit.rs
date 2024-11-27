use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::io::Read;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use flate2::read::GzDecoder;
use futures::future::{select, Either};
use futures::{pin_mut, FutureExt, TryFutureExt};
use log::{debug, error, info, warn};
use rand::seq::SliceRandom;
use routecore::bgp::message::PduParseInfo;
use routecore::bgp::nlri::afisafi::{Ipv4UnicastNlri, Nlri};
use routecore::bgp::types::AfiSafiType;
use routecore::bgp::workshop::route::RouteWorkshop;
use routecore::bgp::ParseError;
use routecore::mrt::MrtFile;
use serde::Deserialize;
use smallvec::SmallVec;
use tokio::pin;
use tokio::sync::mpsc;

use crate::roto_runtime::types::{Provenance, RouteContext};
use crate::common::unit::UnitActivity;
use crate::comms::{GateStatus, Terminated};
use crate::ingress::{self, IngressInfo};
use crate::manager::{Component, WaitPoint};
use crate::payload::{Payload, RotondaPaMap, RotondaRoute, Update};
use crate::units::{Gate, Unit};

#[derive(Clone, Debug, Deserialize)]
pub struct MrtFileIn {
    pub filename: PathBuf,
}

pub struct MrtInRunner {
    config: MrtFileIn,
    gate: Gate,
    ingresses: Arc<ingress::Register>,
    // TODO register an HTTP endpoint using this sender to queue additional
    // files to process
    queue_tx: mpsc::Sender<PathBuf>,
    processing: Option<PathBuf>,
    processed: Vec<PathBuf>,
}

impl MrtFileIn {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated> {
        gate.process_until(waitpoint.ready()).await?;
        waitpoint.running().await;

        let (tx, rx) = mpsc::channel(10);

        let ingresses = component.ingresses().clone();
        let _ = tx.send(self.filename.clone()).await;

        MrtInRunner::new(self, gate, ingresses, tx).run(rx).await
    }
}

impl MrtInRunner {
    fn new(
        mrtin: MrtFileIn,
        gate: Gate,
        ingresses: Arc<ingress::Register>,
        queue_tx: mpsc::Sender<PathBuf>,
    ) -> Self {
        Self {
            gate,
            config: mrtin,
            ingresses,
            queue_tx,
            processing: None,
            processed: vec![],
        }
    }

    async fn process_file(
        gate: Gate,
        ingresses: Arc<ingress::Register>,
        filename: PathBuf,
    ) -> Result<(), MrtError> {
        info!("processing {}", filename.to_string_lossy());
        #[allow(unused_variables)] // false positive, used in info!() below)
        let t0 = Instant::now();

        let file = std::fs::File::open(&filename)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let mut buf = Vec::<u8>::new();

        let t0 = Instant::now();
        let mrt_file = match filename.as_path().extension()
            .and_then(std::ffi::OsStr::to_str)
        {
            Some("gz") => {
                let mut gz = GzDecoder::new(&mmap[..]);
                gz.read_to_end(&mut buf)
                    .map_err(|_e| MrtError::other("gz decoding failed"))?;
                info!("decompressed {} in {}ms",
                    &filename.to_string_lossy(),
                    t0.elapsed().as_millis());
                MrtFile::new(&buf[..])
            }
            _ => {
                MrtFile::new(&mmap[..])
            }
        };

        let peer_index_table = mrt_file.pi()?;
        let mut ingress_map = Vec::with_capacity(peer_index_table.len());
        for peer_entry in &peer_index_table[..] {
            let id = ingresses.register();
            ingresses.update_info(
                id,
                IngressInfo::new()
                    .with_remote_addr(peer_entry.addr)
                    .with_remote_asn(peer_entry.asn)
                    .with_filename(filename.clone()),
            );
            ingress_map.push(id);
        }

        let rib_entries = mrt_file.rib_entries()?;

        let mut routes_sent = 0;

        for (afisafi, peer_id, peer_entry, prefix, raw_attr) in rib_entries {
            let rr = match afisafi {
                AfiSafiType::Ipv4Unicast => {
                    RotondaRoute::Ipv4Unicast(
                        prefix.try_into().map_err(MrtError::other)?,
                        RotondaPaMap(routecore::bgp::path_attributes::OwnedPathAttributes::new(PduParseInfo::modern(), raw_attr))
                    )
                }
                AfiSafiType::Ipv6Unicast => {
                    RotondaRoute::Ipv6Unicast(
                        prefix.try_into().map_err(MrtError::other)?,
                        RotondaPaMap(routecore::bgp::path_attributes::OwnedPathAttributes::new(PduParseInfo::modern(), raw_attr))
                    )
                }
                AfiSafiType::Ipv4Multicast |
                AfiSafiType::Ipv4MplsUnicast |
                AfiSafiType::Ipv4MplsVpnUnicast |
                AfiSafiType::Ipv4RouteTarget |
                AfiSafiType::Ipv4FlowSpec |
                AfiSafiType::Ipv6Multicast |
                AfiSafiType::Ipv6MplsUnicast |
                AfiSafiType::Ipv6MplsVpnUnicast |
                AfiSafiType::Ipv6FlowSpec |
                AfiSafiType::L2VpnVpls |
                AfiSafiType::L2VpnEvpn |
                AfiSafiType::Unsupported(_, _) => {
                    debug!("unsupported AFI/SAFI {}, skipping", afisafi);
                    continue
                }
            };
            let provenance = Provenance::for_bgp(
                ingress_map[usize::from(peer_id)],
                peer_entry.addr,
                peer_entry.asn,
            );
            let ctx = RouteContext::for_mrt_dump(provenance);
            let update = Update::Single(Payload::new(rr, ctx, None));

            gate.update_data(update).await;
            routes_sent += 1;
        }

        info!(
            "mrt-in: done processing {}, emitted {} routes in {}s",
            filename.to_string_lossy(),
            routes_sent,
            t0.elapsed().as_secs()
        );

        Ok(())
    }

    async fn run(
        mut self,
        mut queue: mpsc::Receiver<PathBuf>,
    ) -> Result<(), Terminated> {
        loop {
            let until_fut = queue.recv().then(|o| async {
                o.ok_or(std::io::Error::other("MRT queue closed"))
            });

            match self.process_until(until_fut).await {
                ControlFlow::Continue(c) => match c {
                    Ok(ref filename) => {
                        self.processing = Some(filename.clone());
                        let gate = self.gate.clone();
                        let ingresses = self.ingresses.clone();
                        let r = self
                            .process_until(
                                Self::process_file(
                                    gate,
                                    ingresses,
                                    filename.clone(),
                                )
                                .map_err(Into::into),
                            )
                            .await;
                        match r {
                            ControlFlow::Continue(Ok(..)) => {
                                if let Some(filename) = self.processing.take()
                                {
                                    self.processed.push(filename)
                                }
                            }
                            ControlFlow::Continue(Err(e)) => {
                                error!(
                                    "failed to process {}: {e}",
                                    filename.to_string_lossy()
                                );
                            }
                            ControlFlow::Break(_terminated) => {
                                debug!("mrt-in got Terminated");
                                return Err(Terminated);
                            }
                        }
                    }
                    Err(e) => error!("{e}"),
                },
                ControlFlow::Break(_) => {
                    info!("terminating unit, processed {:?}", self.processed);
                    return Err(Terminated);
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

            match res {
                Either::Left((Ok(gate_status), next_fut)) => {
                    match gate_status {
                        GateStatus::Active | GateStatus::Dormant => {}
                        GateStatus::Reconfiguring {
                            new_config:
                                Unit::MrtFileIn(MrtFileIn {
                                    filename: new_filename,
                                    ..
                                }),
                        } => {
                            if new_filename != self.config.filename {
                                info!("Reloading mrt-in, processing new file {}", &new_filename.to_string_lossy());
                                if let Err(e) = self
                                    .queue_tx
                                    .send(new_filename.clone())
                                    .await
                                {
                                    error!(
                                        "Failed to process {}: {}",
                                        new_filename.to_string_lossy(),
                                        e
                                    );
                                }
                                //self.config.file
                            }
                        }
                        GateStatus::Reconfiguring { .. } => {
                            // reconfiguring for other unit types, ignore
                        }
                        GateStatus::ReportLinks { report } => {
                            report.declare_source();
                            //report.set_graph_status(self.metrics.clone());
                        }
                        GateStatus::Triggered { .. } => {
                            warn!("got unexpected Triggered for this unit");
                        }
                    }
                    until_fut = next_fut;
                }
                Either::Left((Err(Terminated), _next_fut)) => {
                    debug!("self.process_until Left Terminated");
                    return ControlFlow::Break(Terminated);
                }
                Either::Right((Ok(until_res), _next_fut)) => {
                    return ControlFlow::Continue(Ok(until_res))
                }
                Either::Right((Err(err), _next_fut)) => {
                    return ControlFlow::Continue(Err(err))
                }
            }
        }
    }
}

#[derive(Debug)]
enum MrtErrorType {
    Io(std::io::Error),
    Parse(ParseError),
    Other(&'static str),
}
#[derive(Debug)]
pub struct MrtError(MrtErrorType);

impl MrtError {
    fn other(s: &'static str) -> Self {
        Self(MrtErrorType::Other(s))
    }
}

impl std::error::Error for MrtError {}
impl std::fmt::Display for MrtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            MrtErrorType::Io(e) => write!(f, "io error: {}", e),
            MrtErrorType::Parse(e) => write!(f, "parse error: {}", e),
            MrtErrorType::Other(e) => write!(f, "error: {}", e),
        }
    }
}

impl From<ParseError> for MrtError {
    fn from(e: ParseError) -> Self {
        Self(MrtErrorType::Parse(e))
    }
}

impl From<std::io::Error> for MrtError {
    fn from(e: std::io::Error) -> Self {
        Self(MrtErrorType::Io(e))
    }
}
impl From<MrtError> for std::io::Error {
    fn from(e: MrtError) -> Self {
        std::io::Error::other(e.to_string())
    }
}
