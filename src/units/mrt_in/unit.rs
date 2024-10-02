use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::time::Instant;

use tokio::pin;
use tokio::sync::mpsc;
use futures::future::{select, Either};
use futures::{pin_mut, FutureExt};
use log::{debug, error, info, warn};
use mrtin::MrtFile;
use routecore::bgp::nlri::afisafi::{Ipv4UnicastNlri, Nlri};
use routecore::bgp::types::AfiSafiType;
use routecore::bgp::workshop::route::RouteWorkshop;
use serde::Deserialize;
use smallvec::SmallVec;

use crate::common::roto_new::{Provenance, RouteContext};
use crate::common::unit::UnitActivity;
use crate::comms::{GateStatus, Terminated};
use crate::manager::{Component, WaitPoint};
use crate::payload::{Payload, RotondaRoute, Update};
use crate::units::{Gate, Unit};


#[derive(Clone, Debug, Deserialize)]
pub struct MrtIn {
    pub filename: PathBuf,
}

pub struct MrtInRunner {
    config: MrtIn,
    gate: Gate,
    // TODO register an HTTP endpoint using this sender to queue additional
    // files to process
    queue_tx: mpsc::Sender<PathBuf>,
    processing: Option<PathBuf>,
    processed: Vec<PathBuf>,
}

impl MrtIn {
    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), crate::comms::Terminated> {

        gate.process_until(waitpoint.ready()).await?;
        waitpoint.running().await;

        let (tx, rx) = mpsc::channel(10);

        let _  = tx.send(self.filename.clone()).await;

        MrtInRunner::new(
            self,
            gate,
            tx,
        ).run(rx).await

    }
}

impl MrtInRunner {
    fn new(
        mrtin: MrtIn,
        gate: Gate,
        queue_tx: mpsc::Sender<PathBuf>
        //queue: mpsc::Receiver<PathBuf>
        )  -> Self {
        Self {
            gate,
            config: mrtin,
            queue_tx,
            processing: None,
            processed: vec![],
        }
    }

    async fn process_file(gate: Gate, filename: PathBuf) -> std::io::Result<()> {
        info!("processing {}", filename.to_string_lossy());
        let t0 = Instant::now();
        let file = std::fs::File::open(&filename)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)?  };
        let mrt_file = MrtFile::new(&mmap[..]);
        let rib_entries = mrt_file.rib_entries().unwrap();
        let mut routes_sent = 0;

        for (afisafi, peer_id, peer_entry, prefix, pa_map) in rib_entries {
            let rr = match afisafi {
                AfiSafiType::Ipv4Unicast => {
                    let mut rws = RouteWorkshop::new(prefix.try_into().unwrap());
                    rws.set_attributes(pa_map);
                    RotondaRoute::Ipv4Unicast(rws)
                }
                AfiSafiType::Ipv4Multicast => todo!(),
                AfiSafiType::Ipv4MplsUnicast => todo!(),
                AfiSafiType::Ipv4MplsVpnUnicast => todo!(),
                AfiSafiType::Ipv4RouteTarget => todo!(),
                AfiSafiType::Ipv4FlowSpec => todo!(),
                AfiSafiType::Ipv6Unicast => {
                    let mut rws = RouteWorkshop::new(prefix.try_into().unwrap());
                    rws.set_attributes(pa_map);
                    RotondaRoute::Ipv6Unicast(rws)
                }
                AfiSafiType::Ipv6Multicast => todo!(),
                AfiSafiType::Ipv6MplsUnicast => todo!(),
                AfiSafiType::Ipv6MplsVpnUnicast => todo!(),
                AfiSafiType::Ipv6FlowSpec => todo!(),
                AfiSafiType::L2VpnVpls => todo!(),
                AfiSafiType::L2VpnEvpn => todo!(),
                AfiSafiType::Unsupported(_, _) => todo!(),
            };
            let provenance = Provenance::for_bgp(
                peer_id.into(),
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

    async fn run(mut self, mut queue: mpsc::Receiver<PathBuf>)
        -> Result<(), Terminated>
    {
        loop {
            let until_fut = queue.recv().then(|o| async {
                o.ok_or(std::io::Error::other("MRT queue closed"))
            });

            match self.process_until(until_fut).await {
                ControlFlow::Continue(c) => {
                    match c {
                        Ok(ref filename) => {
                            self.processing = Some(filename.clone());
                            if let Err(e) = Self::process_file(
                                self.gate.clone(), filename.clone()
                            ).await {
                                error!("failed to process {}: {e}",
                                    filename.to_string_lossy()
                                );
                            }
                            if let Some(filename) = self.processing.take() {
                                self.processed.push(filename)
                            }
                        }
                        Err(e) => error!("{e}"),
                    }
                }
                ControlFlow::Break(_) => {
                    info!("terminating unit, processed {:?}", self.processed);
                    return Err(Terminated)
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
                        GateStatus::Active | GateStatus::Dormant => { },
                        GateStatus::Reconfiguring { .. } => {
                            warn!("TODO implement hot-reload support for this unit")
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
                },
                Either::Left((Err(Terminated), _next_fut)) => {
                    return ControlFlow::Break(Terminated)
                },
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
