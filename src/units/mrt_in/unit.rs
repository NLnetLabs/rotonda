use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use routecore::bgp::message::PduParseInfo;
use tokio::pin;
use tokio::sync::mpsc;
use futures::future::{select, Either};
use futures::{pin_mut, FutureExt};
use log::{debug, error, info, warn};
use mrtin::MrtFile;
use rand::seq::SliceRandom;
use routecore::bgp::nlri::afisafi::{Ipv4UnicastNlri, Nlri};
use routecore::bgp::types::AfiSafiType;
use routecore::bgp::workshop::route::RouteWorkshop;
use serde::Deserialize;
use smallvec::SmallVec;

use crate::common::roto_new::{Provenance, RouteContext};
use crate::common::unit::UnitActivity;
use crate::comms::{GateStatus, Terminated};
use crate::ingress::{self, IngressInfo};
use crate::manager::{Component, WaitPoint};
use crate::payload::{Payload, RotondaPaMap, RotondaRoute, Update};
use crate::units::{Gate, Unit};


#[derive(Clone, Debug, Deserialize)]
pub struct MrtIn {
    pub filename: PathBuf,
}

pub struct MrtInRunner {
    config: MrtIn,
    gate: Gate,
    ingresses: Arc<ingress::Register>,
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

        let ingresses = component.ingresses().clone();
        let _  = tx.send(self.filename.clone()).await;

        MrtInRunner::new(
            self,
            gate,
            ingresses,
            tx,
        ).run(rx).await

    }
}

impl MrtInRunner {
    fn new(
        mrtin: MrtIn,
        gate: Gate,
        ingresses: Arc<ingress::Register>,
        queue_tx: mpsc::Sender<PathBuf>
        //queue: mpsc::Receiver<PathBuf>
        )  -> Self {
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
        filename: PathBuf
    ) -> std::io::Result<()> {
        info!("processing {}", filename.to_string_lossy());
        let t0 = Instant::now();
        
        let file = std::fs::File::open(&filename)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)?  };
        let mrt_file = MrtFile::new(&mmap[..]);

        //let entire_file = std::fs::read(filename.clone()).unwrap();
        //let mrt_file = MrtFile::new(&entire_file);


        let peer_index_table = mrt_file.pi(); 
        let mut ingress_map = Vec::with_capacity(peer_index_table.len());
        for peer_entry in &peer_index_table[..] {
            let id = ingresses.register();
            ingresses.update_info(
                id, 
                IngressInfo::new()
                    .with_remote_addr(peer_entry.addr)
                    .with_remote_asn(peer_entry.asn)
                    .with_filename(filename.clone())
            );
            ingress_map.push(id);
        }

        let rib_entries = mrt_file.rib_entries().unwrap();
        let mut routes_sent = 0;

        //const BUFSIZE: usize = 256;
        //let mut buffer = Vec::with_capacity(BUFSIZE);

        for (afisafi, peer_id, peer_entry, prefix, raw_attr) in rib_entries {
            let rr = match afisafi {
                AfiSafiType::Ipv4Unicast => {
                    // tmp test: skip v4
                    //continue;
                    RotondaRoute::Ipv4Unicast(
                        prefix.try_into().unwrap(),
                        RotondaPaMap(routecore::bgp::path_attributes::OwnedPathAttributes::new(PduParseInfo::modern(), raw_attr))
                    )
                }
                AfiSafiType::Ipv4Multicast => todo!(),
                AfiSafiType::Ipv4MplsUnicast => todo!(),
                AfiSafiType::Ipv4MplsVpnUnicast => todo!(),
                AfiSafiType::Ipv4RouteTarget => todo!(),
                AfiSafiType::Ipv4FlowSpec => todo!(),
                AfiSafiType::Ipv6Unicast => {
                    // tmp test: skip v6
                    //continue;
                    RotondaRoute::Ipv6Unicast(
                        prefix.try_into().unwrap(),
                        RotondaPaMap(routecore::bgp::path_attributes::OwnedPathAttributes::new(PduParseInfo::modern(), raw_attr))
                    )
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
                ingress_map[usize::from(peer_id)],
                peer_entry.addr,
                peer_entry.asn,
            );
            let ctx = RouteContext::for_mrt_dump(provenance);
            let update = Update::Single(Payload::new(rr, ctx, None));

            gate.update_data(update).await;

            //buffer.push(update);
            //if buffer.len() == BUFSIZE {
            //    buffer.shuffle(&mut rand::thread_rng());
            //    for u in buffer.drain(..) {
            //        gate.update_data(u).await;
            //        //
            //        //if let Update::Single(ref p) = u {
            //        //    eprintln!("{}", p.rx_value);
            //        //    gate.update_data(u).await;
            //        //}
            //    }
            //}

            routes_sent += 1;
        }

        //for u in buffer.drain(..) {
        //    gate.update_data(u).await;
        //}

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
                            let gate = self.gate.clone();
                            let ingresses = self.ingresses.clone();
                            let r = self.process_until(Self::process_file(
                                    gate,
                                    ingresses,
                                    filename.clone()
                            )).await;
                            debug!("lvl2: got {r:?}");
                            match r {
                                ControlFlow::Continue(Ok(..)) => {
                                    if let Some(filename) = self.processing.take() {
                                        self.processed.push(filename)
                                    }
                                }
                                ControlFlow::Continue(Err(e)) => {
                                    error!("failed to process {}: {e}",
                                        filename.to_string_lossy()
                                    );
                                    return Err(Terminated)
                                }
                                ControlFlow::Break(_terminated) => {
                                    info!("got Termintaed in lvl2");
                                    return Err(Terminated)
                                }
                            }
                            /*
                            if let Err(e) = Self::process_file(
                                self.gate.clone(), filename.clone()
                            ).await {
                                error!("failed to process {}: {e}",
                                    filename.to_string_lossy()
                                );
                            }
                            */
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
                    debug!("self.process_until Left Terminated");
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
