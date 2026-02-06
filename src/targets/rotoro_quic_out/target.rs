use std::{future::Future, net::SocketAddr, ops::ControlFlow, sync::Arc};

use async_trait::async_trait;
use futures::{future::{Either, select}, pin_mut};
use log::{debug, error, info};
use non_empty_vec::NonEmpty;
use quinn::{crypto::rustls::QuicClientConfig, rustls::{self, pki_types::PrivatePkcs8KeyDer}};
use serde::Deserialize;
use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::{common::unit::UnitActivity, comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Terminated}, manager::{Component, TargetCommand, WaitPoint}, payload::Update};

#[derive(Clone, Debug, Deserialize)]
pub struct RotoroQuicOut {
    #[serde(default = "RotoroQuicOut::default_local_addr")]
    pub local_addr: SocketAddr,

    pub destination: SocketAddr,
    sources: NonEmpty<DirectLink>,
}

impl RotoroQuicOut {
    fn default_local_addr() -> SocketAddr {
         "[::]:0".parse().unwrap()
    }
    
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {

        let res = RotoroRunner::new(
            self.local_addr,
            self.destination,
        ).run(component, cmd, waitpoint, self.sources).await;

        debug!("end of RotoroQuicOut::run()");
        res
    }
}

#[derive(Clone, Debug)]
pub struct RotoroRunner {
    local_addr: SocketAddr,
    destination: SocketAddr,
}

impl RotoroRunner {
    pub fn new(
        local_addr: SocketAddr,
        destination: SocketAddr,
    ) -> Self {
        Self {
            local_addr,
            destination,
        }
    }

    async fn run(
        self,
        component: Component,
        mut cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
        mut sources: NonEmpty<DirectLink>,
    ) -> Result<(), Terminated> {


        let arc_self = Arc::new(self);
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), true).await.unwrap();
        }

        waitpoint.running().await;
        debug!("post waitpoint.running() in RotoroRunner");

        let mut roots = rustls::RootCertStore::empty();
        roots.add(rustls::pki_types::CertificateDer::from(std::fs::read("./certs/selfsigned.der").unwrap())).unwrap();

        let client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        //client_crypto.alpn_protocols = vec![b"ro-ro".into()];

        let client_config =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto).unwrap()));
        dbg!(&client_config);

        let mut endpoint = quinn::Endpoint::client(arc_self.local_addr)
            .map_err(|e| {
                error!("rotoro-out error making client: {e}");
                Terminated
            })?;
        endpoint.set_default_client_config(client_config);

        debug!("rotoro-out pre connect");
        //std::thread::sleep(std::time::Duration::from_secs(5));

        loop {
            let Ok(fut) = endpoint.connect(arc_self.destination, "localhost") else {
                error!("endpoint.connect() failed, continue loop");
                continue;
            };
            match arc_self.process_until(&mut cmd, &mut sources, fut).await {
                ControlFlow::Continue(Ok(conn)) => {
                    match arc_self.process_connection(&mut cmd, &mut sources, conn).await {
                        Ok(_) => { }
                        Err(_) => return Err(Terminated)
                    }
                }
                ControlFlow::Continue(Err(e)) => {
                    error!("connection error: {e}");
                    return Err(Terminated);
                }
                ControlFlow::Break(_) => { return Err(Terminated); }
            }
        }

    }

    async fn process_connection(
        &self,
        cmd_rx: &mut mpsc::Receiver<TargetCommand>,
        sources: &mut NonEmpty<DirectLink>,
        connection: quinn::Connection
    ) -> Result<(), Terminated> {

        let connection2 = connection.clone();
        let handle = tokio::spawn(async move {
            debug!("short stream spawner spawned");
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                let Ok(mut tx) = connection2.open_uni().await else {
                    error!("error while open_uni() in spawner");
                    break;
                };

                if tx.write_all(b"1234").await.is_err() {
                    error!("error writing to short stream");
                    continue;
                }
                if tx.finish().is_err() {
                    error!("error closing short stream");
                }
                debug!("short stream {} finished", tx.id());

            }
        });

        let mut tx = connection.open_uni().await.map_err(|e|{
            error!("error while open_uni(): {e}");
            Terminated
        })?;



        info!("Connection, side {:?}, local {:?}, remote {}, pre open_uni()", connection.side(), connection.local_ip(), connection.remote_address());

        //let mut tx = conn.open_uni().await.map_err(|e| {
        //        error!("failed to open stream: {}", e);
        //        Terminated
        //    })?;
        debug!("open_uni ok, got tx with id {}", tx.id());
        info!("Connection, side {:?}, local {:?}, remote {}, post open_uni()", connection.side(), connection.local_ip(), connection.remote_address());
        

        //let mut n = 0;
        loop {
            //n += 1;
            //if n == 5 {
            //    break Ok(());
            //}

            let _ = tx.write_all(b"abcd").await;
            let fut = tx.write_all(b"1234");

            match self.process_until(cmd_rx, sources, fut).await {
                ControlFlow::Continue(Ok(())) => {
                    //debug!("pre tx.flush");
                    let _ = tx.flush().await;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    
                    //tx.finish().unwrap();
                    //match tx.flush().await {
                    //    Ok(_) => {
                    //        debug!("flushed tx {}", tx.id());
                    //    }
                    //    Err(_) => {
                    //        error!("error while flushing tx");
                    //    }
                    //}
                },
                ControlFlow::Continue(Err(e)) => {
                    error!("failed to send request: {}", e);
                    return Err(Terminated);
                },
                ControlFlow::Break(Terminated) => {
                    info!("terminating rotoro runner");
                    break;
                }
            }
        }
        debug!("pre abort_handle().abort()");
        handle.abort_handle().abort();
        debug!("post abort_handle().abort()");
        Err(Terminated)
    }

    async fn process_until<T, U>(
        &self,
        cmd_rx: &mut mpsc::Receiver<TargetCommand>,
        sources: &mut NonEmpty<DirectLink>,
        until_fut: T,
    ) -> ControlFlow<Terminated, U>
    where
        T: Future<Output = U>,
        U: std::fmt::Debug
    {
        let mut until_fut = Box::pin(until_fut);

        loop {
            //debug!("in process_until loop");
            let process_fut = cmd_rx.recv();
            pin_mut!(process_fut);

            let res = select(process_fut, until_fut).await;
            match res {
                Either::Left((cmd, next_fut)) => {
                    until_fut = next_fut;
                    match cmd {
                        Some(TargetCommand::Reconfigure { ..}) => {
                            todo!()
                        }
                        Some(TargetCommand::ReportLinks { report }) => {
                            debug!("rotoro out reportlinks");
                            report.set_sources(sources);
                        }

                        None | Some(TargetCommand::Terminate) => {
                            //connection.disconnect().await;
                            return ControlFlow::Break(Terminated);
                        }
                    }
                }
                Either::Right((conn, _)) => {
                    //debug!("until_fut completed, got {conn:?}");
                    return ControlFlow::Continue(conn);
                }
            }
        }
    }
}

#[async_trait]
impl DirectUpdate for RotoroRunner {
    async fn direct_update(&self, _update: Update)  {
        debug!("in direct_update of RotoroRunner");
        
    }
}

impl AnyDirectUpdate for RotoroRunner { }
