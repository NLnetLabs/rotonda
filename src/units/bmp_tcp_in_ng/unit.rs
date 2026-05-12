use std::{
    future::Future, net::SocketAddr, ops::ControlFlow, path::PathBuf,
    time::Duration,
};

use futures::{
    future::{select, Either},
    pin_mut,
};
use log::{error, info};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use tokio::net::{TcpListener, TcpStream};

use crate::{
    comms::{Gate, GateStatus, Terminated},
    manager::{Component, WaitPoint},
};

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
    pub roto_filter_name: Option<String>,

    /// Read stream from binary file
    pub read_from_file: Option<PathBuf>,
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

        let _ = Runner::new(self.clone(), gate).run().await;

        Ok(())
    }
}

struct Runner {
    config: BmpTcpIn,
    gate: Gate,
}

impl Runner {
    fn new(config: BmpTcpIn, gate: Gate) -> Self {
        Self { config, gate }
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
        tokio::spawn(async move {
            info!(
                "handler spawn for tcp stream from {:?}",
                stream.peer_addr()
            );
            todo!()
        });
    }
    fn spawn_file_handler(&mut self, filename: PathBuf) {
        tokio::spawn(async move {
            info!("handler spawned for file {}", filename.to_string_lossy());
            todo!()
        });
    }

    fn process_gate_status(&self, gate_status: GateStatus) {
        match gate_status {
            GateStatus::Active => todo!(),
            GateStatus::Dormant => todo!(),
            GateStatus::Reconfiguring { new_config: _ } => {
                todo!()
            }
            GateStatus::ReportLinks { report } => {
                report.declare_source();
            }
            GateStatus::Triggered { data: _ } => todo!(),
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
