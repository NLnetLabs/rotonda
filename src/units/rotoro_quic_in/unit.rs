use std::{future::Future, net::SocketAddr, ops::ControlFlow, sync::Arc};

use futures::{future::{Either, select}, pin_mut};
use log::{debug, error, info};
use quinn::rustls::{self, pki_types::PrivatePkcs8KeyDer};
use serde::Deserialize;

use crate::{common::unit::UnitActivity, comms::{Gate, GateStatus, Terminated}, manager::{Component, WaitPoint}};

#[derive(Clone, Debug, Deserialize)]
pub struct RotoroQuicIn {
    #[serde(default = "RotoroQuicIn::default_listen")]
    pub listen: SocketAddr,

}

impl RotoroQuicIn {
    fn default_listen() -> SocketAddr {
         "[::1]:9001".parse().unwrap()
    }
    
    pub async fn run(
        self,
        mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated>
    {

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        std::fs::write("./certs/selfsigned.der", &cert.cert.der()).unwrap();
        let cert = cert.cert.into();

        let Ok(server_crypto) = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key.into()) else {
                error!("failed to create ServerConfig");
                return Err(Terminated);

            };

        //server_crypto.alpn_protocols = vec![b"ro-ro".into()];

        let mut server_config =
            quinn::ServerConfig::with_crypto(
                Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap()));

        dbg!(&server_config);

        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_concurrent_uni_streams(10_u8.into());

        let Ok(endpoint) = quinn::Endpoint::server(server_config, self.listen) else {
            error!("");
            return Err(Terminated);
        };

        gate.process_until(waitpoint.ready()).await?;
        waitpoint.running().await;

        let res = RotoroRunner::new(
            gate,
            endpoint,
        ).run().await;
        debug!("end of RotoroQuicIn::run()");
        res

    }
}
pub struct RotoroRunner {
    gate: Gate,
    endpoint: quinn::Endpoint,
}

impl RotoroRunner {
    pub fn new(gate: Gate, endpoint: quinn::Endpoint) -> Self {
        Self {
            gate,
            endpoint,

        }
    }
    async fn run(self) -> Result<(), Terminated> {

        debug!("RotoroRunnerin run()");
        //let Some(incoming) = self.endpoint.accept().await
        //    else { return Err(Terminated) };

        //let mut rx = match incoming.await.unwrap().accept_uni().await {
        //    Ok(rx) => rx,
        //    Err(_) => todo!(),
        //};

        'outer: loop {
            let fut = self.endpoint.accept();
            match self.process_until(fut).await {
                //ControlFlow::Continue(Ok(v)) => {
                //    debug!("got msg: {v:?}");
                //}
                ControlFlow::Continue(Some(inc)) => {
                    debug!("New Incoming, awaiting Connection");
                    match inc.await {
                        Ok(conn) => {
                            info!("Connection, side {:?}, local {:?}, remote {}, awaiting accept_uni()", conn.side(), conn.local_ip(), conn.remote_address());
                            loop {
                                match self.process_until(conn.accept_uni()).await {
                                    ControlFlow::Continue(Ok(mut rx)) => {
                                        tokio::spawn( async move {
                                            let mut buf = vec![0u8; 1024];
                                            loop {
                                                match rx.read(&mut buf).await {
                                                    Ok(Some(len)) => {
                                                        debug!(
                                                            "received {len} bytes from {}: {:?}",
                                                            rx.id(),
                                                            &buf[..len]
                                                            );
                                                    }
                                                    Ok(None) => {
                                                        break;
                                                    }
                                                    // TODO find out, is a ReadError always fatal
                                                    // for this Connection (conn)? Or are we able
                                                    // to continue with some of the variants?
                                                    Err(quinn::ReadError::ClosedStream) => {
                                                        error!("Stream closed, breaking");
                                                        break;
                                                    }
                                                    Err(quinn::ReadError::ConnectionLost(r)) => {
                                                        error!("Connection lost: {r}");
                                                        break;
                                                    }
                                                    Err(e) => {
                                                        error!("error, but trying to continue: {e:?}");
                                                    }
                                                }
                                            }


                                        });
                                    }
                                    ControlFlow::Continue(Err(e)) => {
                                        //use quinn::ConnectionError::*;
                                        //match e {
                                        //    TransportError(error) => todo!(),
                                        //    ConnectionClosed(connection_close) => todo!(),
                                        //    ApplicationClosed(application_close) => todo!(),
                                        //    VersionMismatch => todo!(),
                                        //    Reset => todo!(),
                                        //    TimedOut => todo!(),
                                        //    LocallyClosed => todo!(),
                                        //    CidsExhausted => todo!(),
                                        //}
                                        error!("error in inner process_until: {e:?}");
                                        break;
                                    }
                                    ControlFlow::Break(_) => {
                                        error!("break in inner accept_uni loop");
                                        self.endpoint.close(0u8.into(), b"rotonda-terminated");
                                        break 'outer Err(Terminated);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("failed to connect (4): {e}"); 
                            continue;
                        }
                    }
                    //.await {
                    //    Ok(_) => { },
                    //    Err(_) => { return Err(Terminated) }
                    //}
                },
                ControlFlow::Continue(None) => {
                    info!("endpoint is closed, terminating");
                    return Err(Terminated);
                },
                ControlFlow::Break(Terminated) => {
                    info!("Break rotoro-in runner");
                    return Err(Terminated);
                }
            }
        }
    }

    async fn process_stream(&self, mut rx: quinn::RecvStream) -> Result<(), Terminated> {
        debug!("process_stream for stream {}", rx.id());
        loop {
            let mut buf = vec![0u8; 1024];
            let fut = rx.read(&mut buf);

            match self.process_until(fut).await {
                ControlFlow::Continue(Ok(Some(len))) => {
                    debug!("received {len} bytes: {:?}", &buf[..len]);
                }
                ControlFlow::Continue(Ok(None)) => {
                    error!("end of stream");
                    return Err(Terminated);
                }
                ControlFlow::Continue(Err(e)) => {
                    error!("error in stream: {e}");
                    return Err(Terminated);
                }
                ControlFlow::Break(_) => {
                    debug!("Break in process_stream");
                    return Err(Terminated);
                }
            }

        }
    }

    async fn process_until<T, U>(
        &self,
        until_fut: T,
    ) -> ControlFlow<Terminated, U>
    where
        T: Future<Output = U>
    {
        let mut until_fut = Box::pin(until_fut);

        loop {
            let process_fut = self.gate.process();
            pin_mut!(process_fut);

            let res = select(process_fut, until_fut).await;
            match res {
                Either::Left((a, next_fut)) => {
                    match a {
                        Ok(g) => {
                            match g {
                                GateStatus::Reconfiguring { new_config: _ } => todo!(),
                                GateStatus::ReportLinks { report } => {
                                    report.declare_source();
                                }
                                GateStatus::Active |
                                GateStatus::Dormant |
                                GateStatus::Triggered {..} => { }
                            }
                            until_fut = next_fut;
                        }
                        Err(Terminated) => {
                            return ControlFlow::Break(Terminated);
                        }
                    }
                }
                Either::Right((conn, _)) => {
                    return ControlFlow::Continue(conn);
                }
            }

            /*
            match (&self.status_reporter, res).into() {
                UnitActivity::GateStatusChanged(status, next_fut) => {
                    match status {
                        GateStatus::Reconfiguring {
                            new_config:
                                Unit::BmpTcpIn(BmpTcpIn {
                                    listen: new_listen,
                                    router_id_template: new_router_id_template,
                                    filter_name: new_filter_name,
                                    tracing_mode: new_tracing_mode,
                                }),
                        } => {
                            // Runtime reconfiguration of this unit has
                            // been requested. New connections will be
                            // handled using the new configuration,
                            // existing connections handled by
                            // router_handler() tasks will receive their
                            // own copy of this Reconfiguring status
                            // update and can react to it accordingly.
                            let rebind = self.listen != new_listen;

                            self.listen = new_listen;
                            self.filter_name.store(new_filter_name.into());
                            self.router_id_template
                                .store(new_router_id_template.into());
                            self.tracing_mode.store(new_tracing_mode.into());

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
                            report.set_graph_status(
                                self.bmp_in_metrics.clone(),
                            );
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
                    return ControlFlow::Break(Terminated);
                }
            }
        */
        }
    }
}
