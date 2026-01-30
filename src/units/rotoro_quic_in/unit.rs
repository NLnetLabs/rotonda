use std::{future::Future, net::SocketAddr, ops::ControlFlow, sync::Arc};

use futures::{future::{Either, select}, pin_mut};
use log::{error, info};
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

        self, mut component: Component,
        gate: Gate,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated>
    {

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let cert = cert.cert.into();

        let Ok(mut server_crypto) = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key.into()) else {
                error!("");
                return Err(Terminated);

            };

        //server_crypto.alpn_protocols = vec![b"ro-ro".into()];

        let mut server_config =
            quinn::ServerConfig::with_crypto(
                Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap()));

        //let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        //transport_config.max_concurrent_uni_streams(0_u8.into());

        let Ok(endpoint) = quinn::Endpoint::server(server_config, self.listen) else {
            error!("");
            return Err(Terminated);
        };

        gate.process_until(waitpoint.ready()).await?;
        waitpoint.running().await;

        RotoroRunner::new(
            gate,
            endpoint,
        ).run().await
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

        loop {
            let fut = self.endpoint.accept();
            match self.process_until(fut).await {
                ControlFlow::Continue(Some(conn)) => {
                    info!("New connection: {conn:#?}");
                    TODO start consuming the stream
                    perhaps already create a rotoro_quic_out target?

                    let fut = handle_connection(root.clone(), conn);
                    tokio::spawn(async move {
                        if let Err(e) = fut.await {
                            error!("connection failed: {reason}", reason = e.to_string())
                        }
                    });
                },
                ControlFlow::Continue(None) => {
                    info!("endpoint returned None for some reason");
                },
                ControlFlow::Break(Terminated) => {
                    info!("terminating rotoro runner");
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
