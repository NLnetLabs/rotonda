use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use super::{
    metrics::BmpProxyMetrics, proxy::BmpProxy,
    status_reporter::BmpProxyStatusReporter,
};

use crate::{
    common::{
        frim::FrimMap,
        status_reporter::{
            AnyStatusReporter, Chainable, TargetStatusReporter,
        },
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Payload, SourceId, Update, UpstreamStatus},
};

use async_trait::async_trait;
use non_empty_vec::NonEmpty;
use roto::types::{
    builtin::BuiltinTypeValue, collections::BytesRecord, typevalue::TypeValue,
};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Deserialize)]
pub struct BmpTcpOut {
    /// The set of units to receive messages from.
    sources: NonEmpty<DirectLink>,

    #[serde(flatten)]
    config: Config,
}

#[derive(Clone, Debug, Deserialize)]
struct Config {
    /// A colon separated IP address and port number to proxy incoming BMP
    /// over TCP connections to.
    /// On change: should we terminate current proxy tasks?
    pub destination: Arc<String>,

    /// Router IPs to proxy BMP messages for. If set then ONLY these router
    /// IPs will be proxied for, otherwise all non-excluded router IPs will
    /// be proxied for.
    /// On change: should we terminate or somehow update current proxy tasks?
    #[serde(default)]
    pub accept: Arc<Vec<IpAddr>>,

    /// Router IPs to NOT proxy BMP messages for.
    /// On change: should we terminate or somehow update current proxy tasks?
    #[serde(default)]
    pub reject: Arc<Vec<IpAddr>>,
}

impl BmpTcpOut {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        BmpTcpOutRunner::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

struct BmpTcpOutRunner {
    component: Component,
    config: Arc<Config>,
    proxies: Arc<FrimMap<SourceId, Arc<RwLock<BmpProxy>>>>,
    status_reporter: Arc<BmpProxyStatusReporter>,
}

impl BmpTcpOutRunner {
    fn new(config: Config, mut component: Component) -> Self {
        let config = Arc::new(config);

        let proxies = Arc::new(FrimMap::default());

        let metrics = Arc::new(BmpProxyMetrics::new());
        component.register_metrics(metrics.clone());

        let status_reporter =
            Arc::new(BmpProxyStatusReporter::new(component.name(), metrics));

        Self {
            component,
            config,
            proxies,
            status_reporter,
        }
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let _unit_name = component.name().clone();

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        while let Some(cmd) = cmd_rx.recv().await {
            arc_self.status_reporter.command_received(&cmd);
            match cmd {
                TargetCommand::Reconfigure { new_config } => {
                    if let crate::targets::Target::BmpTcpOut(BmpTcpOut {
                        sources: new_sources,
                        ..
                        // config
                    }) = new_config
                    {
                        // Register as a direct update receiver with the new
                        // set of linked gates.
                        arc_self
                            .status_reporter
                            .upstream_sources_changed(sources.len(), new_sources.len());
                        sources = new_sources;
                        for link in sources.iter_mut() {
                            link.connect(arc_self.clone(), false).await.unwrap();
                        }
                    }
                }

                TargetCommand::ReportLinks { report } => {
                    report.set_sources(&sources);
                }

                TargetCommand::Terminate => break,
            }
        }

        // Terminate any running proxies
        for (_socket_addr, proxy) in arc_self.proxies.guard().iter() {
            proxy.write().await.terminate().await;
        }

        Err(Terminated)
    }

    fn spawn_proxy(&self, router_addr: SocketAddr) -> BmpProxy {
        let child_name = format!(
            "proxy_handler[{}:{}]",
            router_addr.ip(),
            router_addr.port()
        );
        let proxy_status_reporter =
            Arc::new(self.status_reporter.add_child(child_name));

        let accepted = self.config.accept.is_empty()
            || self.config.accept.contains(&router_addr.ip());

        let rejected = self.config.reject.contains(&router_addr.ip());

        if accepted && !rejected {
            BmpProxy::new(
                Some(self.config.destination.clone()),
                proxy_status_reporter,
                router_addr,
            )
        } else {
            self.status_reporter.proxy_router_excluded(
                &router_addr,
                accepted,
                rejected,
            );
            BmpProxy::new(None, proxy_status_reporter, router_addr)
        }
    }
}

#[async_trait]
impl DirectUpdate for BmpTcpOutRunner {
    async fn direct_update(&self, update: Update) {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                source_id,
            }) => {
                // The connection to the router has been lost so drop the connection state machine we have for it, if
                // any.
                self.proxies.remove(&source_id);
            }

            Update::Single(Payload {
                source_id,
                value: TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bytes)),
                ..
            }) => {
                // Dispatch the message to the proxy for this router. Create the proxy if it doesn't exist yet.
                if let Some(addr) = source_id.socket_addr() {
                    let bytes = BytesRecord::as_ref(&bytes);

                    let proxy =
                        self.proxies.entry(source_id.clone()).or_insert_with(
                            || Arc::new(RwLock::new(self.spawn_proxy(*addr))),
                        );
                    proxy.write().await.send(bytes).await;
                }
            }

            _ => {
                self.status_reporter
                    .input_mismatch("Update::Single(BmpMessage)", update);
            }
        }
    }
}

impl AnyDirectUpdate for BmpTcpOutRunner {}

impl std::fmt::Debug for BmpTcpOutRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BmpTcpOutRunner").finish()
    }
}
