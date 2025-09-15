use std::{net::SocketAddr, sync::{Arc, OnceLock}};

use axum::routing::get;
use log::{debug, error};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{ingress::{self, http_ng::IngressApi}, units::rib_unit::rib::Rib, webui::WebUI};



#[derive(Default)]
pub struct Api {
    /// Interfaces to listen on
    interfaces: Vec<SocketAddr>,

    /// The Rib (wrapping rotonda-store)
    store: OnceLock<Arc<Rib>>,

    /// The `ingress::Register`
    ingress_register: Arc<ingress::Register>,

    /// The metrics collection
    metrics: crate::metrics::Collection,

    /// The axum Router, populated with endpoints
    router: axum::Router<ApiState>,

    /// Sending side of channels to signal serving tasks to shutdown
    signal_txs: Vec<mpsc::Sender<()>>,

    /// Handles of serving tasks to be awaited upon shutdown/restart
    serve_handles: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ApiState {
    /// The Rib (wrapping rotonda-store) to be set by the RibUnit
    pub(crate) store: OnceLock<Arc<Rib>>,

    /// The `ingress::Register`
    pub(crate) ingress_register: Arc<ingress::Register>,
    // roto::Compiled: < >

    /// The metrics collection
    pub(crate) metrics: crate::metrics::Collection,
}



impl Api {

    /// Create a new HTTP Api based on configured interfaces and an `ingress::Register`
    ///
    /// The reference to the Rib (rotonda-store) will be unset, and is to be set by a/the RibUnit
    pub fn new(
        interfaces: Vec<SocketAddr>,
        ingress_register: Arc<ingress::Register>,
        metrics: crate::metrics::Collection,
    ) -> Self {
        let state = ApiState {
            store: Default::default(),
            ingress_register: ingress_register.clone(),
            metrics: metrics.clone(),
            
        };

        let router = axum::Router::<ApiState>::new()
            .route("/", get(|| async {"new HTTP api"}))
            .route("/metrics", get(Self::metrics))
            .with_state(state)
            ;


        let mut res = Self {
            store: Default::default(),
            interfaces,
            ingress_register,
            metrics,
            router,
            signal_txs: vec![],
            serve_handles: vec![],
        };

        IngressApi::register_routes(&mut res);
        WebUI::register_routes(&mut res);
        res
    }

    async fn metrics(state: axum::extract::State<ApiState>) -> Result<String, String> {
        Ok(state.metrics.assemble(crate::metrics::OutputFormat::Prometheus))
    }

    /// Clone an `ApiState` based on the references to the store an ingress registry
    pub fn cloned_api_state(&self) -> ApiState {
        ApiState { 
            store: self.store.clone(),
            ingress_register: self.ingress_register.clone(),
            metrics: self.metrics.clone(),
        }
    }

    /// Set the reference to the Rib (once)
    ///
    /// When this method is called while `self.store` is already set, the call is basically a
    /// no-op.
    pub fn set_rib(&mut self, rib: Arc<Rib>) {
        if let Err(_) = self.store.set(rib) {
            debug!("http_ng set_rib(): Rib already set")
        }
    }

    /// Set the interfaces
    pub fn set_interfaces(&mut self, interfaces: impl IntoIterator<Item=SocketAddr>) { //Vec<SocketAddr>) {
        self.interfaces = interfaces.into_iter().collect();
    }


    /// Add an HTTP GET endpoint
    pub fn add_get<H, T>(&mut self, path: impl AsRef<str>, handler: H)
        where
            H: axum::handler::Handler<T, ApiState>,
            T: 'static,
    {
        self.router = self.router.clone()
            .route(path.as_ref(), get(handler))
            .with_state(
                self.cloned_api_state()
            );
    }

    /// Start the HTTP API listeners on the configured interfaces
    pub fn start(&mut self) {
        self.signal_txs = vec![];
        self.serve_handles = vec![];
        for interface in self.interfaces.clone() {

            let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
            self.signal_txs.push(signal_tx);

            let app = self.router.clone().with_state(
                self.cloned_api_state()
            );

            let h = tokio::spawn(async move {
                let listener = match tokio::net::TcpListener::bind(interface).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        error!("Could not bind on {}: {}", interface, e);
                        return;
                    }
                };
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(Self::shutdown(signal_rx))
                    .await;
            });
            self.serve_handles.push(h);
        }
    }
    
    async fn shutdown(mut rx: mpsc::Receiver<()>) {
        rx.recv().await;
        debug!("in Api::shutdown(), got signal");
        ()
        
    }

    /// Stop all listeners and start on the configured interfaces again
    pub fn restart(&mut self) {
        debug!("in restart(), draining {} signal_txs...", self.signal_txs.len());
        for tx in self.signal_txs.drain(..) {
            debug!("try_send for tx $x");
            let _ = dbg!(tx.try_send(()));
        }

        debug!("pre joinhandle for loop");
        for h in self.serve_handles.drain(..) {
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(move || {
                dbg!(&h.is_finished());
                let _ = handle.block_on(h);
            });
        }
        debug!("post joinhandle for loop");
        self.start();
    }

}
