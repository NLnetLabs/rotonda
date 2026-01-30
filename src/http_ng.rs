use std::{net::SocketAddr, sync::{Arc, OnceLock}};

use axum::routing::get;
#[cfg(feature = "http-api-gzip")]
use tower_http::compression::CompressionLayer;
use log::{debug, error};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{ingress::{self, http_ng::IngressApi}, units::rib_unit::rib::Rib, webui::WebUI};



#[derive(Default)]
pub struct Api {
    /// Interfaces to listen on
    interfaces: Vec<SocketAddr>,

    /// The /api/v1 or similar for this API
    api_root: String,

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
            .route("/metrics", get(Self::metrics))
            .with_state(state)
            ;


        let mut res = Self {
            api_root: "".into(),
            store: Default::default(),
            interfaces,
            ingress_register,
            metrics,
            router,
            signal_txs: vec![],
            serve_handles: vec![],
        };

        // The web-ui lives on /
        WebUI::register_routes(&mut res);

        // All other endpoints go under /api/v1
        res.api_root = "/api/v1".into();

        IngressApi::register_routes(&mut res);

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
        if self.store.set(rib).is_err() {
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
            .route(&format!("{}{}", self.api_root, path.as_ref()), get(handler))
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

            let mut app = self.router.clone().with_state(
                self.cloned_api_state()
            );

            #[cfg(feature = "http-api-gzip")]
            {
                app = app.layer(CompressionLayer::new());
            }

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
        //debug!("in Api::shutdown(), got signal");
    }

    /// Stop all listeners and start on the configured interfaces again
    pub fn restart(&mut self) {
        //debug!("in restart(), draining {} signal_txs...", self.signal_txs.len());
        for tx in self.signal_txs.drain(..) {
            //debug!("try_send for tx $x");
            let _ = tx.try_send(());
        }

        for h in self.serve_handles.drain(..) {
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(move || {
                h.is_finished();
                let _ = handle.block_on(h);
            });
        }
        self.start();
    }

}

pub enum ApiError {
    BadRequest(String),
    InternalServerError(String),
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        debug!("in into_response()");
        fn to_json(msg: String) -> String {
            debug!("in to_json() in into_response()");
            serde_json::json!({
                "data": None::<()>,
                "error": msg
            }).to_string()
        }
        (
            [("content-type", "application/json")],
            match self {
                ApiError::BadRequest(msg) => {
                    (axum::http::StatusCode::BAD_REQUEST, to_json(msg))
                }
                ApiError::InternalServerError(msg) => {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, to_json(msg))
                }
            }
        ).into_response()
    }
}
