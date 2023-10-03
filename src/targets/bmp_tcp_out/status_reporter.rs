use std::{
    fmt::Display,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};

use log::{debug, info, trace, warn};

use crate::common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, TargetStatusReporter,
};

use super::{metrics::BmpProxyMetrics, proxy::ProxyState};

#[derive(Debug, Default)]
pub struct BmpProxyStatusReporter {
    name: String,
    metrics: Arc<BmpProxyMetrics>,
}

impl BmpProxyStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BmpProxyMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    /// Ensure only when necessary that metric counters for a new (or changed)
    /// router address are initialised.
    ///
    /// This must be called prior to calling any other member function that
    /// takes a router address as input, otherwise metrics will not be updated.
    pub fn init_per_proxy_metrics(&self, router_addr: SocketAddr, queue_capacity: usize) {
        self.metrics
            .init_metrics_for_proxy(router_addr, queue_capacity);
    }

    pub fn proxy_router_excluded(&self, router_addr: &SocketAddr, accepted: bool, rejected: bool) {
        sr_log!(
            warn: self,
            "Proxying denied for router {} (accepted={}, rejected={})",
            router_addr,
            accepted,
            rejected
        );
    }

    pub fn proxy_handler_started(&self) {
        sr_log!(debug: self, "Proxy handler: started");
    }

    pub fn proxy_handler_connecting(&self) {
        sr_log!(debug: self, "Proxy handler: connecting");
    }

    pub fn proxy_handler_connected(&self) {
        sr_log!(debug: self, "Proxy handler: connected");
    }

    pub fn proxy_handler_io_error(&self, err: std::io::Error) {
        sr_log!(debug: self, "Proxy handler: I/O error: {}", err);
    }

    pub fn proxy_handler_sender_gone(&self) {
        sr_log!(debug: self, "Proxy handler: disconnected from sender");
    }

    pub fn proxy_handler_terminated(&self, router_addr: SocketAddr) {
        sr_log!(debug: self, "Proxy handler: terminated");
        self.metrics
            .proxy_metrics(router_addr)
            .proxy_state
            .store(ProxyState::None, Ordering::SeqCst);
    }

    fn update_proxy_state_metrics(&self, router_addr: SocketAddr, new_state: ProxyState) {
        let metrics = self.metrics.proxy_metrics(router_addr);
        metrics.proxy_state.store(new_state, Ordering::SeqCst);
        metrics
            .proxy_handler_state
            .store(new_state.as_handler_state(), Ordering::SeqCst);
    }

    pub fn proxy_queue_started(&self, router_addr: SocketAddr) {
        sr_log!(info: self, "Proxy queue: started");
        self.update_proxy_state_metrics(router_addr, ProxyState::Flowing);
    }

    pub fn proxy_queue_ok(&self, router_addr: SocketAddr) {
        sr_log!(info: self, "Proxy queue: recovered");
        self.update_proxy_state_metrics(router_addr, ProxyState::Recovered);
    }

    pub fn proxy_queue_error<T: Display>(&self, router_addr: SocketAddr, err: T) {
        sr_log!(warn: self, "Proxy queue: stalled: {}", err);
        self.update_proxy_state_metrics(router_addr, ProxyState::Stalled);
    }

    pub fn proxy_message_undeliverable(&self, router_addr: SocketAddr) {
        sr_log!(trace: self, "Proxy queue: unable to queue message");
        self.metrics
            .proxy_metrics(router_addr)
            .num_undeliverable_messages
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn proxy_terminate(&self, router_addr: SocketAddr) {
        sr_log!(info: self, "Proxy: terminating");
        self.metrics.remove_metrics_for_proxy(router_addr);
    }

    pub fn proxy_queue_capacity(&self, router_addr: SocketAddr, capacity: usize) {
        self.metrics
            .proxy_metrics(router_addr)
            .queue_capacity
            .store(capacity, Ordering::SeqCst);
    }
}

impl TargetStatusReporter for BmpProxyStatusReporter {}

impl AnyStatusReporter for BmpProxyStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for BmpProxyStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpProxyStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
