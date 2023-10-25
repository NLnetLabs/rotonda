use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    common::frim::FrimMap,
    metrics::{
        self, util::append_per_router_metric, Metric, MetricType, MetricUnit,
    },
};

use super::proxy::AtomicProxyState;

#[derive(Debug, Default)]
pub struct BmpProxyMetrics {
    proxies: Arc<FrimMap<Arc<SocketAddr>, Arc<ProxyMetrics>>>,
}

impl BmpProxyMetrics {
    pub fn proxy_metrics(
        &self,
        socket_addr: SocketAddr,
    ) -> Arc<ProxyMetrics> {
        self.proxies.get(&Arc::new(socket_addr)).unwrap()
    }
}

#[derive(Debug, Default)]
pub struct ProxyMetrics {
    pub proxy_state: Arc<AtomicProxyState>,
    pub proxy_handler_state: Arc<AtomicU8>,
    pub num_undeliverable_messages: Arc<AtomicUsize>,
    pub queue_capacity: Arc<AtomicUsize>,
}

impl BmpProxyMetrics {
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const PROXY_STATE_METRIC: Metric = Metric::new(
        "bmp_tcp_out_connection_state",
        "the state of the proxy connection, if any",
        MetricType::Text,
        MetricUnit::Info,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const PROXY_HANDLER_STATE_METRIC: Metric = Metric::new(
        "bmp_tcp_out_handler_state",
        "the state of the proxy handler, if any",
        MetricType::Gauge,
        MetricUnit::State,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_UNDELIVERABLE_MESSAGES_METRIC: Metric = Metric::new(
        "bmp_tcp_out_num_undeliverable_messages",
        "the number of messages that could not be delivered due to the proxy being in the stalled state",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const QUEUE_CAPACITY_METRIC: Metric = Metric::new(
        "bmp_tcp_out_queue_capacity",
        "the number of messages that can still be queued before the queue is full",
        MetricType::Gauge,
        MetricUnit::Total,
    );
}

impl BmpProxyMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init_metrics_for_proxy(
        &self,
        socket_addr: SocketAddr,
        queue_capacity: usize,
    ) -> Arc<ProxyMetrics> {
        let metrics = self
            .proxies
            .entry(Arc::new(socket_addr))
            .or_insert_with(Default::default);
        metrics
            .queue_capacity
            .store(queue_capacity, Ordering::SeqCst);
        metrics
    }

    pub fn remove_metrics_for_proxy(&self, socket_addr: SocketAddr) {
        self.proxies.remove(&Arc::new(socket_addr));
    }
}

impl metrics::Source for BmpProxyMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        for (proxy_addr, metrics) in self.proxies.guard().iter() {
            let owned_proxy_addr = proxy_addr.to_string();
            let proxy_addr = owned_proxy_addr.as_str();
            append_per_router_metric(
                unit_name,
                target,
                proxy_addr,
                Self::PROXY_STATE_METRIC,
                metrics.proxy_state.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                proxy_addr,
                Self::PROXY_HANDLER_STATE_METRIC,
                metrics.proxy_handler_state.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                proxy_addr,
                Self::NUM_UNDELIVERABLE_MESSAGES_METRIC,
                metrics.num_undeliverable_messages.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                proxy_addr,
                Self::QUEUE_CAPACITY_METRIC,
                metrics.queue_capacity.load(Ordering::SeqCst),
            );
        }
    }
}
