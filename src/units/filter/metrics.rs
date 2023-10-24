use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    common::frim::FrimMap,
    comms::{Gate, GateMetrics},
    metrics::{self, util::append_per_router_metric, Metric, MetricType, MetricUnit},
};

#[derive(Debug, Default)]
pub struct RotoFilterMetrics {
    gate: Arc<GateMetrics>,
    routers: Arc<FrimMap<Arc<SocketAddr>, Arc<RouterMetrics>>>,
    pub num_filtered_messages: Arc<AtomicUsize>,
}

impl RotoFilterMetrics {
    pub fn router_metrics(&self, socket_addr: SocketAddr) -> Arc<RouterMetrics> {
        self.routers
            .entry(Arc::new(socket_addr))
            .or_insert_with(Default::default)
    }
}

#[derive(Debug, Default)]
pub struct RouterMetrics {
    pub num_filtered_messages: Arc<AtomicUsize>,
}

impl RotoFilterMetrics {
    pub fn new(gate: &Arc<Gate>) -> Self {
        RotoFilterMetrics {
            gate: gate.metrics(),
            ..Default::default()
        }
    }
}

impl RotoFilterMetrics {
    const NUM_FILTERED_MESSAGES_METRIC: Metric = Metric::new(
        "roto_filter_num_filtered_messages",
        "the number of messages filtered out by this unit",
        MetricType::Counter,
        MetricUnit::Total,
    );
}

impl metrics::Source for RotoFilterMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        self.gate.append(unit_name, target);

        for (socket_addr, metrics) in self.routers.guard().iter() {
            let owned_socket_addr = socket_addr.to_string();
            let socket_addr = owned_socket_addr.as_str();
            append_per_router_metric(
                unit_name,
                target,
                socket_addr,
                Self::NUM_FILTERED_MESSAGES_METRIC,
                metrics.num_filtered_messages.load(Ordering::SeqCst),
            );
        }

        target.append_simple(
            &Self::NUM_FILTERED_MESSAGES_METRIC,
            Some(unit_name),
            self.num_filtered_messages.load(Ordering::SeqCst),
        );
    }
}
