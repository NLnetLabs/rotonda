use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

use crate::{
    common::frim::FrimMap,
    comms::{Gate, GateMetrics, GraphStatus},
    metrics::{
        self, util::append_per_router_metric, Metric, MetricType, MetricUnit,
    },
    payload::RouterId,
};

#[derive(Debug, Default)]
pub struct BmpTcpInMetrics {
    gate: Option<Arc<GateMetrics>>, // optional to make testing easier
    pub listener_bound_count: Arc<AtomicUsize>,
    pub connection_accepted_count: Arc<AtomicUsize>,
    pub connection_lost_count: Arc<AtomicUsize>,
    routers: Arc<FrimMap<Arc<RouterId>, Arc<RouterMetrics>>>,
}

#[derive(Debug, Default)]
pub struct RouterMetrics {
    pub connection_count: Arc<AtomicUsize>,
    pub num_bmp_messages_received: AtomicUsize,
    pub num_invalid_bmp_messages: AtomicUsize,
}

impl BmpTcpInMetrics {
    const LISTENER_BOUND_COUNT_METRIC: Metric = Metric::new(
        "bmp_tcp_in_listener_bound_count",
        "the number of times the TCP listen port was bound to",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_ACCEPTED_COUNT_METRIC: Metric = Metric::new(
        "bmp_tcp_in_connection_accepted_count",
        "the number of times a connection from a router was accepted",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_LOST_COUNT_METRIC: Metric = Metric::new(
        "bmp_tcp_in_connection_lost_count",
        "the number of times the connection to a router was lost",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_COUNT_METRIC: Metric = Metric::new(
        "bmp_tcp_in_connection_count",
        "the number of router initiation messages seen per router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_BMP_MESSAGES_RECEIVED_METRIC: Metric = Metric::new(
        "bmp_tcp_in_num_bmp_messages_received",
        "the number of BMP messages successfully received",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_INVALID_BMP_MESSAGES_METRIC: Metric = Metric::new(
        "bmp_in_num_invalid_bmp_messages",
        "the number of received BMP messages that were invalid (e.g. not RFC compliant, could not be parsed, etc)",
        MetricType::Counter,
        MetricUnit::Total,
    );
}

impl BmpTcpInMetrics {
    pub fn new(gate: &Gate) -> Self {
        Self {
            gate: Some(gate.metrics()),
            ..Default::default()
        }
    }

    pub fn contains(&self, router_id: &Arc<RouterId>) -> bool {
        self.routers.contains_key(&router_id)
    }

    /// Warning: This fn will create a metric set for the given router id if
    /// it doesn't already exist. Use `contains()` to test if metrics exist
    /// for a given router id.
    pub fn router_metrics(
        &self,
        router_id: Arc<RouterId>,
    ) -> Arc<RouterMetrics> {
        self.routers
            .entry(router_id)
            .or_insert_with(Default::default)
    }
}

impl metrics::Source for BmpTcpInMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        if let Some(gate) = &self.gate {
            gate.append(unit_name, target);
        }

        target.append_simple(
            &Self::LISTENER_BOUND_COUNT_METRIC,
            Some(unit_name),
            self.listener_bound_count.load(SeqCst),
        );

        target.append_simple(
            &Self::CONNECTION_ACCEPTED_COUNT_METRIC,
            Some(unit_name),
            self.connection_accepted_count.load(SeqCst),
        );

        target.append_simple(
            &Self::CONNECTION_LOST_COUNT_METRIC,
            Some(unit_name),
            self.connection_lost_count.load(SeqCst),
        );

        for (router_id, metrics) in self.routers.guard().iter() {
            let router_id = router_id.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::CONNECTION_COUNT_METRIC,
                metrics.connection_count.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BMP_MESSAGES_RECEIVED_METRIC,
                metrics.num_bmp_messages_received.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_INVALID_BMP_MESSAGES_METRIC,
                metrics.num_invalid_bmp_messages.load(SeqCst),
            );
        }
    }
}

impl GraphStatus for BmpTcpInMetrics {
    fn status_text(&self) -> String {
        let num_clients = self.connection_accepted_count.load(SeqCst)
            - self.connection_lost_count.load(SeqCst);
        let num_msgs_out = self
            .gate
            .as_ref()
            .map(|gate| gate.num_updates.load(SeqCst))
            .unwrap_or_default();
        format!("clients: {}\nout: {}", num_clients, num_msgs_out)
    }

    fn okay(&self) -> Option<bool> {
        let connection_accepted_count =
            self.connection_accepted_count.load(SeqCst);
        if connection_accepted_count > 0 {
            let connection_lost_count =
                self.connection_lost_count.load(SeqCst);
            let num_clients =
                connection_accepted_count - connection_lost_count;
            Some(num_clients > 0)
        } else {
            None
        }
    }
}
