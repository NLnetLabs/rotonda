use std::{
    net::SocketAddr,
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    common::frim::FrimMap,
    comms::{Gate, GateMetrics, GraphStatus},
    metrics::{self, util::append_per_router_metric, Metric, MetricType, MetricUnit},
};

#[derive(Debug, Default)]
pub struct BmpTcpInMetrics {
    gate: Option<Arc<GateMetrics>>, // optional to make testing easier
    pub listener_bound_count: Arc<AtomicUsize>,
    pub connection_accepted_count: Arc<AtomicUsize>,
    pub connection_lost_count: Arc<AtomicUsize>,
    routers: Arc<FrimMap<Arc<SocketAddr>, Arc<RouterMetrics>>>,
}

impl GraphStatus for BmpTcpInMetrics {
    fn status_text(&self) -> String {
        let num_clients = self.connection_accepted_count.load(Ordering::SeqCst) - self.connection_lost_count.load(Ordering::SeqCst);
        let num_msgs_out = self.gate.as_ref().map(|gate| gate.num_updates.load(Ordering::SeqCst)).unwrap_or_default();
        format!("clients: {}\nout: {}", num_clients, num_msgs_out)
    }

    fn okay(&self) -> Option<bool> {
        let connection_accepted_count = self.connection_accepted_count.load(Ordering::SeqCst);
        if connection_accepted_count > 0 {
            let connection_lost_count = self.connection_lost_count.load(Ordering::SeqCst);
            let num_clients = connection_accepted_count - connection_lost_count;
            Some(num_clients > 0)
        } else {
            None
        }
    }
}

impl BmpTcpInMetrics {
    pub fn router_metrics(&self, socket_addr: SocketAddr) -> Arc<RouterMetrics> {
        self.routers
            .entry(Arc::new(socket_addr))
            .or_insert_with(Default::default)
    }
}

#[derive(Debug, Default)]
pub struct RouterMetrics {
    pub connection_count: Arc<AtomicUsize>,
    pub num_bmp_messages_received: Arc<AtomicUsize>,
    pub num_receive_io_errors: Arc<AtomicUsize>,
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
    const NUM_RECEIVE_IO_ERRORS_METRIC: Metric = Metric::new(
        "bmp_tcp_in_num_receive_io_errors",
        "the number of BMP messages that could not be received due to an I/O error",
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
}

impl metrics::Source for BmpTcpInMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        if let Some(gate) = &self.gate {
            gate.append(unit_name, target);
        }

        target.append_simple(
            &Self::LISTENER_BOUND_COUNT_METRIC,
            Some(unit_name),
            self.listener_bound_count.load(atomic::Ordering::SeqCst),
        );

        target.append_simple(
            &Self::CONNECTION_ACCEPTED_COUNT_METRIC,
            Some(unit_name),
            self.connection_accepted_count
                .load(atomic::Ordering::SeqCst),
        );

        target.append_simple(
            &Self::CONNECTION_LOST_COUNT_METRIC,
            Some(unit_name),
            self.connection_lost_count.load(atomic::Ordering::SeqCst),
        );

        for (router_addr, metrics) in self.routers.guard().iter() {
            let owned_router_addr = router_addr.to_string();
            let router_addr = owned_router_addr.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_addr,
                Self::CONNECTION_COUNT_METRIC,
                metrics.connection_count.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_addr,
                Self::NUM_BMP_MESSAGES_RECEIVED_METRIC,
                metrics.num_bmp_messages_received.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_addr,
                Self::NUM_RECEIVE_IO_ERRORS_METRIC,
                metrics.num_receive_io_errors.load(Ordering::SeqCst),
            );
        }
    }
}
