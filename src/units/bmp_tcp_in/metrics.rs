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

#[derive(Debug, Default)]
pub struct RouterMetrics {
    pub num_receive_io_errors: Arc<AtomicUsize>,
    pub num_bmp_messages_received: [AtomicUsize; 7], // One counter per RFC 7854 BMP message type
    pub num_bmp_messages_processed: AtomicUsize,
    pub num_invalid_bmp_messages: AtomicUsize,
}

impl BmpTcpInMetrics {
    /// From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.1
    ///
    /// o  Message Type (1 byte): This identifies the type of the BMP
    ///    message.  A BMP implementation MUST ignore unrecognized message
    ///    types upon receipt.
    ///    
    ///    *  Type = 0: Route Monitoring
    ///    *  Type = 1: Statistics Report
    ///    *  Type = 2: Peer Down Notification
    ///    *  Type = 3: Peer Up Notification
    ///    *  Type = 4: Initiation Message
    ///    *  Type = 5: Termination Message
    ///    *  Type = 6: Route Mirroring Message
    const BMP_RFC_7854_MSG_TYPE_NAMES: [&'static str; 7] = [
        "Route Monitoring",
        "Statistics Report",
        "Peer Down Notification",
        "Peer Up Notification",
        "Initiation Message",
        "Termination Message",
        "Route Mirroring Message",
    ];

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
    const NUM_BMP_MESSAGES_RECEIVED_METRIC: Metric = Metric::new(
        "bmp_tcp_in_num_bmp_messages_received",
        "the number of BMP messages successfully received by RFC 7854 message type code",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_BMP_MESSAGES_PROCESSED_METRIC: Metric = Metric::new(
        "bmp_tcp_in_num_bmp_messages_processed",
        "the number of BMP messages passed to the BMP state machine for processing",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_RECEIVE_IO_ERRORS_METRIC: Metric = Metric::new(
        "bmp_tcp_in_num_receive_io_errors",
        "the number of BMP messages that could not be received due to an I/O error",
        MetricType::Counter,
        MetricUnit::Total,
    );
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

    //pub fn contains(&self, router_id: &Arc<RouterId>) -> bool {
    //    self.routers.contains_key(router_id)
    //}

    /// Warning: This fn will create a metric set for the given router id if
    /// it doesn't already exist. Use `contains()` to test if metrics exist
    /// for a given router id.
    pub fn router_metrics(
        &self,
        router_id: Arc<RouterId>,
    ) -> Arc<RouterMetrics> {
        #[allow(clippy::unwrap_or_default)]
        self.routers
            .entry(router_id)
            .or_insert_with(Default::default)
    }

    pub fn remove_router(&self, router_id: &Arc<RouterId>) {
        self.routers.remove(router_id);
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

            for (msg_type_code, metric_value) in
                metrics.num_bmp_messages_received.iter().enumerate()
            {
                let router_label = ("router", router_id);
                let msg_type_label = (
                    "msg_type",
                    Self::BMP_RFC_7854_MSG_TYPE_NAMES[msg_type_code],
                );

                target.append(
                    &Self::NUM_BMP_MESSAGES_RECEIVED_METRIC,
                    Some(unit_name),
                    |records| {
                        records.label_value(
                            &[router_label, msg_type_label],
                            metric_value.load(SeqCst),
                        )
                    },
                );
            }

            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_RECEIVE_IO_ERRORS_METRIC,
                metrics.num_receive_io_errors.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BMP_MESSAGES_PROCESSED_METRIC,
                metrics.num_bmp_messages_processed.load(SeqCst),
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
