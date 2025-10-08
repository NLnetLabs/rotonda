use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use crate::comms::{Gate, GateMetrics, GraphStatus};

use crate::metrics::{self, Metric, MetricType, MetricUnit};

#[derive(Debug, Default)]
pub struct BgpTcpInMetrics {
    gate: Option<Arc<GateMetrics>>,
    pub listener_bound_count: Arc<AtomicUsize>,
    pub connection_accepted_count: Arc<AtomicUsize>,
    #[allow(dead_code)]
    pub established_session_count: Arc<AtomicUsize>,
    pub connection_lost_count: Arc<AtomicUsize>,
    pub disconnect_count: Arc<AtomicUsize>,
}

impl BgpTcpInMetrics {
    pub fn new(gate: &Gate) -> Self {
        Self {
            gate: Some(gate.metrics()),
            ..Default::default()
        }
    }
}

impl GraphStatus for BgpTcpInMetrics {
    fn status_text(&self) -> String {
        //let num_sessions = self.established_session_count.load(SeqCst);
        let num_msgs_out = self
            .gate
            .as_ref()
            .map(|gate| gate.num_updates.load(SeqCst))
            .unwrap_or_default();

        format!(
            "out: {}",
            //"sessions: {}\nout: {}",
            //num_sessions, // TODO
            num_msgs_out,
        )
    }
}

impl BgpTcpInMetrics {
    const LISTENER_BOUND_COUNT_METRIC: Metric = Metric::new(
        "bgp_tcp_in_listener_bound_count",
        "the number of times the TCP listen port was bound to",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_ACCEPTED_COUNT_METRIC: Metric = Metric::new(
        "bgp_tcp_in_connection_accepted_count",
        "the number of times a connection from a peer was accepted",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_LOST_COUNT_METRIC: Metric = Metric::new(
        "bgp_tcp_in_connection_lost_count",
        "the number of times the connection to a peer was lost",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const DISCONNECT_COUNT_METRIC: Metric = Metric::new(
        "bgp_tcp_in_disconnect_count",
        "the number of times the connection to a peer was actively disconnected",
        MetricType::Counter,
        MetricUnit::Total,
    );
}

impl metrics::Source for BgpTcpInMetrics {
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

        target.append_simple(
            &Self::DISCONNECT_COUNT_METRIC,
            Some(unit_name),
            self.disconnect_count.load(SeqCst),
        );

        // TODO per peer stats:

        //target.append_simple(
        //    &Self::NUM_BGP_MESSAGES_RECEIVED_METRIC,
        //    Some(unit_name),
        //    self.NUM_BGP_MESSAGES_RECEIVED_METRIC.load(atomic::SeqCst),
        //);

        //target.append_simple(
        //    &Self::NUM_RECEIVE_IO_ERRORS_METRIC,
        //    Some(unit_name),
        //    self.num_receive_io_errors.load(atomic::SeqCst),
        //);
    }
}
