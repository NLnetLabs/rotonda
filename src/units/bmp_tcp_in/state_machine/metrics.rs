use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    common::frim::FrimMap,
    metrics::{
        self, util::append_per_router_metric, Metric, MetricType, MetricUnit,
    },
    payload::RouterId,
};

use super::machine::{AtomicBmpStateIdx, BmpStateIdx};

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use hex_slice::AsHex;
use std::sync::RwLock;
const MAX_RECENT_PARSE_ERRORS: usize = 10;

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct ParseError {
    pub when: DateTime<Utc>,
    pub msg: String,
    /// https://www.wireshark.org/docs/man-pages/text2pcap.html
    pub pcaptext: Option<String>,
    pub recoverable: bool,
}

impl ParseError {
    pub fn new(msg: String, bytes: Option<Bytes>, recoverable: bool) -> Self {
        Self {
            when: Utc::now(),
            msg,
            pcaptext: bytes
                .map(|bytes| format!("000000 {:02x}", bytes.plain_hex(true))),
            recoverable,
        }
    }
}

/// A very primitive "ring buffer" of recent parse error messages.
#[derive(Debug, Default)]
pub struct ParseErrorsRingBuffer {
    errors: Arc<RwLock<Vec<ParseError>>>,
    next_idx: AtomicUsize,
}

impl Display for ParseErrorsRingBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseErrorsRingBuffer")
    }
}

impl ParseErrorsRingBuffer {
    pub fn push(
        &self,
        error: String,
        bytes: Option<Bytes>,
        recoverable: bool,
    ) {
        if let Ok(mut locked) = self.errors.write() {
            let next_idx = self.next_idx.load(SeqCst);
            if next_idx + 1 > locked.len() {
                locked.resize(next_idx + 1, ParseError::default());
            }
            locked[next_idx] = ParseError::new(error, bytes, recoverable);
            self.next_idx
                .fetch_update(SeqCst, SeqCst, |v| {
                    if v >= MAX_RECENT_PARSE_ERRORS - 1 {
                        Some(0)
                    } else {
                        Some(v + 1)
                    }
                })
                .ok();
        }
    }

    #[allow(dead_code)]
    pub fn get(&self) -> (Vec<ParseError>, Vec<ParseError>) {
        if let Ok(locked) = self.errors.read() {
            if locked.len() < MAX_RECENT_PARSE_ERRORS {
                // never wrapped
                (locked.clone(), vec![])
            } else {
                let (start, end) =
                    locked.split_at(self.next_idx.load(SeqCst));
                (end.to_vec(), start.to_vec())
            }
        } else {
            (vec![], vec![])
        }
    }
}

// TODO: We should only store metrics here for a single router. That would
// avoid the need to find the right metric to update, which in a concurrent
// access scenario requires some sort of concurrent safe map.
#[derive(Debug, Default)]
pub struct BmpStateMachineMetrics {
    routers: Arc<FrimMap<Arc<RouterId>, Arc<RouterBmpMetrics>>>,
}

impl BmpStateMachineMetrics {
    pub fn router_metrics(
        &self,
        router_id: Arc<RouterId>,
    ) -> Arc<RouterBmpMetrics> {
        self.routers.entry(router_id).or_insert_with(|| {
            // We only want to collect metrics about a router based on some sort of name. To permit that to be based on the
            // mandatory sysName information TLV which MUST be provided in the initial "Initiation" BMP message, we take in
            // a "RouterId" string, and assume we are starting from state "Dumping" ()
            let metrics = RouterBmpMetrics::default();
            metrics
                .bmp_state_machine_state
                .store(BmpStateIdx::Dumping, SeqCst);
            Arc::new(metrics)
        })
    }

    pub fn remove_router_metrics(&self, router_id: &Arc<RouterId>) {
        self.routers.remove(router_id);
    }
}

#[derive(Debug, Default)]
pub struct RouterBmpMetrics {
    pub bmp_state_machine_state: Arc<AtomicBmpStateIdx>,
    pub num_received_prefixes: Arc<AtomicUsize>,
    pub num_bmp_route_monitoring_msgs_with_unknown_peer: Arc<AtomicUsize>,
    pub num_bgp_updates_reparsed_due_to_incorrect_header_flags:
        Arc<AtomicUsize>,
    pub num_unprocessable_bmp_messages: Arc<AtomicUsize>,
    pub num_announcements: Arc<AtomicUsize>,
    pub num_withdrawals: Arc<AtomicUsize>,
    pub num_peers_up: Arc<AtomicUsize>,
    pub num_peers_up_eor_capable: Arc<AtomicUsize>,
    pub num_peers_up_dumping: Arc<AtomicUsize>,
    pub parse_errors: Arc<ParseErrorsRingBuffer>,
}

impl BmpStateMachineMetrics {
    const NUM_CONNECTED_ROUTERS_METRIC: Metric = Metric::new(
        "bmp_num_connected_routers",
        "the number of BMP routers connected to this unit",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const BMP_STATE_MACHINE_STATE_METRIC: Metric = Metric::new(
        "bmp_state_machine_state",
        "the current state machine state for this monitored router connection",
        MetricType::Text,
        MetricUnit::State,
    );
    const NUM_RECEIVED_PREFIXES_METRIC: Metric = Metric::new(
        "bmp_state_num_received_prefixes",
        "the number of prefixes received from this router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_BMP_ROUTE_MONITORING_MSGS_WITH_UNKNOWN_PEER_METRIC: Metric = Metric::new(
        "bmp_state_num_bmp_route_monitoring_msgs_with_unknown_peer",
        "the number of BMP Route Monitoring messages for which no Peer Up notification was seen",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_BGP_UPDATES_REPARSED_DUE_TO_INCORRECT_HEADER_FLAGS: Metric = Metric::new(
        "bmp_state_num_bgp_updates_reparsed_due_to_incorrect_header_flags",
        "the number of BGP UPDATE messages that could only be parsed by not obeying the BMP common header flags",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_UNPROCESSABLE_BMP_MESSAGES: Metric = Metric::new(
        "bmp_state_num_unprocessable_bmp_messages",
        "the number of BMP messages that could not be parsed, were invbmp_state_num_unprocessable_bmp_messagesalid or otherwise could not be processed",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_ANNOUNCEMENTS_METRIC: Metric = Metric::new(
        "bmp_state_num_announcements",
        "the number of route announcements seen from this router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_WITHDRAWALS_METRIC: Metric = Metric::new(
        "bmp_state_num_withdrawals",
        "the number of route withdrawals seen from this router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_PEERS_UP_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers",
        "the number of peers connected and in the up state",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_PEERS_UP_EOR_CAPABLE_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers_eor_capable",
        "the number of up peers that support the graceful restart capability and thus are expected to signal End-of-Rib",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const NUM_PEERS_UP_WITH_PENDING_EORS_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers_with_pending_eors",
        "the number of up peers with at least one pending End-of-RIB signal",
        MetricType::Gauge,
        MetricUnit::Total,
    );
}

impl BmpStateMachineMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl metrics::Source for BmpStateMachineMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        target.append_simple(
            &Self::NUM_CONNECTED_ROUTERS_METRIC,
            Some(unit_name),
            self.routers.len(),
        );

        for (router_id, metrics) in self.routers.guard().iter() {
            let router_id = router_id.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::BMP_STATE_MACHINE_STATE_METRIC,
                metrics.bmp_state_machine_state.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_RECEIVED_PREFIXES_METRIC,
                metrics.num_received_prefixes.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BMP_ROUTE_MONITORING_MSGS_WITH_UNKNOWN_PEER_METRIC,
                metrics
                    .num_bmp_route_monitoring_msgs_with_unknown_peer
                    .load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_REPARSED_DUE_TO_INCORRECT_HEADER_FLAGS,
                metrics
                    .num_bgp_updates_reparsed_due_to_incorrect_header_flags
                    .load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_UNPROCESSABLE_BMP_MESSAGES,
                metrics.num_unprocessable_bmp_messages.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_ANNOUNCEMENTS_METRIC,
                metrics.num_announcements.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_WITHDRAWALS_METRIC,
                metrics.num_withdrawals.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_METRIC,
                metrics.num_peers_up.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_EOR_CAPABLE_METRIC,
                metrics.num_peers_up_eor_capable.load(SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_WITH_PENDING_EORS_METRIC,
                metrics.num_peers_up_dumping.load(SeqCst),
            );
        }
    }
}
