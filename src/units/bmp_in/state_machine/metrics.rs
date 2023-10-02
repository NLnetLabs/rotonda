use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    common::frim::FrimMap,
    metrics::{self, util::append_per_router_metric, Metric, MetricType, MetricUnit},
    payload::RouterId,
};

use super::machine::{AtomicBmpStateIdx, BmpStateIdx};

use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "router-list")]
use std::sync::RwLock;

#[cfg(feature = "router-list")]
use hex_slice::AsHex;

#[cfg(feature = "router-list")]
const MAX_RECENT_PARSE_ERRORS: usize = 10;

#[derive(Clone, Debug, Default)]
pub struct ParseError {
    pub when: DateTime<Utc>,
    pub msg: String,
    /// https://www.wireshark.org/docs/man-pages/text2pcap.html
    pub pcaptext: Option<String>,
    pub recoverable: bool,
}

#[cfg(feature = "router-list")]
impl ParseError {
    pub fn new(msg: String, bytes: Option<Bytes>, recoverable: bool) -> Self {
        Self {
            when: Utc::now(),
            msg,
            pcaptext: bytes.map(|bytes| format!("000000 {:02x}", bytes.plain_hex(true))),
            recoverable,
        }
    }
}

/// A very primitive "ring buffer" of recent parse error messages.
#[cfg(feature = "router-list")]
#[derive(Debug, Default)]
pub struct ParseErrorsRingBuffer {
    errors: Arc<RwLock<Vec<ParseError>>>,
    next_idx: AtomicUsize,
}

#[cfg(not(feature = "router-list"))]
#[derive(Debug, Default)]
pub struct ParseErrorsRingBuffer;

impl Display for ParseErrorsRingBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseErrorsRingBuffer")
    }
}

#[cfg(not(feature = "router-list"))]
impl ParseErrorsRingBuffer {
    pub fn push(&self, _error: String, _bytes: &[u8], _recoverable: bool) {
        // Nothing to do
    }
}

#[cfg(feature = "router-list")]
impl ParseErrorsRingBuffer {
    pub fn push(&self, error: String, bytes: Option<Bytes>, recoverable: bool) {
        if let Ok(mut locked) = self.errors.write() {
            let next_idx = self.next_idx.load(Ordering::SeqCst);
            if next_idx + 1 > locked.len() {
                locked.resize(next_idx + 1, ParseError::default());
            }
            locked[next_idx] = ParseError::new(error, bytes, recoverable);
            self.next_idx
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                    if v >= MAX_RECENT_PARSE_ERRORS - 1 {
                        Some(0)
                    } else {
                        Some(v + 1)
                    }
                })
                .ok();
        }
    }

    pub fn get(&self) -> (Vec<ParseError>, Vec<ParseError>) {
        if let Ok(locked) = self.errors.read() {
            if locked.len() < MAX_RECENT_PARSE_ERRORS {
                // never wrapped
                (locked.clone(), vec![])
            } else {
                let (start, end) = locked.split_at(self.next_idx.load(Ordering::SeqCst));
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
pub struct BmpMetrics {
    routers: Arc<FrimMap<Arc<RouterId>, Arc<RouterBmpMetrics>>>,
}

impl BmpMetrics {
    pub fn router_metrics(&self, router_id: Arc<RouterId>) -> Arc<RouterBmpMetrics> {
        self.routers.entry(router_id).or_insert_with(|| {
            // We only want to collect metrics about a router based on some sort of name. To permit that to be based on the
            // mandatory sysName information TLV which MUST be provided in the initial "Initiation" BMP message, we take in
            // a "RouterId" string, and assume we are starting from state "Dumping" ()
            let metrics = RouterBmpMetrics::default();
            metrics
                .bmp_state_machine_state
                .store(BmpStateIdx::Dumping, Ordering::SeqCst);
            Arc::new(metrics)
        })
    }
}

#[derive(Debug, Default)]
pub struct RouterBmpMetrics {
    pub bmp_state_machine_state: Arc<AtomicBmpStateIdx>,
    pub num_received_prefixes: Arc<AtomicUsize>,
    pub num_stored_prefixes: Arc<AtomicUsize>,
    pub num_bgp_updates_processed: Arc<AtomicUsize>,
    pub num_bgp_updates_for_unknown_peer: Arc<AtomicUsize>,
    pub num_bgp_updates_with_recoverable_parsing_failures_for_known_peers: Arc<AtomicUsize>,
    pub num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers: Arc<AtomicUsize>,
    pub num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers: Arc<AtomicUsize>,
    pub num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers: Arc<AtomicUsize>,
    pub num_bgp_updates_filtered: Arc<AtomicUsize>,
    pub num_announcements: Arc<AtomicUsize>,
    pub num_withdrawals: Arc<AtomicUsize>,
    pub num_peers_up: Arc<AtomicUsize>,
    pub num_peers_up_eor_capable: Arc<AtomicUsize>,
    pub num_peers_up_dumping: Arc<AtomicUsize>,
    pub parse_errors: Arc<ParseErrorsRingBuffer>,
}

impl BmpMetrics {
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const BMP_STATE_MACHINE_STATE_METRIC: Metric = Metric::new(
        "bmp_state_machine_state",
        "the current state machine state for this monitored router connection",
        MetricType::Text,
        MetricUnit::State,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_RECEIVED_PREFIXES_METRIC: Metric = Metric::new(
        "bmp_state_num_received_prefixes",
        "the number of prefixes received from this router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_STORED_PREFIXES_METRIC: Metric = Metric::new(
        "bmp_state_num_stored_prefixes",
        "the number of prefixes stored for this router",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_PROCESSED_METRIC: Metric = Metric::new(
        "bmp_state_num_bgp_updates_processed",
        "the number of processed BGP UPDATE messages",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_FOR_UNKNOWN_PEER_METRIC: Metric = Metric::new(
        "bmp_state_num_bgp_updates_with_unknown_peer",
        "the number of BGP UPDATE messages for which no Peer Up notification was seen",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_WITH_RECOVERABLE_PARSING_FAILURE_FOR_KNOWN_PEER: Metric = Metric::new(
        "bmp_state_num_bgp_updates_with_recoverable_parsing_failure_for_known_peer",
        "the number of BGP UPDATE messages from known peers that could not be parsed with the expected session config",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_WITH_RECOVERABLE_PARSING_FAILURE_FOR_UNKNOWN_PEER: Metric = Metric::new(
        "bmp_state_num_bgp_updates_with_recoverable_parsing_failure_for_unknown_peer",
        "the number of BGP UPDATE messages from unknown peers that could not be parsed with the expected session config",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_WITH_UNRECOVERABLE_PARSING_FAILURE_FOR_KNOWN_PEER: Metric = Metric::new(
        "bmp_state_num_bgp_updates_with_unrecoverable_parsing_failure_for_known_peer",
        "the number of BGP UPDATE messages from known peers that could not be parsed with the expected or generated session config",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_WITH_UNRECOVERABLE_PARSING_FAILURE_FOR_UNKNOWN_PEER: Metric = Metric::new(
        "bmp_state_num_bgp_updates_with_unrecoverable_parsing_failure_for_unknown_peer",
        "the number of BGP UPDATE messages from unknown peers that could not be parsed with the expected or generated session config",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_BGP_UPDATES_FILTERED: Metric = Metric::new(
        "bmp_state_num_bgp_updates_filtered",
        "the number of BGP UPDATE messages filtered out by accept/reject rules",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_ANNOUNCEMENTS_METRIC: Metric = Metric::new(
        "bmp_state_num_announcements",
        "the number of route announcements contained in the last BMP message",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_WITHDRAWALS_METRIC: Metric = Metric::new(
        "bmp_state_num_withdrawals",
        "the number of route withdrawals contained in the last BMP message",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_PEERS_UP_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers",
        "the number of peers connected and in the up state",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_PEERS_UP_EOR_CAPABLE_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers_eor_capable",
        "the number of up peers that support the graceful restart capability and thus are expected to signal End-of-Rib",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_PEERS_UP_DUMPING_METRIC: Metric = Metric::new(
        "bmp_state_num_up_peers_dumping",
        "the number of up peers expected to but have not yet signalled End-of-RIB",
        MetricType::Gauge,
        MetricUnit::Total,
    );
}

impl BmpMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl metrics::Source for BmpMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        for (router_id, metrics) in self.routers.guard().iter() {
            let router_id = router_id.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::BMP_STATE_MACHINE_STATE_METRIC,
                metrics.bmp_state_machine_state.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_RECEIVED_PREFIXES_METRIC,
                metrics.num_received_prefixes.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_STORED_PREFIXES_METRIC,
                metrics.num_stored_prefixes.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_PROCESSED_METRIC,
                metrics.num_bgp_updates_processed.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_FOR_UNKNOWN_PEER_METRIC,
                metrics
                    .num_bgp_updates_for_unknown_peer
                    .load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_WITH_RECOVERABLE_PARSING_FAILURE_FOR_KNOWN_PEER,
                metrics
                    .num_bgp_updates_with_recoverable_parsing_failures_for_known_peers
                    .load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_WITH_RECOVERABLE_PARSING_FAILURE_FOR_UNKNOWN_PEER,
                metrics
                    .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
                    .load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_WITH_UNRECOVERABLE_PARSING_FAILURE_FOR_KNOWN_PEER,
                metrics
                    .num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers
                    .load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_WITH_UNRECOVERABLE_PARSING_FAILURE_FOR_UNKNOWN_PEER,
                metrics
                    .num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers
                    .load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_BGP_UPDATES_FILTERED,
                metrics.num_bgp_updates_filtered.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_ANNOUNCEMENTS_METRIC,
                metrics.num_announcements.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_WITHDRAWALS_METRIC,
                metrics.num_withdrawals.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_METRIC,
                metrics.num_peers_up.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_EOR_CAPABLE_METRIC,
                metrics.num_peers_up_eor_capable.load(Ordering::SeqCst),
            );
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_PEERS_UP_DUMPING_METRIC,
                metrics.num_peers_up_dumping.load(Ordering::SeqCst),
            );
        }
    }
}
