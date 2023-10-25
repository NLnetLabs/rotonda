use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
};

use bytes::Bytes;

use crate::{
    common::status_reporter::{AnyStatusReporter, Chainable, Named, UnitStatusReporter},
    payload::RouterId,
};

use super::{machine::BmpStateIdx, metrics::BmpMetrics};

#[derive(Debug, Default)]
pub struct BmpStatusReporter {
    name: String,
    metrics: Arc<BmpMetrics>,
}

impl BmpStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BmpMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    // This is done by the owner of the metrics, BmpInRunner, directly via
    // the BmpMetrics interface, so it isn't needed here. However, someone has
    // to make sure they call it so leaving this here as a reminder of why it
    // ISN'T done here.
    // pub fn init_per_proxy_metrics(&self, router_id: Arc<RouterId>) {
    //     self.metrics.init_per_proxy_metrics(router_id);
    // }

    pub fn bmp_update_message_processed(&self, router_id: Arc<RouterId>) {
        self.metrics
            .router_metrics(router_id)
            .num_bgp_updates_processed
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn change_state(
        &self,
        router_id: Arc<RouterId>,
        _old_state_idx: BmpStateIdx,
        new_state_idx: BmpStateIdx,
    ) {
        self.metrics
            .router_metrics(router_id)
            .bmp_state_machine_state
            .store(new_state_idx, Ordering::SeqCst);
    }

    pub fn peer_up(&self, router_id: Arc<RouterId>, eor_capable: bool) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics.num_peers_up.fetch_add(1, Ordering::SeqCst);
        if eor_capable {
            metrics
                .num_peers_up_eor_capable
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn peer_down(&self, router_id: Arc<RouterId>, eor_capable: Option<bool>) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics.num_peers_up.fetch_sub(1, Ordering::SeqCst);
        if Some(true) == eor_capable {
            metrics
                .num_peers_up_eor_capable
                .fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn peer_unknown(&self, router_id: Arc<RouterId>) {
        self.metrics
            .router_metrics(router_id)
            .num_bgp_updates_for_unknown_peer
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn bgp_update_parse_soft_fail(
        &self,
        router_id: Arc<RouterId>,
        known_peer: Option<bool>,
        err: String,
        bytes: Option<Bytes>,
    ) {
        let metrics = self.metrics.router_metrics(router_id);
        if matches!(known_peer, Some(true)) {
            metrics
                .num_bgp_updates_with_recoverable_parsing_failures_for_known_peers
                .fetch_add(1, Ordering::SeqCst);
        } else {
            metrics
                .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
                .fetch_add(1, Ordering::SeqCst);
        }

        metrics.parse_errors.push(err, bytes, true);
    }

    pub fn bgp_update_parse_hard_fail(
        &self,
        router_id: Arc<RouterId>,
        known_peer: Option<bool>,
        err: String,
        bytes: Option<Bytes>,
    ) {
        let metrics = self.metrics.router_metrics(router_id);
        if matches!(known_peer, Some(true)) {
            metrics
                .num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers
                .fetch_add(1, Ordering::SeqCst);
        } else {
            metrics
                .num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers
                .fetch_add(1, Ordering::SeqCst);
        }

        metrics.parse_errors.push(err, bytes, false);
    }

    pub fn pending_eors_update(&self, router_id: Arc<RouterId>, n_peers_dumping: usize) {
        self.metrics
            .router_metrics(router_id)
            .num_peers_up_dumping
            .store(n_peers_dumping, Ordering::SeqCst);
    }

    pub fn routing_update(
        &self,
        router_id: Arc<RouterId>,
        n_new_prefixes: usize,
        n_announcements: usize,
        n_withdrawals: usize,
        n_total_prefixes: usize,
    ) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics
            .num_received_prefixes
            .fetch_add(n_new_prefixes, Ordering::SeqCst);
        metrics
            .num_stored_prefixes
            .store(n_total_prefixes, Ordering::SeqCst);
        metrics
            .num_announcements
            .fetch_add(n_announcements, Ordering::SeqCst);
        metrics
            .num_withdrawals
            .fetch_add(n_withdrawals, Ordering::SeqCst);
    }
}

impl UnitStatusReporter for BmpStatusReporter {}

impl AnyStatusReporter for BmpStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for BmpStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
