use std::{
    fmt::Display,
    sync::{atomic::Ordering::SeqCst, Arc},
};

use bytes::Bytes;

use crate::{
    common::status_reporter::{
        AnyStatusReporter, Chainable, Named, UnitStatusReporter,
    },
    payload::RouterId,
};

use super::{machine::BmpStateIdx, metrics::BmpStateMachineMetrics};

#[derive(Debug, Default)]
pub struct BmpStateMachineStatusReporter {
    name: String,
    metrics: Arc<BmpStateMachineMetrics>,
}

impl BmpStateMachineStatusReporter {
    pub fn new<T: Display>(
        name: T,
        metrics: Arc<BmpStateMachineMetrics>,
    ) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
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
            .store(new_state_idx, SeqCst);
    }

    pub fn peer_up(&self, router_id: Arc<RouterId>, eor_capable: bool) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics.num_peers_up.fetch_add(1, SeqCst);
        if eor_capable {
            metrics.num_peers_up_eor_capable.fetch_add(1, SeqCst);
        }
    }

    pub fn peer_down(
        &self,
        router_id: Arc<RouterId>,
        eor_capable: Option<bool>,
    ) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics.num_peers_up.fetch_sub(1, SeqCst);
        if Some(true) == eor_capable {
            metrics.num_peers_up_eor_capable.fetch_sub(1, SeqCst);
        }
    }

    pub fn peer_unknown(&self, router_id: Arc<RouterId>) {
        self.metrics
            .router_metrics(router_id)
            .num_bmp_route_monitoring_msgs_with_unknown_peer
            .fetch_add(1, SeqCst);
    }

    pub fn bgp_update_parse_soft_fail(
        &self,
        router_id: Arc<RouterId>,
        err: String,
        bytes: Option<Bytes>,
    ) {
        let metrics = self.metrics.router_metrics(router_id);

        metrics
            .num_bgp_updates_reparsed_due_to_incorrect_header_flags
            .fetch_add(1, SeqCst);

        metrics.parse_errors.push(err, bytes, true);
    }

    pub fn bgp_update_parse_hard_fail(
        &self,
        router_id: Arc<RouterId>,
        err: String,
        bytes: Option<Bytes>,
    ) {
        let metrics = self.metrics.router_metrics(router_id);

        metrics.num_unprocessable_bmp_messages.fetch_add(1, SeqCst);

        metrics.parse_errors.push(err, bytes, false);
    }

    pub fn pending_eors_update(
        &self,
        router_id: Arc<RouterId>,
        n_peers_dumping: usize,
    ) {
        self.metrics
            .router_metrics(router_id)
            .num_peers_up_dumping
            .store(n_peers_dumping, SeqCst);
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
            .fetch_add(n_new_prefixes, SeqCst);
        metrics.num_stored_prefixes.store(n_total_prefixes, SeqCst);
        metrics.num_announcements.fetch_add(n_announcements, SeqCst);
        metrics.num_withdrawals.fetch_add(n_withdrawals, SeqCst);
    }
}

impl UnitStatusReporter for BmpStateMachineStatusReporter {}

impl AnyStatusReporter for BmpStateMachineStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for BmpStateMachineStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpStateMachineStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
