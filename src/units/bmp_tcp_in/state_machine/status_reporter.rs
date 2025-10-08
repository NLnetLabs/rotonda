use std::{
    fmt::Display,
    sync::{atomic::Ordering::SeqCst, Arc},
};

use bytes::Bytes;
use routecore::bgp::ParseError;

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
        update_report_msg: UpdateReportMessage,
        // router_id: Arc<RouterId>,
        // n_new_prefixes: usize,
        // n_announcements: usize,
        // n_withdrawals: usize,
        // n_total_prefixes: usize,
    ) {
        // let n_valid_announcements = update_report_msg.get_n_valid_announcements();
        // let n_valid_withdrawals = update_report_msg.get_n_valid_withdrawals();
        // let n_new_prefixes = update_report_msg.get_n_new_prefixes();
        // let n_stored_prefixes = update_report_msg.get_n_stored_prefixes();
        let metrics =
            self.metrics.router_metrics(update_report_msg.router_id);
        metrics
            .num_received_prefixes
            .fetch_add(update_report_msg.n_new_prefixes, SeqCst);
        metrics
            .num_announcements
            .fetch_add(update_report_msg.n_valid_announcements, SeqCst);
        metrics
            .num_withdrawals
            .fetch_add(update_report_msg.n_valid_withdrawals, SeqCst);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct UpdateReportMessage {
    pub router_id: Arc<RouterId>,
    pub n_new_prefixes: usize,
    pub n_valid_announcements: usize,
    pub n_valid_withdrawals: usize,
    pub n_invalid_announcements: usize,
    pub n_stored_prefixes: usize,
    pub n_invalid_withdrawals: usize,
    pub last_invalid_announcement: Option<ParseError>,
    pub last_invalid_withdrawal: Option<ParseError>,
}

#[allow(dead_code)]
impl UpdateReportMessage {
    pub fn new(router_id: Arc<RouterId>) -> Self {
        Self {
            router_id,
            n_new_prefixes: 0,
            n_valid_announcements: 0,
            n_valid_withdrawals: 0,
            n_invalid_announcements: 0,
            n_invalid_withdrawals: 0,
            n_stored_prefixes: 0,
            last_invalid_announcement: None,
            last_invalid_withdrawal: None,
        }
    }

    pub fn inc_new_prefixes(&mut self) {
        self.n_new_prefixes += 1;
    }

    pub fn inc_valid_announcements(&mut self) {
        self.n_valid_announcements += 1;
    }

    pub fn inc_invalid_announcements(&mut self) {
        self.n_invalid_announcements += 1;
    }

    pub fn inc_valid_withdrawals(&mut self) {
        self.n_valid_withdrawals += 1;
    }

    pub fn inc_invalid_withdrawals(&mut self) {
        self.n_invalid_withdrawals += 1;
    }

    pub fn set_invalid_announcement(&mut self, err: ParseError) {
        self.last_invalid_announcement = Some(err);
    }

    pub fn set_invalid_withdrawal(&mut self, err: ParseError) {
        self.last_invalid_announcement = Some(err);
    }

    pub fn set_n_stored_prefixes(&mut self, n_stored: usize) {
        self.n_stored_prefixes = n_stored;
    }

    pub fn _get_n_stored_prefixes(&self) -> usize {
        self.n_stored_prefixes
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
