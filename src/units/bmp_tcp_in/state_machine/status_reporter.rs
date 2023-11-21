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

use super::{machine::BmpStateIdx, metrics::BmpMetrics};

#[derive(Debug, Default)]
pub struct BmpTcpInStatusReporter {
    name: String,
    metrics: Arc<BmpMetrics>,
}

impl BmpTcpInStatusReporter {
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
            .fetch_add(1, SeqCst);
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
            .num_bgp_updates_for_unknown_peer
            .fetch_add(1, SeqCst);
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
                .fetch_add(1, SeqCst);
        } else {
            metrics
                .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
                .fetch_add(1, SeqCst);
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
                .fetch_add(1, SeqCst);
        } else {
            metrics
                .num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers
                .fetch_add(1, SeqCst);
        }

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
        let n_valid_announcements = update_report_msg.get_n_valid_announcements();
        let n_valid_withdrawals = update_report_msg.get_n_valid_withdrawals();
        let n_new_prefixes = update_report_msg.get_n_new_prefixes();
        let metrics = self.metrics.router_metrics(update_report_msg.router_id);
        metrics
            .num_received_prefixes
            .fetch_add(update_report_msg.n_new_prefixes, SeqCst);
        metrics.num_stored_prefixes.store(n_new_prefixes, SeqCst);
        metrics.num_announcements.fetch_add(n_valid_announcements, SeqCst);
        metrics.num_withdrawals.fetch_add(n_valid_withdrawals, SeqCst);
    }
}

pub struct UpdateReportMessage {
    router_id: Arc<RouterId>,
    n_new_prefixes: usize,
    n_valid_announcements: usize,
    n_valid_withdrawals: usize,
    n_invalid_announcements: usize,
    n_invalid_withdrawals: usize,
    last_invalid_announcement: Option<ParseError>,
    last_invalid_withdrawal: Option<ParseError>
}

impl UpdateReportMessage {
    pub fn new(router_id: Arc<RouterId>) -> Self {
        Self {
            router_id,
            n_new_prefixes: 0,
            n_valid_announcements: 0,
            n_valid_withdrawals: 0,
            n_invalid_announcements: 0,
            n_invalid_withdrawals: 0,
            last_invalid_announcement: None,
            last_invalid_withdrawal: None
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

    pub fn get_n_new_prefixes(&self) -> usize {
        self.n_new_prefixes
    }

    pub fn get_n_valid_announcements(&self) -> usize {
        self.n_valid_announcements
    }

    pub fn get_n_invalid_announcements(&self) -> usize {
        self.n_invalid_announcements
    }

    pub fn get_n_valid_withdrawals(&self) -> usize {
        self.n_valid_withdrawals
    }

    pub fn get_n_invalid_withdrawals(&self) -> usize {
        self.n_invalid_withdrawals
    }

    pub fn get_last_invalid_announcement(&self) -> Option<ParseError> {
        self.last_invalid_announcement
    }

    pub fn get_last_invalid_withdrawal(&self) -> Option<ParseError> {
        self.last_invalid_withdrawal
    }
}

impl UnitStatusReporter for BmpTcpInStatusReporter {}

impl AnyStatusReporter for BmpTcpInStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for BmpTcpInStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpTcpInStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
