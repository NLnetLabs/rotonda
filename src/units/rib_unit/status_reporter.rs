use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use log::{debug, error, warn};

use crate::{
    common::status_reporter::{sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter},
    payload::RouterId,
};

use super::metrics::RibUnitMetrics;

#[derive(Debug, Default)]
pub struct RibUnitStatusReporter {
    name: String,
    metrics: Arc<RibUnitMetrics>,
}

impl RibUnitStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<RibUnitMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    pub fn insert_failed<P: Display, E: Display>(&self, pfx: P, err: E) {
        sr_log!(debug: self, "Failed to insert prefix {}: {}", pfx, err);
        self.metrics
            .num_insert_hard_failures
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn insert_ok(
        &self,
        router_id: Arc<RouterId>,
        insert_delay: i64,
        propagation_delay: i64,
        num_retries: u32,
        is_announcement: bool,
        item_count_delta: isize,
    ) {
        self.metrics
            .last_insert_duration
            .store(insert_delay, Ordering::SeqCst);

        self.insert_or_update(router_id, propagation_delay, num_retries, item_count_delta);

        if !is_announcement {
            self.metrics
                .num_route_withdrawals_without_announcement
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn update_ok(
        &self,
        router_id: Arc<RouterId>,
        insert_delay: i64,
        propagation_delay: i64,
        num_retries: u32,
        item_count_delta: isize,
    ) {
        self.metrics
            .last_update_duration
            .store(insert_delay, Ordering::SeqCst);

        self.insert_or_update(router_id, propagation_delay, num_retries, item_count_delta);
    }

    fn insert_or_update(
        &self,
        router_id: Arc<String>,
        propagation_delay: i64,
        num_retries: u32,
        item_count_delta: isize,
    ) {
        let metrics = self.metrics.router_metrics(router_id);
        metrics
            .last_e2e_delay
            .store(propagation_delay, Ordering::SeqCst);
        metrics.last_e2e_delay_at.store(Arc::new(Instant::now()));

        if num_retries > 0 {
            self.metrics
                .num_insert_retries
                .fetch_add(num_retries as usize, Ordering::SeqCst);
        }

        if item_count_delta > 0 {
            self.metrics
                .num_items
                .fetch_add(item_count_delta as usize, Ordering::SeqCst);
            self.metrics
                .num_routes_announced
                .fetch_add(item_count_delta as usize, Ordering::SeqCst);
        } else {
            self.metrics
                .num_items
                .fetch_sub(-item_count_delta as usize, Ordering::SeqCst);
            self.metrics
                .num_routes_withdrawn
                .fetch_add(-item_count_delta as usize, Ordering::SeqCst);
        }
    }

    pub fn update_processed(
        &self,
        new_announcements: usize,
        modified_announcements: usize,
        new_withdrawals: usize,
    ) {
        if new_announcements > 0 {
            self.metrics
                .num_routes_announced
                .fetch_add(new_announcements, Ordering::SeqCst);
        }
        if modified_announcements > 0 {
            self.metrics
                .num_modified_route_announcements
                .fetch_add(modified_announcements, Ordering::SeqCst);
        }
        if new_withdrawals > 0 {
            self.metrics
                .num_routes_withdrawn
                .fetch_add(new_withdrawals, Ordering::SeqCst);
        }
    }

    pub fn unique_prefix_count_updated(&self, num_unique_prefixes: usize) {
        self.metrics
            .num_unique_prefixes
            .store(num_unique_prefixes, Ordering::SeqCst);
    }

    pub fn message_filtering_failure<T: Display>(&self, err: T) {
        sr_log!(error: self, "Filtering error: {}", err);
    }
}

impl UnitStatusReporter for RibUnitStatusReporter {}

impl AnyStatusReporter for RibUnitStatusReporter {}

impl Chainable for RibUnitStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for RibUnitStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
