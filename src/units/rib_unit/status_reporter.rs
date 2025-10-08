use std::{
    fmt::Display,
    sync::{atomic::Ordering::SeqCst, Arc},
    time::{Duration, Instant},
};

use log::{error, info, warn};

use crate::{
    common::status_reporter::{
        sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
    },
    ingress::IngressId,
    payload::RouterId,
    roto_runtime::types::FilterName,
};

use super::{metrics::RibUnitMetrics, rib::StoreInsertionEffect};

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

    pub fn filter_name_changed(
        &self,
        old: &FilterName,
        new: Option<&FilterName>,
    ) {
        match new {
            Some(new) => {
                sr_log!(info: self, "Using Roto filter '{}' instead of '{}'", new, old);
            }
            None => {
                sr_log!(info: self, "No longer using Roto filter '{}'", old);
            }
        }
    }

    pub fn insert_failed<P: Display, E: Display>(&self, pfx: P, err: E) {
        sr_log!(error: self, "Failed to upsert prefix {}: {}", pfx, err);
        self.metrics.num_insert_hard_failures.fetch_add(1, SeqCst);
    }

    pub fn insert_ok(
        &self,
        //router_id: Arc<RouterId>,
        ingress_id: IngressId,
        insert_delay: Duration,
        propagation_delay: Duration,
        num_retries: u32,
        change: StoreInsertionEffect,
    ) {
        self.metrics.last_insert_duration_micros.store(
            u64::try_from(insert_delay.as_micros()).unwrap_or(u64::MAX),
            SeqCst,
        );

        self.insert_or_update(
            //router_id,
            ingress_id,
            propagation_delay,
            num_retries,
            change,
        );
    }

    #[allow(dead_code)]
    pub fn update_ok(
        &self,
        //router_id: Arc<RouterId>,
        ingress_id: IngressId,
        update_delay: Duration,
        propagation_delay: Duration,
        num_retries: u32,
        change: StoreInsertionEffect,
    ) {
        self.metrics.last_update_duration_micros.store(
            u64::try_from(update_delay.as_micros()).unwrap_or(u64::MAX),
            SeqCst,
        );

        self.insert_or_update(
            //router_id,
            ingress_id,
            propagation_delay,
            num_retries,
            change,
        );
    }

    fn insert_or_update(
        &self,
        //router_id: Arc<String>,
        ingress_id: IngressId,
        propagation_delay: Duration,
        num_retries: u32,
        change: StoreInsertionEffect,
    ) {
        let router_specific_metrics = self.metrics.router_metrics(ingress_id);
        router_specific_metrics.last_e2e_delay_millis.store(
            u64::try_from(propagation_delay.as_millis()).unwrap_or(u64::MAX),
            SeqCst,
        );
        router_specific_metrics
            .last_e2e_delay_at
            .store(Arc::new(Instant::now()));

        if num_retries > 0 {
            self.metrics
                .num_insert_retries
                .fetch_add(num_retries as usize, SeqCst);
        }

        match change {
            StoreInsertionEffect::RoutesWithdrawn(0)
            | StoreInsertionEffect::RoutesRemoved(0) => {
                self.metrics
                    .num_route_withdrawals_without_announcement
                    .fetch_add(1, SeqCst);
            }

            StoreInsertionEffect::RoutesWithdrawn(n) => {
                self.metrics.num_routes_announced.fetch_sub(n, SeqCst);
                self.metrics.num_routes_withdrawn.fetch_add(n, SeqCst);
            }

            StoreInsertionEffect::RoutesRemoved(n) => {
                self.metrics.num_routes_announced.fetch_sub(n, SeqCst);
                self.metrics.num_items.fetch_sub(n, SeqCst);
            }

            StoreInsertionEffect::RouteAdded => {
                self.metrics.num_routes_announced.fetch_add(1, SeqCst);
                self.metrics.num_unique_prefixes.fetch_add(1, SeqCst);
                self.metrics.num_items.fetch_add(1, SeqCst);
            }

            StoreInsertionEffect::RouteUpdated => {
                self.metrics
                    .num_modified_route_announcements
                    .fetch_add(1, SeqCst);
            }
        }
    }

    #[allow(dead_code)]
    pub fn unique_prefix_count_updated(&self, num_unique_prefixes: usize) {
        self.metrics
            .num_unique_prefixes
            .store(num_unique_prefixes, SeqCst);
    }

    #[allow(dead_code)]
    pub fn message_filtering_failure<T: Display>(&self, err: T) {
        sr_log!(error: self, "Filtering error: {}", err);
    }

    pub fn _filter_load_failure<T: Display>(&self, err: T) {
        sr_log!(warn: self, "Filter could not be loaded and will be ignored: {}", err);
    }
}

impl UnitStatusReporter for RibUnitStatusReporter {}

impl AnyStatusReporter for RibUnitStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

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
