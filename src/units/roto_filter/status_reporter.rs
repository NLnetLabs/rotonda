use std::{
    fmt::Display,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};

use crate::common::status_reporter::{AnyStatusReporter, Chainable, Named, UnitStatusReporter};

use super::metrics::RotoFilterMetrics;

#[derive(Debug, Default)]
pub struct RotoFilterStatusReporter {
    name: String,
    metrics: Arc<RotoFilterMetrics>,
}

impl RotoFilterStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<RotoFilterMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    // We don't have an init_per_proxy_metrics() fn in this struct because
    // we don't have a moment where we "handle" a new router when we are
    // filtering, we just pass everything through looking at specific details
    // of the BMP messages not caring if they are for a not seen before router
    // or for an existing router. Instead we use the pattern
    // metric.entry().and_modify().or_insert(1) to ensure that the key exists
    // if it didn't already.

    pub fn message_filtered(&self, router_addr: SocketAddr) {
        self.metrics
            .router_metrics(router_addr)
            .num_filtered_messages
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl UnitStatusReporter for RotoFilterStatusReporter {}

impl AnyStatusReporter for RotoFilterStatusReporter {}

impl Chainable for RotoFilterStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for RotoFilterStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
