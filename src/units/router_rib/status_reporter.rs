use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
};

use log::error;

use crate::{
    common::status_reporter::{sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter},
    payload::RouterId,
};

use super::metrics::{RouterMetrics, RouterRibMetrics};

#[derive(Debug, Default)]
pub struct RouterRibStatusReporter {
    name: String,
    metrics: Arc<RouterRibMetrics>,
}

impl RouterRibStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<RouterRibMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    #[cfg(feature = "router-list")]
    pub fn metrics(&self) -> Arc<RouterRibMetrics> {
        self.metrics.clone()
    }

    /// Ensure only when necessary that metric counters for a new (or changed)
    /// router ID are initialised.
    ///
    /// This must be called prior to calling any other member function that
    /// takes a [RouterId] as input otherwise metrics will not be properly
    /// counted.
    pub fn router_id_changed(&self, router_id: Arc<RouterId>) -> Arc<RouterMetrics> {
        self.metrics.init_metrics_for_router(router_id)
    }

    pub fn invalid_bmp_message_received(&self, router_id: Arc<RouterId>) {
        if let Some(router_metrics) = self.metrics.router_metrics(router_id) {
            router_metrics.num_invalid_bmp_messages.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn internal_error<T: Display>(&self, err: T) {
        sr_log!(error: self, "Internal error: {}", err);
    }
}

impl UnitStatusReporter for RouterRibStatusReporter {}

impl AnyStatusReporter for RouterRibStatusReporter {}

impl Chainable for RouterRibStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for RouterRibStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
