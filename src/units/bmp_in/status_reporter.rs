use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
};

use log::error;

use crate::{
    common::status_reporter::{sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter},
    payload::RouterId,
};

use super::metrics::{BmpInMetrics, RouterMetrics};

#[derive(Debug, Default)]
pub struct BmpInStatusReporter {
    name: String,
    metrics: Arc<BmpInMetrics>,
}

impl BmpInStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BmpInMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    #[cfg(feature = "router-list")]
    pub fn metrics(&self) -> Arc<BmpInMetrics> {
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
            router_metrics
                .num_invalid_bmp_messages
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn internal_error<T: Display>(&self, err: T) {
        sr_log!(error: self, "Internal error: {}", err);
    }
}

impl UnitStatusReporter for BmpInStatusReporter {}

impl AnyStatusReporter for BmpInStatusReporter {}

impl Chainable for BmpInStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpInStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
