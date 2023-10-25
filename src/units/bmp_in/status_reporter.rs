use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
};

use log::{debug, error};

use crate::{
    common::status_reporter::{
        sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
    },
    payload::RouterId,
};

use super::metrics::BmpInMetrics;

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
    pub fn typed_metrics(&self) -> Arc<BmpInMetrics> {
        self.metrics.clone()
    }

    pub fn router_id_changed(
        &self,
        old_router_id: Arc<RouterId>,
        new_router_id: Arc<RouterId>,
    ) {
        sr_log!(debug: self, "Router id changed from '{}' to '{}'", old_router_id, new_router_id);
    }

    pub fn invalid_bmp_message_received(&self, router_id: Arc<RouterId>) {
        self.metrics
            .router_metrics(router_id)
            .num_invalid_bmp_messages
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn internal_error<T: Display>(&self, err: T) {
        sr_log!(error: self, "Internal error: {}", err);
    }
}

impl UnitStatusReporter for BmpInStatusReporter {}

impl AnyStatusReporter for BmpInStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

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
