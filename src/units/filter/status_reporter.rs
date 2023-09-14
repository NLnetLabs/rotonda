use std::{
    fmt::Display,
    sync::{atomic::Ordering, Arc},
};

use log::{error, warn};

use crate::{
    common::{
        roto::RotoError,
        status_reporter::{sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter},
    },
    payload::{FilterError, SourceId},
};

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

    pub fn message_filtered(&self, source_id: SourceId) {
        if let SourceId::SocketAddr(router_addr) = source_id {
            self.metrics
                .router_metrics(router_addr)
                .num_filtered_messages
                .fetch_add(1, Ordering::Relaxed);
        }

        self.metrics
            .num_filtered_messages
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn message_filtering_failure(&self, err: &FilterError) {
        // Avoid reporting the same error over and over again because the roto
        // script is unusable.
        if !matches!(
            err,
            FilterError::RotoScriptError(RotoError::Unusable { .. })
        ) {
            sr_log!(error: self, "Filtering error: {}", err);
        }
    }

    pub fn filter_load_failure<T: Display>(&self, err: T) {
        sr_log!(warn: self, "Filter could not be loaded and will be ignored: {}", err);
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
