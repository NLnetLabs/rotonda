use std::{
    fmt::Display,
    sync::{atomic::Ordering::SeqCst, Arc},
};

use log::error;

//use roto::types::builtin::SourceId;

use crate::{
    common::status_reporter::{
        sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
    },
    ingress::IngressId,
    // payload::FilterError
};

use super::metrics::RotoFilterMetrics;

#[derive(Debug, Default)]
pub struct RotoFilterStatusReporter {
    name: String,
    metrics: Arc<RotoFilterMetrics>,
}

#[allow(dead_code)]
impl RotoFilterStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<RotoFilterMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    // We don't have an init_per_proxy_metrics() fn in this struct because we
    // don't have a moment where we "handle" a new router when we are
    // filtering, we just pass everything through looking at specific details
    // of the BMP messages not caring if they are for a not seen before router
    // or for an existing router. Instead we use the pattern
    // metric.entry().and_modify().or_insert(1) to ensure that the key exists
    // if it didn't already.

    //pub fn message_filtered(&self, source_id: SourceId) {
    pub fn message_filtered(&self, ingress_id: IngressId) {
        self.metrics
            .router_metrics(ingress_id)
            .num_filtered_messages
            .fetch_add(1, SeqCst);

        self.metrics.num_filtered_messages.fetch_add(1, SeqCst);
    }

    /*
    pub fn message_filtering_failure(&self, err: &FilterError) {
        sr_log!(error: self, "Filtering error: {}", err);
    }
    */
}

impl UnitStatusReporter for RotoFilterStatusReporter {}

impl AnyStatusReporter for RotoFilterStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

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
