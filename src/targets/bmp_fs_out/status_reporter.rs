use std::{fmt::Display, sync::Arc};

use log::error;

use crate::{
    common::status_reporter::{
        sr_log, AnyStatusReporter, Chainable, Named, TargetStatusReporter,
    },
    payload::SourceId,
};

use super::metrics::BmpFsOutMetrics;

#[derive(Debug, Default)]
pub struct BmpFsOutStatusReporter {
    name: String,
    metrics: Arc<BmpFsOutMetrics>,
}

impl BmpFsOutStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BmpFsOutMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    pub fn write_error<T: Display>(&self, source_id: SourceId, err: T) {
        sr_log!(
            error: self,
            "Failed to write BMP message file for router '{}': {}",
            source_id,
            err
        );
    }
}

impl TargetStatusReporter for BmpFsOutStatusReporter {}

impl AnyStatusReporter for BmpFsOutStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for BmpFsOutStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpFsOutStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
