use std::{fmt::Display, net::SocketAddr, sync::Arc};

use log::error;

use crate::common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, TargetStatusReporter,
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

    pub fn write_error<T: Display>(&self, router_addr: SocketAddr, err: T) {
        sr_log!(
            error: self,
            "Failed to write BMP message file for router '{}': {}",
            router_addr,
            err
        );
    }
}

impl TargetStatusReporter for BmpFsOutStatusReporter {}

impl AnyStatusReporter for BmpFsOutStatusReporter {}

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
