use std::fmt::Display;
use std::sync::{atomic::Ordering::Relaxed, Arc};

use log::{debug, trace, warn};

use crate::common::status_reporter::{
    AnyStatusReporter, Chainable, Named, sr_log, UnitStatusReporter
};

use super::metrics::BgpTcpInMetrics;

pub struct BgpTcpInStatusReporter {
    name: String,
    metrics: Arc<BgpTcpInMetrics>,
}

impl BgpTcpInStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BgpTcpInMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics
        }
    }

    pub fn listener_listening(&self, server_uri: &str) {
        sr_log!(trace: self, "Listening for connections on: {}", server_uri);
        self.metrics.listener_bound_count.fetch_add(1, Relaxed);
    }
    
}

impl UnitStatusReporter for BgpTcpInStatusReporter { }
impl AnyStatusReporter for BgpTcpInStatusReporter { }

impl Chainable for BgpTcpInStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BgpTcpInStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
