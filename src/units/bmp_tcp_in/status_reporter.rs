use std::{
    fmt::Display,
    net::SocketAddr,
    sync::{atomic::Ordering::SeqCst, Arc},
};

use log::{debug, error, info, trace};

use crate::{
    common::status_reporter::{
        sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
    },
    payload::RouterId,
};

use super::metrics::BmpTcpInMetrics;

#[derive(Debug, Default)]
pub struct BmpTcpInStatusReporter {
    name: String,
    metrics: Arc<BmpTcpInMetrics>,
}

impl BmpTcpInStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BmpTcpInMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    #[cfg(feature = "router-list")]
    pub fn typed_metrics(&self) -> Arc<BmpTcpInMetrics> {
        self.metrics.clone()
    }

    pub fn listener_listening(&self, server_uri: &str) {
        sr_log!(info: self, "Listening for connections on {}", server_uri);
        self.metrics.listener_bound_count.fetch_add(1, SeqCst);
    }

    pub fn listener_connection_accepted(&self, router_addr: SocketAddr) {
        sr_log!(debug: self, "Router connected from {}", router_addr);
        self.metrics.connection_accepted_count.fetch_add(1, SeqCst);
    }

    pub fn router_id_changed(
        &self,
        old_router_id: Arc<RouterId>,
        new_router_id: Arc<RouterId>,
    ) {
        sr_log!(debug: self, "Router id changed from '{}' to '{}'", old_router_id, new_router_id);
    }

    pub fn bmp_message_received(&self, router_id: Arc<RouterId>) {
        sr_log!(trace: self, "BMP message received from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_bmp_messages_received
            .fetch_add(1, SeqCst);
    }

    pub fn bmp_message_processed(&self, router_id: Arc<RouterId>) {
        sr_log!(trace: self, "BMP message processed from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_bmp_messages_received
            .fetch_add(1, SeqCst);
    }

    pub fn invalid_bmp_message_received(&self, router_id: Arc<RouterId>) {
        sr_log!(trace: self, "Invalid BMP message received from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_invalid_bmp_messages
            .fetch_add(1, SeqCst);
    }

    pub fn message_filtering_failure<T: Display>(&self, err: T) {
        sr_log!(error: self, "Filtering error: {}", err);
    }

    pub fn internal_error<T: Display>(&self, err: T) {
        sr_log!(error: self, "Internal error: {}", err);
    }
}

impl UnitStatusReporter for BmpTcpInStatusReporter {}

impl AnyStatusReporter for BmpTcpInStatusReporter {}
//     fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
//         Some(self.metrics.clone())
//     }
// }

impl Chainable for BmpTcpInStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for BmpTcpInStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
