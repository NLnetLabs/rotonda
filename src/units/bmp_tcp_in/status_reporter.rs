use std::{
    fmt::Display,
    net::SocketAddr,
    sync::{atomic::Ordering::SeqCst, Arc},
};

use log::{debug, error, info, trace, warn};

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

    pub fn _typed_metrics(&self) -> Arc<BmpTcpInMetrics> {
        self.metrics.clone()
    }

    pub fn bind_error<T: Display>(&self, listen_addr: &str, err: T) {
        sr_log!(warn: self, "Error while listening for connections on {}: {}", listen_addr, err);
    }

    pub fn listener_listening(&self, server_uri: &str) {
        sr_log!(info: self, "Listening for connections on {}", server_uri);
        self.metrics.listener_bound_count.fetch_add(1, SeqCst);
    }

    pub fn listener_connection_accepted(&self, router_addr: SocketAddr) {
        sr_log!(debug: self, "Router connected from {}", router_addr);
        self.metrics.connection_accepted_count.fetch_add(1, SeqCst);
    }

    pub fn listener_io_error<T: Display>(&self, err: T) {
        sr_log!(warn: self, "Error while listening for connections: {}", err);
    }

    pub fn router_connection_lost(&self, router_id: &Arc<RouterId>) {
        sr_log!(debug: self, "Router connection lost: {}", router_id);
        self.metrics.connection_lost_count.fetch_add(1, SeqCst);
        self.metrics.remove_router(router_id);
    }

    pub fn router_connection_aborted<T: Display>(
        &self,
        router_id: &Arc<RouterId>,
        err: T,
    ) {
        sr_log!(warn: self, "Router connection aborted: {}. Reason: {}", router_id, err);
        self.metrics.connection_lost_count.fetch_add(1, SeqCst);
        self.metrics.remove_router(router_id);
    }

    pub fn router_id_changed(
        &self,
        old_router_id: Arc<RouterId>,
        new_router_id: Arc<RouterId>,
    ) {
        sr_log!(debug: self, "Router id changed from '{}' to '{}'", old_router_id, new_router_id);
    }

    pub fn receive_io_error<T: Display>(
        &self,
        router_id: Arc<RouterId>,
        err: T,
    ) {
        sr_log!(warn: self, "Error while receiving BMP messages: {}", err);
        self.metrics
            .router_metrics(router_id)
            .num_receive_io_errors
            .fetch_add(1, SeqCst);
    }

    pub fn message_received(
        &self,
        router_id: Arc<RouterId>,
        rfc_7854_msg_type_code: u8,
    ) {
        sr_log!(trace: self, "BMP message received from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_bmp_messages_received[rfc_7854_msg_type_code as usize]
            .fetch_add(1, SeqCst);
    }

    pub fn message_processed(&self, router_id: Arc<RouterId>) {
        sr_log!(trace: self, "BMP message processed from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_bmp_messages_processed
            .fetch_add(1, SeqCst);
    }

    pub fn message_processing_failure(&self, router_id: Arc<RouterId>) {
        sr_log!(trace: self, "BMP message processing failed for message from router '{}'", router_id);
        self.metrics
            .router_metrics(router_id)
            .num_invalid_bmp_messages
            .fetch_add(1, SeqCst);
    }

}

impl UnitStatusReporter for BmpTcpInStatusReporter {}

impl AnyStatusReporter for BmpTcpInStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

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
