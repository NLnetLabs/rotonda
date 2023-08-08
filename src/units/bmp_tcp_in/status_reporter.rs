use std::{
    fmt::Display,
    net::SocketAddr,
    sync::{
        atomic::Ordering::{self, Relaxed},
        Arc,
    },
};

use log::{debug, trace, warn};

use crate::{common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
}};

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

    pub fn listener_listening(&self, server_uri: &str) {
        sr_log!(trace: self, "Listening for connections on: {}", server_uri);
        self.metrics.listener_bound_count.fetch_add(1, Relaxed);
    }

    pub fn listener_connection_accepted(&self, router_addr: SocketAddr) {
        sr_log!(debug: self, "Router connected from: {}", router_addr);
        self.metrics.connection_accepted_count.fetch_add(1, Relaxed);
    }

    pub fn listener_io_error(&self, err: &std::io::Error) {
        sr_log!(warn: self, "Error while listening for connections: {}", err);
    }

    pub fn receive_io_error(&self, router_addr: SocketAddr, err: &std::io::Error) {
        sr_log!(warn: self, "Error while receiving BMP messages: {}", err);
        self.metrics
            .router_metrics(router_addr)
            .num_receive_io_errors
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Ensure only when necessary that metric counters for a new (or changed)
    /// router ID are initialised.
    ///
    /// This must be called prior to calling any other member function that
    /// takes a [RouterId] as input otherwise metrics will not be properly
    /// counted.
    pub fn router_id_changed(&self, router_addr: SocketAddr) {
        self.metrics
            .router_metrics(router_addr)
            .connection_count
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn bmp_message_received(&self, router_addr: SocketAddr) {
        self.metrics
            .router_metrics(router_addr)
            .num_bmp_messages_received
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn router_connection_lost(&self, router_addr: SocketAddr) {
        sr_log!(debug: self, "Router connection lost: {}", router_addr);
        self.metrics.connection_lost_count.fetch_add(1, Relaxed);
        self.metrics.remove_router(router_addr);
    }
}

impl UnitStatusReporter for BmpTcpInStatusReporter {}

impl AnyStatusReporter for BmpTcpInStatusReporter {}

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
