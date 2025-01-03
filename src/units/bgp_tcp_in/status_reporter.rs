use std::fmt::Display;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use log::{debug, info, warn};

use crate::common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, UnitStatusReporter,
};

use super::metrics::BgpTcpInMetrics;

#[derive(Debug, Default)]
pub struct BgpTcpInStatusReporter {
    name: String,
    metrics: Arc<BgpTcpInMetrics>,
}

impl BgpTcpInStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<BgpTcpInMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
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

    pub fn peer_connection_lost(&self, peer_addr: Option<SocketAddr>) {
        if let Some(socket) = peer_addr {
            sr_log!(debug: self, "Router connection lost: {}", socket);
        } else {
            sr_log!(debug: self, "Router without unknown socket address lost");
        }
        self.metrics.connection_lost_count.fetch_add(1, SeqCst);
    }

    pub fn disconnect(&self, peer_addr: IpAddr) {
        sr_log!(debug: self, "Disconnected from: {}", peer_addr);
        self.metrics.disconnect_count.fetch_add(1, SeqCst);
    }
}

impl UnitStatusReporter for BgpTcpInStatusReporter {}

impl AnyStatusReporter for BgpTcpInStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

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
