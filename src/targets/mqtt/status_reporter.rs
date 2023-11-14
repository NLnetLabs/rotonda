use std::{
    fmt::{Debug, Display},
    sync::{atomic::Ordering::SeqCst, Arc},
    time::Duration,
};

use chrono::{DateTime, Utc};
use log::{debug, info, trace, warn};

use crate::common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, TargetStatusReporter,
};

use super::{config::Destination, metrics::MqttMetrics};

#[derive(Debug, Default)]
pub struct MqttStatusReporter {
    name: String,
    metrics: Arc<MqttMetrics>,
}

impl MqttStatusReporter {
    pub fn new<T: Display>(name: T, metrics: Arc<MqttMetrics>) -> Self {
        Self {
            name: format!("{}", name),
            metrics,
        }
    }

    pub fn metrics(&self) -> Arc<MqttMetrics> {
        self.metrics.clone()
    }

    pub fn connecting(&self, broker_address: &Destination) {
        sr_log!(debug: self, "Connecting to MQTT server {}", broker_address);
    }

    pub fn connected(&self, broker_address: &Destination) {
        sr_log!(info: self, "Connected to MQTT server at {}", broker_address);
        self.metrics.connection_state.store(true, SeqCst);
    }

    pub fn connection_error<T: Display>(&self, err: T) {
        sr_log!(warn: self, "MQTT connection error: {}", err);
    }

    pub fn reconnecting(&self, connect_retry_secs: Duration) {
        sr_log!(
            info: self,
            "Reconnecting in {} seconds",
            connect_retry_secs.as_secs()
        );
        self.metrics.connection_state.store(false, SeqCst);
    }

    pub fn publishing<T: Display, C: Display>(&self, topic: T, content: C) {
        self.metrics.in_flight_count.fetch_add(1, SeqCst);

        sr_log!(
            trace: self,
            "Publishing message {} to topic {}",
            content, topic
        );
    }

    pub fn publish_ok(&self, topic: String, received: DateTime<Utc>) {
        let delay = Utc::now() - received;

        sr_log!(
            debug: self,
            "Published message to topic {}",
            topic
        );

        let metrics = self.metrics.topic_metrics(Arc::new(topic));

        metrics.publish_counts.fetch_add(1, SeqCst);
        metrics
            .last_e2e_delay
            .store(delay.num_milliseconds(), SeqCst);

        self.metrics.in_flight_count.fetch_sub(1, SeqCst);
    }

    pub fn publish_error<T: Display>(&self, err: T) {
        sr_log!(warn: self, "Publishing failed: {}", err);
        self.metrics.transmit_error_count.fetch_add(1, SeqCst);
        self.metrics.in_flight_count.fetch_sub(1, SeqCst);
    }
}

impl TargetStatusReporter for MqttStatusReporter {}

impl AnyStatusReporter for MqttStatusReporter {
    fn metrics(&self) -> Option<Arc<dyn crate::metrics::Source>> {
        Some(self.metrics.clone())
    }
}

impl Chainable for MqttStatusReporter {
    fn add_child<T: Display>(&self, child_name: T) -> Self {
        Self::new(self.link_names(child_name), self.metrics.clone())
    }
}

impl Named for MqttStatusReporter {
    fn name(&self) -> &str {
        &self.name
    }
}
