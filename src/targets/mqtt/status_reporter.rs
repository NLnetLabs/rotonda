use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use log::{debug, info, trace, warn};

use crate::common::status_reporter::{
    sr_log, AnyStatusReporter, Chainable, Named, TargetStatusReporter,
};

use super::metrics::MqttMetrics;

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

    pub fn connecting(&self, server_uri: &str) {
        sr_log!(debug: self, "Connecting to MQTT server {}", server_uri);
    }

    pub fn connected(&self, server_uri: &str) {
        sr_log!(info: self, "Connected to MQTT server {}", server_uri);
        self.metrics
            .connection_established
            .store(true, atomic::Ordering::Relaxed);
    }

    pub fn connection_error<T: Display>(&self, err: T, connect_retry_secs: Duration) {
        sr_log!(warn: self, "MQTT connection error: {}", err);
        sr_log!(
            info: self,
            "Retrying in {} seconds",
            connect_retry_secs.as_secs()
        );
        self.metrics
            .connection_established
            .store(false, atomic::Ordering::Relaxed);
    }

    pub fn publishing<T: Display, C: Display>(&self, topic: T, content: C) {
        self.metrics
            .in_flight_count
            .fetch_add(1, atomic::Ordering::Relaxed);

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

        metrics.publish_counts.fetch_add(1, Ordering::Relaxed);
        metrics
            .last_e2e_delay
            .store(delay.num_milliseconds(), Ordering::Relaxed);

        self.metrics
            .in_flight_count
            .fetch_sub(1, atomic::Ordering::Relaxed);
    }

    pub fn publish_error<T: Display>(&self, err: T) {
        sr_log!(warn: self, "Publishing failed: {}", err);
        self.metrics
            .transmit_error_count
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.metrics
            .in_flight_count
            .fetch_sub(1, atomic::Ordering::Relaxed);
    }
}

impl TargetStatusReporter for MqttStatusReporter {}

impl AnyStatusReporter for MqttStatusReporter {}

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
