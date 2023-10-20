use std::sync::{
    atomic::{self, AtomicBool, AtomicI64, AtomicUsize, Ordering},
    Arc,
};

use crate::{
    common::frim::FrimMap,
    comms::GraphStatus,
    metrics::{self, util::append_per_router_metric, Metric, MetricType, MetricUnit},
};

#[derive(Debug, Default)]
pub struct MqttMetrics {
    pub connection_established: AtomicBool,
    pub connection_lost_count: AtomicUsize,
    pub in_flight_count: AtomicUsize,
    pub transmit_error_count: AtomicUsize,
    topics: Arc<FrimMap<Arc<String>, Arc<TopicMetrics>>>,
}

impl MqttMetrics {
    pub fn topic_metrics(&self, topic: Arc<String>) -> Arc<TopicMetrics> {
        self.topics.entry(topic).or_insert_with(Default::default)
    }
}

#[derive(Debug, Default)]
pub struct TopicMetrics {
    pub publish_counts: Arc<AtomicUsize>,
    pub last_e2e_delay: Arc<AtomicI64>,
}

impl GraphStatus for MqttMetrics {
    fn status_text(&self) -> String {
        match self.connection_established.load(Ordering::Relaxed) {
            true => {
                format!(
                    "in-flight: {}\npublished: {}\nerrors: {}",
                    self.in_flight_count.load(Ordering::Relaxed),
                    self.topics
                        .guard()
                        .iter()
                        .fold(0, |acc, v| acc + v.1.publish_counts.load(Ordering::Relaxed)),
                    self.transmit_error_count.load(Ordering::Relaxed),
                )
            }
            false => "N/A".to_string(),
        }
    }

    fn okay(&self) -> Option<bool> {
        Some(self.connection_established.load(Ordering::Relaxed))
    }
}

impl MqttMetrics {
    const UP_METRIC: Metric = Metric::new(
        "mqtt_target_connection_established_count",
        "the number of times the connection to the MQTT broker was established",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const CONNECTION_LOST_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_connection_lost_count",
        "the number of times the connection to the MQTT broker was lost",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const PUBLISH_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_publish_count",
        "the number of messages published to the MQTT broker per topic",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const IN_FLIGHT_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_in_flight_count",
        "the number of messages currently in flight",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const TRANSMIT_ERROR_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_transmit_error_count",
        "the number of messages that could not be published to the MQTT broker",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const LAST_END_TO_END_DELAY_PER_ROUTER_METRIC: Metric = Metric::new(
        "mqtt_target_e2e_duration",
        "the time taken from initial receipt to completed publication for a prefix to the MQTT server",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
}

impl MqttMetrics {
    pub fn new() -> Self {
        MqttMetrics::default()
    }
}

impl metrics::Source for MqttMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        target.append_simple(
            &Self::UP_METRIC,
            Some(unit_name),
            self.connection_established.load(atomic::Ordering::Relaxed) as u8,
        );
        target.append_simple(
            &Self::CONNECTION_LOST_COUNT_METRIC,
            Some(unit_name),
            self.connection_lost_count.load(atomic::Ordering::Relaxed),
        );
        target.append_simple(
            &Self::IN_FLIGHT_COUNT_PER_TOPIC_METRIC,
            Some(unit_name),
            self.in_flight_count.load(atomic::Ordering::Relaxed),
        );
        target.append_simple(
            &Self::TRANSMIT_ERROR_COUNT_METRIC,
            Some(unit_name),
            self.transmit_error_count.load(atomic::Ordering::Relaxed),
        );
        for (topic, metrics) in self.topics.guard().iter() {
            let topic = topic.as_str();
            append_per_router_metric(
                unit_name,
                target,
                topic,
                Self::PUBLISH_COUNT_PER_TOPIC_METRIC,
                metrics.publish_counts.load(Ordering::Relaxed),
            );
            append_per_router_metric(
                unit_name,
                target,
                topic,
                Self::LAST_END_TO_END_DELAY_PER_ROUTER_METRIC,
                metrics.last_e2e_delay.load(Ordering::Relaxed),
            );
        }
    }
}
