use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::SeqCst},
    Arc,
};

use crate::{
    common::frim::FrimMap,
    comms::GraphStatus,
    metrics::{
        self, util::append_labelled_metric, Metric, MetricType, MetricUnit,
    },
};

#[derive(Debug, Default)]
pub struct MqttMetrics {
    pub connection_established_state: AtomicBool,
    pub connection_lost_count: AtomicUsize,
    pub connection_error_count: AtomicUsize,
    pub in_flight_count: AtomicUsize,
    pub transmit_error_count: AtomicUsize,
    topics: Arc<FrimMap<Arc<String>, Arc<TopicMetrics>>>,
}

impl MqttMetrics {
    pub fn topic_metrics(&self, topic: Arc<String>) -> Arc<TopicMetrics> {
        #[allow(clippy::unwrap_or_default)]
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
        match self.connection_established_state.load(SeqCst) {
            true => {
                format!(
                    "in-flight: {}\npublished: {}\nerrors: {}",
                    self.in_flight_count.load(SeqCst),
                    self.topics.guard().iter().fold(0, |acc, v| acc
                        + v.1.publish_counts.load(SeqCst)),
                    self.transmit_error_count.load(SeqCst),
                )
            }
            false => "N/A".to_string(),
        }
    }

    fn okay(&self) -> Option<bool> {
        Some(self.connection_established_state.load(SeqCst))
    }
}

impl MqttMetrics {
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const CONNECTION_ESTABLISHED_METRIC: Metric = Metric::new(
        "mqtt_target_connection_established",
        "the state of the connection to the MQTT broker: 0=down, 1=up",
        MetricType::Gauge,
        MetricUnit::State,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const CONNECTION_LOST_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_connection_lost_count",
        "the number of times the connection to the MQTT broker was lost",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const CONNECTION_ERROR_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_connection_error_count",
        "the number of times an error occurred with the connection to the MQTT broker",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const PUBLISH_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_publish_count",
        "the number of messages published to the MQTT broker per topic",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const IN_FLIGHT_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_in_flight_count",
        "the number of messages currently in flight",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TRANSMIT_ERROR_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_transmit_error_count",
        "the number of messages that could not be published to the MQTT broker",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
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
            &Self::CONNECTION_ESTABLISHED_METRIC,
            Some(unit_name),
            self.connection_established_state.load(SeqCst) as u8,
        );
        target.append_simple(
            &Self::CONNECTION_LOST_COUNT_METRIC,
            Some(unit_name),
            self.connection_lost_count.load(SeqCst),
        );
        target.append_simple(
            &Self::CONNECTION_ERROR_COUNT_METRIC,
            Some(unit_name),
            self.connection_error_count.load(SeqCst),
        );
        target.append_simple(
            &Self::IN_FLIGHT_COUNT_PER_TOPIC_METRIC,
            Some(unit_name),
            self.in_flight_count.load(SeqCst),
        );
        target.append_simple(
            &Self::TRANSMIT_ERROR_COUNT_METRIC,
            Some(unit_name),
            self.transmit_error_count.load(SeqCst),
        );
        for (topic, metrics) in self.topics.guard().iter() {
            let topic = topic.as_str();
            append_labelled_metric(
                unit_name,
                target,
                "topic",
                topic,
                Self::PUBLISH_COUNT_PER_TOPIC_METRIC,
                metrics.publish_counts.load(SeqCst),
            );
            append_labelled_metric(
                unit_name,
                target,
                "topic",
                topic,
                Self::LAST_END_TO_END_DELAY_PER_ROUTER_METRIC,
                metrics.last_e2e_delay.load(SeqCst),
            );
        }
    }
}
