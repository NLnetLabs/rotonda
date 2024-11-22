use std::sync::{
    atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering::SeqCst},
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
    pub publish_error_count: AtomicUsize,
    pub in_flight_count: AtomicU16,
    // pub not_acknowledged_count: AtomicUsize,
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
                    self.publish_error_count.load(SeqCst),
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
    const CONNECTION_ESTABLISHED_METRIC: Metric = Metric::new(
        "mqtt_target_connection_established",
        "the state of the connection to the MQTT broker: 0=down, 1=up",
        MetricType::Gauge,
        MetricUnit::State,
    );
    const CONNECTION_LOST_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_connection_lost_count",
        "the number of times the connection to the MQTT broker was lost",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const CONNECTION_ERROR_COUNT_METRIC: Metric = Metric::new(
        "mqtt_target_connection_error_count",
        "the number of times an error occurred with the connection to the MQTT broker",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const PUBLISH_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_publish_count",
        "the number of messages requested for publication to the MQTT broker per topic",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const PUBLISH_ERROR_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_publish_error_count",
        "the number of messages that could not be queued for publication",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const IN_FLIGHT_COUNT_PER_TOPIC_METRIC: Metric = Metric::new(
        "mqtt_target_in_flight_count",
        "the number of messages requested for publication but not yet sent to the MQTT broker per topic",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // The rumqttc library has this count internally but doesn't expose it to
    // us. const PUBLISH_NOT_ACKNOWLEDGED_COUNT_METRIC: Metric = Metric::new(
    //     "mqtt_target_published_not_acknowledged_count", "the number of QoS
    //     1 or QoS 2 messages for which confirmation by the MQTT broker is
    //     pending (QoS 1) or incomplete (QoS 2)", MetricType::Counter,
    //     MetricUnit::Total, );
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
            &Self::PUBLISH_ERROR_COUNT_PER_TOPIC_METRIC,
            Some(unit_name),
            self.publish_error_count.load(SeqCst),
        );
        // target.append_simple(
        //     &Self::PUBLISH_NOT_ACKNOWLEDGED_COUNT_METRIC,
        //     Some(unit_name),
        //     self.not_acknowledged_count.load(SeqCst),
        // );
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
        }
    }
}
