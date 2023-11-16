use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use mqtt::{ClientError, ConnectionError, Event, MqttOptions, Packet, QoS};
use roto::types::{
    collections::Record, outputs::OutputStreamMessage, typedef::TypeDef,
};
use serde_json::json;
use tokio::{sync::mpsc, time::Instant};

use crate::{
    comms::Terminated,
    targets::mqtt::config::ClientId,
    tests::util::{
        assert_json_eq,
        internal::{enable_logging, get_testable_metrics_snapshot},
    }, manager::{TargetCommand, Component},
};

use super::{
    config::Config,
    connection::{Client, Connection, ConnectionFactory, EventLoop},
    status_reporter::MqttStatusReporter,
    target::*,
};

#[test]
fn destination_and_client_id_config_settings_must_be_provided() {
    let empty = r###""###;
    let empty_destination = r###"
        client_id = "some-client-id"
        destination = ""
    "###;
    let empty_client_id = r###"
        client_id = ""
        destination = "some_host_name"
    "###;
    let leading_whitespace_client_id = r###"
        client_id = " "
        destination = "some_host_name"
    "###;
    let destination_with_host_only = r###"
        client_id = "some-client-id"
        destination = "some_host_name"
    "###;
    let destination_with_host_and_invalid_port = r###"
        client_id = "some-client-id"
        destination = "some_host_name:invalid_port"
    "###;
    let destination_with_host_and_port = r###"
        client_id = "some-client-id"
        destination = "some_host_name:12345"
    "###;

    assert!(mk_config_from_toml(empty).is_err());
    assert!(mk_config_from_toml(empty_client_id).is_err());
    assert!(mk_config_from_toml(leading_whitespace_client_id).is_err());
    assert!(mk_config_from_toml(empty_destination).is_err());
    assert!(
        mk_config_from_toml(destination_with_host_and_invalid_port).is_err()
    );

    assert!(mk_config_from_toml(destination_with_host_only).is_ok());
    assert!(mk_config_from_toml(destination_with_host_and_port).is_ok());
}

#[test]
fn generate_correct_json_for_publishing_from_output_stream_roto_type_value() {
    // Given an MQTT target runner
    let runner = mk_mqtt_runner();

    // And a payload that should be published
    let output_stream = mk_roto_output_stream_payload();

    // Then the candidate should be selected for publication
    let SenderMsg { content, topic, .. } =
        runner.output_stream_message_to_msg(output_stream).unwrap();

    // And the topic should be based on the rouuter id recorded with the route, if any
    assert_eq!(topic, "rotonda/my-topic");

    // And the produced message to be published should match the expected JSON format
    let expected_json = json!({
        "some-asn": 1818,
        "some-str": "some-value",
    });

    let actual_json = serde_json::from_str(&content).unwrap();
    assert_json_eq(actual_json, expected_json);
}

#[tokio::test]
async fn connection_refused() {
    enable_logging("trace");
    let mut config = Config::default();
    config.client_id = ClientId("conn-refused".to_string());
    let config = Arc::new(ArcSwap::from_pointee(config));
    let (runner, status_reporter) = MqttRunner::mock(config);
    let runner = Arc::new(runner);

    // If either tx or cmd_tx are dropped the spawned runner will exit, so
    // we don't do let (_, ..) here as that would drop them immediately.
    let (_tx, rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::channel(100);

    let spawned_runner = runner.clone();

    let join_handle = tokio::spawn(async move {
        spawned_runner
            .do_run::<MockConnectionFactory>(None, cmd_rx, rx)
            .await
    });

    const MAX_WAIT: Duration = Duration::from_secs(10);
    let start_time = Instant::now();

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        if metrics.with_name::<usize>("mqtt_target_connection_lost_count") > 0
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert!(
        metrics.with_name::<usize>("mqtt_target_connection_lost_count") > 0
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        0
    );
}

// --- Test helpers -----------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct MockClient;

#[async_trait]
impl Client for MockClient {
    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send,
    {
        todo!()
    }
}

#[derive(Debug)]
struct MockEventLoop {
    options: MqttOptions,
}

impl MockEventLoop {
    fn new(options: MqttOptions) -> Self {
        Self { options }
    }
}

#[async_trait]
impl EventLoop for MockEventLoop {
    async fn poll(&mut self) -> Result<Event, ConnectionError> {
        match self.options.client_id().as_str() {
            "conn-refused" => Err(ConnectionError::ConnectionRefused(
                mqtt::ConnectReturnCode::ServiceUnavailable,
            )),

            _ => Ok(Event::Incoming(Packet::PingReq)),
        }
    }

    fn mqtt_options(&self) -> &MqttOptions {
        &self.options
    }
}

struct MockConnectionFactory;

impl ConnectionFactory for MockConnectionFactory {
    type EventLoopType = MockEventLoop;

    type ClientType = MockClient;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> (MockClient, Connection<MockEventLoop>) {
        let client = MockClient;

        let options = MqttOptions::new(
            config.client_id.clone(),
            config.destination.host.clone(),
            config.destination.port,
        );

        let connection = Connection::new(
            MockEventLoop::new(options),
            Duration::from_secs(1),
            status_reporter,
        );

        (client, connection)
    }
}

fn mk_mqtt_runner() -> MqttRunner {
    let config = Config {
        topic_template: Config::default_topic_template(),
        ..Default::default()
    };
    MqttRunner::new(config, Component::default())
}

fn mk_config_from_toml(toml: &str) -> Result<Config, toml::de::Error> {
    toml::from_str::<Config>(toml)
}

fn mk_roto_output_stream_payload() -> Arc<OutputStreamMessage> {
    let typedef = TypeDef::new_record_type(vec![
        ("name", Box::new(TypeDef::StringLiteral)),
        ("topic", Box::new(TypeDef::StringLiteral)),
        ("some-str", Box::new(TypeDef::StringLiteral)),
        ("some-asn", Box::new(TypeDef::Asn)),
    ])
    .unwrap();

    let fields = vec![
        ("name", "MOCK".into()),
        ("topic", "my-topic".into()),
        ("some-str", "some-value".into()),
        ("some-asn", routecore::asn::Asn::from_u32(1818).into()),
    ];
    let record = Record::create_instance_with_sort(&typedef, fields).unwrap();
    Arc::new(OutputStreamMessage::from(record))
}
