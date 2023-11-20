use std::{borrow::Cow, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use mqtt::{
    ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event,
    MqttOptions, NetworkOptions, Packet, QoS,
};
use roto::{
    types::{
        collections::Record, outputs::OutputStreamMessage, typedef::TypeDef,
    },
    vm::OutputStreamQueue,
};
use serde_json::json;
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    comms::{DirectUpdate, Terminated},
    manager::TargetCommand,
    payload::Payload,
    targets::mqtt::config::ClientId,
    tests::util::{
        assert_json_eq,
        internal::{enable_logging, get_testable_metrics_snapshot},
    },
};

use super::{
    config::{Config, Destination},
    connection::{Client, Connection, ConnectionFactory, EventLoop},
    status_reporter::MqttStatusReporter,
    target::*,
};

#[test]
fn destination_and_client_id_config_settings_must_be_provided() {
    let empty = r#""#;
    let empty_destination = r#"
        client_id = "some-client-id"
        destination = ""
    "#;
    let empty_client_id = r#"
        client_id = ""
        destination = "some_host_name"
    "#;
    let leading_whitespace_client_id = r#"
        client_id = " "
        destination = "some_host_name"
    "#;
    let destination_with_host_only = r#"
        client_id = "some-client-id"
        destination = "some_host_name"
    "#;
    let destination_with_host_and_invalid_port = r#"
        client_id = "some-client-id"
        destination = "some_host_name:invalid_port"
    "#;
    let destination_with_host_and_port = r#"
        client_id = "some-client-id"
        destination = "some_host_name:12345"
    "#;

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
    let (runner, _) = mk_mqtt_runner();

    // And a payload that should be published
    let output_stream = Arc::new(mk_roto_output_stream_payload());

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
async fn connection_established() {
    enable_logging("trace");

    // Simulate connection establishment.
    static MOCK_POLL_RESULTS: &'static [MockMqttPollResults] =
        &[&[Ok(Event::Incoming(Packet::ConnAck(ConnAck {
            code: ConnectReturnCode::Success,
            session_present: false,
        })))]];

    let (join_handle, _, status_reporter, cmd_tx) =
        mk_mqtt_runner_task(MOCK_POLL_RESULTS);

    const MAX_WAIT: Duration = Duration::from_secs(3);
    let start_time = Instant::now();

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        // dbg!(&metrics);
        if metrics.with_name::<usize>("mqtt_target_connection_established")
            == 1
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        1
    );

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<usize>("mqtt_target_connection_error_count"),
        0
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        0
    );
    // We never lost the connection because we disconnected cleanly.
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_lost_count"),
        0
    );
}

#[tokio::test]
async fn publish_msg() {
    enable_logging("trace");

    // Simulate connection establishment.
    static MOCK_POLL_RESULTS: &'static [MockMqttPollResults] =
        &[&[Ok(Event::Incoming(Packet::ConnAck(ConnAck {
            code: ConnectReturnCode::Success,
            session_present: false,
        })))]];

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task(MOCK_POLL_RESULTS);

    const MAX_WAIT: Duration = Duration::from_secs(3);
    let start_time = Instant::now();

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        // dbg!(&metrics);
        if metrics.with_name::<usize>("mqtt_target_connection_established")
            == 1
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        1
    );

    let test_output_stream_message = mk_roto_output_stream_payload();
    let mut output_stream_queue = OutputStreamQueue::new();
    output_stream_queue.push(test_output_stream_message.clone());
    let payload =
        Payload::from_output_stream_queue(output_stream_queue, None);
    runner.direct_update(payload.into()).await;

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        if metrics.with_name::<usize>("mqtt_target_in_flight_count") == 1 {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        if metrics.with_labels::<usize>(
            "mqtt_target_publish_count",
            &[("topic", "rotonda/my-topic")],
        ) == 1
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(metrics.with_name::<usize>("mqtt_target_in_flight_count"), 0);
    assert_eq!(
        metrics.with_labels::<usize>(
            "mqtt_target_publish_count",
            &[("topic", "rotonda/my-topic")],
        ),
        1
    );

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<usize>("mqtt_target_connection_error_count"),
        0
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        0
    );
    // We never lost the connection because we disconnected cleanly.
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_lost_count"),
        0
    );
}

#[tokio::test]
async fn connection_refused() {
    enable_logging("trace");

    // Simulate 3 critical MQTT issues in a row.
    static MOCK_POLL_RESULTS: &'static [MockMqttPollResults] = &[
        &[Err(MockCriticalConnectionError)],
        &[Err(MockCriticalConnectionError)],
        &[Err(MockCriticalConnectionError)],
    ];

    let (join_handle, _, status_reporter, cmd_tx) =
        mk_mqtt_runner_task(MOCK_POLL_RESULTS);

    const MAX_WAIT: Duration = Duration::from_secs(3);
    let start_time = Instant::now();

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        dbg!(metrics.with_name::<usize>("mqtt_target_connection_error_count"));
        if metrics.with_name::<usize>("mqtt_target_connection_error_count")
            == 3
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<usize>("mqtt_target_connection_error_count"),
        3
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        0
    );
    // We never lost the connection because we never successfully connected.
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_lost_count"),
        0
    );
}

#[tokio::test]
async fn connection_loss_and_reconnect() {
    enable_logging("trace");

    // Simulate connection establishment, then a critical error, then reconnection.
    static MOCK_POLL_RESULTS: &'static [MockMqttPollResults] = &[
        &[
            // First connection, connection established.
            Ok(Event::Incoming(Packet::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                session_present: false,
            }))),
            // First connection, connection lost.
            Err(MockCriticalConnectionError),
        ],
        &[
            // Second connection, connection established.
            Ok(Event::Incoming(Packet::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                session_present: false,
            }))),
        ],
    ];

    let (join_handle, _, status_reporter, cmd_tx) =
        mk_mqtt_runner_task(MOCK_POLL_RESULTS);

    const MAX_WAIT: Duration = Duration::from_secs(3);
    let start_time = Instant::now();

    while Instant::now().duration_since(start_time) < MAX_WAIT {
        let metrics =
            get_testable_metrics_snapshot(&status_reporter.metrics());
        // dbg!(&metrics);
        if metrics.with_name::<usize>("mqtt_target_connection_established")
            == 1
        {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        1
    );

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    let metrics = get_testable_metrics_snapshot(&status_reporter.metrics());
    assert_eq!(
        metrics.with_name::<usize>("mqtt_target_connection_error_count"),
        1
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_established"),
        0
    );
    assert_eq!(
        metrics.with_name::<u8>("mqtt_target_connection_lost_count"),
        1
    );
}

// --- Test helpers -----------------------------------------------------------------------------------------------

/// Zero or more results to return from a mocked MQTT clients event loop.
/// Each result is paired with a connection index which should start at zero.
/// Results are returned in sequence for the current connection index until
/// there are none left and then the mock client will return a pending future
/// indicating that it is waiting for more results (which will never come).
///
/// If results exist for a connection index higher than zero they should be
/// grouped together in the array and come after the results for lower
/// connection indices. Each connection index represents a single MQTT client
/// session.
///
/// As the MQTT event loop is self-healing if there is a critical error (as
/// the real MQTT event loop automatically attempts to reconnect to the broker
/// if needed) these additional connection indices are not about results after
/// the broker connection is lost and re-established. Instead they represent
/// results to send after a deliberate termination of the MQTT client due to
/// target reconfiguration with changed MQTT client settings.
pub(crate) type MockMqttPollResults =
    &'static [Result<Event, MockCriticalConnectionError>];

// rumqttc ConnectionError is neither Copy, Clone, Send or Sync so we can't
// pass it between threads or Tokio tasks at all. However, as the rustdoc
// comment on ConnectionError says "Critical errors during eventloop polling"
// it doesn't really matter which error we simulate as they are all critical.
// So this type is used to instruct the mock event loop to raise a
// ConnectionError and we don't care which one so there's no additional data
// stored with this type. This type does however support passing it across
// thread/task boundaries which means we can specify it in the test thread
// and handle it in the MQTT event loop task.
#[derive(Clone, Copy, Debug)]
pub struct MockCriticalConnectionError;

#[derive(Clone, Debug, Default)]
struct MockClient;

#[async_trait]
impl Client for MockClient {
    type EventLoopType = MockEventLoop;

    fn new(
        options: MqttOptions,
        _cap: usize,
        #[cfg(test)] mock_poll_results: MockMqttPollResults,
    ) -> (Self, Self::EventLoopType) {
        (Self, MockEventLoop::new(options, mock_poll_results))
    }

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
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), ClientError> {
        // NO OP
        Ok(())
    }
}

struct MockEventLoop {
    options: MqttOptions,
    network_options: NetworkOptions,
    mock_poll_results: MockMqttPollResults,
    last_mock_res_idx: usize,
}

impl MockEventLoop {
    fn new(
        options: MqttOptions,
        mock_poll_results: MockMqttPollResults,
    ) -> Self {
        Self {
            options,
            network_options: Default::default(),
            last_mock_res_idx: 0,
            mock_poll_results,
        }
    }
}

#[async_trait]
impl EventLoop for MockEventLoop {
    async fn poll(&mut self) -> Result<Cow<Event>, ConnectionError> {
        match self.mock_poll_results.get(self.last_mock_res_idx) {
            Some(Ok(mock_result)) => {
                self.last_mock_res_idx += 1;
                std::future::ready(Ok(Cow::Borrowed(mock_result))).await
            }

            Some(Err(MockCriticalConnectionError)) => {
                self.last_mock_res_idx += 1;
                // Create any ConnectionError, they are all "critical" according
                // to the rustdoc on ConnectionError.
                std::future::ready(Err(ConnectionError::NetworkTimeout)).await
            }

            None => std::future::pending().await,
        }
    }

    fn mqtt_options(&self) -> &MqttOptions {
        &self.options
    }

    fn network_options(&self) -> NetworkOptions {
        self.network_options.clone()
    }

    fn set_network_options(
        &mut self,
        network_options: NetworkOptions,
    ) -> &mut Self {
        self.network_options = network_options;
        self
    }
}

struct MockConnectionFactory;

impl ConnectionFactory for MockConnectionFactory {
    type EventLoopType = MockEventLoop;

    type ClientType = MockClient;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
        #[cfg(test)] mock_poll_results: MockMqttPollResults,
    ) -> Connection<MockClient> {
        let options = MqttOptions::new(
            config.client_id.clone(),
            config.destination.host.clone(),
            config.destination.port,
        );

        Connection::new(
            options,
            Duration::from_secs(1),
            status_reporter,
            #[cfg(test)]
            mock_poll_results,
        )
    }
}

fn mk_mqtt_runner_config() -> Config {
    let client_id = ClientId("mock".to_string());
    Config {
        client_id,
        connect_retry_secs: Duration::from_secs(2),
        destination: Destination::try_from("mockhost".to_string()).unwrap(),
        publish_max_secs: Duration::from_secs(3),
        queue_size: Config::default_queue_size(),
        topic_template: Config::default_topic_template(),
        ..Default::default()
    }
}

fn mk_mqtt_runner() -> (MqttRunner, Arc<MqttStatusReporter>) {
    let config = mk_mqtt_runner_config();
    let config = Arc::new(ArcSwap::from_pointee(config));
    MqttRunner::mock(config, None)
}

#[allow(clippy::type_complexity)]
fn mk_mqtt_runner_task(
    mock_poll_results: &'static [MockMqttPollResults],
) -> (
    JoinHandle<Result<(), Terminated>>,
    Arc<MqttRunner>,
    Arc<MqttStatusReporter>,
    Sender<TargetCommand>,
) {
    // Warning: If either tx or cmd_tx are dropped the spawned runner will
    // exit, so hold on to them, don't do `let _ =` as that will drop them
    // immediately.
    let (pub_q_tx, pub_q_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::channel(100);

    let config = mk_mqtt_runner_config();
    let config = Arc::new(ArcSwap::from_pointee(config));

    let (runner, status_reporter) = MqttRunner::mock(config, Some(pub_q_tx));
    let runner = Arc::new(runner);

    let spawned_runner = runner.clone();
    let join_handle = tokio::spawn(async move {
        spawned_runner
            .do_run::<MockConnectionFactory>(
                None,
                cmd_rx,
                pub_q_rx,
                mock_poll_results,
            )
            .await
    });

    (join_handle, runner, status_reporter, cmd_tx)
}

fn mk_config_from_toml(toml: &str) -> Result<Config, toml::de::Error> {
    toml::from_str::<Config>(toml)
}

fn mk_roto_output_stream_payload() -> OutputStreamMessage {
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
    OutputStreamMessage::from(record)
}
