use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use mqtt::{
    ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event,
    Incoming, MqttOptions, NetworkOptions, Outgoing, PubAck, QoS,
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
    metrics::Target,
    payload::Update,
    roto_runtime::types::{LogEntry, OutputStreamMessage},
    targets::{mqtt::config::ClientId, Target::Mqtt},
    tests::util::{
        assert_json_eq,
        internal::{enable_logging, get_testable_metrics_snapshot},
    }
};

use super::{
    config::{Config, Destination},
    connection::{
        Client, Connection, ConnectionFactory, EventLoop, MqttPollResult,
    },
    metrics::MqttMetrics,
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
#[ignore = "this is based on old topics"]
fn generate_correct_json_for_publishing_from_output_stream_roto_type_value() {
    // Given an MQTT target runner
    let (runner, _) = mk_mqtt_runner();

    // And a payload that should be published
    //let output_stream = Arc::new(mk_roto_output_stream_payload());
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
async fn connection_established() {
    enable_logging("trace");

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task();

    let metrics = status_reporter.metrics();

    assert_metrics(&metrics, (0, 0, 0));

    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    assert_metrics(&metrics, (0, 0, 0));
}

// MQTT message publication can be done in one of three ways depending on the
// QoS setting:
//   - QoS 0: "At most once delivery" - Fire and forget, there is no
//            confirmation that the message was actually published. The best
//            we could do is detect if it has been sent by the rumqttc library
//            and not just queued for sending.
//   - QoS 1: "At least once delivery" - The remote MQTT broker should confirm
//            receipt of the packet by sending back a PUBACK message.
//   - QoS 2: "Exactly once delivery" - The remote MQTT broker should confirm
//            receipt of the message with a PUBREC message and a PUBCOMP
//            message.
#[tokio::test]
#[ignore = "needs refactoring because of unbounded channel" ]
async fn publish_msg() {
    enable_logging("trace");

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task();

    let metrics = status_reporter.metrics();

    assert_metrics(&metrics, (0, 0, 0));

    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;
    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_in_flight_count") == 0,
        "mqtt_target_in_flight_count != 0",
    )
    .await;

    let test_output_stream_message = mk_roto_output_stream_payload();

    let payload = Update::OutputStream(smallvec::smallvec![test_output_stream_message]);


    runner.direct_update(payload).await;

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_in_flight_count") == 1,
        "mqtt_target_in_flight_count != 1",
    )
    .await;

    // Simulate acknowledgement by the remote broker of the publication
    // attempt.
    client.simulate_publish_ack();

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_in_flight_count") == 0,
        "mqtt_target_in_flight_count != 0",
    )
    .await;
    assert_metric(
        &metrics,
        |m| {
            m.with_labels::<usize>(
                "mqtt_target_publish_count",
                &[("topic", "rotonda/my-topic")],
            ) == 1
        },
        "mqtt_target_publish_count != 1",
    )
    .await;

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    assert_metrics(&metrics, (0, 0, 0));
}

// Not tested here because the real rumqttc library event loop saves unsent
// messages on connection error and sends them once it reconnects, there's
// nothing to unit test without the real rumqttc library event loop and that
// cannot be made to run without making a real outbound connection.
// #[tokio::test] async fn publishing_resumes_after_reconnect() {}
//
// #[tokio::test] #[ignore = "to do"] async fn publishing_errors_are_counted()
// {}
//
// #[tokio::test] #[ignore = "to do"] async fn
// retryable_publishing_error_is_retried() {}

#[tokio::test]
#[ignore = "to do"]
async fn end_to_end_time_metric_is_reported_correctly() {}

#[tokio::test]
async fn mqtt_target_can_be_reconfigured_while_running() {
    enable_logging("trace");

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task();

    let metrics = status_reporter.metrics();

    assert_metrics(&metrics, (0, 0, 0));

    // Simulate a successful connection to the MQTT broker
    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);
    assert_eq!(client.broker_addr(), &("mockhost".to_string(), 1883));

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;

    // Reconfigure the MQTT target
    let mut config = mk_mqtt_runner_config();
    config.destination =
        Destination::try_from("othermockhost:12345".to_string()).unwrap();
    let new_config = Mqtt(config.into());
    cmd_tx
        .send(TargetCommand::Reconfigure { new_config })
        .await
        .unwrap();

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 0,
        "mqtt_target_connection_established != 0",
    )
    .await;

    // Simulate a successful connection to the MQTT broker
    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);
    assert_eq!(client.broker_addr(), &("othermockhost".to_string(), 12345));

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    assert_metrics(&metrics, (0, 0, 0));
}

#[tokio::test]
async fn connection_refused() {
    enable_logging("trace");

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task();

    let metrics = status_reporter.metrics();

    assert_metrics(&metrics, (0, 0, 0));

    // Simulate the various ways a connection attempt can be refused
    use ConnectReturnCode::*;
    let mut num_expected_errors = 0;
    for reason in [
        BadClientId,
        BadUserNamePassword,
        NotAuthorized,
        RefusedProtocolVersion,
        ServiceUnavailable,
    ] {
        let client = assert_client_becomes_available(&runner).await;
        client.simulate_connect_ack(reason);

        num_expected_errors += 1;

        assert_metric(
            &metrics,
            |m| {
                m.with_name::<usize>("mqtt_target_connection_error_count")
                    == num_expected_errors
            },
            format!(
                "mqtt_target_connection_error_count != {num_expected_errors}"
            ),
        )
        .await;
    }

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    assert_metrics(&metrics, (5, 0, 0));
}

#[tokio::test]
async fn connection_loss_and_reconnect() {
    enable_logging("trace");

    let (join_handle, runner, status_reporter, cmd_tx) =
        mk_mqtt_runner_task();

    let metrics = status_reporter.metrics();

    assert_metrics(&metrics, (0, 0, 0));

    // Simulate a successful connection to the MQTT broker
    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;
    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_lost_count") == 0,
        "mqtt_target_connection_lost_count != 0",
    )
    .await;

    // Simulate an error while connected to the MQTT broker
    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_err(ConnectionError::NetworkTimeout);

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 0,
        "mqtt_target_connection_established != 0",
    )
    .await;
    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_error_count") == 1,
        "mqtt_target_connection_error_count != 1",
    )
    .await;
    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_lost_count") == 1,
        "mqtt_target_connection_lost_count != 1",
    )
    .await;

    // Simulate a successful re-connection to the MQTT broker
    let client = assert_client_becomes_available(&runner).await;
    client.simulate_connect_ack(ConnectReturnCode::Success);

    assert_metric(
        &metrics,
        |m| m.with_name::<usize>("mqtt_target_connection_established") == 1,
        "mqtt_target_connection_established != 1",
    )
    .await;

    cmd_tx.send(TargetCommand::Terminate).await.unwrap();
    assert_eq!(join_handle.await.unwrap(), Err(Terminated));

    assert_metrics(&metrics, (1, 0, 1));
}

// --- Test helpers ----------------------------------------------------------

#[derive(Clone, Debug)]
struct MockClient {
    broker_addr: (String, u16),
    mock_poll_result_sender: Arc<mpsc::UnboundedSender<MqttPollResult>>,
}

#[async_trait]
impl Client for MockClient {
    type EventLoopType = MockEventLoop;

    fn new(options: MqttOptions, _cap: usize) -> (Self, Self::EventLoopType) {
        let broker_addr = options.broker_address();

        let (event_loop, mock_poll_result_sender) =
            MockEventLoop::new(options);

        let res = Self {
            broker_addr,
            mock_poll_result_sender: Arc::new(mock_poll_result_sender),
        };

        (res, event_loop)
    }

    async fn publish<S, V>(
        &self,
        _topic: S,
        _qos: QoS,
        _retain: bool,
        _payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send,
    {
        let publish_event = Ok(Event::Outgoing(Outgoing::Publish(0)));
        self.mock_poll_result_sender.send(publish_event).unwrap();
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), ClientError> {
        // NO OP
        Ok(())
    }
}

impl MockClient {
    /// The MockClient can become stale if its link to the mock event loop,
    /// through which it sends events to simulate, becomes closed. This can
    /// happen if a connection failure or disconnection occurs and the event
    /// loop has terminated but the client instance being used by a test
    /// hasn't yet been updated to the new one that will be created for the
    /// subsequent connection attempt.
    fn is_stale(&self) -> bool {
        self.mock_poll_result_sender.is_closed()
    }

    pub fn simulate_connect_ack(&self, code: ConnectReturnCode) {
        let conn_ack_event =
            Ok(Event::Incoming(Incoming::ConnAck(ConnAck {
                session_present: false,
                code,
            })));
        self.mock_poll_result_sender.send(conn_ack_event).unwrap();
    }

    pub fn simulate_connect_err(&self, conn_err: ConnectionError) {
        self.mock_poll_result_sender.send(Err(conn_err)).unwrap();
    }

    pub fn simulate_publish_ack(&self) {
        let pub_ack_event =
            Ok(Event::Incoming(Incoming::PubAck(PubAck { pkid: 0 })));
        self.mock_poll_result_sender.send(pub_ack_event).unwrap();
    }

    fn broker_addr(&self) -> &(String, u16) {
        &self.broker_addr
    }
}

struct MockEventLoop {
    #[allow(dead_code)]
    options: MqttOptions,
    network_options: NetworkOptions,
    mock_poll_result_rx: mpsc::UnboundedReceiver<MqttPollResult>,
    inflight: Arc<AtomicU16>,
}

impl MockEventLoop {
    fn new(
        options: MqttOptions,
    ) -> (Self, mpsc::UnboundedSender<MqttPollResult>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let res = Self {
            options,
            network_options: NetworkOptions::default(),
            mock_poll_result_rx: rx,
            inflight: Arc::new(AtomicU16::new(0)),
        };
        (res, tx)
    }
}

#[async_trait]
impl EventLoop for MockEventLoop {
    async fn poll(&mut self) -> MqttPollResult {
        self.mock_poll_result_rx.recv().await.unwrap().map(|event| {
            match event {
                Event::Outgoing(Outgoing::Publish(_)) => {
                    self.inflight.fetch_add(1, Ordering::SeqCst);
                }
                Event::Incoming(Incoming::PubAck(PubAck { .. })) => {
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                }
                _ => { /* NO OP */ }
            }
            event
        })
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

    fn inflight(&self) -> u16 {
        self.inflight.load(Ordering::SeqCst)
    }
}

impl ConnectionFactory for MqttRunner<MockClient> {
    type EventLoopType = MockEventLoop;

    type ClientType = MockClient;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> Connection<MockClient> {
        let options = MqttOptions::new(
            config.client_id.clone(),
            config.destination.host.clone(),
            config.destination.port,
        );

        Connection::new(options, Duration::from_secs(1), status_reporter)
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

fn mk_mqtt_runner() -> (MqttRunner<MockClient>, Arc<MqttStatusReporter>) {
    let config = mk_mqtt_runner_config();
    let config = Arc::new(ArcSwap::from_pointee(config));
    MqttRunner::<MockClient>::mock(config, None)
}

#[allow(clippy::type_complexity)]
fn mk_mqtt_runner_task() -> (
    JoinHandle<Result<(), Terminated>>,
    Arc<MqttRunner<MockClient>>,
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
            .do_run::<MqttRunner<MockClient>>(None, cmd_rx, pub_q_rx)
            .await
    });

    (join_handle, runner, status_reporter, cmd_tx)
}

fn mk_config_from_toml(toml: &str) -> Result<Config, toml::de::Error> {
    toml::from_str::<Config>(toml)
}

fn mk_roto_output_stream_payload() -> OutputStreamMessage {
    let mut entry = LogEntry::new();
    entry.custom = Some("test payload".into());
    let ingress_id = 1;
    OutputStreamMessage::entry(LogEntry::new(), Some(ingress_id))

}

async fn assert_metric<D: Display, F: Fn(&Target) -> bool>(
    metrics: &Arc<MqttMetrics>,
    check: F,
    msg: D,
) {
    assert_wait_succeeds(
        || match check(&get_testable_metrics_snapshot(metrics)) {
            true => Some(()),
            false => None,
        },
        format!(
            "Metric check failed: {msg}\nAvailable metrics are:\n{:#?}",
            get_testable_metrics_snapshot(metrics)
        ),
    )
    .await;
}

async fn assert_client_becomes_available(
    runner: &Arc<MqttRunner<MockClient>>,
) -> Arc<MockClient> {
    assert_wait_succeeds(
        || {
            runner.client().and_then(|client| match client.is_stale() {
                false => Some(client),
                true => None,
            })
        },
        "MQTT client did not become available in the time allowed",
    )
    .await
}

async fn assert_wait_succeeds<D: Display, R, F: Fn() -> Option<R>>(
    check: F,
    msg: D,
) -> R {
    const MAX_WAIT: Duration = Duration::from_secs(3);

    let start_time = Instant::now();
    loop {
        if let Some(res) = check() {
            return res;
        } else if Instant::now().duration_since(start_time) < MAX_WAIT {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            break;
        }
    }

    panic!("{}", msg);
}

fn assert_metrics(
    metrics: &Arc<MqttMetrics>,
    expected_values: (usize, usize, usize),
) {
    let metrics_snapshot = get_testable_metrics_snapshot(metrics);
    let actual_values = (
        metrics_snapshot
            .with_name::<usize>("mqtt_target_connection_error_count"),
        metrics_snapshot
            .with_name::<usize>("mqtt_target_connection_established"),
        metrics_snapshot
            .with_name::<usize>("mqtt_target_connection_lost_count"),
    );
    assert_eq!(expected_values, actual_values);
}
