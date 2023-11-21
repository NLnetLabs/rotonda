use std::{fmt::Display, sync::Arc, time::Duration};

use super::{metrics::MqttMetrics, status_reporter::MqttStatusReporter};

use crate::{
    common::status_reporter::{AnyStatusReporter, TargetStatusReporter},
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Payload, Update, UpstreamStatus},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mqtt::{
    AsyncClient, ClientError, ConnAck, ConnectReturnCode, Event, EventLoop,
    Incoming, MqttOptions, QoS,
};
use non_empty_vec::NonEmpty;
use roto::types::{outputs::OutputStreamMessage, typevalue::TypeValue};
use serde::Deserialize;
use serde_with::serde_as;
use tokio::{
    sync::mpsc,
    time::{interval, timeout},
};

#[derive(Debug)]
enum MqttError {
    Error(ClientError),
    Timeout,
}

impl Display for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MqttError::Error(err) => err.fmt(f),
            MqttError::Timeout => f.write_str("MQTT Timeout"),
        }
    }
}

pub const DEF_MQTT_PORT: u16 = 1883;

#[derive(Debug, Deserialize)]
pub struct Mqtt {
    /// The set of units to receive messages from.
    sources: NonEmpty<DirectLink>,

    #[serde(flatten)]
    config: Config,
}

#[derive(Debug, Default, Deserialize)]
#[serde(try_from = "String")]
struct Destination {
    /// MQTT server host name or IP address
    host: String,

    /// MQTT server TCP port number
    port: u16,
}

impl TryFrom<String> for Destination {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let (host, port) = match value.split_once(':') {
            Some((host, port)) => (
                host.to_string(),
                port.parse::<u16>().map_err(|err| err.to_string())?,
            ),
            None => (value, DEF_MQTT_PORT),
        };

        if host.is_empty() {
            Err("Host port of MQTT server address must not be empty"
                .to_string())
        } else {
            Ok(Self { host, port })
        }
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize)]
struct Config {
    /// MQTT server host[:port] to publish to
    destination: Destination,

    #[serde(default = "Config::default_qos")]
    qos: i32,

    #[serde(default)]
    client_id: String,

    #[serde(default = "Config::default_topic_template")]
    topic_template: String,

    /// How long to wait in seconds before connecting again if the connection is closed.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_connect_retry_secs")]
    connect_retry_secs: Duration,

    /// How long to wait before timing out an attempt to publish a message.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_publish_max_secs")]
    publish_max_secs: Duration,

    /// How many messages to buffer if publishing encounters delays
    #[serde(default = "Config::default_queue_size")]
    queue_size: u16,
}

impl Config {
    /// The default MQTT quality of service setting to use
    ///   0 - At most once delivery
    ///   1 - At least once delivery
    ///   2 - Exactly once delivery
    pub fn default_qos() -> i32 {
        2
    }

    /// The default re-connect timeout in seconds.
    pub fn default_connect_retry_secs() -> Duration {
        Duration::from_secs(60)
    }

    /// The default publish timeout in seconds.
    pub fn default_publish_max_secs() -> Duration {
        Duration::from_secs(5)
    }

    /// The default MQTT topic prefix.
    pub fn default_topic_template() -> String {
        "rotonda/{id}".to_string()
    }

    /// The default re-connect timeout in seconds.
    pub fn default_queue_size() -> u16 {
        1000
    }
}

struct SenderMsg {
    received: DateTime<Utc>,
    content: String,
    topic: String,
}

impl Mqtt {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        MqttRunner::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

// Being generic over T enables use of a mock file I/O implementation when testing.
struct MqttRunner {
    component: Component,
    config: Arc<Config>,
    sender: Option<mpsc::UnboundedSender<SenderMsg>>,
    status_reporter: Arc<MqttStatusReporter>,
}

impl MqttRunner {
    fn new(config: Config, mut component: Component) -> Self {
        let config = Arc::new(config);

        let metrics = Arc::new(MqttMetrics::new());
        component.register_metrics(metrics.clone());

        let status_reporter =
            Arc::new(MqttStatusReporter::new(component.name(), metrics));

        Self {
            component,
            config,
            sender: None,
            status_reporter,
        }
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let _unit_name = component.name().clone();

        let (tx, rx) = mpsc::unbounded_channel();
        self.sender = Some(tx);

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        let mut create_opts = MqttOptions::new(
            arc_self.config.client_id.clone(),
            arc_self.config.destination.host.clone(),
            arc_self.config.destination.port,
        );
        create_opts
            .set_request_channel_capacity(arc_self.config.queue_size.into());
        create_opts.set_clean_session(false);
        create_opts.set_inflight(1000);
        create_opts.set_keep_alive(Duration::from_secs(20));

        // Create a client & connect
        arc_self.status_reporter.connecting(&format!(
            "{}:{}",
            &arc_self.config.destination.host,
            arc_self.config.destination.port
        ));
        let cap = arc_self.config.queue_size.into();
        let (client, connection) = AsyncClient::new(create_opts, cap);
        let client = Arc::new(client);

        crate::tokio::spawn(
            "mqtt-publisher",
            Self::message_publisher(
                rx,
                arc_self.config.clone(),
                arc_self.status_reporter.clone(),
                client.clone(),
                Some(connection),
                waitpoint,
            ),
        );

        while let Some(cmd) = cmd_rx.recv().await {
            arc_self.status_reporter.command_received(&cmd);
            match cmd {
                TargetCommand::Reconfigure { new_config } => {
                    if let crate::targets::Target::Mqtt(Mqtt {
                        sources: new_sources,
                        ..
                        // config
                    }) = new_config
                    {
                        // Register as a direct update receiver with the new
                        // set of linked gates.
                        arc_self
                            .status_reporter
                            .upstream_sources_changed(sources.len(), new_sources.len());
                        sources = new_sources;
                        for link in sources.iter_mut() {
                            link.connect(arc_self.clone(), false).await.unwrap();
                        }
                    }
                }

                TargetCommand::ReportLinks { report } => {
                    report.set_sources(&sources);
                    report
                        .set_graph_status(arc_self.status_reporter.metrics());
                }

                TargetCommand::Terminate => break,
            }
        }

        let _ = client.try_disconnect();

        arc_self.status_reporter.terminated();
        Err(Terminated)
    }

    async fn message_publisher(
        mut rx: mpsc::UnboundedReceiver<SenderMsg>,
        config: Arc<Config>,
        status_reporter: Arc<MqttStatusReporter>,
        client: Arc<AsyncClient>,
        connection: Option<EventLoop>,
        waitpoint: WaitPoint,
    ) {
        // TODO: support dynamic reconfiguration while we are running, e.g.
        // change of MQTT client settings.
        if let Some(mut connection) = connection {
            let status_reporter = status_reporter.clone();
            let connect_retry_secs = config.connect_retry_secs;
            let server_uri = format!(
                "{}:{}",
                config.destination.host, config.destination.port
            );

            crate::tokio::spawn("MQTT connection", async move {
                let mut interval = interval(connect_retry_secs);
                interval.tick().await; // the first tick completes immediately

                // reconnects should be automatic
                loop {
                    status_reporter.connecting(&server_uri);
                    loop {
                        match connection.poll().await {
                            Ok(Event::Incoming(Incoming::ConnAck(
                                ConnAck {
                                    code: ConnectReturnCode::Success,
                                    ..
                                },
                            ))) => {
                                status_reporter.connected(&server_uri);
                            }

                            Ok(_) => { /* No other events are handled specially at this time */
                            }

                            Err(err) => {
                                status_reporter.connection_error(
                                    err.to_string(),
                                    connect_retry_secs,
                                );
                                interval.tick().await;
                                break;
                            }
                        }
                    }
                }
            });
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        while let Some(SenderMsg {
            received,
            content,
            topic,
        }) = rx.recv().await
        {
            Self::publish_msg(
                status_reporter.clone(),
                Some(client.clone()),
                topic,
                received,
                content,
                config.qos,
                config.publish_max_secs,
                None::<fn() -> Result<(), MqttError>>,
            )
            .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_msg<F>(
        status_reporter: Arc<MqttStatusReporter>,
        client: Option<Arc<AsyncClient>>,
        topic: String,
        received: DateTime<Utc>,
        content: String,
        qos: i32,
        duration: Duration,
        test_publish: Option<F>,
    ) where
        F: Fn() -> Result<(), MqttError> + Send + 'static,
    {
        status_reporter.publishing(&topic, &content);

        match Self::do_publish(
            client,
            &topic,
            content,
            qos,
            duration,
            test_publish,
        )
        .await
        {
            Ok(_) => {
                status_reporter.publish_ok(topic, received);
            }
            Err(err) => {
                status_reporter.publish_error(err);
            }
        }
    }

    async fn do_publish<F>(
        client: Option<Arc<AsyncClient>>,
        topic: &str,
        content: String,
        qos: i32,
        duration: Duration,
        test_publish: Option<F>,
    ) -> Result<(), MqttError>
    where
        F: Fn() -> Result<(), MqttError> + Send + 'static,
    {
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };

        if let Some(client) = client {
            match timeout(
                duration,
                client.publish(topic, qos, false, content),
            )
            .await
            {
                Err(_) => Err(MqttError::Timeout),
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(MqttError::Error(err)),
            }
        } else if let Some(test_publish) = test_publish {
            // instead of using duration as a timeout, use it as a delay
            tokio::time::sleep(duration).await;
            test_publish()
        } else {
            Ok(())
        }
    }

    fn output_stream_message_to_msg(
        &self,
        osm: Arc<OutputStreamMessage>,
    ) -> Option<SenderMsg> {
        if osm.get_name() == self.component.name() {
            match serde_json::to_string(osm.get_record()) {
                Ok(content) => {
                    let topic = self
                        .config
                        .topic_template
                        .replace("{id}", osm.get_topic());
                    return Some(SenderMsg {
                        received: Utc::now(),
                        content,
                        topic,
                    });
                }
                Err(_err) => {
                    // TODO
                }
            }
        }

        None
    }
}

#[async_trait]
impl DirectUpdate for MqttRunner {
    async fn direct_update(&self, update: Update) {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                ..
            }) => {
                // Nothing to do
            }

            Update::Single(Payload {
                value: TypeValue::OutputStreamMessage(osm),
                ..
            }) => {
                if let Some(msg) = self.output_stream_message_to_msg(osm) {
                    if let Err(_err) = self.sender.as_ref().unwrap().send(msg)
                    {
                        // TODO
                    }
                }
            }

            Update::Bulk(payloads) => {
                for payload in payloads {
                    if let Payload {
                        value: TypeValue::OutputStreamMessage(osm),
                        ..
                    } = payload
                    {
                        if let Some(msg) =
                            self.output_stream_message_to_msg(osm)
                        {
                            if let Err(_err) =
                                self.sender.as_ref().unwrap().send(msg)
                            {
                                // TODO
                            }
                        }
                    }
                }
            }

            _ => { /* We may have received the output from another unit, but we are not interested in it */ }
        }
    }
}

impl AnyDirectUpdate for MqttRunner {}

impl std::fmt::Debug for MqttRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttRunner").finish()
    }
}

//------------ Tests ---------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, str::FromStr};

    use bytes::Bytes;
    use roto::types::{
        builtin::{
            BgpUpdateMessage, BuiltinTypeValue, RawRouteWithDeltas,
            RotondaId, RouteStatus, UpdateMessage,
        },
        collections::{BytesRecord, Record},
        typedef::TypeDef,
        typevalue::TypeValue,
    };
    use routecore::{
        addr::Prefix,
        asn::Asn,
        bgp::message::SessionConfig,
        bmp::message::{Message as BmpMsg, PeerType},
    };
    use serde_json::json;

    use crate::{
        bgp::encode::{
            mk_bgp_update, mk_initiation_msg, mk_route_monitoring_msg,
            Announcements, MyPeerType, PerPeerHeader, Prefixes,
        },
        payload::SourceId,
        tests::util::assert_json_eq,
    };

    use super::*;

    #[test]
    fn server_host_config_setting_must_be_provided() {
        let empty = r#""#;
        let empty_destination = r#"destination = """#;
        let destination_with_host_only = r#"destination = "some_host_name""#;
        let destination_with_host_and_invalid_port =
            r#"destination = "some_host_name:invalid_port""#;
        let destination_with_host_and_port =
            r#"destination = "some_host_name:12345""#;

        assert!(mk_config_from_toml(empty).is_err());
        assert!(mk_config_from_toml(empty_destination).is_err());
        assert!(mk_config_from_toml(destination_with_host_only).is_ok());
        assert!(mk_config_from_toml(destination_with_host_and_invalid_port)
            .is_err());
        assert!(mk_config_from_toml(destination_with_host_and_port).is_ok());
    }

    #[test]
    fn generate_correct_json_for_publishing_from_output_stream_roto_type_value(
    ) {
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

    // --- Test helpers -----------------------------------------------------------------------------------------------

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

    fn mk_raw_bmp_payload(bmp_bytes: Bytes) -> Payload {
        let source_id =
            SourceId::SocketAddr("10.0.0.1:1818".parse().unwrap());
        let bmp_msg = BmpMsg::from_octets(bmp_bytes).unwrap();
        let bmp_msg = Arc::new(BytesRecord(bmp_msg));
        let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg));
        Payload::new(source_id, value, None)
    }

    fn mk_raw_route_with_deltas_payload(prefix: Prefix) -> Payload {
        let bytes = bgp_route_announce(prefix);
        let update_msg = UpdateMessage::new(bytes, SessionConfig::modern())
            .unwrap();
        let delta_id = (RotondaId(0), 0);
        let bgp_update_msg =
            Arc::new(BgpUpdateMessage::new(delta_id, update_msg));
        let route = RawRouteWithDeltas::new_with_message_ref(
            (RotondaId(0), 0),
            prefix.into(),
            &bgp_update_msg,
            RouteStatus::InConvergence,
        )
        .with_peer_asn("AS1818".parse().unwrap())
        .with_peer_ip("4.5.6.7".parse().unwrap())
        .with_router_id("test-router".to_string().into());

        let value = TypeValue::Builtin(BuiltinTypeValue::Route(route));
        Payload::new("test", value, None)
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
        let record =
            Record::create_instance_with_sort(&typedef, fields).unwrap();
        Arc::new(OutputStreamMessage::from(record))
    }

    fn bmp_initiate() -> Bytes {
        mk_initiation_msg("test-router", "Mock BMP router")
    }

    fn bmp_peer_up_notification() -> Bytes {
        crate::bgp::encode::mk_peer_up_notification_msg(
            &mk_per_peer_header(),
            "10.0.0.1".parse().unwrap(),
            11019,
            4567,
            111,
            222,
            0,
            0,
            vec![],
            false,
        )
    }

    fn bmp_peer_down_notification() -> Bytes {
        crate::bgp::encode::mk_peer_down_notification_msg(
            &mk_per_peer_header(),
        )
    }

    fn bmp_route_announce(prefix: Prefix) -> Bytes {
        let per_peer_header = mk_per_peer_header();
        let withdrawals = Prefixes::default();
        let announcements = Announcements::from_str(&format!(
            "e [123,456] 10.0.0.1 BLACKHOLE,rt:34:54536,AS34:256:512 {}",
            prefix
        ))
        .unwrap();

        mk_route_monitoring_msg(
            &per_peer_header,
            &withdrawals,
            &announcements,
            &[],
        )
    }

    fn bgp_route_announce(prefix: Prefix) -> Bytes {
        let withdrawals = Prefixes::default();
        let announcements = Announcements::from_str(&format!(
            "e [123,456] 10.0.0.1 BLACKHOLE,rt:34:54536,AS34:256:512 {}",
            prefix
        ))
        .unwrap();
        mk_bgp_update(&withdrawals, &announcements, &[])
    }

    fn mk_per_peer_header() -> PerPeerHeader {
        let peer_type: MyPeerType = PeerType::GlobalInstance.into();
        let peer_flags: u8 = 0;
        let peer_address: IpAddr = IpAddr::from_str("10.0.0.1").unwrap();
        let peer_as: Asn = Asn::from_u32(12345);
        let peer_bgp_id = 0u32.to_be_bytes();
        let peer_distinguisher: [u8; 8] = [0; 8];

        PerPeerHeader {
            peer_type,
            peer_flags,
            peer_distinguisher,
            peer_address,
            peer_as,
            peer_bgp_id,
        }
    }
}
