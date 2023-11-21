use std::{ops::ControlFlow, sync::Arc, time::Duration};

use super::{
    config::Config,
    connection::{Client, Connection, ConnectionFactory},
    error::MqttError,
    metrics::MqttMetrics,
    status_reporter::MqttStatusReporter,
};

use crate::{
    common::status_reporter::{AnyStatusReporter, TargetStatusReporter},
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Payload, Update, UpstreamStatus},
    targets::Target,
};

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mqtt::{MqttOptions, QoS};
use non_empty_vec::NonEmpty;
use roto::types::{outputs::OutputStreamMessage, typevalue::TypeValue};
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};

#[derive(Debug, Deserialize)]
pub struct Mqtt {
    /// The set of units to receive messages from.
    sources: NonEmpty<DirectLink>,

    #[serde(flatten)]
    config: Config,
}

#[cfg(test)]
impl From<Config> for Mqtt {
    fn from(config: Config) -> Self {
        let (_gate, mut gate_agent) = crate::comms::Gate::new(0);
        let link = gate_agent.create_link();
        let sources = NonEmpty::new(link.into());
        Self { sources, config }
    }
}

pub(super) struct SenderMsg {
    pub received: DateTime<Utc>,
    pub content: String,
    pub topic: String,
}

impl Mqtt {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        MqttRunner::<mqtt::AsyncClient>::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

// Being generic over T enables use of a mock file I/O implementation when testing.
pub(super) struct MqttRunner<C> {
    component: Component,
    config: Arc<ArcSwap<Config>>,
    client: Arc<ArcSwapOption<C>>,
    pub_q_tx: Option<mpsc::UnboundedSender<SenderMsg>>,
    status_reporter: Arc<MqttStatusReporter>,
}

impl<C: Client> MqttRunner<C>
where
    Self: ConnectionFactory<ClientType = C>,
{
    pub fn new(config: Config, mut component: Component) -> Self {
        let config = Arc::new(ArcSwap::from_pointee(config));

        let metrics = Arc::new(MqttMetrics::new());
        component.register_metrics(metrics.clone());

        let status_reporter =
            Arc::new(MqttStatusReporter::new(component.name(), metrics));

        Self {
            component,
            config,
            client: Default::default(),
            pub_q_tx: None,
            status_reporter,
        }
    }

    #[cfg(test)]
    pub fn mock(
        config: Arc<ArcSwap<Config>>,
        pub_q_tx: Option<mpsc::UnboundedSender<SenderMsg>>,
    ) -> (Self, Arc<MqttStatusReporter>) {
        let metrics = Arc::new(MqttMetrics::new());

        let status_reporter =
            Arc::new(MqttStatusReporter::new("mock", metrics));

        let res = Self {
            component: Default::default(),
            config,
            client: Default::default(),
            pub_q_tx,
            status_reporter: status_reporter.clone(),
        };

        (res, status_reporter)
    }

    #[cfg(test)]
    pub fn client(&self) -> Option<Arc<C>> {
        use std::ops::Deref;
        self.client.load().deref().clone()
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let _unit_name = component.name().clone();

        let (pub_q_tx, pub_q_rx) = mpsc::unbounded_channel();
        self.pub_q_tx = Some(pub_q_tx);

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        arc_self
            .do_run::<Self>(Some(sources), cmd_rx, pub_q_rx)
            .await
    }

    pub async fn do_run<F: ConnectionFactory<ClientType = C>>(
        self: &Arc<Self>,
        mut sources: Option<NonEmpty<DirectLink>>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        mut pub_q_rx: mpsc::UnboundedReceiver<SenderMsg>,
    ) -> Result<(), Terminated>
    where
        <F as ConnectionFactory>::EventLoopType: 'static,
    {
        loop {
            let connection =
                F::connect(&self.config.load(), self.status_reporter.clone());

            if let Err(Terminated) = self
                .process_events(
                    connection,
                    &mut sources,
                    &mut cmd_rx,
                    &mut pub_q_rx,
                )
                .await
            {
                self.status_reporter.terminated();
                return Err(Terminated);
            }
        }
    }

    pub async fn process_events(
        self: &Arc<Self>,
        mut connection: Connection<C>,
        sources: &mut Option<NonEmpty<DirectLink>>,
        cmd_rx: &mut mpsc::Receiver<TargetCommand>,
        pub_q_rx: &mut mpsc::UnboundedReceiver<SenderMsg>,
    ) -> Result<(), Terminated> {
        while connection.active() {
            tokio::select! {
                biased; // Disable tokio::select!() random branch selection

                client = connection.process() => {
                    self.client.store(client.map(Arc::new));
                }

                // If nothing happened above, check for new internal Rotonda target commands
                // to handle.
                cmd = cmd_rx.recv() => {
                    if let Some(cmd) = &cmd {
                        self.status_reporter.command_received(cmd);
                    }

                    match cmd {
                        Some(TargetCommand::Reconfigure { new_config: Target::Mqtt(new_config) }) => {
                            if self.reconfigure(sources, new_config, &mut connection).await.is_break() {
                                connection.disconnect().await;
                            }
                        }

                        Some(TargetCommand::Reconfigure { .. }) => unreachable!(),

                        Some(TargetCommand::ReportLinks { report }) => {
                            if let Some(sources) = sources {
                                report.set_sources(sources);
                            }
                            report.set_graph_status(
                                self.status_reporter.metrics(),
                            );
                        }

                        None | Some(TargetCommand::Terminate) => {
                            connection.disconnect().await;
                            return Err(Terminated);
                        }
                    }
                }

                // And finally if not doing anything else we can process messages
                // waiting in our internal queue to be published, which were
                // enqueued by the direct_update() method below.
                msg = pub_q_rx.recv() => {
                    match msg {
                        Some(SenderMsg {
                            received,
                            content,
                            topic,
                        }) => {
                            Self::publish_msg(
                                self.status_reporter.clone(),
                                connection.client(),
                                topic,
                                received,
                                content,
                                self.config.load().qos,
                                self.config.load().publish_max_secs,
                                None::<fn() -> Result<(), MqttError>>,
                            )
                            .await;
                        }

                        None => {
                            connection.disconnect().await;
                            return Err(Terminated);
                        }
                    }
                }
            }
        }

        self.client.store(None);

        Ok(())
    }

    async fn reconfigure(
        self: &Arc<Self>,
        sources: &mut Option<NonEmpty<DirectLink>>,
        Mqtt {
            sources: new_sources,
            config: new_config,
        }: Mqtt,
        connection: &mut Connection<C>,
    ) -> ControlFlow<()> {
        if let Some(sources) = sources {
            self.status_reporter
                .upstream_sources_changed(sources.len(), new_sources.len());

            *sources = new_sources;

            // Register as a direct update receiver with the new
            // set of linked gates.
            for link in sources.iter_mut() {
                link.connect(self.clone(), false).await.unwrap();
            }
        }

        // Check if the config changes impact the MQTT client
        let config = self.config.load();
        let reconnect = new_config.client_id != config.client_id
            || new_config.destination != config.destination
            || new_config.queue_size != config.queue_size;

        // Re-create the reconnect delay interval based on the new config
        connection.set_retry_delay(config.connect_retry_secs);

        // Store the changed configuration
        self.config.store(Arc::new(new_config));

        // Report that we have finished handling the reconfigure command
        self.status_reporter.reconfigured();

        if reconnect {
            // Advise the caller to stop using the current MQTT client
            // and instead to re-create it using the new config
            ControlFlow::Break(())
        } else {
            // Advise the caller to keep using the current MQTT client
            ControlFlow::Continue(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_msg<F>(
        status_reporter: Arc<MqttStatusReporter>,
        client: Option<C>,
        topic: String,
        _received: DateTime<Utc>,
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
                status_reporter.publish_ok(topic);
            }
            Err(err) => {
                status_reporter.publish_error(err);
            }
        }
    }

    async fn do_publish<F>(
        client: Option<C>,
        topic: &str,
        content: String,
        qos: i32,
        duration: Duration,
        test_publish: Option<F>,
    ) -> Result<(), MqttError>
    where
        F: Fn() -> Result<(), MqttError> + Send + 'static,
        C: Client,
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

    pub fn output_stream_message_to_msg(
        &self,
        osm: Arc<OutputStreamMessage>,
    ) -> Option<SenderMsg> {
        if osm.get_name() == self.component.name() {
            match serde_json::to_string(osm.get_record()) {
                Ok(content) => {
                    let topic = self
                        .config
                        .load()
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

impl ConnectionFactory for MqttRunner<mqtt::AsyncClient> {
    type EventLoopType = mqtt::EventLoop;

    type ClientType = mqtt::AsyncClient;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> Connection<Self::ClientType> {
        let mut create_opts = MqttOptions::new(
            config.client_id.clone(),
            config.destination.host.clone(),
            config.destination.port,
        );
        create_opts.set_request_channel_capacity(config.queue_size.into());
        create_opts.set_clean_session(true);
        create_opts.set_inflight(1000);
        create_opts.set_keep_alive(Duration::from_secs(20));

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            create_opts.set_credentials(username, password);
        }

        Connection::new(
            create_opts,
            config.connect_retry_secs,
            status_reporter,
        )
    }
}

#[async_trait]
impl<C: Client> DirectUpdate for MqttRunner<C>
where
    Self: ConnectionFactory<ClientType = C>,
{
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
                    if let Err(_err) =
                        self.pub_q_tx.as_ref().unwrap().send(msg)
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
                                self.pub_q_tx.as_ref().unwrap().send(msg)
                            {
                                // TODO
                            }
                        }
                    }
                }
            }

            _ => { /* We may have received the output from another unit, but we are not interested in it */
            }
        }
    }
}

impl<C: Client> AnyDirectUpdate for MqttRunner<C> where
    Self: ConnectionFactory<ClientType = C>
{
}

impl<C: Client> std::fmt::Debug for MqttRunner<C> {
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
