use std::{ops::ControlFlow, sync::Arc, time::Duration};

use super::{
    config::Config, connection::Connection, error::MqttError,
    metrics::MqttMetrics, status_reporter::MqttStatusReporter,
};

use crate::{
    common::status_reporter::{AnyStatusReporter, TargetStatusReporter},
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Payload, Update, UpstreamStatus},
    targets::Target,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mqtt::{AsyncClient, MqttOptions, QoS};
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
        MqttRunner::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

// Being generic over T enables use of a mock file I/O implementation when testing.
pub(super) struct MqttRunner {
    component: Component,
    config: Arc<ArcSwap<Config>>,
    sender: Option<mpsc::UnboundedSender<SenderMsg>>,
    status_reporter: Arc<MqttStatusReporter>,
}

impl MqttRunner {
    pub fn new(config: Config, mut component: Component) -> Self {
        let config = Arc::new(ArcSwap::from_pointee(config));

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

        let (tx, mut rx) = mpsc::unbounded_channel();
        self.sender = Some(tx);

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        'run: loop {
            let connect = arc_self.connect();
            let (client, mut connection) = connect;

            while connection.connected() {
                tokio::select! {
                    biased; // Disable tokio::select!() random branch selection

                    _ = connection.process() => { }

                    // If nothing happened above, check for new internal Rotonda target commands
                    // to handle.
                    cmd = cmd_rx.recv() => {
                        if let Some(cmd) = &cmd {
                            arc_self.status_reporter.command_received(cmd);
                        }

                        match cmd {
                            Some(TargetCommand::Reconfigure { new_config: Target::Mqtt(new_config) }) => {
                                if arc_self.reconfigure(&mut sources, new_config, &mut connection).await.is_break() {
                                    connection.reconnect();
                                }
                            }

                            Some(TargetCommand::Reconfigure { .. }) => unreachable!(),

                            Some(TargetCommand::ReportLinks { report }) => {
                                report.set_sources(&sources);
                                report.set_graph_status(
                                    arc_self.status_reporter.metrics(),
                                );
                            }

                            None | Some(TargetCommand::Terminate) => break 'run,
                        }
                    }

                    // And finally if not doing anything else we can process messages
                    // waiting in our internal queue to be published, which were
                    // enqueued by the direct_update() method below.
                    msg = rx.recv() => {
                        match msg {
                            Some(SenderMsg {
                                received,
                                content,
                                topic,
                            }) => {
                                Self::publish_msg(
                                    arc_self.status_reporter.clone(),
                                    Some(client.clone()),
                                    topic,
                                    received,
                                    content,
                                    arc_self.config.load().qos,
                                    arc_self.config.load().publish_max_secs,
                                    None::<fn() -> Result<(), MqttError>>,
                                )
                                .await;
                            }

                            None => {
                                break 'run;
                            }
                        }
                    }
                }
            }
        }

        arc_self.status_reporter.terminated();
        Err(Terminated)
    }

    async fn reconfigure(
        self: &Arc<Self>,
        sources: &mut NonEmpty<DirectLink>,
        Mqtt {
            sources: new_sources,
            config: new_config,
        }: Mqtt,
        connection: &mut Connection,
    ) -> ControlFlow<()> {
        // Register as a direct update receiver with the new
        // set of linked gates.
        self.status_reporter
            .upstream_sources_changed(sources.len(), new_sources.len());
        *sources = new_sources;

        for link in sources.iter_mut() {
            link.connect(self.clone(), false).await.unwrap();
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
            return ControlFlow::Break(());
        } else {
            // Advise the caller to keep using the current MQTT client
            ControlFlow::Continue(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_msg<F>(
        status_reporter: Arc<MqttStatusReporter>,
        client: Option<AsyncClient>,
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
        client: Option<AsyncClient>,
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

    fn connect(self: &Arc<Self>) -> (AsyncClient, Connection) {
        let config = self.config.load();

        let mut create_opts = MqttOptions::new(
            config.client_id.clone(),
            config.destination.host.clone(),
            config.destination.port,
        );
        create_opts.set_request_channel_capacity(config.queue_size.into());
        create_opts.set_clean_session(false);
        create_opts.set_inflight(1000);
        create_opts.set_keep_alive(Duration::from_secs(20));

        // Create the MQTT client & initiate connecting to the MQTT broker
        let (client, mut raw_connection) =
            AsyncClient::new(create_opts, config.queue_size.into());

        let mut conn_opts = raw_connection.network_options();
        conn_opts.set_connection_timeout(1);
        raw_connection.set_network_options(conn_opts);

        let connection = Connection::new(
            raw_connection,
            config.connect_retry_secs,
            self.status_reporter.clone(),
        );

        (client, connection)
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

            _ => { /* We may have received the output from another unit, but we are not interested in it */
            }
        }
    }
}

impl AnyDirectUpdate for MqttRunner {}

impl std::fmt::Debug for MqttRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttRunner").finish()
    }
}
