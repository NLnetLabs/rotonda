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
    ingress,
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Update, UpstreamStatus},
    targets::Target,
};
use crate::roto_runtime::types::OutputStreamMessage;

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::error;
use mqtt::{MqttOptions, QoS};
use non_empty_vec::NonEmpty;
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
    pub_q_tx: Option<mpsc::Sender<SenderMsg>>,
    status_reporter: Arc<MqttStatusReporter>,
    ingresses: Arc<ingress::Register>,
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

        let ingresses = component.ingresses().clone();
        Self {
            component,
            config,
            client: Default::default(),
            pub_q_tx: None,
            status_reporter,
            ingresses,
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

        let (pub_q_tx, pub_q_rx) = mpsc::channel(10);
        self.pub_q_tx = Some(pub_q_tx);

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components
        // that we are, ready to start. All units and targets start together,
        // otherwise data passed from one component to another may be lost if
        // the receiving component is not yet ready to accept it.
        waitpoint.running().await;

        arc_self
            .do_run::<Self>(Some(sources), cmd_rx, pub_q_rx)
            .await
    }

    pub async fn do_run<F: ConnectionFactory<ClientType = C>>(
        self: &Arc<Self>,
        mut sources: Option<NonEmpty<DirectLink>>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        mut pub_q_rx: mpsc::Receiver<SenderMsg>,
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
        pub_q_rx: &mut mpsc::Receiver<SenderMsg>,
    ) -> Result<(), Terminated> {
        while connection.active() {
            tokio::select! {
                // Disable tokio::select!() random branch selection
                biased;

                client = connection.process() => {
                    self.client.store(client.map(Arc::new));
                }

                // If nothing happened above, check for new internal Rotonda
                // target commands to handle.
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

                // And finally if not doing anything else we can process
                // messages waiting in our internal queue to be published,
                // which were enqueued by the direct_update() method below.
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
        //osm: Arc<OutputStreamMessage>,
        osm: OutputStreamMessage,
    ) -> Option<SenderMsg> {
        if *osm.get_name() == **self.component.name() {
            let ingress_info =
                osm.get_ingress_id().and_then(|id| self.ingresses.get(id));

            match serde_json::to_string(&(ingress_info, osm.get_record())) {
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
                Err(err) => {
                    // TODO
                    error!("{err}");
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

        if let (Some(username), Some(password)) =
            (&config.username, &config.password)
        {
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

            Update::OutputStream(msgs) => {
                for osm in msgs {
                    if let Some(msg) = self.output_stream_message_to_msg(osm)
                    {
                        if let Err(err) =
                            self.pub_q_tx.as_ref().unwrap().send(msg).await
                        {
                            error!("failed to send MQTT message: {err}");
                        }
                    }
                }
            }

            /*
            Update::Single(Payload {
                rx_value: TypeValue::OutputStreamMessage(osm),
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
                        rx_value: TypeValue::OutputStreamMessage(osm),
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
            */
            _ => {
                // We may have received the output from another unit, but we
                // are not interested in it
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
