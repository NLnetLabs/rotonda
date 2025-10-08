use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use mqtt::{
    ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event,
    Incoming, MqttOptions, NetworkOptions, QoS,
};
use tokio::{sync::oneshot, task::JoinHandle, time::interval};
use ConnectionState::*;

use super::{config::Config, status_reporter::MqttStatusReporter};

// TODO: Add a state transition diagram here.
#[derive(Debug)]
pub enum ConnectionState<C: Client> {
    New,
    Running(C, JoinHandle<()>),
    Stopped,
}

impl<C: Client> PartialEq for ConnectionState<C> {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

pub type MqttPollResult = Result<Event, ConnectionError>;

#[async_trait]
pub trait EventLoop: Send {
    async fn poll(&mut self) -> MqttPollResult;

    #[allow(dead_code)]
    fn mqtt_options(&self) -> &MqttOptions;

    fn network_options(&self) -> NetworkOptions;

    fn set_network_options(
        &mut self,
        network_options: NetworkOptions,
    ) -> &mut Self;

    fn inflight(&self) -> u16;
}

#[async_trait]
impl EventLoop for mqtt::EventLoop {
    async fn poll(&mut self) -> MqttPollResult {
        self.poll().await
    }

    fn mqtt_options(&self) -> &MqttOptions {
        &self.mqtt_options
    }

    fn network_options(&self) -> NetworkOptions {
        self.network_options()
    }

    fn set_network_options(
        &mut self,
        network_options: NetworkOptions,
    ) -> &mut Self {
        self.set_network_options(network_options)
    }

    fn inflight(&self) -> u16 {
        self.state.inflight()
    }
}

pub struct Connection<C: Client> {
    mqtt_options: MqttOptions,
    retry_delay: Arc<ArcSwap<Duration>>,
    status_reporter: Arc<MqttStatusReporter>,
    state: ConnectionState<C>,
}

impl<C: Client> Connection<C> {
    pub fn new(
        mqtt_options: MqttOptions,
        retry_delay: Duration,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> Self {
        Self {
            mqtt_options,
            retry_delay: Arc::new(ArcSwap::from_pointee(retry_delay)),
            status_reporter,
            state: New,
        }
    }

    pub async fn process(&mut self) -> Option<C> {
        match &mut self.state {
            New => {
                let broker_address =
                    self.mqtt_options.broker_address().into();

                self.status_reporter.connecting(&broker_address);

                // Spawn a task that will poll the MQTT event loop and query
                // the received events or errors away from a tokio::select!
                // macro block.
                //
                // Why? Unless I'm misreading the rumqttc v0.23.0 code, we
                // can't use a tokio::select! block to poll the MQTT event
                // loop while it is connecting as futures polled by
                // tokio::select! must be cancellation safe but the MQTT event
                // loop, when not yet connected, is not.
                //
                // Even if it was, between calling poll().await and then
                // inspecting any returned Event the future for this fn could
                // be cancelled at the .await point and the resulting ConnAck
                // packet would be lost. We would then stay in the Connecting
                // state without realizing we were connected. Worse, there is
                // an .await point inside rumqttc poll() between receiving the
                // ConnAck and setting its own internal network state to know
                // that it was connected, and thus the network socket would be
                // dropped and a subsequent call to rumqttc poll() would have
                // to try connecting via a socket again.

                let mqtt_options = self.mqtt_options.clone();
                let status_reporter = self.status_reporter.clone();
                let (client_handover_tx, client_handover_rx) =
                    oneshot::channel();

                let join_handle = crate::tokio::spawn(
                    "MQTT Event Loop",
                    Self::mqtt_event_loop(
                        mqtt_options,
                        client_handover_tx,
                        self.retry_delay.clone(),
                        status_reporter,
                    ),
                );

                match client_handover_rx.await {
                    Ok(client) => {
                        self.state = Running(client.clone(), join_handle);
                        return Some(client);
                    }

                    Err(err) => {
                        self.status_reporter.connection_error(err);
                        self.disconnect().await;
                    }
                }
            }

            Running(_, join_handle) => {
                // This is cancel safe, if cancelled the task keeps running
                // and we can await it again next time we are called.
                if let Err(err) = join_handle.await {
                    // There was an internal Tokio problem
                    self.status_reporter.connection_error(err);
                    self.disconnect().await;
                }

                self.state = Stopped;
            }

            // If we're disconnected, we shouldn't try to connect or to
            // exchange MQTT protocol messages, i.e. we shouldn't run the
            // rumqttc event loop, in fact we shouldn't do anything.
            Stopped => {
                // NO OP
            }
        }

        // Caller should stop using any reference it has to the client
        None
    }

    pub async fn disconnect(&mut self) {
        if let Running(client, join_handle) = &mut self.state {
            // TODO: Should we attempt to wait for any in-flight messages to
            // be sent?
            if let Err(err) = client.disconnect().await {
                self.status_reporter.connection_error(err);
            }
            self.status_reporter
                .disconnected(&self.mqtt_options.broker_address().into());
            // TODO: Shutdown gracefully allowing in-flight messages in the
            // rumqqtc library time to be sent?
            join_handle.abort();
        }

        self.state = Stopped;
    }

    pub fn active(&self) -> bool {
        !matches!(self.state, Stopped)
    }

    pub fn client(&self) -> Option<C> {
        match &self.state {
            Running(client, _) => Some(client.clone()),
            _ => None,
        }
    }

    pub fn set_retry_delay(&mut self, retry_delay: Duration) {
        self.retry_delay.store(Arc::new(retry_delay));
    }
}

impl<C: Client> Connection<C> {
    /// Returns false if the initial connection never succeeded, true otherwise.
    async fn mqtt_event_loop(
        mqtt_options: MqttOptions,
        client_handover_tx: oneshot::Sender<C>,
        retry_delay: Arc<ArcSwap<Duration>>,
        status_reporter: Arc<MqttStatusReporter>,
    ) {
        let broker_address = mqtt_options.broker_address().into();
        let cap = mqtt_options.request_channel_capacity();
        let (client, mut event_loop) = C::new(mqtt_options, cap);

        if client_handover_tx.send(client).is_err() {
            // Abort
            return;
        }

        let mut conn_opts = event_loop.network_options();
        conn_opts.set_connection_timeout(1);
        event_loop.set_network_options(conn_opts);

        let mut conn_count = 0;
        let mut retry = false;
        loop {
            match event_loop.poll().await {
                Ok(event) => {
                    match event {
                        Event::Incoming(Incoming::ConnAck(ConnAck {
                            code: ConnectReturnCode::Success,
                            ..
                        })) => {
                            // Great!
                            status_reporter.connected(&broker_address);
                            conn_count += 1;
                        }

                        Event::Incoming(Incoming::ConnAck(ConnAck {
                            code,
                            ..
                        })) => {
                            // Connection failed
                            status_reporter
                                .connection_error(format!("{code:?}"));
                            retry = true;
                        }

                        _ => {
                            // Ignored for now
                        }
                    }
                }

                Err(err) => {
                    // Any error reported by rumqttc already resulted in it
                    // calling its own `clean()` fn internally which forgets
                    // the network connection causing the next call to
                    // `poll()` to reconnect. However, we don't want to
                    // reconnect immediately and potentially repeatedly, we
                    // want to have a reconnection backoff strategy. So break
                    // out of here to allow the caller to wait before
                    // reconnecting.
                    status_reporter.connection_error(err);
                    retry = true;
                }
            }

            status_reporter.inflight_update(event_loop.inflight());

            if retry {
                let retry_delay = **retry_delay.load();

                if conn_count > 0 {
                    status_reporter.reconnecting(retry_delay);
                }

                // Delay until polling as .poll() will attempt to
                // reconnect. Consume the first tick as it always
                // completes immediately (per the docs).
                let mut interval = interval(retry_delay);
                interval.tick().await;
                interval.tick().await;

                retry = false;
            }
        }
    }
}

#[async_trait]
pub trait Client: Clone + Send + Sync + 'static {
    type EventLoopType: EventLoop;

    fn new(options: MqttOptions, cap: usize) -> (Self, Self::EventLoopType);

    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send;

    async fn disconnect(&self) -> Result<(), ClientError>;
}

#[async_trait]
impl Client for mqtt::AsyncClient {
    type EventLoopType = mqtt::EventLoop;

    fn new(options: MqttOptions, cap: usize) -> (Self, Self::EventLoopType) {
        Self::new(options, cap)
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
        self.publish(topic, qos, retain, payload).await
    }

    async fn disconnect(&self) -> Result<(), ClientError> {
        self.disconnect().await
    }
}

pub trait ConnectionFactory {
    type EventLoopType: EventLoop;

    type ClientType: Client;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> Connection<Self::ClientType>;
}
