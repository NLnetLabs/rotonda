use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use mqtt::{
    ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event,
    Incoming, MqttOptions, QoS,
};
use tokio::time::{interval, Interval};
use ConnectionState::*;

use super::{config::Config, status_reporter::MqttStatusReporter};

#[derive(Debug)]
pub enum ConnectionState {
    Connecting,
    Connected,
    RetryBackoff(Interval),
    Disconnected,
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

#[async_trait]
pub trait EventLoop {
    async fn poll(&mut self) -> Result<Event, ConnectionError>;

    fn mqtt_options(&self) -> &MqttOptions;
}

#[async_trait]
impl EventLoop for mqtt::EventLoop {
    async fn poll(&mut self) -> Result<Event, ConnectionError> {
        mqtt::EventLoop::poll(self).await
    }

    fn mqtt_options(&self) -> &MqttOptions {
        &self.mqtt_options
    }
}

pub struct Connection<T: EventLoop> {
    event_loop: T,
    retry_delay: Duration,
    status_reporter: Arc<MqttStatusReporter>,
    state: ConnectionState,
}

impl<T: EventLoop> Connection<T> {
    pub fn new(
        event_loop: T,
        retry_delay: Duration,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> Self {
        status_reporter
            .connecting(&event_loop.mqtt_options().broker_address().into());

        Self {
            event_loop,
            retry_delay,
            status_reporter,
            state: ConnectionState::Connecting,
        }
    }

    pub async fn process(&mut self) {
        match self.state {
            Connecting | Connected => {
                let res = self.event_loop.poll().await;
                self.handle_event(res).await;
            }
            RetryBackoff(ref mut delay) => {
                let _ = delay.tick().await;
                self.state = Disconnected;
            }
            Disconnected => unreachable!(),
        }
    }

    pub fn disconnect(&mut self) {
        self.state = Disconnected;
    }

    pub fn connected(&self) -> bool {
        !matches!(self.state, ConnectionState::Disconnected)
    }

    pub async fn handle_event(
        &mut self,
        conn_event: Result<Event, ConnectionError>,
    ) {
        match conn_event {
            Ok(Event::Incoming(Incoming::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                ..
            }))) => {
                self.state = Connected;
                self.status_reporter.connected(
                    &self.event_loop.mqtt_options().broker_address().into(),
                );
            }

            Ok(_) => { /* No other events are handled specially at this time */
            }

            Err(err) => {
                self.status_reporter.connection_error(err);

                self.state = match self.state {
                    Connecting => {
                        let mut interval = interval(self.retry_delay);
                        interval.tick().await; // the first tick completes immediately

                        self.status_reporter.reconnecting(interval.period());

                        RetryBackoff(interval)
                    }

                    Connected => Disconnected,

                    _ => unreachable!(),
                };
            }
        }
    }

    pub fn set_retry_delay(&mut self, retry_delay: Duration) {
        self.retry_delay = retry_delay;
    }
}

#[async_trait]
pub trait Client: Clone {
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
}

#[async_trait]
impl Client for mqtt::AsyncClient {
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
        mqtt::AsyncClient::publish(&self, topic, qos, retain, payload).await
    }
}

pub trait ConnectionFactory {
    type EventLoopType: EventLoop;

    type ClientType: Client;

    fn connect(
        config: &Config,
        status_reporter: Arc<MqttStatusReporter>,
    ) -> (Self::ClientType, Connection<Self::EventLoopType>);
}
