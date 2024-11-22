use std::{fmt::Display, time::Duration};

use serde::{self, Deserialize};
use serde_with::serde_as;

pub const DEF_MQTT_PORT: u16 = 1883;

// -------- Destination ------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct Destination {
    /// MQTT server host name or IP address
    pub host: String,

    /// MQTT server TCP port number
    pub port: u16,
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
            Err("Host part of MQTT server address must not be empty"
                .to_string())
        } else {
            Ok(Self { host, port })
        }
    }
}

impl From<(String, u16)> for Destination {
    fn from((host, port): (String, u16)) -> Self {
        Self { host, port }
    }
}

impl Display for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.host, self.port))
    }
}

// -------- ClientId ---------------------------------------------------------

/// MQTT client ID
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct ClientId(pub String);

impl std::ops::Deref for ClientId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for ClientId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with(' ') {
            Err("MQTT client id must not contain leading whitespace")
        } else if value.is_empty() {
            Err("MQTT client id must not be empty")
        } else {
            Ok(Self(value))
        }
    }
}

impl From<ClientId> for String {
    fn from(val: ClientId) -> Self {
        val.0
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize)]
pub struct Config {
    /// MQTT server host[:port] to publish to
    pub destination: Destination,

    #[serde(default = "Config::default_qos")]
    pub qos: i32,

    pub client_id: ClientId,

    #[serde(default = "Config::default_topic_template")]
    pub topic_template: String,

    /// How long to wait in seconds before connecting again if the connection
    /// is closed.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_connect_retry_secs")]
    pub connect_retry_secs: Duration,

    /// How long to wait before timing out an attempt to publish a message.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_publish_max_secs")]
    pub publish_max_secs: Duration,

    /// How many messages to buffer if publishing encounters delays
    #[serde(default = "Config::default_queue_size")]
    pub queue_size: u16,

    #[serde(default)]
    pub username: Option<String>,

    #[serde(default)]
    pub password: Option<String>,
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
