use std::fmt::Display;

use mqtt::ClientError;

#[derive(Debug)]
pub enum MqttError {
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
