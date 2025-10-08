//! The targets for RPKI data.
//!
//! A target is anything that produces the final output from payload data.
//! Each target is connected to exactly one unit and constantly converts its
//! payload set into some form of output.
//!
//! This module contains all the different kinds of targets currently
//! available. It provides access to them via the enum [`Target`] that
//! contains all types as variants.
//!
//! are started by spawning them into an async runtime and then just keep
//! Targets can be created from configuration via serde deserialization. They
//! running there.

//------------ Sub-modules ---------------------------------------------------
//
// These contain all the actual unit types grouped by shared functionality.
mod file;
mod mqtt;
mod null;

pub use mqtt::DEF_MQTT_PORT;

use tokio::sync::mpsc;

//------------ Target --------------------------------------------------------

use crate::manager::{TargetCommand, WaitPoint};
use crate::{comms::Terminated, manager::Component};
use serde::Deserialize;

/// The component for outputting data.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Target {
    #[serde(rename = "file-out")]
    File(file::target::File),

    #[serde(rename = "mqtt-out")]
    Mqtt(mqtt::target::Mqtt),

    #[serde(rename = "null-out")]
    Null(null::Target),
}

impl Target {
    /// Runs the target.
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        match self {
            Target::File(target) => {
                target.run(component, cmd, waitpoint).await
            }
            Target::Mqtt(target) => {
                target.run(component, cmd, waitpoint).await
            }
            Target::Null(target) => {
                target.run(component, cmd, waitpoint).await
            }
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Target::File(_) => "file-out",
            Target::Mqtt(_) => "mqtt-out",
            Target::Null(_) => "null-out",
        }
    }
}
