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
//! Targets can be created from configuration via serde deserialization. They
//! are started by spawning them into an async runtime and then just keep
//! running there.

//------------ Sub-modules ---------------------------------------------------
//
// These contain all the actual unit types grouped by shared functionality.
mod bmp_fs_out;
mod bmp_tcp_out;
mod mqtt;
mod null;

pub use self::mqtt::target::DEF_MQTT_PORT;

use tokio::sync::mpsc;

//------------ Target --------------------------------------------------------

use crate::manager::{TargetCommand, WaitPoint};
use crate::{comms::Terminated, manager::Component};
use serde::Deserialize;

/// The component for outputting data.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]

pub enum Target {
    #[serde(rename = "bmp-fs-out")]
    BmpFsOut(bmp_fs_out::target::BmpFsOut),

    #[serde(rename = "bmp-tcp-out")]
    BmpTcpOut(bmp_tcp_out::target::BmpTcpOut),

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
            Target::BmpFsOut(target) => target.run(component, cmd, waitpoint).await,
            Target::BmpTcpOut(target) => target.run(component, cmd, waitpoint).await,
            Target::Mqtt(target) => target.run(component, cmd, waitpoint).await,
            Target::Null(target) => target.run(component, cmd, waitpoint).await,
        }
    }
}
