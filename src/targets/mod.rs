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
#[cfg(feature = "mqtt")]
mod mqtt;
mod null;
// mod rotoro;

// pub use rotoro::{RotoroCoder, RotoroCommand, WireMsg};
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

    #[cfg(feature = "mqtt")]
    #[serde(rename = "mqtt-out")]
    Mqtt(mqtt::target::Mqtt),

    // #[serde(rename = "rotoro-out")]
    // Rotoro(rotoro::Target),
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
            #[cfg(feature = "mqtt")]
            Target::Mqtt(target) => target.run(component, cmd, waitpoint).await,
            // Target::Rotoro(target) => target.run(component, cmd).await,
            Target::Null(target) => target.run(component, cmd, waitpoint).await,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Target::BmpFsOut(_) => "bmp-fs-out",
            Target::BmpTcpOut(_) => "bmp-tcp-out",
            Target::Mqtt(_) => "mqtt-out",
            Target::Null(_) => "null-out",
        }
    }
}
