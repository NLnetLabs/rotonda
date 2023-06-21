//! Null target.

use serde::Deserialize;
use tokio::sync::mpsc;

use crate::comms::Terminated;
use crate::manager::{Component, WaitPoint};
use crate::{comms::Link, manager::TargetCommand};

#[derive(Debug, Deserialize)]
pub struct Target {
    #[allow(dead_code)]
    source: Link,
}

impl Target {
    /// Runs the target.
    pub async fn run(
        mut self,
        _: Component,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        // Prevent wasted time and effort on sending us updates that we won't
        // use by suspending our link to upstream units. Don't just exit the
        // function because that will cause the upstream units to terminate if
        // we are their only downstream.
        self.source.suspend().await;

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                TargetCommand::Reconfigure { new_config } => {
                    if let crate::targets::Target::Null(Target {
                        source: new_source,
                        ..
                        // config
                    }) = new_config
                    {
                        self.source = new_source;
                        self.source.suspend().await;
                    }
                }

                TargetCommand::ReportLinks { report } => {
                    report.set_source(&self.source);
                }

                TargetCommand::Terminate => {
                    break;
                }
            }
        }

        Err(Terminated)
    }
}
