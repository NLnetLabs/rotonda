use std::sync::Arc;
use std::time::Duration;

use chrono::SecondsFormat;
//use async_trait::async_trait;
use futures::future::{select, Either};
use futures::FutureExt;
use log::{debug, error, warn};
//use non_empty_vec::NonEmpty;
use serde::Deserialize;

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time::Instant;

//use crate::comms::{AnyDirectUpdate, DirectLink, DirectUpdate};
use crate::comms::{Link, Terminated};
use crate::config::ConfigPath;
use crate::ingress;
use crate::payload::Update;
use crate::roto_runtime::types::OutputStreamMessageRecord;
use crate::targets::Component;
use crate::targets::TargetCommand;
use crate::targets::WaitPoint;

// For low-traffic logging, make sure we flush to disk at least every N secs:
const LAST_FLUSH_TIMEOUT_SECS: u64 = 1;


#[derive(Debug, Deserialize)]
pub struct File {
    #[serde(flatten)]
    config: Config,
    sources: Link,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    format: Format,
    filename: ConfigPath,
}

#[derive(Debug, Deserialize)]
#[serde()]
pub enum Format {
    #[serde(rename = "csv")]
    Csv,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "json-min")]
    JsonMin,
}


impl File {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        FileRunner::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }

}


#[allow(dead_code)]
pub struct FileRunner {
    component: Component,
    config: Config,
    ingresses: Arc<ingress::Register>,
    target_file: Option<BufWriter<tokio::fs::File>>,
    last_flush: Instant,
}


impl FileRunner {
    pub fn new(config: Config, component: Component) -> Self {
        let ingresses = component.ingresses().clone();
        Self {
            config,
            component,
            ingresses,
            target_file: None,
            last_flush: Instant::now(),

        }
    }

    async fn flush(&mut self) {
        if let Some(dst) = self.target_file.as_mut() {
            let _ = dst.flush().await;
            self.last_flush = Instant::now();
        }
    }

    pub async fn run(
        mut self,
        mut sources: Link,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let f = tokio::fs::File::create(self.config.filename.clone())
            .await
            .inspect_err(|e| error!("{}", e))
            .map_err(|_| Terminated)
            ?;

        self.target_file = Some(BufWriter::new(f));

        //let arc_self = Arc::new(self);
        // Register as a direct update receiver with the linked gates.

        //for link in sources.iter_mut() {
        //    // DirectLink
        //    //link.connect(arc_self.clone(), false).await.unwrap();
        //    
        //    link.connect(false).await.unwrap();
        //}

        sources.connect(false).await.unwrap();
        let sources2 = sources.clone();

        waitpoint.running().await;

        loop {
            let select_fut = select(
                cmd_rx.recv().boxed(),
                sources.query().boxed(),
            );
            let select = match tokio::time::timeout(std::time::Duration::from_secs(LAST_FLUSH_TIMEOUT_SECS), select_fut).await {
                Ok(select) => {
                    if self.last_flush + Duration::from_secs(LAST_FLUSH_TIMEOUT_SECS) < Instant::now() {
                        self.flush().await;
                    }
                    select
                }
                Err(_timeout) => {
                    self.flush().await;
                    continue;
                }
            };
            match select {
                Either::Left((gate_cmd, _)) => { 
                    match gate_cmd {
                        Some(cmd) => match cmd {
                            TargetCommand::Reconfigure { .. } => {
                                warn!("Reconfiguration for FileOut component not yet implemented");
                            }
                            TargetCommand::ReportLinks { report } => {
                                report.set_source(&sources2);

                                // If we switch to logging from >1 component:
                                //if let Some(sources) = sources {
                                //    report.set_sources(sources);
                                //}
                                //report.set_graph_status(
                                //    self.status_reporter.metrics(),
                                //);
                            }
                            TargetCommand::Terminate => {
                                break
                            }
                        }
                        None => break
                    }
                }
                Either::Right((update, _)) => {
                    let update = match update {
                        Ok(upd) => upd,
                        Err(e) =>  {
                            debug!("Gate error in file-out target: {}", e);
                            break
                        }
                    };

                    match update {
                        Update::OutputStream(msgs) => {
                            for m in msgs {
                                let m = m.into_record();
                                if let Some(dst) = self.target_file.as_mut() {
                                    if let OutputStreamMessageRecord::Entry(ref e) = m {
                                        if let Some(ref custom_str) = e.custom {
                                            if e.timestamp != chrono::DateTime::UNIX_EPOCH {
                                                dst.write_all(
                                                    format!(
                                                        "[{}] ",
                                                        e.timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
                                                    ).as_ref()
                                                ).await.unwrap();
                                            }
                                            dst.write_all(custom_str.as_ref()).await.unwrap();
                                            dst.write_all(b"\n").await.unwrap();
                                            continue;
                                        } 
                                    }
                                    match self.config.format {
                                        Format::Csv => {
                                            let mut wrt = csv::WriterBuilder::new().has_headers(false).from_writer(vec![]);
                                            wrt.serialize(m).unwrap();
                                            dst.write_all(&wrt.into_inner().unwrap()).await.unwrap();
                                        }
                                        Format::Json => {
                                            if let Ok(bytes) = serde_json::to_vec(&m) {
                                                dst.write_all(&bytes).await.unwrap();
                                                dst.write_all(b"\n").await.unwrap();
                                            }
                                        }
                                        Format::JsonMin => {
                                            if let OutputStreamMessageRecord::Entry(e) = m {
                                                if let Ok(bytes) = serde_json::to_vec(&e.into_minimal()) {
                                                    dst.write_all(&bytes).await.unwrap();
                                                    dst.write_all(b"\n").await.unwrap();
                                                }
                                            } else {
                                                // same as Json case
                                                if let Ok(bytes) = serde_json::to_vec(&m) {
                                                    dst.write_all(&bytes).await.unwrap();
                                                    dst.write_all(b"\n").await.unwrap();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // No action on any of the other Update types
                        Update::Single(..) |
                            Update::Bulk(..)  |
                            Update::Withdraw(..)  |
                            Update::WithdrawBulk(..)  |
                            Update::IngressReappeared(..) |
                            Update::UpstreamStatusChange(..) |
                            Update::Rtr(..) =>  { }
                    }
                }
            }

        }
        self.flush().await;
        Ok(())
    }
}

/*
impl AnyDirectUpdate for FileRunner { }

#[async_trait]
impl DirectUpdate for FileRunner {
    async fn direct_update(&self, update: Update) {
        match update {
            Update::OutputStream(msgs) => {
                for m in msgs {
                    if let Some(mut dst) = self.target_file.as_mut() {
                        match self.config.format {
                            Format::Csv => {
                                //let mut wrt = csv::Writer::from_writer(dst);
                                //wrt.serialize(m);
                            }
                            Format::Json => {
                                if let Ok(bytes) = serde_json::to_vec(m.get_record()) {
                                    dst.write_all(&bytes);
                                }
                            }
                        }
                    }
                }
            }

            // No action on any of the other Update types
            Update::Single(..) |
            Update::Bulk(..)  |
            Update::Withdraw(..)  |
            Update::WithdrawBulk(..)  |
            Update::UpstreamStatusChange(..) =>  { }
        }
    }
}

impl fmt::Debug for FileRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileRunner").finish()
    }
}
*/
