use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::Display,
    fs::File,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use roto::types::builtin::SourceId;

use super::{
    metrics::BmpFsOutMetrics, status_reporter::BmpFsOutStatusReporter,
};

use crate::{
    common::{
        file_io::{FileIo, TheFileIo},
        frim::FrimMap,
        routecore_extra::generate_alternate_config,
        status_reporter::{AnyStatusReporter, TargetStatusReporter},
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Payload, Update, UpstreamStatus},
};

use async_trait::async_trait;
use chrono::Utc;
use non_empty_vec::NonEmpty;
use roto::types::{
    builtin::BuiltinTypeValue, collections::BytesRecord, typevalue::TypeValue,
};
use routecore::{
    bgp::message::{
        open::CapabilityType,
        SessionConfig,
    },
    bmp::message::{InformationTlvType, Message as BmpMsg},
};
use serde::Deserialize;
use smallvec::SmallVec;
use tokio::sync::mpsc;

#[derive(Debug, Deserialize)]
pub struct BmpFsOut {
    /// The set of units to receive messages from.
    sources: NonEmpty<DirectLink>,

    #[serde(flatten)]
    config: Config,
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Log,
    PcapText,
    Raw,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Split,

    Merge,
}

#[derive(Clone, Debug, Deserialize)]
struct Config {
    /// A path prefix to write the most recently received BMP message in PCAP
    /// text format to. Pass this to text2pcap (from the Wireshark tool suite)
    /// like so in order to prepare it for loading into Wireshark:
    ///   text2pcap -T 100,11019 <bmp_bin_trace_path>/<file>.pcaptext output.pcap
    /// Where 100 is a dummy source port.
    /// On change: no impact
    pub path: Arc<PathBuf>,

    pub format: Format,

    #[serde(default)]
    pub mode: Mode,
}

type SenderMsg = Payload;

type Sender = mpsc::UnboundedSender<SenderMsg>;

type Receiver = mpsc::UnboundedReceiver<SenderMsg>;

impl BmpFsOut {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        BmpFsOutRunner::<TheFileIo>::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

// Being generic over T enables use of a mock file I/O implementation when testing.
struct BmpFsOutRunner<T: FileIo + Send + Sync> {
    component: Component,
    config: Arc<Config>,
    senders: Arc<FrimMap<SourceId, Arc<Sender>>>,
    status_reporter: Arc<BmpFsOutStatusReporter>,
    _phantom: PhantomData<T>,
}

impl<T: FileIo + Sync + Send + 'static> BmpFsOutRunner<T> {
    fn new(config: Config, component: Component) -> Self {
        let config = Arc::new(config);

        let senders = Arc::new(FrimMap::default());

        let metrics = Arc::new(BmpFsOutMetrics::new());

        let _status_reporter =
            Arc::new(BmpFsOutStatusReporter::new(component.name(), metrics));

        Self {
            component,
            config,
            senders,
            status_reporter: _status_reporter,
            _phantom: PhantomData,
        }
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let _unit_name = component.name().clone();
        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        while let Some(cmd) = cmd_rx.recv().await {
            arc_self.status_reporter.command_received(&cmd);
            match cmd {
                TargetCommand::Reconfigure { new_config } => {
                    if let crate::targets::Target::BmpFsOut(BmpFsOut {
                        sources: new_sources,
                        ..
                        // config
                    }) = new_config
                    {
                        // Register as a direct update receiver with the new
                        // set of linked gates.
                        arc_self
                            .status_reporter
                            .upstream_sources_changed(sources.len(), new_sources.len());
                        sources = new_sources;
                        for link in sources.iter_mut() {
                            link.connect(arc_self.clone(), false).await.unwrap();
                        }
                    }
                }

                TargetCommand::ReportLinks { report } => {
                    report.set_sources(&sources);
                }

                TargetCommand::Terminate => break,
            }
        }

        Err(Terminated)
    }

    fn spawn_writer<G: Display>(&self, source_id: G) -> Sender {
        let file_io = T::default();
        let writer = self.build_file_writer(source_id);
        let format = self.config.format;

        let (tx, rx) = mpsc::unbounded_channel::<SenderMsg>();
        crate::tokio::spawn(
            "disk-writer",
            Self::message_transformer(rx, format, file_io, writer),
        );

        tx
    }

    fn build_file_writer<G: Display>(
        &self,
        source_id: G,
    ) -> tokio::io::BufWriter<tokio::fs::File> {
        let file_path = match self.config.mode {
            Mode::Merge => {
                // Merge all router messages into a single file at the given
                // path.
                // TODO: This mode seems to result in binary files when format is 'log'... why?
                self.config.path.deref().clone()
            }

            Mode::Split => {
                // Write router messages into one file per router inside the
                // given directory path per router.
                let name = self.component.name();
                let ext = match self.config.format {
                    Format::Log => ".log",
                    Format::PcapText => ".pcaptext",
                    Format::Raw => ".raw",
                };
                let filename = format!("{name}-{source_id}.{ext}");
                let filename = sanitise_file_name::sanitise(&filename);
                self.config.path.join(filename)
            }
        };

        // TODO: log a better error than: hread 'tokio-runtime-worker' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 21, kind: IsADirectory, message: "Is a directory" }', src/targets/bmp_fs_out/target.rs:206:44
        let file = File::create(file_path).unwrap();

        tokio::io::BufWriter::new(file.into())
    }

    async fn message_transformer(
        mut rx: Receiver,
        format: Format,
        mut file_io: T,
        mut file: tokio::io::BufWriter<tokio::fs::File>,
    ) {
        let mut router_ids = HashMap::new();
        let mut peer_configs: HashMap<u64, (SessionConfig, bool)> =
            HashMap::new();

        while let Some(Payload {
            source_id,
            value: TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bytes)),
            ..
        }) = rx.recv().await
        {
            let bytes = BytesRecord::as_ref(&bytes);
            match format {
                Format::Log => {
                    let bmp_msg = BmpMsg::from_octets(bytes).unwrap();

                    let (msg_type, extra) = match bmp_msg {
                        BmpMsg::InitiationMessage(msg) => {
                            let sys_name = msg
                                .information_tlvs()
                                .filter(|tlv| {
                                    tlv.typ() == InformationTlvType::SysName
                                })
                                .map(|tlv| {
                                    String::from_utf8_lossy(tlv.value())
                                        .into_owned()
                                })
                                .collect::<Vec<_>>()
                                .join("|");

                            let sys_descr = msg
                                .information_tlvs()
                                .filter(|tlv| {
                                    tlv.typ() == InformationTlvType::SysDesc
                                })
                                .map(|tlv| {
                                    String::from_utf8_lossy(tlv.value())
                                        .into_owned()
                                })
                                .collect::<Vec<_>>()
                                .join("|");

                            let router_id =
                                format!("{source_id} [{sys_name}]");
                            router_ids.insert(source_id.clone(), router_id);

                            (
                                "initiation message",
                                format!(
                                    ": sysName={}, sysDescr={}",
                                    sys_name, sys_descr
                                ),
                            )
                        }

                        BmpMsg::RouteMonitoring(msg) => {
                            let mut tried_peer_configs =
                                SmallVec::<[SessionConfig; 4]>::new();
                            let pph_hash = mk_hash(&msg.per_peer_header());
                            let (
                                known_peer,
                                (chosen_peer_config, has_gr_cap),
                            ) = peer_configs.get(&pph_hash).map_or_else(
                                || {
                                    static FALLBACK_CONFIG: SessionConfig =
                                        SessionConfig::modern();
                                    (false, (&FALLBACK_CONFIG, false))
                                },
                                |(config, has_gr_cap)| {
                                    (true, (config, *has_gr_cap))
                                },
                            );

                            let mut chosen_peer_config = *chosen_peer_config;
                            let mut retry_count = 0;
                            let extra = loop {
                                match msg.bgp_update(chosen_peer_config) {
                                    Ok(update) => {
                                        let num_withdrawals =
                                            update.withdrawn_routes_len();
                                        let wit_s_afis = match num_withdrawals
                                            > 0
                                        {
                                            true => format!(
                                                "[{:?}, {:?}]",
                                                update.withdrawals().map(|n| n.filter_map(|w| w.map(|w| w.afi_safi().afi()).ok()).collect::<Vec<routecore::bgp::types::Afi>>()),
                                                update.withdrawals().map(|n| n.filter_map(|w| w.map(|w| w.afi_safi().safi()).ok()).collect::<Vec<routecore::bgp::types::Safi>>()),
                                            ),
                                            false => String::new(),
                                        };

                                        let num_announcements =
                                            update.announcements().map(|n| n.filter_map(|w| w.ok()).count()).unwrap_or(0);
                                        let ann_s_afis =
                                            match num_announcements > 0 {
                                                true => format!(
                                                    "[{:?}, {:?}]",
                                                    update.announcements().map(|n| n.filter_map(|w| w.map(|w| w.afi_safi().afi()).ok()).collect::<Vec<routecore::bgp::types::Afi>>()),
                                                    update.announcements().map(|n| n.filter_map(|w| w.map(|w| w.afi_safi().safi()).ok()).collect::<Vec<routecore::bgp::types::Safi>>()),
                                                ),
                                                false => String::new(),
                                            };

                                        if let Ok(Some((afi, safi))) =
                                            update.is_eor()
                                        {
                                            break format!(" [peer: {}, peer up seen: {}] EOR<{}, {}>, caps[GR: {}]", msg.per_peer_header(), known_peer, afi, safi, has_gr_cap);
                                        } else {
                                            break format!(" [peer: {}, peer up seen: {}] # withdrawals: {}{}, # announcements: {}{}",
                                            msg.per_peer_header(), known_peer,
                                            num_withdrawals, wit_s_afis,
                                            num_announcements, ann_s_afis);
                                        }
                                    }

                                    Err(err) => {
                                        tried_peer_configs
                                            .push(chosen_peer_config);
                                        if let Some(alt_config) =
                                            generate_alternate_config(
                                                &chosen_peer_config,
                                            )
                                        {
                                            if !tried_peer_configs
                                                .contains(&alt_config)
                                            {
                                                chosen_peer_config =
                                                    alt_config;
                                                retry_count += 1;
                                                continue;
                                            }
                                        }

                                        break format!(
                                            " [peer: {}, peer up seen: {}] parse error (after {retry_count} retries): {}",
                                            msg.per_peer_header(),
                                            known_peer,
                                            err
                                        );
                                    }
                                }
                            };

                            ("route monitoring", extra)
                        }

                        BmpMsg::StatisticsReport(msg) => (
                            "statistics report",
                            format!(" [peer: {}]", msg.per_peer_header()),
                        ),

                        BmpMsg::PeerDownNotification(msg) => (
                            "peer down notification",
                            format!(" [peer: {}]", msg.per_peer_header()),
                        ),

                        BmpMsg::PeerUpNotification(msg) => {
                            let pph_hash = mk_hash(&msg.per_peer_header());
                            let has_gr_cap = msg
                                .bgp_open_rcvd()
                                .capabilities()
                                .any(|cap| {
                                    cap.typ()
                                        == CapabilityType::GracefulRestart
                                });
                            peer_configs.insert(
                                pph_hash,
                                (msg.session_config(), has_gr_cap),
                            );

                            (
                                "peer up notification",
                                format!(
                                    " [peer: {}], caps[GR: {}]",
                                    msg.per_peer_header(),
                                    has_gr_cap
                                ),
                            )
                        }

                        BmpMsg::TerminationMessage(msg) => {
                            let reason = msg
                                .information()
                                .map(|tlv| tlv.to_string())
                                .collect::<Vec<String>>()
                                .join("|");

                            (
                                "termination message",
                                format!(" [reason: {}]", reason),
                            )
                        }

                        BmpMsg::RouteMirroring(msg) => (
                            "route mirroring",
                            format!(" [peer: {}]", msg.per_peer_header()),
                        ),
                    };

                    let router_id = router_ids
                        .entry(source_id.clone())
                        .or_insert_with(|| format!("{source_id} \"-\""));

                    let received = Utc::now();
                    let log_msg = format!(
                        "{} [{}] {}{}\n",
                        router_id,
                        received.format("%d/%b/%Y:%H:%M:%S%f %z"),
                        msg_type,
                        extra
                    );

                    file_io
                        .write_all(&mut file, log_msg.as_bytes())
                        .await
                        .unwrap();
                }

                Format::PcapText => {
                    // output the packet offset
                    file_io
                        .write_all(&mut file, "000000 ".as_bytes())
                        .await
                        .unwrap();

                    // output the packet bytes
                    for b in bytes {
                        file_io
                            .write_all(&mut file, &format!("{:02x} ", b))
                            .await
                            .unwrap();
                    }

                    file_io
                        .write_all(&mut file, "\n".as_bytes())
                        .await
                        .unwrap();
                }

                Format::Raw => {
                    // Each message from the same router is appended to a file
                    // specific to that router. To aid replay later, the byte
                    // length of each message is written in big endian order
                    // prior to the bytes of the message itself.
                    let received = Utc::now();
                    let timestamp = received.timestamp_millis() as u128;
                    file_io
                        .write_all(&mut file, timestamp.to_be_bytes())
                        .await
                        .unwrap();
                    file_io
                        .write_all(&mut file, bytes.len().to_be_bytes())
                        .await
                        .unwrap();
                    file_io.write_all(&mut file, bytes).await.unwrap();
                }
            }

            file_io.flush(&mut file).await.unwrap();
        }
    }
}

#[async_trait]
impl<T: FileIo + Send + Sync + 'static> DirectUpdate for BmpFsOutRunner<T> {
    async fn direct_update(&self, update: Update) {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                source_id,
            }) => {
                if self.config.mode == Mode::Split {
                    // Drop the sender to the writer for this router. Any
                    // queued messages will be written by the writer after
                    // which the background writer task will finish.
                    self.senders.remove(&source_id);
                }
            }

            Update::Single(
                payload @ Payload {
                    value: TypeValue::Builtin(BuiltinTypeValue::BmpMessage(_)),
                    ..
                },
            ) => {
                // Dispatch the message to the writer for this
                // router. Create the writer if it doesn't
                // exist yet.
                let source_id = payload.source_id().clone();
                let tx =
                    self.senders.entry(source_id.clone()).or_insert_with(
                        || Arc::new(self.spawn_writer(source_id.clone())),
                    );
                if let Err(err) = tx.send(payload) {
                    self.status_reporter.write_error(source_id, err);
                }
            }

            _ => {
                self.status_reporter.input_mismatch(
                    "Update::Single(Payload::RawBmp)",
                    update,
                );
            }
        }
    }
}

impl<T: FileIo + Sync + Send + 'static> AnyDirectUpdate
    for BmpFsOutRunner<T>
{
}

impl<T: FileIo + Send + Sync> std::fmt::Debug for BmpFsOutRunner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetRunner").finish()
    }
}

pub fn mk_hash<T: Hash>(val: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}
