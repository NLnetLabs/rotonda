//! Controlling the entire operation.

use crate::common::file_io::TheFileIo;
use crate::http_ng;
use crate::roto_runtime::metrics::RotoMetricsWrapper;
use crate::roto_runtime::types::FilterName;
use crate::roto_runtime::types::RotoPackage;
use crate::roto_runtime::create_runtime;
use crate::comms::{
    DirectLink, Gate, GateAgent, GraphStatus, Link, DEF_UPDATE_QUEUE_LEN,
};
use crate::config::{Config, ConfigFile, Marked};
use crate::log::Terminate;
use crate::targets::Target;
use crate::tracing::Tracer;
use crate::units::Unit;
use crate::{ingress, metrics};
use arc_swap::ArcSwap;
use futures::future::{join_all, select, Either};
use log::{debug, error, info, log_enabled, trace, warn};
use non_empty_vec::NonEmpty;
use serde::Deserialize;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use std::{cell::RefCell, fmt::Display};
use std::{collections::HashMap, mem::Discriminant};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Barrier;
use uuid::Uuid;

//------------ Component -----------------------------------------------------

/// Facilities available to all components.
///
/// Upon being started, every component receives one of these. It provides
/// access to information and services available to all components.
pub struct Component {
    /// The component’s name.
    name: Arc<str>,

    /// The component's type name.
    type_name: &'static str,

    /// A reference to the metrics collection.
    metrics: Option<metrics::Collection>,

    /// A reference to the compiled Roto script.
    roto_package: Option<Arc<RotoPackage>>,

    /// The metrics source for user-defined metrics in roto scripts.
    roto_metrics: Option<Arc<RotoMetricsWrapper>>,

    /// A reference to the Tracer
    tracer: Arc<Tracer>,

    /// A reference to the ingress sources.
    ingresses: Arc<ingress::Register>,

    http_ng_api: Arc<Mutex<http_ng::Api>>,
}

#[cfg(test)]
impl Default for Component {
    fn default() -> Self {
        Self {
            name: "MOCK".into(),
            type_name: "MOCK",
            metrics: Default::default(),
            roto_package: Default::default(),
            roto_metrics: Default::default(),
            tracer: Default::default(),
            ingresses: Default::default(),
            http_ng_api: Default::default(),
        }
    }
}

#[allow(clippy::too_many_arguments)] // FIXME
impl Component {
    /// Creates a new component from its, well, components.
    fn new(
        name: String,
        type_name: &'static str,
        metrics: metrics::Collection,
        roto_package: Option<Arc<RotoPackage>>,
        roto_metrics: Option<Arc<RotoMetricsWrapper>>,
        tracer: Arc<Tracer>,
        ingresses: Arc<ingress::Register>,
        http_ng_api: Arc<Mutex<http_ng::Api>>,
    ) -> Self {
        Component {
            name: name.into(),
            type_name,
            metrics: Some(metrics),
            roto_package,
            roto_metrics,
            tracer,
            ingresses,
            http_ng_api,
        }
    }

    /// Returns the name of the component.
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// Returns the type name of the component.
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }


    pub fn roto_package(
        &self,
    ) -> &Option<Arc<RotoPackage>> {
        &self.roto_package
    }
    
    pub fn roto_metrics(&self) -> &Option<Arc<RotoMetricsWrapper>> {
        &self.roto_metrics
    }

    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    /// Register a metrics source.
    pub fn register_metrics(&mut self, source: Arc<dyn metrics::Source>) {
        if let Some(metrics) = &self.metrics {
            metrics.register(self.name.clone(), Arc::downgrade(&source));
        }
    }

    pub fn register_ingress(&self) -> ingress::IngressId {
        self.ingresses.register()
    }

    pub fn ingresses(&self) -> Arc<ingress::Register> {
        self.ingresses.clone()
    }

    pub fn http_ng_api_arc(&self) -> Arc<Mutex<http_ng::Api>> {
        self.http_ng_api.clone()
    }
}

//------------ Manager -------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum LinkType {
    Queued,
    Direct,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct LinkInfo {
    link_type: LinkType,
    id: Uuid,
    gate_id: Uuid,
    connected_gate_slot: Option<Uuid>,
}

impl From<&Link> for LinkInfo {
    fn from(link: &Link) -> Self {
        Self {
            link_type: LinkType::Queued,
            id: link.id(),
            gate_id: link.gate_id(),
            connected_gate_slot: link.connected_gate_slot(),
        }
    }
}

impl From<&DirectLink> for LinkInfo {
    fn from(link: &DirectLink) -> Self {
        Self {
            link_type: LinkType::Direct,
            id: link.id(),
            gate_id: link.gate_id(),
            connected_gate_slot: link.connected_gate_slot(),
        }
    }
}

#[derive(Debug, Default)]
pub struct LinkReport {
    gates: HashMap<String, Uuid>,
    links: HashMap<String, UpstreamLinkReport>,
}

impl LinkReport {
    fn new() -> Self {
        Default::default()
    }

    fn add_gate(&mut self, name: String, id: Uuid) {
        self.gates.insert(name, id);
    }

    fn add_link(&mut self, name: String, report: UpstreamLinkReport) {
        self.links.insert(name, report);
    }

    fn ready(&self) -> Result<(), usize> {
        let remaining = self
            .links
            .iter()
            .filter(|(_name, report)| !report.ready())
            .count();
        if remaining > 0 {
            Err(remaining)
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn get_gate_id(&self, name: &str) -> Option<Uuid> {
        self.gates.get(name).copied()
    }

}

#[derive(Clone, Default)]
pub struct UpstreamLinkReport {
    links: Arc<Mutex<Option<Vec<LinkInfo>>>>,
    // this is actually about downstream sending, not about upstream links ...
    graph_status: Arc<Mutex<Option<Weak<dyn GraphStatus>>>>,
}

impl std::fmt::Debug for UpstreamLinkReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.links.lock().unwrap().deref() {
            Some(links) => {
                let graph_status = self.graph_status.lock().unwrap();
                f.debug_struct("UpstreamLinkReport")
                    .field("links", links)
                    .field("graph_status", &graph_status.is_some())
                    .finish()
            }
            _ => f.debug_struct("UpstreamLinkReport").finish(),
        }
    }
}

impl UpstreamLinkReport {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn ready(&self) -> bool {
        self.links.lock().unwrap().is_some()
    }

    pub fn set_sources<T>(&self, links: &NonEmpty<T>)
    where
        // this strange for<'a> &'a construct is a Higher-Rank Trait Bound and
        // is needed here in order to express that a reference to T can be
        // made into a LinkInfo, i.e. to match impl From<&Link> for LinkInfo {
        // .. }.
        for<'a> &'a T: Into<LinkInfo>,
    {
        let mut lock = self.links.lock().unwrap();
        let typed_links = links.iter().map(|link| link.into()).collect();
        lock.replace(typed_links);
    }

    pub fn set_source<T>(&self, link: &T)
    where
        // this strange for<'a> &'a construct is a Higher-Rank Trait Bound and
        // is needed here in order to express that a reference to T can be
        // made into a LinkInfo, i.e. to match impl From<&Link> for LinkInfo {
        // .. }.
        for<'a> &'a T: Into<LinkInfo>,
    {
        let mut lock = self.links.lock().unwrap();
        lock.replace(vec![link.into()]);
    }

    /// Units that are themselves the source do not have upstream sources and
    /// so should
    /// notify via declare_source() that they have no sources.
    pub fn declare_source(&self) {
        let mut lock = self.links.lock().unwrap();
        lock.replace(vec![]);
    }

    pub fn set_graph_status(&self, graph_status: Arc<dyn GraphStatus>) {
        *self.graph_status.lock().unwrap() =
            Some(Arc::downgrade(&graph_status));
    }

    pub fn graph_status(&self) -> Option<Weak<dyn GraphStatus>> {
        self.graph_status.lock().unwrap().clone()
    }

    pub fn into_vec(&self) -> Vec<LinkInfo> {
        if let Some(v) = self.links.lock().unwrap().as_ref() {
            v.clone()
        } else {
            vec![]
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum TargetCommand {
    Reconfigure { new_config: Target },

    ReportLinks { report: UpstreamLinkReport },

    Terminate,
}

impl Display for TargetCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetCommand::Reconfigure { .. } => f.write_str("Reconfigure"),
            TargetCommand::ReportLinks { .. } => f.write_str("ReportLinks"),
            TargetCommand::Terminate => f.write_str("Terminate"),
        }
    }
}

/// A manager for components and auxiliary services.
///
/// Requires a running Tokio reactor that has been "entered" (see Tokio
/// `Handle::enter()`).
pub struct Manager {
    /// The currently active units represented by agents to their gates.
    running_units: HashMap<String, (Discriminant<Unit>, GateAgent)>,

    /// The currently active targets represented by their command senders.
    running_targets:
        HashMap<String, (Discriminant<Target>, mpsc::Sender<TargetCommand>)>,

    /// Gates for newly loaded, not yet spawned units.
    pending_gates: HashMap<String, (Gate, GateAgent)>,

    /// The metrics collection maintained by this manager.
    metrics: metrics::Collection,

    /// A reference to the compiled Roto script.
    roto_package: Option<Arc<RotoPackage>>,

    roto_metrics: Option<Arc<RotoMetricsWrapper>>,

    graph_svg_data: Arc<ArcSwap<(Instant, LinkReport)>>,

    #[allow(dead_code)]
    file_io: TheFileIo,

    tracer: Arc<Tracer>,

    ingresses: Arc<ingress::Register>,

    http_ng_api: Arc<Mutex<http_ng::Api>>,
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

impl Manager {
    /// Creates a new manager.
    pub fn new() -> Self {
        let graph_svg_data = Arc::new(ArcSwap::from_pointee((
            Instant::now(),
            LinkReport::new(),
        )));
        let tracer = Arc::new(Tracer::new());
        let ingresses = Arc::new(ingress::Register::new());
        let metrics: metrics::Collection = Default::default();
        let roto_metrics = Some(Arc::new(RotoMetricsWrapper::default()));
        metrics.register("roto_metrics".into(), Arc::downgrade(roto_metrics.as_ref().unwrap()) as Weak<dyn metrics::Source>);
        let http_ng_api = Arc::new(Mutex::new(http_ng::Api::new(
            Vec::with_capacity(1), // interfaces come from config, later on
            ingresses.clone(),
            metrics.clone()
        )));

        #[allow(
            clippy::let_and_return,
            clippy::default_constructed_unit_structs
        )]
        let manager = Manager {
            running_units: Default::default(),
            running_targets: Default::default(),
            pending_gates: Default::default(),
            metrics: metrics.clone(),
            roto_package: Default::default(),
            roto_metrics,
            graph_svg_data,
            file_io: TheFileIo::default(),
            tracer,
            ingresses,
            http_ng_api,
        };

        manager
    }

    #[cfg(test)]
    pub fn set_file_io(&mut self, file_io: TheFileIo) {
        self.file_io = file_io;
    }

    /// Loads the given config file.
    ///
    /// Parses the given file as a TOML config file. All links to units
    /// referenced in the configuration are pre-connected.
    ///
    /// If there are any errors in the config file, they are logged as errors
    /// and a generic error is returned.
    ///
    /// If the method succeeds, you need to call ['prepare'](Self::prepare)
    /// followed by the [`spawn`](Self::spawn) method to spawn all units and
    /// targets.
    pub fn load(&mut self, file: &ConfigFile) -> Result<Config, Terminate> {
        // Now load the config file, e.g. something like this:
        //
        //     [units.a]
        //     type = "some-unit"
        //
        //     [units.b]
        //     type = "other-unit"
        //     sources = ["a"]
        //
        //     [targets.c]
        //     type = "some-target"
        //     upstream = "b"
        //
        // Results in something like this:

        //   unit             unit             target
        //   ┌───┐            ┌───┐            ┌───┐
        //   │ a │gate◀───link│ b │gate◀───link│ c │
        //   └───┘            └───┘            └───┘
        //
        // And where:
        //                  link
        //                   ▲
        //                   │
        //                  agent
        //                   ▲
        //                   │
        //                  gate
        //                   ▲
        //                   │
        //                  load
        //                  unit
        //                   ▲
        //                   │
        //                  load
        //                  link
        //                   ▲
        //                   │
        //          unit◀───serde───▶target
        //                   ▲
        //                   │
        //                  config
        //                  file

        // Where for Unit (and not shown here but similar for Target):
        //
        //     #[derive(Debug, Deserialize)]
        //     #[serde(tag = "type")]
        //     pub enum Unit {
        //         #[serde(rename = "some-unit")] SomeUnit(SomeUnitConfig),
        //         #[serde(rename = "other-unit")] SomeUnit(OtherUnitConfig),
        //         ...
        //     }
        //
        // Deserializes into something like:
        //
        //     #[derive(Deserialize)]
        //     pub struct Config {
        //         units: UnitSet { HashMap<String, Unit> }
        //                                  "a" --> Unit::SomeUnit{..}
        //                                  "b" --> Unit::OtherUnit{..}
        //
        //         targets: TargetSet { HashMap<String, Target> }
        //                                      "c" --> Target::SomeTarget{..}
        //     }
        //
        // Where references to Link like: pub struct SomeUnit { sources:
        //     Vec<Link> } or: pub struct SomeTarget { upstream: Link }
        //
        // Are resolved via Serde like so:
        //
        //     From<String> for Link via manager::load_link(name)
        //
        // Where manager::load_link(): - Creates a name -> LoadUnit entry in
        //     the static GATES map. - Each LoadUnit consists of a new Gate
        //     and GateAgent. - An MPSC channel is created, with: - The Gate
        //     getting the Receiver (RxGate) to receive commands. - The
        //       GateAgent getting the Sender (TxGate) for cloning to enable
        //       other parties to send commands to the Gate. - The GateAgent
        //         is used to create a Link, with: - The Link getting a clone
        //     of the Sender to send commands to the Gate.
        //
        // At this point **DISCONNECTED** Gates and Links have been created: -
        //     The Gates have not been given to Units yet so no data updates
        //       will be pushed (from Units) via the Gates. - The Gates have
        //     empty update Sender collections (Vec<TxUpdate>) so have no
        //       corresponding LinkConnection Receiver instances to send data
        //       updates to. - The Links have been given to Targets but not
        //     yet connected to Gates, i.e. have no LinkConnection Sender yet.
        //
        // To "wire up" these disconnected components: - Manager::spawn() will
        //     assign Gates to Units.
        //
        //     - On first use Link::query() will invoke Link::connect() which
        //       will:
        //         - Construct a oneshot channel, keeping the Receiver
        //           (RxTemp) for itself, and passing the Sender (TxTemp) as
        //           the payload of a GateCommmand::Subscribe command to the
        //           Gate via the Links' cloned copy of TxGate.
        //         - The Gate will create a new MPSC channel and keep the
        //           Sender (TxUpdate) for itself and will pass the Receiver
        //           (RxUpdate) as the payload of a SubscribeResponse via
        //           TxTemp back to RxTemp.
        //         - TxTemp and RxTemp are discarded. The Gate now has a
        //           TxUpdate Sender which it can use to send data updates to
        //           to the RxUpdate Receiver which is now held by the Link
        //           (stored in a LinkConnection object).
        Config::from_bytes(file.bytes(), file.dir()).map_err(|err| {
            match file.path() {
                Some(path) => error!("{}: {}", path.display(), err),
                None => error!("{}", err),
            }
            Terminate::error()
        })
    }

    /// Prepare for spawning.
    ///
    /// Expects that the static GATES singleton has been populated with gates
    /// (actually LoadUnit values mapped to the name of the unit they are for,
    /// where a LoadUnit includes a Gate) for the units defined in the given
    /// Config.
    ///
    /// Primarily intended for testing purposes, allowing the prepare phase of
    /// the load -> prepare -> spawn pipeline to be tested independently of
    /// the other phases.
    pub fn prepare(
        &mut self,
        config: &Config,
        file: &ConfigFile,
    ) -> Result<(), Terminate> {
        let roto_script =
            config.roto_script.as_ref().and_then(|roto_script| {
                file.path()
                    .and_then(|p| p.parent())
                    .map(|d| d.to_path_buf())
                    .map(|mut dir| {
                        dir.push(roto_script);
                        dir
                    })
            });

        if let Err(err) = self.compile_roto_script(&roto_script) {
            let msg = format!("Unable to load main Roto script: {err}.");
            error!("{msg}");
            Err(Terminate::error())?
        }

        // Drain the singleton static GATES contents to a local variable.
        let gates = GATES
            .with(|gates| gates.replace(Some(Default::default())))
            .unwrap();

        // A Gate was created for each Link (e.g. for 'sources = ["a"]' and
        // 'upstream = "b"') but does the config file define units with
        // corresponding names ("a" and "b")? If not that means that the config
        // file includes unresolvable links between components. For resolvable
        // links the corresponding Gate will be moved to the pending
        // collection to be handled later by spawn(). For unresolvable links
        // the corresponding Gate will be dropped here.
        for (name, load) in gates {
            if let Some(mut gate) = load.gate {
                gate.set_tracer(self.tracer.clone());
                if !config.units.units.contains_key(&name) {
                    for mut link in load.links {
                        link.resolve_config(file);
                        error!(
                            "{}",
                            link.mark(format!(
                                "unresolved link to unit '{}'",
                                name
                            ))
                        );
                    }
                    return Err(Terminate::error());
                } else {
                    self.pending_gates
                        .insert(name.clone(), (gate, load.agent));
                }
            }
        }

        // At this point self.pending contains the newly created but
        // disconnected Gates, and GateAgents for sending commands to them,
        // and the Config object contains the newly created but not yet
        // started Units and Targets. The caller should invoke spawn() to run
        // each Unit and Target and assign Gates to Units by name.

        Ok(())
    }

    pub fn compile_roto_script(
        &mut self,
        roto_scripts_path: &Option<std::path::PathBuf>,
    ) -> Result<(), String> {
        let path = if let Some(p) = roto_scripts_path {
            p
        } else {
            info!("no roto scripts path to load filters from");
            return Ok(());
        };

        let i = roto::FileTree::read(path);
            // .map_err(|e| e.to_string())?;
        let roto_package = i
            .compile(&create_runtime().unwrap())
            .map_err(|e| e.to_string())?;

        self.roto_package = Some(Arc::new(Mutex::new(roto_package)));
        Ok(())
    }

    /// Spawns all units and targets in the config into the given runtime.
    ///
    /// # Panics
    ///
    /// The method panics if the config hasn’t been successfully prepared via
    /// the same manager earlier.
    ///
    /// # Hot reloading
    ///
    /// Running units and targets that do not exist in the config by the same
    /// name, or exist by the same name but have a different type, will be
    /// terminated.
    ///
    /// Running units and targets with the same name and type as in the config
    /// will be signalled to reconfigure themselves per the new config (even
    /// if unchanged, it is the responsibility of the unit/target to decide
    /// how to react to the "new" config).
    ///
    /// Units receive the reconfigure signal via their gate. The gate will
    /// automatically update itself and its clones to use the new set of
    /// Senders that correspond to changes in the set of downstream units and
    /// targets that link to the unit.
    ///
    /// Units and targets receive changes to their set of links (if any) as
    /// part of the "new" config payload of the reconfigure signal. It is the
    /// responsibility of the unit/target to switch from the old links to the
    /// new links and, if desired, to drain old link queues before ceasing to
    /// query them further.
    pub fn spawn(&mut self, config: &mut Config) {
        self.spawn_internal(
            config,
            Self::spawn_unit,
            Self::spawn_target,
            Self::reconfigure_unit,
            Self::reconfigure_target,
            Self::terminate_unit,
            Self::terminate_target,
        )
    }

    /// Separated out from [spawn](Self::spawn) for testing purposes.
    ///
    /// Pass the new unit to the existing unit to reconfigure itself. If the
    /// set of downstream units and targets that refer to the unit being
    /// reconfigured have changed, we need to ensure that the gates and links
    /// in use correspond to the newly configured topology. For example:
    ///
    /// Before:
    ///
    /// ```text
    ///     unit                                    target
    ///     ┌───┐                                   ┌───┐
    ///     │ a │gate◀─────────────────────────link│ c │
    ///     └───┘                                   └───┘
    /// ```
    /// After:
    ///
    /// ```text
    ///     running:
    ///     --------
    ///     unit                                    target
    ///     ┌───┐                                   ┌───┐
    ///     │ a │gate◀─────────────────────────link│ c │
    ///     └───┘                                   └───┘
    ///
    ///     pending:
    ///     --------
    ///     unit                unit                target
    ///     ┌───┐               ┌───┐               ┌───┐
    ///     │ a'│gate'◀───link'│ b'│gate'◀───link'│ c'│
    ///     └───┘               └───┘               └───┘
    /// ```
    /// In this example unit a and target c still exist in the config file,
    /// possibly with changed settings, and new unit b has been added. The
    /// pending gates and links for new units that correspond to existing
    /// units are NOT the same links and gates, hence they have been marked in
    /// the diagram with ' to distinguish them.
    ///
    /// At this point we haven't started unit b' yet so what we actually have
    /// is:
    ///
    /// ```text
    ///     current:
    ///     --------
    ///     unit                                    target
    ///     ┌───┐                                   ┌───┐
    ///     │ a │gate◀─────────────────────────link│ c │
    ///     └───┘                                   └───┘
    ///
    ///     unit                 unit               target
    ///     ┌───┐               ┌───┐               ┌───┐
    ///     │ a'│gate◀╴╴╴╴link'│ b'│gate'◀╴╴╴link'│ c'│
    ///     └───┘               └───┘               └───┘
    /// ```
    /// Versus:
    ///
    /// ```text
    ///     desired:
    ///     --------
    ///     unit                unit                target
    ///     ┌───┐               ┌───┐               ┌───┐
    ///     │ a │gate◀────link'│ b'│gate'◀───link'│ c │
    ///     └───┘               └───┘               └───┘
    /// ```
    /// If we blindly replace unit a with a' and target c with c' we risk
    /// breaking existing connections or discarding state unnecessarily. So
    /// instead we want the existing units and targets to decide for
    /// themselves what needs to be done to adjust to the configuration
    /// changes.
    ///
    /// If we weren't reconfiguring unit a we wouldn't have a problem, the new
    /// a' would correctly establish a link with the new b'. So it's unit a
    /// that we have to fix.
    ///
    /// Unit a has a gate with a data update Sender that corresponds with the
    /// Receiver of the link held by target c. The gate of unit a may actually
    /// have been cloned and thus there may be multiple Senders corresponding
    /// to target c. The gate of unit a may also be linked to other links held
    /// by other units and targets than in our simple example. We need to drop
    /// the wrong data update Senders and replace them with new ones referring
    /// to unit b' (well, at this point we don't know that it's unit b' we're
    /// talking about, just that we want gate a to have the same update
    /// Senders as gate a' (as all of the newly created gates and links have
    /// the desired topological setup).
    ///
    /// Note that the only way we have of influencing the data update senders
    /// of clones to match changes we make to the update sender of the
    /// original gate that was cloned is to send commands to the cloned gates.
    ///
    /// So, to reconfigure units we send their gate a Reconfigure command
    /// containing the new configuration. Note that this includes any newly
    /// constructed Links so the unit can .close() its current link(s) to
    /// prevent new incoming messages, process any messages still queued in
    /// the link, then when Link::query() returns UnitStatus::Gone because the
    /// queue is empty and the receiver is closed, we can at that point switch
    /// to using the new links. This is done inside each unit.
    ///
    /// The Reconfiguring GateStatus update will be propagated by the gate to
    /// its clones, if any. This allows spawned tasks each handling a client
    /// connection, e.g. from different routers, to each handle any
    /// reconfiguration required in the best way.
    ///
    /// For Targets we do something similar but as they don't have a Gate we
    /// pass a new MPSC Channel receiver to them and hold the corresponding
    /// sender here.
    ///
    /// Finally, unit and/or target configurations that have been commented
    /// out but for which a unit/target was already running, require that we
    /// detect the missing config and send a Terminate command to the orphaned
    /// unit/target.
    #[allow(clippy::too_many_arguments)]
    fn spawn_internal<
        SpawnUnit,
        SpawnTarget,
        ReconfUnit,
        ReconfTarget,
        TermUnit,
        TermTarget,
    >(
        &mut self,
        config: &mut Config,
        spawn_unit: SpawnUnit,
        spawn_target: SpawnTarget,
        reconfigure_unit: ReconfUnit,
        reconfigure_target: ReconfTarget,
        terminate_unit: TermUnit,
        terminate_target: TermTarget,
    ) where
        SpawnUnit: Fn(Component, Unit, Gate, WaitPoint),
        SpawnTarget:
            Fn(Component, Target, Receiver<TargetCommand>, WaitPoint),
        ReconfUnit: Fn(&str, GateAgent, Unit, Gate),
        ReconfTarget: Fn(&str, Sender<TargetCommand>, Target),
        TermUnit: Fn(&str, Arc<GateAgent>),
        TermTarget: Fn(&str, Arc<Sender<TargetCommand>>),
    {
        // We checked in load() for unresolvable links from units and targets
        // to upstreams. Here we do the opposite, we check for created Units
        // that are not referenced by any downstream units or targets. Any
        // so-called "unused" unit will either be discarded, or if already
        // running (due to a previous invocation of spawn()) will be commanded
        // to terminate.
        let mut new_running_units = HashMap::new();
        let mut new_running_targets = HashMap::new();

        let num_targets = config.targets.targets.len();
        let num_units = config.units.units.len();
        let coordinator = Coordinator::new(num_targets + num_units);

        // Spawn, reconfigure and terminate targets according to the config
        for (name, new_target) in config.targets.targets.drain() {
            if let Some(running_target) = self.running_targets.remove(&name) {
                let (running_target_type, running_target_sender) =
                    running_target;
                let new_target_type = std::mem::discriminant(&new_target);
                if new_target_type != running_target_type {
                    // Terminate the current target. The new one replacing it
                    // will be spawned below.
                    terminate_target(&name, running_target_sender.into());
                } else {
                    reconfigure_target(
                        &name,
                        running_target_sender.clone(),
                        new_target,
                    );
                    new_running_targets.insert(
                        name,
                        (running_target_type, running_target_sender),
                    );
                    // Skip spawning a new target.
                    continue;
                }
            }

            // Spawn the new target
            let component = Component::new(
                name.clone(),
                new_target.type_name(),
                self.metrics.clone(),
                self.roto_package.clone(),
                self.roto_metrics.clone(),
                self.tracer.clone(),
                self.ingresses.clone(),
                self.http_ng_api.clone(),
            );

            let target_type = std::mem::discriminant(&new_target);
            let (cmd_tx, cmd_rx) = mpsc::channel(100);
            spawn_target(
                component,
                new_target,
                cmd_rx,
                coordinator.clone().track(name.clone()),
            );
            new_running_targets.insert(name, (target_type, cmd_tx));
        }

        // Spawn, reconfigure and terminate units according to the config
        for (name, new_unit) in config.units.units.drain() {
            let (mut new_gate, new_agent) =
                match self.pending_gates.remove(&name) {
                    Some((gate, agent)) => (gate, agent),
                    None => {
                        if let Some(running_unit) =
                            self.running_units.remove(&name)
                        {
                            warn!(
                                "Unit '{}' is unused and will be stopped.",
                                name
                            );
                            let running_unit_agent = running_unit.1;
                            terminate_unit(&name, running_unit_agent.into());
                        } else {
                            error!(
                            "Unit '{}' is unused and will not be started.",
                            name
                        );
                        }
                        continue;
                    }
                };

            new_gate.set_name(&name);

            // For the Unit that was created for configuration file section
            // [units.<name>], see if we already have a GateAgent for a Unit
            // by that name, i.e. a Unit by that name is already running.
            if let Some(running_unit) = self.running_units.remove(&name) {
                // Yes, a Unit by that name is already running. Is it the same
                // type? If so, command it to reconfigure itself to match the
                // new Unit settings (if changed). Otherwise, command it to
                // terminate as it will be replaced by a unit of the same name
                // but different type.
                let (running_unit_type, running_unit_agent) = running_unit;
                let new_unit_type = std::mem::discriminant(&new_unit);
                if new_unit_type != running_unit_type {
                    // Terminate the current unit. The new one replacing it
                    // will be launched below.
                    terminate_unit(&name, running_unit_agent.into());
                } else {
                    reconfigure_unit(
                        &name,
                        running_unit_agent,
                        new_unit,
                        new_gate,
                    );
                    new_running_units
                        .insert(name, (new_unit_type, new_agent));
                    continue;
                }
            }

            // Spawn the new unit
            let component = Component::new(
                name.clone(),
                new_unit.type_name(),
                self.metrics.clone(),
                self.roto_package.clone(),
                self.roto_metrics.clone(),
                self.tracer.clone(),
                self.ingresses.clone(),
                self.http_ng_api.clone(),
            );

            let unit_type = std::mem::discriminant(&new_unit);
            spawn_unit(
                component,
                new_unit,
                new_gate,
                coordinator.clone().track(name.clone()),
            );
            new_running_units.insert(name, (unit_type, new_agent));
        }

        // Terminate running units whose corresponding configuration file
        // block was removed or commented out and thus not encountered above.
        for (name, (_, agent)) in self.running_units.drain() {
            terminate_unit(&name, agent.into());
        }

        // Terminate running targets whose corresponding configuration file
        // block was removed or commented out and thus not encountered above.
        for (name, (_, cmd_tx)) in self.running_targets.drain() {
            terminate_target(&name, cmd_tx.into());
        }

        self.running_units = new_running_units;
        self.running_targets = new_running_targets;

        self.coordinate_and_track_startup(coordinator);
    }

    fn coordinate_and_track_startup(
        &mut self,
        coordinator: Arc<Coordinator>,
    ) {
        let mut reports = LinkReport::new();
        let mut agent_cmd_futures = vec![];
        let mut target_cmd_futures = vec![];

        // Generate report-link commands to send to all running units.
        for (name, (_unit_type, gate_agent)) in &self.running_units {
            let report = UpstreamLinkReport::new();
            reports.add_gate(name.clone(), gate_agent.id());
            reports.add_link(name.clone(), report.clone());
            let agent = gate_agent.clone();
            let name = name.clone();
            agent_cmd_futures.push(async move {
                if let Err(err) = agent.report_links(report).await {
                    error!(
                        "Internal error: Report links command could not be sent to gate of unit '{}': {}",
                        name, err
                    );
                }
            });
        }

        // Generate report-link commands to send to all running targets.
        for (name, (_target_type, cmd_sender)) in &self.running_targets {
            let report = UpstreamLinkReport::new();
            reports.add_link(name.clone(), report.clone());

            let sender = cmd_sender.clone();
            let name = name.clone();
            target_cmd_futures.push(async move {
                if let Err(err) = sender.send(TargetCommand::ReportLinks { report }).await {
                    error!(
                        "Internal error: Report links command could not be sent to target '{}': {}",
                        name, err
                    );
                }
            });
        }

        let graph_svg_data = self.graph_svg_data.clone();
        let arc_api = self.http_ng_api_arc();

        crate::tokio::spawn("coordinator", async move {
            // Wait for all running units and targets to become ready and to
            // finish supplying responses to report-link commands, then log a
            // link report at debug level, and generate an SVG representation
            // of the link report for display at /status/graph.
            debug!("Waiting for coodinator");

            coordinator
                .wait(|pending_component_names, status| {
                    warn!(
                        "Components {} are taking a long time to become {}.",
                        pending_component_names.join(", "),
                        status
                    );
                })
                .await;

            // Now it's safe to send the report link commands. Before this
            // point they might have been ignored because one or more units
            // were not ready to process them yet.
            for future in agent_cmd_futures {
                future.await;
            }
            for future in target_cmd_futures {
                future.await;
            }
            loop {
                match reports.ready() {
                    Ok(()) => break,
                    Err(remaining) => {
                        trace!(
                            "Waiting for {} upstream link reports to become available.",
                            remaining
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            if log_enabled!(log::Level::Debug) {
                debug!(
                    "Dumping gate/link/unit/target relationships:\n{:#?}",
                    &reports
                );
            }

            graph_svg_data.swap(Arc::new((Instant::now(), reports)));
            // XXX
            // so we need to call (re)start here, because at this point we know for sure all other
            // Rotonda components had a chance to add their paths to the axum router
            // BUT
            // on SIGHUP, we also need to apply (changes to) the main config file, e.g. interfaces
            // to listen on, which does not happen here currently.
            // By doing that in the signal handler in main.rs, we effectively reload the http
            // servers twice, which is not nice.
            
            arc_api.lock().unwrap().restart();
        });
    }

    pub fn link_report_updated_at(&self) -> Instant {
        self.graph_svg_data.load().0
    }

    pub fn terminate(&mut self) {
        for (name, (_, agent)) in self.running_units.drain() {
            let agent = Arc::new(agent);
            Self::terminate_unit(&name, agent.clone());
            while !agent.is_terminated() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        for (name, (_, cmd_tx)) in self.running_targets.drain() {
            let cmd_tx = Arc::new(cmd_tx);
            Self::terminate_target(&name, cmd_tx.clone());
            while !cmd_tx.is_closed() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    fn spawn_unit(
        component: Component,
        new_unit: Unit,
        new_gate: Gate,
        waitpoint: WaitPoint,
    ) {
        info!("Starting unit '{}'", component.name);
        crate::tokio::spawn(
            &format!("unit[{}]", component.name),
            new_unit.run(component, new_gate, waitpoint),
        );
    }

    fn spawn_target(
        component: Component,
        new_target: Target,
        cmd_rx: Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) {
        info!("Starting target '{}'", component.name);
        crate::tokio::spawn(
            &format!("target[{}]", component.name),
            new_target.run(component, cmd_rx, waitpoint),
        );
    }

    fn reconfigure_unit(
        name: &str,
        agent: GateAgent,
        new_config: Unit,
        new_gate: Gate,
    ) {
        info!("Reconfiguring unit '{}'", name);
        let name = name.to_owned();
        crate::tokio::spawn("unit-reconfigurer", async move {
            if let Err(err) = agent.reconfigure(new_config, new_gate).await {
                error!(
                    "Internal error: reconfigure command could not be sent to unit '{}': {}",
                    name, err
                );
            }
        });
    }

    fn reconfigure_target(
        name: &str,
        sender: Sender<TargetCommand>,
        new_config: Target,
    ) {
        info!("Reconfiguring target '{}'", name);
        let name = name.to_owned();
        crate::tokio::spawn("target-reconfigurer", async move {
            if let Err(err) =
                sender.send(TargetCommand::Reconfigure { new_config }).await
            {
                error!(
                    "Internal error: reconfigure command could not be sent to target '{}': {}",
                    name, err
                );
            }
        });
    }

    fn terminate_unit(name: &str, agent: Arc<GateAgent>) {
        info!("Stopping unit '{}'", name);
        crate::tokio::spawn("unit-terminator", async move {
            agent.terminate().await;
        });
    }

    fn terminate_target(name: &str, sender: Arc<Sender<TargetCommand>>) {
        info!("Stopping target '{}'", name);
        crate::tokio::spawn("target-terminator", async move {
            let _ = sender.send(TargetCommand::Terminate).await;
        });
    }

    /// Returns a new reference to the manager’s metrics collection.
    pub fn metrics(&self) -> metrics::Collection {
        self.metrics.clone()
    }

    /// Returns a reference to the shared HTTP API
    pub fn http_ng_api_arc(&mut self) -> Arc<Mutex<http_ng::Api>> {
        self.http_ng_api.clone()
    }

    /// Reload the HTTP configuration (listening interfaces)
    pub fn reload_http_ng_config(&mut self, config: &Config) {
        if let Ok(mut lock) = self.http_ng_api.lock(){
           lock.set_interfaces(config.http_ng_listen.clone().into_iter().flatten());
        }
    }

    /// Restart the HTTP API based on the passed Rotonda `Config`
    pub fn restart_http_ng_with_config(&mut self, config: &Config) {
        if let Ok(mut lock) = self.http_ng_api.lock(){
           lock.set_interfaces(config.http_ng_listen.clone().into_iter().flatten());
           lock.restart();
        }
    }

}

//------------ Checkpoint ----------------------------------------------------

pub struct WaitPoint {
    coordinator: Arc<Coordinator>,
    name: String,
    ready: bool,
}

impl WaitPoint {
    pub fn new(coordinator: Arc<Coordinator>, name: String) -> Self {
        Self {
            coordinator,
            name,
            ready: false,
        }
    }

    pub async fn ready(&mut self) {
        self.coordinator.clone().ready(&self.name).await;
        self.ready = true;
    }

    pub async fn running(mut self) {
        // Targets don't need to signal ready & running separately so they
        // just invoke this fn, but we still need to make sure that the
        // barrier is reached twice otherwise the client of the Coordinator
        // will be left waiting forever.
        if !self.ready {
            self.ready().await;
        }
        self.coordinator.ready(&self.name).await
    }
}

pub struct Coordinator {
    barrier: Barrier,
    max_components: usize,
    pending: Arc<RwLock<HashSet<String>>>,
}

impl Coordinator {
    pub const SLOW_COMPONENT_ALARM_DURATION: Duration =
        Duration::from_secs(60);

    pub fn new(max_components: usize) -> Arc<Self> {
        let barrier = Barrier::new(max_components + 1);
        let pending = Arc::new(RwLock::new(HashSet::new()));
        Arc::new(Self {
            barrier,
            max_components,
            pending,
        })
    }

    pub fn track(self: Arc<Self>, name: String) -> WaitPoint {
        if self.pending.write().unwrap().insert(name.clone()) {
            if self.pending.read().unwrap().len() > self.max_components {
                panic!(
                    "Coordinator::track() called more times than expected"
                );
            }
            WaitPoint::new(self, name)
        } else {
            unreachable!();
        }
    }

    // Note: should be invoked twice:
    //   - The first time the the units and targets reach the barrier when all
    //     are ready. The barrier is then automatically reset and ready for
    //     use again.
    //   - The second time the units and targets reach the barrier when all
    //     are running.
    pub async fn ready(self: Arc<Self>, name: &str) {
        if self
            .pending
            .read()
            .unwrap()
            .get(&name.to_string())
            .is_some()
        {
            self.barrier.wait().await;
        } else {
            unreachable!();
        }
    }

    pub async fn wait<T>(self: Arc<Self>, mut alarm: T)
    where
        T: FnMut(Vec<String>, &str),
    {
        // Units and targets need to reach the barrier twice: once to signal
        // that they are ready to run but are not yet actually running, and
        // once when they are running.
        self.clone().wait_internal(&mut alarm, "ready").await;
        self.wait_internal(&mut alarm, "running").await;
    }

    pub async fn wait_internal<T>(
        self: Arc<Self>,
        alarm: &mut T,
        status: &str,
    ) where
        T: FnMut(Vec<String>, &str),
    {
        debug!("Waiting for all components to become {}...", status);
        let num_unused_barriers =
            self.max_components - self.pending.read().unwrap().len() + 1;
        let unused_barriers: Vec<_> = (0..num_unused_barriers)
            .map(|_| self.barrier.wait())
            .collect();
        let slow_startup_alarm =
            Box::pin(tokio::time::sleep(Self::SLOW_COMPONENT_ALARM_DURATION));
        match select(join_all(unused_barriers), slow_startup_alarm).await {
            Either::Left(_) => {}
            Either::Right((_, incomplete_join_all)) => {
                // Raise the alarm about the slow components
                let pending_component_names = self
                    .pending
                    .read()
                    .unwrap()
                    .iter()
                    .cloned()
                    .collect::<Vec<String>>();
                alarm(pending_component_names, status);

                // Previous wait was interrupted, keep waiting
                incomplete_join_all.await;
            }
        }
        info!("All components are {}.", status);
    }
}

//------------ UnitSet -------------------------------------------------------

/// A set of units to be started.
#[derive(Deserialize)]
#[serde(transparent)]
pub struct UnitSet {
    units: HashMap<String, Unit>,
}

impl UnitSet {
    pub fn units(&self) -> &HashMap<String, Unit> {
        &self.units
    }
}

impl From<HashMap<String, Unit>> for UnitSet {
    fn from(v: HashMap<String, Unit>) -> Self {
        Self { units: v }
    }
}

//------------ TargetSet -----------------------------------------------------

/// A set of targets to be started.
#[derive(Default, Deserialize)]
#[serde(transparent)]
pub struct TargetSet {
    targets: HashMap<String, Target>,
}

impl TargetSet {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn targets(&self) -> &HashMap<String, Target> {
        &self.targets
    }
}

impl From<HashMap<String, Target>> for TargetSet {
    fn from(v: HashMap<String, Target>) -> Self {
        Self { targets: v }
    }
}

//------------ LoadUnit ------------------------------------------------------

/// A unit referenced during loading.
struct LoadUnit {
    /// The gate of the unit.
    ///
    /// This is some only if the unit is newly created and has not yet been
    /// spawned onto a runtime.
    gate: Option<Gate>,

    /// A gate agent for the unit.
    agent: GateAgent,

    /// A list of location of links in the config.
    ///
    /// This is only used for generating errors if non-existing units are
    /// referenced in the config file.
    links: Vec<Marked<()>>,
}

impl LoadUnit {
    fn new(queue_size: usize) -> Self {
        let (gate, agent) = Gate::new(queue_size);
        LoadUnit {
            gate: Some(gate),
            agent,
            links: Vec::new(),
        }
    }
}

impl From<GateAgent> for LoadUnit {
    fn from(agent: GateAgent) -> Self {
        LoadUnit {
            gate: None,
            agent,
            links: Vec::new(),
        }
    }
}

//------------ Loading Links -------------------------------------------------

thread_local!(
    static GATES: RefCell<Option<HashMap<String, LoadUnit>>> =
        RefCell::new(Some(Default::default()))
);

/// Loads a link with the given name.
///
/// # Panics
///
/// This function panics if it is called outside of a run of
/// [`Manager::load`].
pub fn load_link(link_id: Marked<String>) -> Link {
    GATES.with(|gates| {
        let mut gates = gates.borrow_mut();
        let gates = gates.as_mut().unwrap();

        let mark = link_id.mark(());
        let link_id = link_id.into_inner();
        let (name, queue_size) = get_queue_size_for_link(link_id);
        let unit = gates
            .entry(name)
            .or_insert_with(|| LoadUnit::new(queue_size));
        unit.links.push(mark);
        unit.agent.create_link()
    })
}

/// Support link names of the form <name>:<queue_size> where queue_size is an
/// unsigned integer value.
///
/// TODO: Don't overload the meaning of the link name, instead support a
/// richer more meaningful configuration syntax for configuring the queue
/// size.
fn get_queue_size_for_link(link_id: String) -> (String, usize) {
    let (name, queue_size) =
        if let Some((name, options)) = link_id.split_once(':') {
            let queue_len = options.parse::<usize>().unwrap_or_else(|err| {
                warn!(
                "Invalid queue length '{}' for '{}', falling back to the \
                default ({}): {}",
                options, name, DEF_UPDATE_QUEUE_LEN, err
            );
                DEF_UPDATE_QUEUE_LEN
            });
            (name.to_string(), queue_len)
        } else {
            (link_id, DEF_UPDATE_QUEUE_LEN)
        };
    (name, queue_size)
}

//------------ Loading FilterName --------------------------------------------

thread_local!(
    static ROTO_FILTER_NAMES: RefCell<Option<HashSet<FilterName>>> =
        RefCell::new(Some(Default::default()))
);

/// Loads a filter name with the given name.
///
/// # Panics
///
/// This function panics if it is called outside of a run of
/// [`Manager::load`].
pub fn load_filter_name(filter_name: FilterName) -> FilterName {
    ROTO_FILTER_NAMES.with(|filter_names| {
        let mut filter_names = filter_names.borrow_mut();
        let filter_names = filter_names.as_mut().unwrap();
        filter_names.insert(filter_name.clone());
        filter_name
    })
}

//------------ Tests ---------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        fmt::Display,
        ops::{Deref, DerefMut},
        sync::atomic::{AtomicU8, Ordering::SeqCst},
    };

    use super::*;

    use crate::config::Source;

    static SOME_COMPONENT: &str = "some-component";
    static OTHER_COMPONENT: &str = "other-component";

    //     #[test]
    //     fn gates_singleton_is_correctly_initialized() {
    //         let gates = GATES.with(|gates| gates.take());
    //         assert!(gates.is_some());
    //         assert!(gates.unwrap().is_empty());
    //     }

    //     #[test]
    //     fn new_manager_is_correctly_initialized() {
    //         let manager = init_manager();
    //         assert!(manager.running_units.is_empty());
    //         assert!(manager.running_targets.is_empty());
    //         assert!(manager.pending_gates.is_empty());
    //     }

    //     #[test]
    //     fn config_without_target_should_fail() {
    //         // given a config without any targets
    //         let toml = r#"
    //         http_listen = []
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager
    //         let mut manager = init_manager();
    //         let res = Config::from_config_file(config_file, &mut manager);

    //         // then it should fail
    //         assert!(res.is_err());
    //     }

    //     #[test]
    //     fn config_with_unresolvable_links_should_fail() {
    //         // given a config with only a single target with a link to a
    //         // missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [targets.null]
    //         type = "null-out"
    //         source = "missing-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager
    //         let mut manager = init_manager();
    //         let res = Config::from_config_file(config_file, &mut manager);

    //         // then it should fail
    //         assert!(res.is_err());
    //     }

    //     #[test]
    //     fn config_with_unresolvable_filter_names_should_fail() {
    //         // given a config with only unit that takes a filter_name
    //         // referring to a roto filter that has not been loaded
    //         let toml = r#"
    //         http_listen = []

    //         [units.filter]
    //         type = "filter"
    //         filter_name = "i-dont-exist"

    //         [targets.null]
    //         type = "null-out"
    //         source = "missing-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager
    //         let mut manager = init_manager();
    //         let res = Config::from_config_file(config_file, &mut manager);

    //         // then it should fail
    //         assert!(res.is_err());
    //     }

    //     #[test]
    //     fn fully_resolvable_config_should_load() {
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager
    //         let mut manager = init_manager();
    //         let res = Config::from_config_file(config_file, &mut manager);

    //         // then it should pass
    //         assert!(res.is_ok());
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn fully_resolvable_config_should_spawn() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);
    //         Ok(())
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn config_reload_should_trigger_reconfigure() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file.clone(), &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);

    //         // and when re-loaded
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // it should cause reconfiguration
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
    //         assert_log_contains(&log, "null", SpawnAction::ReconfigureTarget);
    //         Ok(())
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn unused_unit_should_not_be_spawned() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.unused-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);
    //         Ok(())
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn added_target_should_be_spawned() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a single unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);

    //         // when the config is modified to include a new target
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"

    //         [targets.null2]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the added target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 3);
    //         assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
    //         assert_log_contains(&log, "null", SpawnAction::ReconfigureTarget);
    //         assert_log_contains(&log, "null2", SpawnAction::SpawnTarget);
    //         Ok(())
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn removed_target_should_be_terminated() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"

    //         [targets.null2]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 3);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);
    //         assert_log_contains(&log, "null2", SpawnAction::SpawnTarget);

    //         // when the config is modified to remove a target
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         #[targets.null]
    //         #type = "null-out"
    //         #source = "some-unit"

    //         [targets.null2]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should terminate the removed target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 3);
    //         assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
    //         assert_log_contains(&log, "null", SpawnAction::TerminateTarget);
    //         assert_log_contains(&log, "null2", SpawnAction::ReconfigureTarget);

    //         // Note: we don't check that the gate of some-unit has been updated to
    //         // remove the Sender for the Link to target null because that is logic
    //         // within the Gate itself and should be tested in the Gate unit tests.

    //         Ok(())
    //     }

    //     #[tokio::test(flavor = "multi_thread")]
    //     async fn modified_settings_are_correctly_announced() -> Result<(), Terminate> {
    //         // given a config with only a single target with a link to a missing unit
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = ""

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let mut manager = init_manager();
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should spawn the unit and target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
    //         assert_log_contains(&log, "null", SpawnAction::SpawnTarget);

    //         // when the config is modified
    //         let toml = r#"
    //         http_listen = []

    //         [units.some-unit]
    //         type = "bmp-tcp-in"
    //         listen = "changed"

    //         [targets.null]
    //         type = "null-out"
    //         source = "some-unit"
    //         "#;
    //         let config_file = mk_config_from_toml(toml);

    //         // when loaded into the manager and spawned
    //         let (_source, config) = Config::from_config_file(config_file, &mut manager)?;
    //         spawn(&mut manager, config);

    //         // then it should terminate the removed target
    //         let log = SPAWN_LOG.with(|log| log.take());
    //         assert_eq!(log.len(), 2);
    //         assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
    //         assert_log_contains(&log, "null", SpawnAction::ReconfigureTarget);

    //         let item = get_log_item(&log, "some-unit", SpawnAction::ReconfigureUnit);
    //         if let UnitOrTargetConfig::UnitConfig(Unit::BmpTcpIn(config)) = &item.config {
    //             assert_eq!(config.listen, "changed");
    //         } else {
    //             unreachable!();
    //         }

    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn coordinator_with_no_components_should_finish_immediately() {
    //         let coordinator = Coordinator::new(0);
    //         let mut alarm_fired = false;
    //         coordinator.wait(|_, _| alarm_fired = true).await;
    //         assert!(!alarm_fired);
    //     }

    //     #[tokio::test]
    //     #[should_panic]
    //     async fn coordinator_track_too_many_components_causes_panic() {
    //         let coordinator = Coordinator::new(0);
    //         coordinator.track(SOME_COMPONENT.to_string());
    //     }

    //     #[tokio::test]
    //     #[should_panic]
    //     async fn coordinator_track_component_twice_causes_panic() {
    //         let coordinator = Coordinator::new(0);
    //         coordinator.clone().track(SOME_COMPONENT.to_string());
    //         coordinator.track(SOME_COMPONENT.to_string());
    //     }

    //     #[tokio::test]
    //     #[should_panic]
    //     async fn coordinator_unknown_ready_component_twice_causes_panic() {
    //         let coordinator = Coordinator::new(0);
    //         coordinator.ready(SOME_COMPONENT).await;
    //     }

    //     #[tokio::test(start_paused = true)]
    //     async fn coordinator_with_one_ready_component_should_not_raise_alarm() {
    //         let coordinator = Coordinator::new(1);
    //         let mut alarm_fired = false;
    //         let wait_point = coordinator.clone().track(SOME_COMPONENT.to_string());
    //         let join_handle = tokio::task::spawn(wait_point.running());
    //         assert!(!join_handle.is_finished());
    //         coordinator.wait(|_, _| alarm_fired = true).await;
    //         join_handle.await.unwrap();
    //         assert!(!alarm_fired);
    //     }

    //     #[tokio::test(start_paused = true)]
    //     async fn coordinator_with_two_ready_components_should_not_raise_alarm() {
    //         let coordinator = Coordinator::new(2);
    //         let mut alarm_fired = false;
    //         let wait_point1 = coordinator.clone().track(SOME_COMPONENT.to_string());
    //         let wait_point2 = coordinator.clone().track(OTHER_COMPONENT.to_string());
    //         let join_handle1 = tokio::task::spawn(wait_point1.running());
    //         let join_handle2 = tokio::task::spawn(wait_point2.running());
    //         assert!(!join_handle1.is_finished());
    //         assert!(!join_handle2.is_finished());
    //         coordinator.wait(|_, _| alarm_fired = true).await;
    //         join_handle1.await.unwrap();
    //         join_handle2.await.unwrap();
    //         assert!(!alarm_fired);
    //     }

    //     #[tokio::test(start_paused = true)]
    //     async fn coordinator_with_component_with_slow_ready_phase_should_raise_alarm() {
    //         let coordinator = Coordinator::new(1);
    //         let alarm_fired_count = Arc::new(AtomicU8::new(0));
    //         let wait_point = coordinator.clone().track(SOME_COMPONENT.to_string());

    //         // Deliberately don't call wait_point.ready() or wait_point.running()
    //         let join_handle = {
    //             let alarm_fired_count = alarm_fired_count.clone();
    //             tokio::task::spawn(coordinator.wait(move |_, _| {
    //                 alarm_fired_count.fetch_add(1, Ordering::Relaxed);
    //             }))
    //         };

    //         // Advance time beyond the maximum time allowed for the 'ready' state to be reached
    //         let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
    //         let advance_time_by = advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
    //         tokio::time::sleep(advance_time_by).await;

    //         // Check that the alarm fired once
    //         assert_eq!(alarm_fired_count.load(Ordering::Relaxed), 1);

    //         // Set the component state to the final state 'running'
    //         wait_point.running().await;

    //         // Which should unblock the coordinator wait
    //         join_handle.await.unwrap();
    //     }

    //     #[tokio::test(start_paused = true)]
    //     async fn coordinator_with_component_with_slow_running_phase_should_raise_alarm() {
    //         let coordinator = Coordinator::new(1);
    //         let alarm_fired_count = Arc::new(AtomicU8::new(0));
    //         let mut wait_point = coordinator.clone().track(SOME_COMPONENT.to_string());

    //         // Deliberately don't call wait_point.ready() or wait_point.running()
    //         let join_handle = {
    //             let alarm_fired_count = alarm_fired_count.clone();
    //             tokio::task::spawn(coordinator.wait(move |_, _| {
    //                 alarm_fired_count.fetch_add(1, Ordering::Relaxed);
    //             }))
    //         };

    //         // Advance time beyond the maximum time allowed for the 'ready' state to be reached
    //         let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
    //         let advance_time_by = advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
    //         tokio::time::sleep(advance_time_by).await;

    //         // Check that the alarm fired once
    //         assert_eq!(alarm_fired_count.load(Ordering::Relaxed), 1);

    //         // Achieve the 'ready' state in the component under test, but not yet the 'running' state
    //         wait_point.ready().await;

    //         // Advance time beyond the maximum time allowed for the 'running' state to be reached
    //         let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
    //         let advance_time_by = advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
    //         tokio::time::sleep(advance_time_by).await;

    //         // Check that the alarm fired again
    //         assert_eq!(alarm_fired_count.load(Ordering::Relaxed), 2);

    //         // Set the component state to the final state 'running'
    //         wait_point.running().await;

    //         // Which should unblock the coordinator wait
    //         join_handle.await.unwrap();
    //     }

    //use uuid::Uuid;

    //use crate::{
    //    tracing::{MsgRelation, Trace},
    //};

    //#[test]
    //fn test_empty_extract_msg_indices() {
    //    let empty_trace = Trace::new();
    //    let gate_id = Uuid::new_v4();
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id);
    //    assert_eq!("[]", trace_txt);
    //}

    //#[test]
    //fn test_single_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id = Uuid::new_v4();
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id);
    //    assert_eq!("[0]", trace_txt);
    //}

    //#[test]
    //fn test_long_range_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id = Uuid::new_v4();
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id);
    //    assert_eq!("[0-2]", trace_txt);
    //}

    //#[test]
    //fn test_single_range_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id = Uuid::new_v4();
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    empty_trace.append_msg(gate_id, "blah".to_owned(), MsgRelation::GATE);
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id);
    //    assert_eq!("[0-1]", trace_txt);
    //}

    //#[test]
    //fn test_single_then_range_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id1 = Uuid::new_v4();
    //    let gate_id2 = Uuid::new_v4();
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id2);
    //    assert_eq!("[0, 2-3]", trace_txt);
    //}

    //#[test]
    //fn test_single_range_single_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id1 = Uuid::new_v4();
    //    let gate_id2 = Uuid::new_v4();
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id2);
    //    assert_eq!("[0, 2-3, 5]", trace_txt);
    //}

    //#[test]
    //fn test_single_range_single_long_range_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id1 = Uuid::new_v4();
    //    let gate_id2 = Uuid::new_v4();
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id2);
    //    assert_eq!("[0, 2-3, 5, 7-9]", trace_txt);
    //}

    //#[test]
    //fn test_range_then_single_extract_msg_indices() {
    //    let mut empty_trace = Trace::new();
    //    let gate_id1 = Uuid::new_v4();
    //    let gate_id2 = Uuid::new_v4();
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id1,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    empty_trace.append_msg(
    //        gate_id2,
    //        "blah".to_owned(),
    //        MsgRelation::GATE,
    //    );
    //    let trace_txt = extract_msg_indices(&empty_trace, gate_id2);
    //    assert_eq!("[0-1, 3]", trace_txt);
    //}

    #[tokio::test(flavor = "multi_thread")]
    async fn unused_unit_should_not_be_spawned() -> Result<(), Terminate> {
        // given a config with only a single target with a link to a missing unit
        let toml = r#"
        #http_listen = []

        [units.unused-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [targets.null]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let mut manager = init_manager();
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should spawn the unit and target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 2);
        assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
        assert_log_contains(&log, "null", SpawnAction::SpawnTarget);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn added_target_should_be_spawned() -> Result<(), Terminate> {
        // given a config with only a single target with a link to a single unit
        let toml = r#"
        #http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [targets.null]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let mut manager = init_manager();
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should spawn the unit and target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 2);
        assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
        assert_log_contains(&log, "null", SpawnAction::SpawnTarget);

        // when the config is modified to include a new target
        let toml = r#"
        #http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [targets.null]
        type = "null-out"
        source = "some-unit"

        [targets.null2]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should spawn the added target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 3);
        assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
        assert_log_contains(&log, "null", SpawnAction::ReconfigureTarget);
        assert_log_contains(&log, "null2", SpawnAction::SpawnTarget);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removed_target_should_be_terminated() -> Result<(), Terminate> {
        // given a config with only a single target with a link to a missing unit
        let toml = r#"
        #http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [targets.null]
        type = "null-out"
        source = "some-unit"

        [targets.null2]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let mut manager = init_manager();
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should spawn the unit and target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 3);
        assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
        assert_log_contains(&log, "null", SpawnAction::SpawnTarget);
        assert_log_contains(&log, "null2", SpawnAction::SpawnTarget);

        // when the config is modified to remove a target
        let toml = r#"
        #http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        #[targets.null]
        #type = "null-out"
        #source = "some-unit"

        [targets.null2]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should terminate the removed target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 3);
        assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
        assert_log_contains(&log, "null", SpawnAction::TerminateTarget);
        assert_log_contains(&log, "null2", SpawnAction::ReconfigureTarget);

        // Note: we don't check that the gate of some-unit has been updated to
        // remove the Sender for the Link to target null because that is logic
        // within the Gate itself and should be tested in the Gate unit tests.

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn modified_settings_are_correctly_announced(
    ) -> Result<(), Terminate> {
        // given a config with only a single target with a link to a missing unit
        let toml = r#"
         #http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "1.2.3.4:12345"

        [targets.null]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let mut manager = init_manager();
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should spawn the unit and target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 2);
        assert_log_contains(&log, "some-unit", SpawnAction::SpawnUnit);
        assert_log_contains(&log, "null", SpawnAction::SpawnTarget);

        // when the config is modified
        let toml = r#"
        http_listen = []

        [units.some-unit]
        type = "bmp-tcp-in"
        listen = "5.6.7.8:1818"

        [targets.null]
        type = "null-out"
        source = "some-unit"
        "#;
        let config_file = mk_config_from_toml(toml);

        // when loaded into the manager and spawned
        let (_source, config) =
            Config::from_config_file(config_file, &mut manager)?;
        spawn(&mut manager, config);

        // then it should terminate the removed target
        let log = SPAWN_LOG.with(|log| log.take());
        assert_eq!(log.len(), 2);
        assert_log_contains(&log, "some-unit", SpawnAction::ReconfigureUnit);
        assert_log_contains(&log, "null", SpawnAction::ReconfigureTarget);

        let item =
            get_log_item(&log, "some-unit", SpawnAction::ReconfigureUnit);
        if let UnitOrTargetConfig::UnitConfig(Unit::BmpTcpIn(config)) =
            &item.config
        {
            assert_eq!(config.listen.to_string(), "5.6.7.8:1818");
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[tokio::test]
    async fn coordinator_with_no_components_should_finish_immediately() {
        let coordinator = Coordinator::new(0);
        let mut alarm_fired = false;
        coordinator.wait(|_, _| alarm_fired = true).await;
        assert!(!alarm_fired);
    }

    #[tokio::test]
    #[should_panic]
    async fn coordinator_track_too_many_components_causes_panic() {
        let coordinator = Coordinator::new(0);
        coordinator.track(SOME_COMPONENT.to_string());
    }

    #[tokio::test]
    #[should_panic]
    async fn coordinator_track_component_twice_causes_panic() {
        let coordinator = Coordinator::new(0);
        coordinator.clone().track(SOME_COMPONENT.to_string());
        coordinator.track(SOME_COMPONENT.to_string());
    }

    #[tokio::test]
    #[should_panic]
    async fn coordinator_unknown_ready_component_twice_causes_panic() {
        let coordinator = Coordinator::new(0);
        coordinator.ready(SOME_COMPONENT).await;
    }

    #[tokio::test(start_paused = true)]
    async fn coordinator_with_one_ready_component_should_not_raise_alarm() {
        let coordinator = Coordinator::new(1);
        let mut alarm_fired = false;
        let wait_point =
            coordinator.clone().track(SOME_COMPONENT.to_string());
        let join_handle = tokio::task::spawn(wait_point.running());
        assert!(!join_handle.is_finished());
        coordinator.wait(|_, _| alarm_fired = true).await;
        join_handle.await.unwrap();
        assert!(!alarm_fired);
    }

    #[tokio::test(start_paused = true)]
    async fn coordinator_with_two_ready_components_should_not_raise_alarm() {
        let coordinator = Coordinator::new(2);
        let mut alarm_fired = false;
        let wait_point1 =
            coordinator.clone().track(SOME_COMPONENT.to_string());
        let wait_point2 =
            coordinator.clone().track(OTHER_COMPONENT.to_string());
        let join_handle1 = tokio::task::spawn(wait_point1.running());
        let join_handle2 = tokio::task::spawn(wait_point2.running());
        assert!(!join_handle1.is_finished());
        assert!(!join_handle2.is_finished());
        coordinator.wait(|_, _| alarm_fired = true).await;
        join_handle1.await.unwrap();
        join_handle2.await.unwrap();
        assert!(!alarm_fired);
    }

    #[tokio::test(start_paused = true)]
    async fn coordinator_with_component_with_slow_ready_phase_should_raise_alarm(
    ) {
        let coordinator = Coordinator::new(1);
        let alarm_fired_count = Arc::new(AtomicU8::new(0));
        let wait_point =
            coordinator.clone().track(SOME_COMPONENT.to_string());

        // Deliberately don't call wait_point.ready() or wait_point.running()
        let join_handle = {
            let alarm_fired_count = alarm_fired_count.clone();
            tokio::task::spawn(coordinator.wait(move |_, _| {
                alarm_fired_count.fetch_add(1, SeqCst);
            }))
        };

        // Advance time beyond the maximum time allowed for the 'ready' state to be reached
        let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
        let advance_time_by =
            advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
        tokio::time::sleep(advance_time_by).await;

        // Check that the alarm fired once
        assert_eq!(alarm_fired_count.load(SeqCst), 1);

        // Set the component state to the final state 'running'
        wait_point.running().await;

        // Which should unblock the coordinator wait
        join_handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn coordinator_with_component_with_slow_running_phase_should_raise_alarm(
    ) {
        let coordinator = Coordinator::new(1);
        let alarm_fired_count = Arc::new(AtomicU8::new(0));
        let mut wait_point =
            coordinator.clone().track(SOME_COMPONENT.to_string());

        // Deliberately don't call wait_point.ready() or wait_point.running()
        let join_handle = {
            let alarm_fired_count = alarm_fired_count.clone();
            tokio::task::spawn(coordinator.wait(move |_, _| {
                alarm_fired_count.fetch_add(1, SeqCst);
            }))
        };

        // Advance time beyond the maximum time allowed for the 'ready' state to be reached
        let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
        let advance_time_by =
            advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
        tokio::time::sleep(advance_time_by).await;

        // Check that the alarm fired once
        assert_eq!(alarm_fired_count.load(SeqCst), 1);

        // Achieve the 'ready' state in the component under test, but not yet the 'running' state
        wait_point.ready().await;

        // Advance time beyond the maximum time allowed for the 'running' state to be reached
        let advance_time_by = Coordinator::SLOW_COMPONENT_ALARM_DURATION;
        let advance_time_by =
            advance_time_by.checked_add(Duration::from_secs(1)).unwrap();
        tokio::time::sleep(advance_time_by).await;

        // Check that the alarm fired again
        assert_eq!(alarm_fired_count.load(SeqCst), 2);

        // Set the component state to the final state 'running'
        wait_point.running().await;

        // Which should unblock the coordinator wait
        join_handle.await.unwrap();
    }

    // --- Test helpers ------------------------------------------------------

    fn mk_config_from_toml(toml: &str) -> ConfigFile {
        ConfigFile::new(toml.as_bytes().to_vec(), Source::default()).unwrap()
    }

    type UnitOrTargetName = String;

    #[derive(Debug, Eq, PartialEq)]
    enum SpawnAction {
        SpawnUnit,
        SpawnTarget,
        ReconfigureUnit,
        ReconfigureTarget,
        TerminateUnit,
        TerminateTarget,
    }

    impl Display for SpawnAction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                SpawnAction::SpawnUnit => f.write_str("SpawnUnit"),
                SpawnAction::SpawnTarget => f.write_str("SpawnTarget"),
                SpawnAction::ReconfigureUnit => {
                    f.write_str("ReconfigureUnit")
                }
                SpawnAction::ReconfigureTarget => {
                    f.write_str("ReconfigureTarget")
                }
                SpawnAction::TerminateUnit => f.write_str("TerminateUnit"),
                SpawnAction::TerminateTarget => {
                    f.write_str("TerminateTarget")
                }
            }
        }
    }

    #[derive(Debug)]
    enum UnitOrTargetConfig {
        None,
        UnitConfig(Unit),
        #[allow(dead_code)]
        TargetConfig(Target),
    }

    #[derive(Debug)]
    struct SpawnLogItem {
        pub name: UnitOrTargetName,
        pub action: SpawnAction,
        pub config: UnitOrTargetConfig,
    }

    impl SpawnLogItem {
        fn new(
            name: UnitOrTargetName,
            action: SpawnAction,
            _config: UnitOrTargetConfig,
        ) -> Self {
            Self {
                name,
                action,
                config: _config,
            }
        }
    }

    impl Display for SpawnLogItem {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "'{}' for unit/target '{}'", self.action, self.name)
        }
    }

    #[derive(Debug, Default)]
    struct SpawnLog(pub Vec<SpawnLogItem>);

    impl SpawnLog {
        pub fn new() -> Self {
            Self(vec![])
        }
    }

    impl Display for SpawnLog {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "[")?;
            for item in &self.0 {
                writeln!(f, "  {}", item)?;
            }
            writeln!(f, "]")
        }
    }

    impl Deref for SpawnLog {
        type Target = Vec<SpawnLogItem>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for SpawnLog {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    thread_local!(
        static SPAWN_LOG: RefCell<SpawnLog> = RefCell::new(SpawnLog::new())
    );

    fn assert_log_contains(log: &SpawnLog, name: &str, action: SpawnAction) {
        assert!(
            log.iter()
                .any(|item| item.name == name && item.action == action),
            "No '{}' action for unit/target '{}' found in spawn log: {}",
            action,
            name,
            log
        );
    }

    fn get_log_item<'a>(
        log: &'a SpawnLog,
        name: &str,
        action: SpawnAction,
    ) -> &'a SpawnLogItem {
        let found = log
            .iter()
            .find(|item| item.name == name && item.action == action);
        assert!(found.is_some());
        found.unwrap()
    }

    fn spawn_unit(c: Component, u: Unit, _: Gate, _: WaitPoint) {
        log_spawn_action(
            c.name.to_string(),
            SpawnAction::SpawnUnit,
            UnitOrTargetConfig::UnitConfig(u),
        );
    }

    fn spawn_target(
        c: Component,
        t: Target,
        _: Receiver<TargetCommand>,
        _: WaitPoint,
    ) {
        log_spawn_action(
            c.name.to_string(),
            SpawnAction::SpawnTarget,
            UnitOrTargetConfig::TargetConfig(t),
        );
    }

    fn reconfigure_unit(name: &str, _: GateAgent, u: Unit, _: Gate) {
        log_spawn_action(
            name.to_string(),
            SpawnAction::ReconfigureUnit,
            UnitOrTargetConfig::UnitConfig(u),
        );
    }

    fn reconfigure_target(name: &str, _: Sender<TargetCommand>, t: Target) {
        log_spawn_action(
            name.to_string(),
            SpawnAction::ReconfigureTarget,
            UnitOrTargetConfig::TargetConfig(t),
        );
    }

    fn terminate_unit(name: &str, _: Arc<GateAgent>) {
        log_spawn_action(
            name.to_string(),
            SpawnAction::TerminateUnit,
            UnitOrTargetConfig::None,
        );
    }

    fn terminate_target(name: &str, _: Arc<Sender<TargetCommand>>) {
        log_spawn_action(
            name.to_string(),
            SpawnAction::TerminateTarget,
            UnitOrTargetConfig::None,
        );
    }

    fn clear_spawn_action_log() {
        SPAWN_LOG.with(|log| log.borrow_mut().clear());
    }

    fn log_spawn_action(
        name: String,
        action: SpawnAction,
        cfg: UnitOrTargetConfig,
    ) {
        SPAWN_LOG.with(|log| {
            log.borrow_mut().push(SpawnLogItem::new(name, action, cfg))
        });
    }

    fn spawn(manager: &mut Manager, mut config: Config) {
        clear_spawn_action_log();
        manager.spawn_internal(
            &mut config,
            spawn_unit,
            spawn_target,
            reconfigure_unit,
            reconfigure_target,
            terminate_unit,
            terminate_target,
        );
    }

    fn init_manager() -> Manager {
        GATES.with(|gates| gates.replace(Some(Default::default())));
        ROTO_FILTER_NAMES.with(|filter_names| {
            filter_names.replace(Some(Default::default()))
        });
        Manager::new()
    }
}
