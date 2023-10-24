use std::{
    cell::RefCell, collections::HashSet, fmt::Display, ops::ControlFlow, path::PathBuf, sync::Arc,
    time::Instant,
};

use log::{debug, trace, info};
use roto::{
    ast::{AcceptReject, ShortString},
    compile::{CompileError, Compiler, MirBlock, RotoPack, RotoPackArc},
    types::typevalue::TypeValue,
    vm::{
        ExtDataSource, LinearMemory, OutputStreamQueue, VirtualMachine, VmBuilder, VmError,
        VmResult,
    },
};
use serde::Deserialize;

use crate::{log::BoundTracer, manager};

use super::frim::FrimMap;

//------------ FilterName ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FilterName(ShortString);

impl std::ops::Deref for FilterName {
    type Target = ShortString;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FilterName {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Display for FilterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Default for FilterName {
    fn default() -> Self {
        FilterName(ShortString::from(""))
    }
}

impl From<&str> for FilterName {
    fn from(value: &str) -> Self {
        ShortString::from(value).into()
    }
}

impl From<&ShortString> for FilterName {
    fn from(value: &ShortString) -> Self {
        FilterName(value.clone())
    }
}

impl From<ShortString> for FilterName {
    fn from(value: ShortString) -> Self {
        FilterName(value)
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for FilterName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // This has to be a String, even though we pass a &str to ShortString::from(), because of the way that newer
        // versions of the toml crate work. See: https://github.com/toml-rs/toml/issues/597
        let s: String = Deserialize::deserialize(deserializer)?;
        let filter_name = FilterName(ShortString::from(s.as_str()));
        Ok(manager::load_filter_name(filter_name))
    }
}

//------------ VM -----------------------------------------------------------------------------------------------------

pub type VM = VirtualMachine<Arc<[MirBlock]>, Arc<[ExtDataSource]>>;

pub struct StatefulVM {
    /// The name of the filter this VM instance was compiled for
    pub filter_name: FilterName,

    /// When the VM was built to run the specified filter
    pub built_when: Instant,

    /// Working memory for a single invocation of the VM to use
    pub mem: LinearMemory,

    /// The VM itself
    pub vm: VM,
}

pub type ThreadLocalVM = RefCell<Option<StatefulVM>>;

//------------ RotoError ----------------------------------------------------------------------------------------------

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum LoadErrorKind {
    ReadDirError,
    ReadDirEntryError,
    ReadFileError,
}

impl std::fmt::Display for LoadErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadErrorKind::ReadDirError => f.write_str("read directory"),
            LoadErrorKind::ReadDirEntryError => f.write_str("read directory entry"),
            LoadErrorKind::ReadFileError => f.write_str("read file"),
        }
    }
}

impl LoadErrorKind {
    pub fn read_dir_err<T: Into<PathBuf>>(path: T, err: std::io::Error) -> RotoError {
        RotoError::load_err(path, LoadErrorKind::ReadDirError, err)
    }

    pub fn read_dir_entry_err<T: Into<PathBuf>>(path: T, err: std::io::Error) -> RotoError {
        RotoError::load_err(path, LoadErrorKind::ReadDirEntryError, err)
    }

    pub fn read_file_err<T: Into<PathBuf>>(path: T, err: std::io::Error) -> RotoError {
        RotoError::load_err(path, LoadErrorKind::ReadFileError, err)
    }
}

#[derive(Debug)]
pub enum RotoError {
    LoadError {
        kind: LoadErrorKind,
        path: PathBuf,
        err: std::io::Error,
    },
    CompileError {
        origin: RotoScriptOrigin,
        err: CompileError,
    },
    BuildError {
        filter_name: FilterName,
        err: VmError,
    },
    ExecError {
        filter_name: FilterName,
        err: VmError,
    },
    FilterNotFound {
        filter_name: FilterName,
    },
    PostExecError {
        filter_name: FilterName,
        err: &'static str,
    },
}

impl Display for RotoError {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotoError::LoadError { kind, path, err } => {
                f.write_fmt(format_args!("{kind} error for path {}: {err}", path.display()))
            }
            RotoError::CompileError { origin, err } => {
                f.write_fmt(format_args!("Compilation error for Roto script {origin}: {err}. Note: run with `ROTONDA_ROTO_LOG=1` environment variable to enable trace level Roto logging."))
            }
            RotoError::BuildError { filter_name, err } => {
                f.write_fmt(format_args!("VM build error for Roto filter {filter_name}: {err}"))
            }
            RotoError::ExecError { filter_name, err } => {
                f.write_fmt(format_args!("VM exec error for Roto filter {filter_name}: {err}"))
            }
            RotoError::FilterNotFound { filter_name } => {
                f.write_fmt(format_args!("Unknown Roto filter {filter_name}"))
            }
            RotoError::PostExecError { filter_name, err } => {
                f.write_fmt(format_args!("Post-execution error with Roto filter {filter_name}: {err}"))
            }
        }
    }
}

impl RotoError {
    pub fn compile_err(origin: &RotoScriptOrigin, err: CompileError) -> RotoError {
        Self::CompileError {
            origin: origin.clone(),
            err,
        }
    }

    pub fn build_err(filter_name: &FilterName, err: VmError) -> RotoError {
        Self::BuildError {
            filter_name: filter_name.clone(),
            err,
        }
    }

    pub fn not_found(filter_name: &FilterName) -> RotoError {
        Self::FilterNotFound {
            filter_name: filter_name.clone(),
        }
    }

    pub fn exec_err(filter_name: &FilterName, err: VmError) -> RotoError {
        Self::ExecError {
            filter_name: filter_name.clone(),
            err,
        }
    }

    pub fn post_exec_err(filter_name: &FilterName, err: &'static str) -> RotoError {
        Self::PostExecError {
            filter_name: filter_name.clone(),
            err,
        }
    }

    pub fn load_err<T: Into<PathBuf>>(
        path: T,
        kind: LoadErrorKind,
        err: std::io::Error,
    ) -> RotoError {
        let mut path: PathBuf = path.into();
        if path.is_relative() {
            if let Ok(cwd) = std::env::current_dir() {
                path = cwd.join(path);
            }
        }
        Self::LoadError { kind, path, err }
    }
}

//----------- RotoScripts ---------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RotoScriptOrigin {
    /// A script with no known origin, e.g. from a unit test, but still
    /// distinct (by name) from other scripts.
    Named(String),

    /// A script that originated from a file at the specified filesystem
    /// path.
    Path(PathBuf),
}

impl Display for RotoScriptOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotoScriptOrigin::Named(name) => f.write_str(name),
            RotoScriptOrigin::Path(path) => f.write_fmt(format_args!("{}", path.display())),
        }
    }
}

//------------ RotoScript ---------------------------------------------------

// A RotoScript is a script from one single origin (i.e. a file, a &str), but
// can contain multiple compiled Filter(Map)s. Filter(Map)s cannot live in
// the Global namespace, therefore they always have a NamedScope attached,
// which is the same as the filter name. Each NamedScope, and thus the filter
// name, needs to be unique across all roto scripts loaded by a Rotonda
// instance. Within one roto script this is enforced by the Roto compiler,
// but across the whole instance (so over potentially over multiple roto
// script sources), this is enforced by this RotoScript struct (in
// `add_or_update_script`). The scope and the compiled MIR code live together
// in one RotoPack.
#[derive(Clone, Debug)]
pub struct RotoScript {
    origin: RotoScriptOrigin,
    source_code: String,
    loaded_when: Instant,
    packs: Vec<Arc<RotoPack>>
}

impl RotoScript {
    pub fn contains(&self, search_pack: &Arc<RotoPack>) -> bool {
        self.packs.iter().any(|pack| Arc::ptr_eq(pack, search_pack))
    }
}

// A ScopedRotoScript is a single compiled Filter(Map) in its own scope. It
// is always part of a RotoScript.
#[derive(Clone, Debug)]
pub struct ScopedRotoScript {
    // The script this scoped Filter(Map) is part of.
    parent_script: Arc<RotoScript>,
    // The reference to the pack in the RotoPack vec of its parent.
    pack: Arc<RotoPack>
}

#[derive(Clone, Debug, Default)]
pub struct RotoScripts {
    // RotoScripts keyed on their origin
    scripts_by_origin: Arc<FrimMap<RotoScriptOrigin, Arc<RotoScript>>>,
    // Scoped scripts keyed on their scope name
    scripts_by_filter: Arc<FrimMap<FilterName, ScopedRotoScript>>,
}

impl RotoScripts {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_or_update_script(
        &self,
        origin: RotoScriptOrigin,
        roto_script: &str,
    ) -> Result<(), RotoError> {
        if let Some(script) = self.scripts_by_origin.get(&origin) {
            if script.source_code == roto_script {
                trace!("Roto script {origin} is already loaded");
                return Ok(());
            } else {
                info!("Re-loading modified Roto script {origin}");
            }
        } else {
            info!("Loading new Roto script {origin}");
        }

        // Try and compile it to verify if there are any problems with it.
        // If there are, we are going to stop here. If there are none, we
        // will store the resulting MIR, plus the required arguments (a
        // RotoPack) in the RotoScript struct. This we only have to compile
        // once.
        let rotolo =
            Compiler::build(roto_script)
            .map_err(|err| RotoError::compile_err(&origin, err))?;

        if !rotolo.is_success() {
            let report = rotolo.get_mis_compilations().iter().fold(
                String::new(),
                |mut acc, (scope, err)| {
                    acc.push_str(&format!("Scope: {scope}, Error: {err}; "));
                    acc
                },
            );
            let report = report.trim_end_matches("; ");
            return Err(RotoError::CompileError {
                origin,
                err: report.into(),
            });
        }

        // Extract all the packs that are Filter(Map)s. Filter(Map)s can't
        // live in the Global scope, so these all have filter names.
        let new_filter_maps = rotolo.clean_packs_to_owned();

        // Check if any of the compiled filters have the same name as one that
        // we've already seen. If so, abort, as each Filter(Map) name must be
        // unique across all loaded roto scripts so that they can referenced
        // unambiguously by their name.
        for filter_map in &new_filter_maps {
            let filter_name = filter_map.get_filter_map_name();

            if let Some(found) = 
                self.scripts_by_filter.get(&filter_name.clone().into()) {
                let err = CompileError::from(format!(
                    "Filter {filter_name} in {origin} is already defined \
                        in {}",
                    found.parent_script.origin
                ));
                return Err(RotoError::CompileError { origin, err });
            }
        }

        // Create the new RotoScript for these packs.
        let new_script = Arc::new(RotoScript {
            origin: origin.clone(),
            source_code: roto_script.to_string(),
            loaded_when: Instant::now(),
            packs: new_filter_maps
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>()
        });

        self.scripts_by_origin.insert(origin, new_script.clone());

        // Update the view of all Filter(Map)s keyed by name with the newly
        // loaded Filter(Map)s.
        for scope in &new_script.packs {
            let filter_name = scope.get_filter_map_name();

            self.scripts_by_filter
                .insert(
                filter_name.into(),
                ScopedRotoScript {
                    parent_script: Arc::clone(&new_script),
                        pack: Arc::clone(scope) 
                    }
            );
        }

        if log::log_enabled!(log::Level::Trace) {
            trace!("Dumping loaded roto scripts:");
            trace!("  By origin:");
            for (i, (origin, script)) in self.scripts_by_origin.guard().iter().enumerate() {
                for (j, pack) in script.packs.iter().enumerate() {
                    trace!("    [origin {i}, pack {j}]: {origin} -> {}", pack.get_filter_map_name());
                }
            }

            trace!("  By filter name:");
            for (i, (filter_name, script)) in self.scripts_by_filter.guard().iter().enumerate() {
                trace!("    [filter name {i}]: {filter_name} -> {}", script.parent_script.origin);
            }
        }

        Ok(())
    }

    pub fn remove_script(&self, origin: &RotoScriptOrigin) {
        if let Some(roto_script) = self.scripts_by_origin.remove(origin) {
            self.scripts_by_filter
                .retain(|_, scoped_script| !roto_script.contains(&scoped_script.pack));
        }
    }

    pub fn get_script_origins(&self) -> HashSet<RotoScriptOrigin> {
        self.scripts_by_origin
            .guard()
            .iter()
            .map(|(origin, _script)| origin)
            .cloned()
            .collect::<HashSet<_>>()
    }

    pub fn get_filter_names(&self) -> HashSet<FilterName> {
        self.scripts_by_filter
            .guard()
            .iter()
            .map(|(filter_name, _script)| filter_name)
            .cloned()
            .collect::<HashSet<_>>()
    }

    pub fn contains_filter(&self, filter_name: &FilterName) -> bool {
        self.scripts_by_filter.contains_key(filter_name)
    }

    // TODO: run VM execution in a dedicated thread pool to avoid blocking Tokio
    pub fn exec(
        &self,
        vm_ref: &ThreadLocalVM,
        filter_name: &FilterName,
        rx: TypeValue,
    ) -> FilterResult<RotoError> {
        self.do_exec(vm_ref, filter_name, rx, None)
    }

    pub fn exec_with_tracer(
        &self,
        vm_ref: &ThreadLocalVM,
        filter_name: &FilterName,
        rx: TypeValue,
        tracer: BoundTracer,
        trace_id: Option<u8>,
    ) -> FilterResult<RotoError> {
        if let Some(trace_id) = trace_id {
            self.do_exec(vm_ref, filter_name, rx, Some((tracer, trace_id)))
        } else {
            self.do_exec(vm_ref, filter_name, rx, None)
        }
    }

    pub fn do_exec(
        &self,
        vm_ref: &ThreadLocalVM,
        filter_name: &FilterName,
        rx: TypeValue,
        trace_details: Option<(BoundTracer, u8)>,
    ) -> FilterResult<RotoError> {
        // Let the payload through if it is actually an output stream message as we don't filter those, we just forward them
        // down the pipeline to a target where they can be handled, or if no roto script exists.
        if matches!(rx, TypeValue::OutputStreamMessage(_)) || filter_name.is_empty() {
            if let Some((tracer, trace_id)) = trace_details {
                if matches!(rx, TypeValue::OutputStreamMessage(_)) {
                    tracer.note_event(
                        trace_id,
                        format!("Filtering result: Accepting message as it is of type output stream message. Message was:\n{rx:#?}"),
                    );
                } else {
                    tracer.note_event(
                        trace_id,
                        format!("Filtering result: Accepting message as no filter name is set. Message was:\n{rx:#?}"),
                    );
                }
            }
            return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
        }

        // Initialize the VM if needed.
        let was_initialized_res = self.init_vm(vm_ref, filter_name);

        if matches!(&was_initialized_res, Err(RotoError::FilterNotFound { .. })) {
            if let Some((tracer, trace_id)) = trace_details {
                tracer.note_event(
                    trace_id,
                    format!("Filtering result: Accepting message as filter name '{filter_name}' is not loaded. Message was:\n{rx:#?}"),
                );
            }
            return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
        }

        let was_initialized = was_initialized_res?;

        let stateful_vm = &mut vm_ref.borrow_mut();
        let stateful_vm = stateful_vm.as_mut().unwrap();

        if !was_initialized {
            // Update the VM if the script containing the named filter has changed since last use.
            let found_filter = self
                .scripts_by_filter
                .get(filter_name)
                .ok_or_else(|| RotoError::not_found(filter_name))?;
            if found_filter.parent_script.loaded_when > stateful_vm.built_when {
                debug!("Updating roto VM");
                stateful_vm.vm = self.build_vm(filter_name)?;
                stateful_vm.built_when = found_filter.parent_script.loaded_when;
            } else {
                stateful_vm.vm.reset();
            }
        }

        let tx = None::<TypeValue>;
        let filter_map_args = None;

        if let Some((tracer, trace_id)) = &trace_details {
            tracer.note_event(
                *trace_id,
                format!(
                    "Filtering message with filter name '{filter_name}' using VM built {}s ago. VM inputs are:\n\nrx: {rx:#?}\n\ntx: {tx:#?}\n\nfilter_map_args: {filter_map_args:#?}",
                    Instant::now()
                        .duration_since(stateful_vm.built_when)
                        .as_secs(),
                ),
            );
        }

        // Run the Roto script on the given rx value.
        let res = stateful_vm
            .vm
            .exec(rx, tx, filter_map_args, &mut stateful_vm.mem)
            .map_err(|err| RotoError::exec_err(&stateful_vm.filter_name, err))?;

        if let Some((tracer, trace_id)) = &trace_details {
            tracer.note_event(*trace_id, format!("Filtering result: {res:#?}"));
        }

        match res {
            VmResult {
                accept_reject: AcceptReject::Reject,
                ..
            } => Ok(ControlFlow::Break(())),

            VmResult {
                accept_reject: AcceptReject::Accept,
                rx,
                tx: None,
                output_stream_queue,
            } => Ok(ControlFlow::Continue(FilterOutput::from((
                rx,
                output_stream_queue,
            )))),

            VmResult {
                accept_reject: AcceptReject::Accept,
                tx: Some(tx),
                output_stream_queue,
                ..
            } => Ok(ControlFlow::Continue(FilterOutput::from((
                tx,
                output_stream_queue,
            )))),

            VmResult {
                accept_reject: AcceptReject::NoReturn,
                ..
            } => Err(RotoError::post_exec_err(
                filter_name,
                "Roto filter NoReturn result is unexpected",
            )),
        }
    }

    fn init_vm(&self, vm_ref: &ThreadLocalVM, filter_name: &FilterName) -> Result<bool, RotoError> {
        let vm_ref = &mut vm_ref.borrow_mut();
        if vm_ref.is_none() {
            let filter_name = filter_name.clone();
            let built_when = Instant::now();
            let vm = self.build_vm(&filter_name)?;
            let mem = LinearMemory::uninit();
            vm_ref.replace(StatefulVM {
                filter_name,
                built_when,
                mem,
                vm,
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn build_vm(&self, filter_name: &FilterName) -> Result<VM, RotoError> {
        // Find the script that contains the named filter
        let scoped_script = self
            .scripts_by_filter
            .get(filter_name)
            .ok_or_else(|| RotoError::not_found(filter_name))?;

        let pack = RotoPackArc::from(scoped_script.pack.as_ref());
        VmBuilder::new()
            .with_mir_code(pack.mir)
            .with_data_sources(pack.data_sources)
            .build()
            .map_err(|err| RotoError::build_err(filter_name, err))
    }
}

//----------- Roto Filtering ------------------------------------------------------------------------------------------

pub type FilterResult<E> = Result<ControlFlow<(), FilterOutput>, E>;

pub struct FilterOutput {
    pub east: TypeValue,
    pub south: OutputStreamQueue,
}

impl From<TypeValue> for FilterOutput {
    fn from(east: TypeValue) -> Self {
        Self {
            east,
            south: Default::default(),
        }
    }
}

impl From<(TypeValue, OutputStreamQueue)> for FilterOutput {
    fn from((east, south): (TypeValue, OutputStreamQueue)) -> Self {
        Self { east, south }
    }
}

// #[cfg(test)]
// mod tests {
//     use roto::types::{
//         builtin::U8, collections::Record, outputs::OutputStreamMessage, typedef::TypeDef,
//     };
//     use smallvec::smallvec;

//     use crate::payload::{FilterError, Filterable, Payload};

//     use super::*;

//     #[test]
//     fn single_update_yields_single_result() {
//         let test_value: TypeValue = U8::new(0).into();
//         let in_payload = Payload::new("test", test_value.clone());
//         let out_payloads = in_payload
//             .filter(|payload| Result::<_, FilterError>::Ok(ControlFlow::Continue(payload.into())))
//             .unwrap();
//         assert_eq!(out_payloads.len(), 1);
//         assert!(
//             matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test") && *value == test_value)
//         );
//     }

//     #[test]
//     fn single_update_plus_output_stream_yields_both_as_bulk_update() {
//         let test_value: TypeValue = U8::new(0).into();
//         let in_payload = Payload::new("test", test_value.clone());
//         let test_output_stream_message =
//             mk_roto_output_stream_payload(test_value.clone(), TypeDef::U8);
//         let mut output_stream_queue = OutputStreamQueue::new();
//         output_stream_queue.push(test_output_stream_message.clone());
//         let out_payloads = in_payload
//             .filter(move |payload| {
//                 Result::<_, FilterError>::Ok(ControlFlow::Continue(FilterOutput {
//                     east: payload,
//                     south: output_stream_queue.clone(),
//                 }))
//             })
//             .unwrap();

//         assert_eq!(out_payloads.len(), 2);
//         assert!(
//             matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test") && *value == test_value)
//         );
//         assert!(
//             matches!(&out_payloads[1], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test") && **osm == test_output_stream_message)
//         );
//     }

//     #[test]
//     fn bulk_update_yields_bulk_update() {
//         let test_value1: TypeValue = U8::new(1).into();
//         let test_value2: TypeValue = U8::new(2).into();
//         let payload1 = Payload::new("test1", test_value1.clone());
//         let payload2 = Payload::new("test2", test_value2.clone());
//         let in_payload = smallvec![payload1, payload2];
//         let out_payloads = in_payload
//             .filter(|payload| Result::<_, FilterError>::Ok(ControlFlow::Continue(payload.into())))
//             .unwrap();

//         assert_eq!(out_payloads.len(), 2);
//         assert!(
//             matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test1") && *value == test_value1)
//         );
//         assert!(
//             matches!(&out_payloads[1], Payload { source_id, value } if source_id.name() == Some("test2") && *value == test_value2)
//         );
//     }

//     #[test]
//     fn bulk_update_plus_output_stream_yields_bulk_update() {
//         let test_value1: TypeValue = U8::new(1).into();
//         let test_value2: TypeValue = U8::new(2).into();
//         let payload1 = Payload::new("test1", test_value1.clone());
//         let payload2 = Payload::new("test2", test_value2.clone());
//         let in_payload = smallvec![payload1, payload2];
//         let test_output_stream_message =
//             mk_roto_output_stream_payload(test_value1.clone(), TypeDef::U8);
//         let mut output_stream_queue = OutputStreamQueue::new();
//         output_stream_queue.push(test_output_stream_message.clone());
//         let out_payloads = in_payload
//             .filter(move |payload| {
//                 Result::<_, FilterError>::Ok(ControlFlow::Continue(FilterOutput {
//                     east: payload,
//                     south: output_stream_queue.clone(),
//                 }))
//             })
//             .unwrap();

//         assert_eq!(out_payloads.len(), 4);
//         assert!(
//             matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test1") && *value == test_value1)
//         );
//         assert!(
//             matches!(&out_payloads[1], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test1") && **osm == test_output_stream_message)
//         );
//         assert!(
//             matches!(&out_payloads[2], Payload { source_id, value } if source_id.name() == Some("test2") && *value == test_value2)
//         );
//         assert!(
//             matches!(&out_payloads[3], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test2") && **osm == test_output_stream_message)
//         );
//     }

//     // --- Test helpers ------------------------------------------------------

//     fn mk_roto_output_stream_payload(value: TypeValue, type_def: TypeDef) -> OutputStreamMessage {
//         let typedef = TypeDef::new_record_type(vec![("value", Box::new(type_def))]).unwrap();

//         let fields = vec![("value", value)];

//         let record = Record::create_instance_with_sort(&typedef, fields).unwrap();
//         OutputStreamMessage::from(record)
//     }
// }
