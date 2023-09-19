use std::{
    cell::RefCell, collections::HashSet, fmt::Display, ops::ControlFlow, path::PathBuf, sync::Arc,
    time::Instant,
};

use log::debug;
use roto::{
    ast::{AcceptReject, ShortString},
    blocks::Scope,
    compile::{CompileError, Compiler, MirBlock},
    types::typevalue::TypeValue,
    vm::{
        ExtDataSource, LinearMemory, OutputStreamQueue, VirtualMachine, VmBuilder, VmError,
        VmResult,
    },
};
use serde::Deserialize;

use crate::manager;

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
        f.write_fmt(format_args!("FilterName({})", self.0))
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
        let s = <&'a str>::deserialize(deserializer)?;
        let filter_name = FilterName(ShortString::from(s));
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

#[derive(Debug)]
pub enum RotoError {
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

    fn build_err(filter_name: &FilterName, err: VmError) -> RotoError {
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

    fn exec_err(filter_name: &FilterName, err: VmError) -> RotoError {
        Self::ExecError {
            filter_name: filter_name.clone(),
            err,
        }
    }
    fn post_exec_err(filter_name: &FilterName, err: &'static str) -> RotoError {
        Self::PostExecError {
            filter_name: filter_name.clone(),
            err,
        }
    }
}

//----------- RotoScripts ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RotoScriptOrigin {
    /// A script with no known origin, e.g. from a unit test, but still distinct (by name) from other scripts.
    Named(String),

    /// A script that originated from a file at the specified filesystem path.
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

// TODO: Store compiled roto scripts instead of just script sources. At the time of writing that isn't possible
// because the compiled form is Rotolo which internally has an Arc<dyn RotoRib> where the RotoRib trait lacks Send
// + Sync markers and also lacks Clone causing the Rust compiler to complain about any attempts at using the
// Rotolo type across threads, and we would want to build the Roto VM from a compiled source in each thread that
// we can execute Roto scripts in (as the VM is not thread safe so we store one VM instance per filter per thread).
// We also can't store each of the compiled RotoPack items (one per filter per compiled script) by ref or Arc (via
// RotoPackRef or RotoPackArc) because we cannot iterate over them because Rotolo::iter_all_filter_maps() is private
// so we have to know the names of the filters contained in the script in order to fetch their RotoPack out...
#[derive(Clone, Debug)]
pub struct RotoScript {
    origin: RotoScriptOrigin,
    source_code: String,
    loaded_when: Instant,
}

#[derive(Clone, Debug)]
pub struct ScopedRotoScript {
    roto_script: Arc<RotoScript>,
    scope: Scope,
}

#[derive(Clone, Debug, Default)]
pub struct RotoScripts {
    scripts_by_origin: Arc<FrimMap<RotoScriptOrigin, Arc<RotoScript>>>,
    scripts_by_filter: Arc<FrimMap<FilterName, Arc<ScopedRotoScript>>>,
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
                return Ok(());
            }
        }

        // Try and compile it immediately to verify if there are any problems with it. We'll still need to compile it
        // again later for each thread that wants to run a Roto VM that uses this script, but at least this way we don't
        // have to wait until then to find out if there is a problem.
        let rotolo =
            Compiler::build(roto_script).map_err(|err| RotoError::compile_err(&origin, err))?;

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

        // Check if any of the compiled filters have the same name as one that we've already seen
        let new_filters = rotolo.inspect_all_arguments();

        for filter_scope in new_filters.keys() {
            let filter_name = match filter_scope {
                Scope::Global => continue,
                Scope::FilterMap(filter_name) => filter_name,
                Scope::Filter(filter_name) => filter_name,
            };

            if let Some(found) = self.scripts_by_filter.get(&filter_name.into()) {
                let err = CompileError::from(format!(
                    "Filter {filter_name} in {origin} is already defined in {}",
                    found.roto_script.origin
                ));
                return Err(RotoError::CompileError { origin, err });
            }
        }

        let new_script = Arc::new(RotoScript {
            origin: origin.clone(),
            source_code: roto_script.to_string(),
            loaded_when: Instant::now(),
        });

        self.scripts_by_origin.insert(origin, new_script.clone());

        // Update the view of filter names to compiled scripts
        for (scope, _args) in rotolo.inspect_all_arguments() {
            let filter_name = match scope.clone() {
                Scope::Global => continue,
                Scope::FilterMap(filter_name) => filter_name,
                Scope::Filter(filter_name) => filter_name,
            };

            let scoped_script = Arc::new(ScopedRotoScript {
                roto_script: new_script.clone(),
                scope,
            });

            self.scripts_by_filter
                .insert(filter_name.into(), scoped_script);
        }

        if log::log_enabled!(log::Level::Trace) {
            dbg!(&self);
        }

        Ok(())
    }

    pub fn remove_script(&self, origin: &RotoScriptOrigin) {
        if let Some(roto_script) = self.scripts_by_origin.remove(origin) {
            self.scripts_by_filter
                .retain(|_filter_name, scoped_script| {
                    Arc::ptr_eq(&roto_script, &scoped_script.roto_script)
                });
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
        // Let the payload through if it is actually an output stream message as we don't filter those, we just forward them
        // down the pipeline to a target where they can be handled, or if no roto script exists.
        if matches!(rx, TypeValue::OutputStreamMessage(_)) || filter_name.is_empty() {
            return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
        }

        // Initialize the VM if needed.
        let was_initialized = self.init_vm(vm_ref, filter_name)?;

        let stateful_vm = &mut vm_ref.borrow_mut();
        let stateful_vm = stateful_vm.as_mut().unwrap();

        if !was_initialized {
            // Update the VM if the script containing the named filter has changed since last use.
            let found_filter = self
                .scripts_by_filter
                .get(filter_name)
                .ok_or_else(|| RotoError::not_found(filter_name))?;
            if found_filter.roto_script.loaded_when > stateful_vm.built_when {
                debug!("Updating roto VM");
                stateful_vm.vm = self.build_vm(filter_name)?;
                stateful_vm.built_when = found_filter.roto_script.loaded_when;
            } else {
                stateful_vm.vm.reset();
            }
        }

        // Run the Roto script on the given rx value.
        let res = stateful_vm
            .vm
            .exec(rx, None::<TypeValue>, None, &mut stateful_vm.mem)
            .map_err(|err| RotoError::exec_err(&stateful_vm.filter_name, err))?;

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

        let rotolo = Compiler::build(&scoped_script.roto_script.source_code).unwrap(); // SAFETY: we test compilation in add_or_update_script()

        let pack = rotolo
            .retrieve_public_as_arcs(scoped_script.scope.clone()) // TODO: Remove .clone() when retrieve_public_as_arcs() takes &Scope instead of Scope
            .map_err(|err| RotoError::compile_err(&scoped_script.roto_script.origin, err))?;

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

#[cfg(test)]
mod tests {
    use roto::types::{
        builtin::U8, collections::Record, outputs::OutputStreamMessage, typedef::TypeDef,
    };
    use smallvec::smallvec;

    use crate::payload::{FilterError, Filterable, Payload};

    use super::*;

    #[test]
    fn single_update_yields_single_result() {
        let test_value: TypeValue = U8::new(0).into();
        let in_payload = Payload::new("test", test_value.clone());
        let out_payloads = in_payload
            .filter(|payload| Result::<_, FilterError>::Ok(ControlFlow::Continue(payload.into())))
            .unwrap();
        assert_eq!(out_payloads.len(), 1);
        assert!(
            matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test") && *value == test_value)
        );
    }

    #[test]
    fn single_update_plus_output_stream_yields_both_as_bulk_update() {
        let test_value: TypeValue = U8::new(0).into();
        let in_payload = Payload::new("test", test_value.clone());
        let test_output_stream_message =
            mk_roto_output_stream_payload(test_value.clone(), TypeDef::U8);
        let mut output_stream_queue = OutputStreamQueue::new();
        output_stream_queue.push(test_output_stream_message.clone());
        let out_payloads = in_payload
            .filter(move |payload| {
                Result::<_, FilterError>::Ok(ControlFlow::Continue(FilterOutput {
                    east: payload,
                    south: output_stream_queue.clone(),
                }))
            })
            .unwrap();

        assert_eq!(out_payloads.len(), 2);
        assert!(
            matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test") && *value == test_value)
        );
        assert!(
            matches!(&out_payloads[1], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test") && **osm == test_output_stream_message)
        );
    }

    #[test]
    fn bulk_update_yields_bulk_update() {
        let test_value1: TypeValue = U8::new(1).into();
        let test_value2: TypeValue = U8::new(2).into();
        let payload1 = Payload::new("test1", test_value1.clone());
        let payload2 = Payload::new("test2", test_value2.clone());
        let in_payload = smallvec![payload1, payload2];
        let out_payloads = in_payload
            .filter(|payload| Result::<_, FilterError>::Ok(ControlFlow::Continue(payload.into())))
            .unwrap();

        assert_eq!(out_payloads.len(), 2);
        assert!(
            matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test1") && *value == test_value1)
        );
        assert!(
            matches!(&out_payloads[1], Payload { source_id, value } if source_id.name() == Some("test2") && *value == test_value2)
        );
    }

    #[test]
    fn bulk_update_plus_output_stream_yields_bulk_update() {
        let test_value1: TypeValue = U8::new(1).into();
        let test_value2: TypeValue = U8::new(2).into();
        let payload1 = Payload::new("test1", test_value1.clone());
        let payload2 = Payload::new("test2", test_value2.clone());
        let in_payload = smallvec![payload1, payload2];
        let test_output_stream_message =
            mk_roto_output_stream_payload(test_value1.clone(), TypeDef::U8);
        let mut output_stream_queue = OutputStreamQueue::new();
        output_stream_queue.push(test_output_stream_message.clone());
        let out_payloads = in_payload
            .filter(move |payload| {
                Result::<_, FilterError>::Ok(ControlFlow::Continue(FilterOutput {
                    east: payload,
                    south: output_stream_queue.clone(),
                }))
            })
            .unwrap();

        assert_eq!(out_payloads.len(), 4);
        assert!(
            matches!(&out_payloads[0], Payload { source_id, value } if source_id.name() == Some("test1") && *value == test_value1)
        );
        assert!(
            matches!(&out_payloads[1], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test1") && **osm == test_output_stream_message)
        );
        assert!(
            matches!(&out_payloads[2], Payload { source_id, value } if source_id.name() == Some("test2") && *value == test_value2)
        );
        assert!(
            matches!(&out_payloads[3], Payload { source_id, value: TypeValue::OutputStreamMessage(osm) } if source_id.name() == Some("test2") && **osm == test_output_stream_message)
        );
    }

    // --- Test helpers ------------------------------------------------------

    fn mk_roto_output_stream_payload(value: TypeValue, type_def: TypeDef) -> OutputStreamMessage {
        let typedef = TypeDef::new_record_type(vec![("value", Box::new(type_def))]).unwrap();

        let fields = vec![("value", value)];

        let record = Record::create_instance_with_sort(&typedef, fields).unwrap();
        OutputStreamMessage::from(record)
    }
}
