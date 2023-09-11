use std::{
    cell::RefCell,
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
    ops::{ControlFlow, Deref},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use arc_swap::ArcSwapOption;
use log::debug;
use roto::{
    ast::AcceptReject,
    compile::{CompileError, Compiler, MirBlock},
    traits::RotoType,
    types::typevalue::TypeValue,
    vm::{
        ExtDataSource, LinearMemory, OutputStreamQueue, VirtualMachine, VmBuilder, VmError,
        VmResult,
    },
};

use super::frim::FrimMap;

pub type VM = VirtualMachine<Arc<[MirBlock]>, Arc<[ExtDataSource]>>;
pub type ThreadLocalVM = RefCell<Option<(Instant, VM, LinearMemory)>>;

//------------ RotoError ----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum RotoError {
    CompileError {
        origin: RotoScriptOrigin,
        err: CompileError,
    },
    BuildError {
        origin: RotoScriptOrigin,
        err: VmError,
    },
    ExecError {
        origin: RotoScriptOrigin,
        err: VmError,
    },
    PostExecError {
        origin: RotoScriptOrigin,
        err: String,
    },
    Unusable {
        origin: RotoScriptOrigin,
    },
}

impl Display for RotoError {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotoError::CompileError{ origin, err } => {
                f.write_fmt(format_args!("Compilation error for Roto script {origin}: {err}"))
            }
            RotoError::BuildError{ origin, err } => {
                f.write_fmt(format_args!("VM build error for Roto script {origin}: {err}"))
            }
            RotoError::ExecError{ origin, err } => {
                f.write_fmt(format_args!("VM exec error for Roto script {origin}: {err}"))
            }
            RotoError::PostExecError{ origin, err } => {
                f.write_fmt(format_args!("Post execution error with Roto script {origin}: {err}"))
            }
            RotoError::Unusable{ origin } => {
                f.write_fmt(format_args!("Roto script {origin} was previously determined to be unusable"))
            }
        }
    }
}

//----------- RotoScripts ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum RotoScriptOrigin {
    Unknown,
    Path(PathBuf),
}

impl Display for RotoScriptOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotoScriptOrigin::Unknown => f.write_str("Unknown"),
            RotoScriptOrigin::Path(path) => f.write_fmt(format_args!("{}", path.display())),
        }
    }
}

#[derive(Debug)]
struct RotoScriptInner {
    source_code: String,
    origin: RotoScriptOrigin,
    when: Instant,
}

#[derive(Clone, Debug, Default)]
pub struct RotoScript {
    inner: Arc<ArcSwapOption<RotoScriptInner>>,
    manager: RotoScripts,
}

impl RotoScript {
    pub fn new<T: Into<String>>(
        source_code: T,
        manager: RotoScripts,
        origin: RotoScriptOrigin,
    ) -> Self {
        let inner = RotoScriptInner {
            source_code: source_code.into(),
            origin,
            when: Instant::now(),
        };

        let inner = if inner.source_code.is_empty() {
            None
        } else {
            Some(inner)
        };

        Self {
            inner: Arc::new(ArcSwapOption::from_pointee(inner)),
            manager,
        }
    }

    pub fn origin(&self) -> RotoScriptOrigin {
        match self.inner.load().deref() {
            Some(inner) => inner.origin.clone(),
            None => RotoScriptOrigin::Unknown,
        }
    }

    pub fn update(&self, source_code: String, origin: RotoScriptOrigin) {
        let new_script = RotoScriptInner {
            source_code,
            origin,
            when: Instant::now(),
        };
        let new_script = if new_script.source_code.is_empty() {
            None
        } else {
            Some(Arc::new(new_script))
        };
        self.inner.store(new_script);
    }

    pub fn is_empty(&self) -> bool {
        self.inner.load().is_none()
    }

    pub fn exec(&self, vm: &ThreadLocalVM, rx: TypeValue) -> Result<VmResult, RotoError> {
        // Let the payload through if it is actually an output stream message as we don't filter those, we just forward them
        // down the pipeline to a target where they can be handled, or if no roto script exists.
        if matches!(rx, TypeValue::OutputStreamMessage(_)) || self.is_empty() {
            return Ok(VmResult {
                accept_reject: AcceptReject::Accept,
                rx,
                tx: None,
                output_stream_queue: OutputStreamQueue::new(),
            });
        }

        let guard = self.inner.load();
        let script = guard.as_ref().unwrap(); // SAFETY: checked above

        // Initialize the Roto script VM on first use.
        let prev_vm = &mut vm.borrow_mut();
        if prev_vm.is_none() {
            let when = Instant::now();
            let vm = self.manager.build_vm(&script.source_code, &script.origin)?;
            let mem = LinearMemory::uninit();
            prev_vm.replace((when, vm, mem));
        }

        // Reinitialize the Roto script VM if the Roto script has changed since it was last initialized,
        // otherwise reset its state so that it is ready to process the given rx value.
        let (when, vm, mem) = prev_vm.as_mut().unwrap();
        if script.when > *when {
            debug!("Updating roto VM");
            *vm = self.manager.build_vm(&script.source_code, &script.origin)?;
        } else {
            vm.reset();
        }

        // Run the Roto script on the given rx value.
        vm.exec(rx, None::<TypeValue>, None, mem)
            .map_err(|err| RotoError::ExecError {
                origin: script.origin.clone(),
                err,
            })
    }
}

#[derive(Debug, Default)]
struct RotoScriptsInner {
    unusable_scripts: FrimMap<u64, ()>,
}

#[derive(Clone, Debug, Default)]
pub struct RotoScripts {
    inner: Arc<RotoScriptsInner>,
}

impl RotoScripts {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn build_vm(&self, roto_script: &str, origin: &RotoScriptOrigin) -> Result<VM, RotoError> {
        if !self.known_bad(roto_script) {
            self.build_vm_internal(roto_script, origin).or_else(|err| {
                self.mark_script_unusable(roto_script);
                Err(err)
            })
        } else {
            Err(RotoError::Unusable {
                origin: origin.clone(),
            })
        }
    }

    fn build_vm_internal(
        &self,
        roto_script: &str,
        origin: &RotoScriptOrigin,
    ) -> Result<VM, RotoError> {
        let rotolo = Compiler::build(roto_script).map_err(|err| RotoError::CompileError {
            origin: origin.clone(),
            err,
        })?;
        let pack =
            rotolo
                .retrieve_first_public_as_arcs()
                .map_err(|err| RotoError::CompileError {
                    origin: origin.clone(),
                    err,
                })?;
        VmBuilder::new()
            .with_mir_code(pack.mir)
            .with_data_sources(pack.data_sources)
            .build()
            .map_err(|err| RotoError::BuildError {
                origin: origin.clone(),
                err,
            })
    }

    fn known_bad(&self, roto_script: &str) -> bool {
        let roto_script_hash = Self::hash_script(roto_script);
        self.inner.unusable_scripts.contains_key(&roto_script_hash)
    }

    fn mark_script_unusable(&self, roto_script: &str) {
        let roto_script_hash = Self::hash_script(roto_script);
        self.inner.unusable_scripts.insert(roto_script_hash, ());
    }

    fn hash_script(roto_script: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        roto_script.hash(&mut hasher);
        let roto_script_hash = hasher.finish();
        roto_script_hash
    }
}

//----------- Roto Filtering ------------------------------------------------------------------------------------------

pub type FilterResult<E> = Result<ControlFlow<(), FilterOutput<TypeValue>>, E>;

pub struct FilterOutput<T: RotoType> {
    pub rx: T,
    pub tx: Option<TypeValue>,
    pub output_stream_queue: OutputStreamQueue,
}

impl<T> From<T> for FilterOutput<T>
where
    T: RotoType,
{
    fn from(rx: T) -> Self {
        Self {
            rx,
            tx: None,
            output_stream_queue: Default::default(),
        }
    }
}

pub fn roto_filter(
    vm: &ThreadLocalVM,
    roto_script: RotoScript,
    rx: TypeValue,
) -> FilterResult<RotoError> {
    // Let the payload through if it is actually an output stream message as we don't filter those, we just forward them
    // down the pipeline to a target where they can be handled, or if no roto script exists.
    if matches!(rx, TypeValue::OutputStreamMessage(_)) || roto_script.is_empty() {
        return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
    }

    // Run the Roto script on the given rx value.
    let res = roto_script.exec(vm, rx)?;

    match res {
        VmResult {
            accept_reject: AcceptReject::Reject,
            ..
        } => {
            // The roto filter script said this BGP UPDATE message should be rejected.
            Ok(ControlFlow::Break(()))
        }

        VmResult {
            accept_reject: AcceptReject::Accept,
            rx,
            tx,
            output_stream_queue,
        } => {
            // The roto filter script has given us a, possibly modified, rx_tx output value to continue with. It may be
            // the same value that it was given to check, or it may be a modified version of that value, or a
            // completely new value maybe even a different TypeValue variant.
            Ok(ControlFlow::Continue(FilterOutput {
                rx,
                tx,
                output_stream_queue,
            }))
        }

        VmResult {
            accept_reject: AcceptReject::NoReturn,
            ..
        } => Err(RotoError::PostExecError {
            origin: roto_script.origin(),
            err: "Roto filter NoReturn result is unexpected".to_string(),
        }),
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
                    rx: payload,
                    tx: None,
                    output_stream_queue: output_stream_queue.clone(),
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
                    rx: payload,
                    tx: None,
                    output_stream_queue: output_stream_queue.clone(),
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
