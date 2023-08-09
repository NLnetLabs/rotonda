use std::{cell::RefCell, ops::ControlFlow, sync::Arc, time::Instant};

use log::debug;
use roto::{
    ast::AcceptReject,
    compile::{Compiler, MirBlock},
    traits::RotoType,
    types::typevalue::TypeValue,
    vm::{ExtDataSource, LinearMemory, VirtualMachine, VmBuilder, VmResult},
};

pub type VM = VirtualMachine<Arc<[MirBlock]>, Arc<[ExtDataSource]>>;
pub type ThreadLocalVM = RefCell<Option<(Instant, VM, LinearMemory)>>;

#[allow(clippy::type_complexity)]
pub fn build_vm(source_code: &str) -> Result<VM, String> {
    let rotolo = Compiler::build(source_code)?;
    let pack = rotolo
        .retrieve_first_public_as_arcs()
        .map_err(|err| format!("{}", err))?;
    let vm = VmBuilder::new()
        .with_mir_code(pack.mir)
        .with_data_sources(pack.data_sources)
        .build()
        .map_err(|err| format!("{}", err))?;
    Ok(vm)
}

/// Should return Err if an internal error occurs, otherwise it should return:
///   - Ok(ControlFlow::Abort) if the filter rejects the BGP UPDATE message.
///   - Ok(ControlFlow::Continue(msg)) if the filter accepts the BGP UPDATE message.
/// Note that Ok(ControlFlow::Continue(new_or_modified_msg)) is also valid.
///
/// Pass in NO_FILTER_RECORD_TYPE and NO_FILTER_RECORD_MAKER() if the Roto script doesn't take a custom record type as
/// input.
pub fn is_filtered_in_vm<R: RotoType>(
    vm: &ThreadLocalVM,
    roto_source: Arc<arc_swap::ArcSwapAny<Arc<(Instant, String)>>>,
    rx: R,
) -> Result<ControlFlow<(), TypeValue>, String> {
    let roto_source_ref = roto_source.load();
    if roto_source_ref.1.is_empty() {
        // Empty Roto script supplied, act as if the input is not filtered
        return Ok(ControlFlow::Continue(rx.into()));
    }

    let prev_vm = &mut vm.borrow_mut();
    if prev_vm.is_none() {
        let when = Instant::now();
        match build_vm(&roto_source_ref.1) {
            Ok(vm) => {
                let mem = LinearMemory::uninit();
                prev_vm.replace((when, vm, mem));
            }
            Err(err) => {
                return Err(format!("Error while building Roto VM: {err}"));
            }
        }
    }

    let (when, vm, mem) = prev_vm.as_mut().unwrap();
    if roto_source_ref.0 > *when {
        // Roto source has changed since the VM was created.
        debug!("Updating roto VM");
        match build_vm(&roto_source_ref.1) {
            Ok(new_vm) => *vm = new_vm,
            Err(err) => {
                return Err(format!("Error while re-building Roto VM: {err}"));
            }
        }
    } else {
        vm.reset();
    }

    match vm.exec(rx.into(), None::<TypeValue>, None, mem) {
        Ok(VmResult {
            accept_reject: AcceptReject::Reject,
            ..
        }) => {
            // The roto filter script said this BGP UPDATE message should be rejected.
            Ok(ControlFlow::Break(()))
        }

        Ok(VmResult {
            accept_reject: AcceptReject::Accept,
            rx,
            ..
        }) => {
            // The roto filter script has given us a, possibly modified, rx_tx output value to continue with. It may be
            // the same value that it was given to check, or it may be a modified version of that value, or a
            // completely new value maybe even a different TypeValue variant.
            Ok(ControlFlow::Continue(rx))
        }

        Ok(VmResult {
            accept_reject: AcceptReject::NoReturn,
            ..
        }) => Err(
            "Roto filter NoReturn result is unexpected, BGP UPDATE message will be rejected"
                .to_string(),
        ),

        Err(err) => Err(format!("Error while executing Roto filter: {err}")),
    }
}
