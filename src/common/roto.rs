use std::{cell::RefCell, ops::ControlFlow, sync::Arc, time::Instant};

use arc_swap::ArcSwap;
use log::error;
use roto::{
    compile::{Compiler, MirBlock},
    traits::RotoType,
    types::typevalue::TypeValue,
    vm::{ExtDataSource, LinearMemory, OutputStreamQueue, VirtualMachine, VmBuilder},
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

/// If not in test mode, filter the payload through the provided Roto script
pub fn filter(
    vm: &ThreadLocalVM,
    roto_source: Arc<ArcSwap<(Instant, String)>>,
    rx: TypeValue,
) -> FilterResult<String> {
    use log::debug;
    use roto::{ast::AcceptReject, vm::VmResult};

    // Let the payload through if it is actually an output stream message as we don't filter those, we just forward them
    // down the pipeline to a target where they can be handled.
    if matches!(rx, TypeValue::OutputStreamMessage(_)) {
        return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
    }

    // Let the payload through if no Roto script was supplied.
    let roto_source_ref = roto_source.load();
    if roto_source_ref.1.is_empty() {
        return Ok(ControlFlow::Continue(FilterOutput::from(rx)));
    }

    // Initialize the Roto script VM on first use.
    let prev_vm = &mut vm.borrow_mut();
    if prev_vm.is_none() {
        let when = Instant::now();
        match build_vm(&roto_source_ref.1) {
            Ok(vm) => {
                let mem = LinearMemory::uninit();
                prev_vm.replace((when, vm, mem));
            }
            Err(err) => {
                let err = format!("Error while building Roto VM: {err}");
                error!("{}", err);
                return Err(err);
            }
        }
    }

    // Reinitialize the Roto script VM if the Roto script has changed since it was last initialized,
    // otherwise reset its state so that it is ready to process the given rx value.
    let (when, vm, mem) = prev_vm.as_mut().unwrap();
    if roto_source_ref.0 > *when {
        debug!("Updating roto VM");
        match build_vm(&roto_source_ref.1) {
            Ok(new_vm) => *vm = new_vm,
            Err(err) => {
                let err = format!("Error while re-building Roto VM: {err}");
                error!("{}", err);
                return Err(err);
            }
        }
    } else {
        vm.reset();
    }

    // Run the Roto script on the given rx value.
    match vm.exec(rx, None::<TypeValue>, None, mem) {
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
            tx,
            output_stream_queue,
        }) => {
            // The roto filter script has given us a, possibly modified, rx_tx output value to continue with. It may be
            // the same value that it was given to check, or it may be a modified version of that value, or a
            // completely new value maybe even a different TypeValue variant.
            Ok(ControlFlow::Continue(FilterOutput {
                rx,
                tx,
                output_stream_queue,
            }))
        }

        Ok(VmResult {
            accept_reject: AcceptReject::NoReturn,
            ..
        }) => {
            let err = "Roto filter NoReturn result is unexpected, BGP UPDATE message will be rejected".to_string();
            error!("{}", err);
            Err(err)
        }

        Err(err) => {
            let err = format!("Error while executing Roto filter: {err}");
            error!("{}", err);
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use roto::types::{builtin::U8, outputs::OutputStreamMessage, collections::Record, typedef::TypeDef};
    use smallvec::smallvec;

    use crate::payload::{Filterable, Payload};

    use super::*;

    #[test]
    fn single_update_yields_single_result() {
        let test_value: TypeValue = U8::new(0).into();
        let in_payload = Payload::new("test", test_value.clone());
        let out_payloads = in_payload
            .filter(|payload| Ok(ControlFlow::Continue(payload.into())))
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
        let test_output_stream_message = mk_roto_output_stream_payload(test_value.clone(), TypeDef::U8);
        let mut output_stream_queue = OutputStreamQueue::new();
        output_stream_queue.push(test_output_stream_message.clone());
        let out_payloads = in_payload
            .filter(move |payload| {
                Ok(ControlFlow::Continue(FilterOutput {
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
            .filter(|payload| Ok(ControlFlow::Continue(payload.into())))
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
        let test_output_stream_message = mk_roto_output_stream_payload(test_value1.clone(), TypeDef::U8);
        let mut output_stream_queue = OutputStreamQueue::new();
        output_stream_queue.push(test_output_stream_message.clone());
        let out_payloads = in_payload
            .filter(move |payload| {
                Ok(ControlFlow::Continue(FilterOutput {
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
        let typedef = TypeDef::new_record_type(vec![
            ("value", Box::new(type_def)),
        ])
        .unwrap();

        let fields = vec![
            ("value", value),
        ];

        let record = Record::create_instance_with_sort(&typedef, fields).unwrap();
        OutputStreamMessage::from(record)
    }
}
