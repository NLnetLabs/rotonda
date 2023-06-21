use std::sync::Arc;

use futures::{future::Either, Future};

use crate::comms::{GateStatus, Terminated};

use super::status_reporter::UnitStatusReporter;

pub enum UnitActivity<O, E, F, X>
where
    F: Future<Output = X> + Unpin,
{
    Terminated,
    GateStatusChanged(GateStatus, F),
    InputError(E),
    InputReceived(O),
}

type ProcessGateResult = Result<GateStatus, Terminated>;

type SelectAllOutput<O, E, V> = (Result<O, E>, usize, Vec<V>);

type SelectResult<T, F, Z> = Either<(ProcessGateResult, F), (T, Z)>;

pub fn wait_for_activity<O, E, F, X, Z, USR>(
    status_reporter: &Arc<USR>,
    either: SelectResult<Result<O, E>, F, Z>,
) -> UnitActivity<O, E, F, X>
where
    F: Future<Output = X> + Unpin,
    USR: UnitStatusReporter,
{
    match either {
        Either::Left((Err(Terminated), _)) => {
            status_reporter.terminated();
            UnitActivity::Terminated
        }
        Either::Left((Ok(status), next_fut)) => {
            status_reporter.gate_status_announced(&status);
            if let GateStatus::Reconfiguring { .. } = status {
                status_reporter.reconfigured();
            }
            UnitActivity::GateStatusChanged(status, next_fut)
        }
        Either::Right((Err(err), _process_fut)) => UnitActivity::InputError(err),
        Either::Right((Ok(ok), _process_fut)) => UnitActivity::InputReceived(ok),
    }
}

impl<O, E, F, X, Z, USR> From<(&Arc<USR>, SelectResult<Result<O, E>, F, Z>)>
    for UnitActivity<O, E, F, X>
where
    F: Future<Output = X> + Unpin,
    USR: UnitStatusReporter,
{
    fn from((status_reporter, either): (&Arc<USR>, SelectResult<Result<O, E>, F, Z>)) -> Self {
        wait_for_activity(status_reporter, either)
    }
}

pub fn wait_for_first_activity<O, E, F, V, X, Z, USR>(
    status_reporter: &Arc<USR>,
    either: SelectResult<SelectAllOutput<O, E, V>, F, Z>,
) -> UnitActivity<O, E, F, X>
where
    F: Future<Output = X> + Unpin,
    USR: UnitStatusReporter,
{
    match either {
        Either::Left((Err(Terminated), _)) => {
            status_reporter.terminated();
            UnitActivity::Terminated
        }
        Either::Left((Ok(status), next_fut)) => {
            status_reporter.gate_status_announced(&status);
            UnitActivity::GateStatusChanged(status, next_fut)
        }
        Either::Right(((ready_fut, _fut_index, _other_futs), _)) => match ready_fut {
            Err(err) => UnitActivity::InputError(err),
            Ok(ok) => UnitActivity::InputReceived(ok),
        },
    }
}

impl<O, E, F, V, X, Z, USR> From<(&Arc<USR>, SelectResult<SelectAllOutput<O, E, V>, F, Z>)>
    for UnitActivity<O, E, F, X>
where
    F: Future<Output = X> + Unpin,
    USR: UnitStatusReporter,
{
    fn from(
        (status_reporter, res): (&Arc<USR>, SelectResult<SelectAllOutput<O, E, V>, F, Z>),
    ) -> Self {
        wait_for_first_activity(status_reporter, res)
    }
}
