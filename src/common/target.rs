use std::sync::Arc;

use futures::future::Either;

use crate::manager::TargetCommand;

use super::status_reporter::TargetStatusReporter;

pub enum TargetActivity<E, O> {
    Terminated,
    CommandReceived(TargetCommand),
    InputError(E),
    InputReceived(O),
}

type SelectResult<T, F, Z> = Either<(Option<TargetCommand>, F), (T, Z)>;

pub fn wait_for_activity<O, E, F, Z, TSR>(
    status_reporter: &Arc<TSR>,
    either: SelectResult<Result<O, E>, F, Z>,
) -> TargetActivity<E, O>
where
    TSR: TargetStatusReporter,
{
    match either {
        Either::Left((None, _))
        | Either::Left((Some(TargetCommand::Terminate), _)) => {
            // If None, the sender dropped, which in this case is the
            // manager. Probably a good time to terminate.
            status_reporter.terminated();
            TargetActivity::Terminated
        }
        Either::Left((Some(cmd), _next_fut)) => {
            status_reporter.command_received(&cmd);
            if let TargetCommand::Reconfigure { .. } = cmd {
                status_reporter.reconfigured();
            }
            TargetActivity::CommandReceived(cmd)
        }
        Either::Right((Err(err), _)) => TargetActivity::InputError(err),
        Either::Right((Ok(ok), _)) => TargetActivity::InputReceived(ok),
    }
}

impl<O, E, F, Z, TSR> From<(&Arc<TSR>, SelectResult<Result<O, E>, F, Z>)>
    for TargetActivity<E, O>
where
    TSR: TargetStatusReporter,
{
    fn from(
        (status_reporter, either): (
            &Arc<TSR>,
            SelectResult<Result<O, E>, F, Z>,
        ),
    ) -> Self {
        wait_for_activity(status_reporter, either)
    }
}
