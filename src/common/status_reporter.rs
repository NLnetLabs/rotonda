use std::{
    fmt::Display,
    num::NonZeroUsize,
    sync::Arc,
};

use log::{debug, trace};

use crate::{comms::GateStatus, manager::TargetCommand, metrics};

macro_rules! sr_log {
    ($log_fn:ident: $self:ident, $msg:expr) => (
        #[cfg(test)]
        let _ = env_logger::builder().is_test(true).try_init();
        $log_fn!(concat!("{}: ", $msg), $self.name());
    );

    ($log_fn:ident: $self:ident, $fmt:expr, $($args:expr),*) => (
        #[cfg(test)]
        let _ = env_logger::builder().is_test(true).try_init();
        $log_fn!(concat!("{}: ", $fmt), $self.name(), $($args),*);
    );
}

pub(crate) use sr_log;

pub trait Named {
    fn name(&self) -> &str;
}

pub trait Chainable: Named {
    fn add_child<T: Display>(&self, child_name: T) -> Self;

    fn link_names<T: Display>(&self, child_name: T) -> String {
        format!("{}::{}", self.name(), child_name)
    }
}

pub trait AnyStatusReporter: Chainable {
    #[allow(dead_code)]
    fn metrics(&self) -> Option<Arc<dyn metrics::Source>>;

    fn upstream_sources_changed(
        &self,
        num_old_sources: NonZeroUsize,
        num_new_sources: NonZeroUsize,
    ) {
        sr_log!(
            debug: self,
            "Replacing {} old sources with {} new sources",
            num_old_sources,
            num_new_sources
        );
    }
}

pub trait UnitStatusReporter: AnyStatusReporter {
    fn gate_status_announced(&self, status: &GateStatus) {
        sr_log!(trace: self, "Gate status announced: {}", status);
    }

    fn terminated(&self) {
        sr_log!(trace: self, "Unit terminated");
    }

    fn reconfigured(&self) {
        sr_log!(trace: self, "Unit reconfiguration notice received");
    }
}

pub trait TargetStatusReporter: AnyStatusReporter {
    fn command_received(&self, cmd: &TargetCommand) {
        sr_log!(trace: self, "Target received command: {}", cmd);
    }

    fn terminated(&self) {
        sr_log!(trace: self, "Target terminated");
    }

    fn reconfigured(&self) {
        sr_log!(trace: self, "Target reconfiguration notice received");
    }
}
