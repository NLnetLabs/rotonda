use crate::{
    roto_runtime::types::{FilterName, RotoScripts},
    common::status_reporter::{AnyStatusReporter, UnitStatusReporter},
    comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus,
        Terminated,
    },
    manager::{Component, WaitPoint},
    payload::{
        /*FilterError, Filterable,*/ Payload, RotondaRoute, Update,
        UpstreamStatus,
    },
    tracing::Tracer,
    units::Unit,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::info;
use non_empty_vec::NonEmpty;

use serde::Deserialize;
use std::{cell::RefCell, sync::Arc};

use super::{
    metrics::RotoFilterMetrics, status_reporter::RotoFilterStatusReporter,
};

#[derive(Clone, Debug, Deserialize)]
pub struct Filter {
    /// The set of units to receive updates from.
    sources: NonEmpty<DirectLink>,

    /// The name of the Roto filter to execute.
    filter_name: FilterName,
}

impl Filter {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        RotoFilterRunner::new(gate, component, self.filter_name)
            .run(self.sources, waitpoint)
            .await
    }
}

struct RotoFilterRunner {
    //roto_scripts: RotoScripts,
    gate: Arc<Gate>,
    status_reporter: Arc<RotoFilterStatusReporter>,
    filter_name: Arc<ArcSwap<FilterName>>,
    #[allow(dead_code)]
    tracer: Arc<Tracer>,
}

impl RotoFilterRunner {
    /*
    thread_local!(
        static VM: ThreadLocalVM = RefCell::new(None);
    );
    */

    fn new(
        gate: Gate,
        mut component: Component,
        filter_name: FilterName,
    ) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);

        // Setup metrics
        let metrics = Arc::new(RotoFilterMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter =
            Arc::new(RotoFilterStatusReporter::new(&unit_name, metrics));

        let filter_name = Arc::new(ArcSwap::from_pointee(filter_name));
        //let roto_scripts = component.roto_scripts().clone();
        let tracer = component.tracer().clone();

        Self {
            //roto_scripts,
            gate,
            status_reporter,
            filter_name,
            tracer,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)] // this will go anyway
    fn mock(
        _roto_script: &str,
        filter_name: &str,
    ) -> (Self, crate::comms::GateAgent) {
        //use crate::common::roto::RotoScriptOrigin;

        /*
        let roto_scripts = RotoScripts::default();
        roto_scripts
            .add_or_update_script(
                RotoScriptOrigin::Named("mock".to_string()),
                roto_script,
            )
            .unwrap();
        */
        let (gate, gate_agent) = Gate::new(0);
        let gate = gate.into();
        let status_reporter = RotoFilterStatusReporter::default().into();
        let filter_name =
            Arc::new(ArcSwap::from_pointee(FilterName::from(filter_name.to_string())));
        let tracer = Arc::new(Tracer::new());

        let runner = Self {
            //roto_scripts,
            gate,
            status_reporter,
            filter_name,
            tracer,
        };

        (runner, gate_agent)
    }

    pub async fn run(
        self,
        mut sources: NonEmpty<DirectLink>,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be, and signal to other components
        // that we are, ready to start. All units and targets start together,
        // otherwise data passed from one component to another may be lost if
        // the receiving component is not yet ready to accept it.
        arc_self.gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone
        // waiting to send important gate status updates won't send them while
        // we are in process_until() which will just eat them without handling
        // them.
        waitpoint.running().await;

        loop {
            match arc_self.gate.process().await {
                Ok(status) => {
                    arc_self.status_reporter.gate_status_announced(&status);
                    match status {
                        GateStatus::Reconfiguring {
                            new_config:
                                Unit::Filter(Filter {
                                    sources: new_sources,
                                    filter_name: new_filter_name,
                                }),
                        } => {
                            // Replace the roto script with the new one
                            if **arc_self.filter_name.load()
                                != new_filter_name
                            {
                                info!("Using new roto filter '{new_filter_name}'");
                                arc_self
                                    .filter_name
                                    .store(new_filter_name.into());
                            }

                            // Notify that we have reconfigured ourselves
                            arc_self.status_reporter.reconfigured();

                            // Register as a direct update receiver with the
                            // new set of linked gates.
                            arc_self
                                .status_reporter
                                .upstream_sources_changed(
                                    sources.len(),
                                    new_sources.len(),
                                );
                            sources = new_sources;
                            for link in sources.iter_mut() {
                                link.connect(arc_self.clone(), false)
                                    .await
                                    .unwrap();
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.set_sources(&sources);
                            report.set_graph_status(arc_self.gate.metrics());
                        }

                        _ => { /* Nothing to do */ }
                    }
                }

                Err(Terminated) => {
                    arc_self.status_reporter.terminated();
                    return Err(Terminated);
                }
            }
        }
    }

    async fn process_update(
        &self,
        update: Update,
    ) -> Result<(), String /*FilterError*/> {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                ..
            }) => {
                // Nothing to do, pass it on
                self.gate.update_data(update).await;
            }

            Update::Single(payload) => self.filter_payload([payload]).await?,

            Update::Bulk(payloads) => self.filter_payload(payloads).await?,

            _ => {
                self.status_reporter
                    .input_mismatch("Update::Single(_)", update);
            }
        }

        Ok(())
    }

    async fn filter_payload(
        &self,
        _payloads: impl IntoIterator<Item = Payload>,
    ) -> Result<(), String /*FilterError*/> {
        todo!()

        /*
        let tracer = self.tracer.bind(self.gate.id());

        if let Some(filtered_update) = Self::VM
            .with(|vm| {
                payload
                    .filter(
                        |value, received, trace_id, context| {
                            self.roto_scripts.exec_with_tracer(
                                vm,
                                &self.filter_name.load(),
                                value,
                                received,
                                tracer.clone(),
                                trace_id,
                                context
                            )
                        },
                        |source_id| {
                            self.status_reporter.message_filtered(source_id)
                        },
                    )
                    .map(|mut filtered_payloads| {
                        match filtered_payloads.len() {
                            0 => None,
                            1 => Some(Update::Single(
                                filtered_payloads.pop().unwrap(),
                            )),
                            _ => Some(Update::Bulk(filtered_payloads)),
                        }
                    })
            })
            .map_err(|err| {
                self.status_reporter.message_filtering_failure(&err);
                err
            })?
        {
            self.gate.update_data(filtered_update).await;
        }

        Ok(())
        */
    }
}

#[async_trait]
impl DirectUpdate for RotoFilterRunner {
    async fn direct_update(&self, update: Update) {
        let _ = self.process_update(update).await;
    }
}

impl AnyDirectUpdate for RotoFilterRunner {}

impl std::fmt::Debug for RotoFilterRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BmpFilterRunner").finish()
    }
}

#[cfg(test)]
mod tests {
}
