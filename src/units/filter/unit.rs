use crate::{
    common::{
        roto::{FilterName, RotoScripts, ThreadLocalVM},
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    },
    comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus,
        Terminated,
    },
    manager::{Component, WaitPoint},
    payload::{FilterError, Filterable, Update, UpstreamStatus},
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
    roto_scripts: RotoScripts,
    gate: Arc<Gate>,
    status_reporter: Arc<RotoFilterStatusReporter>,
    filter_name: Arc<ArcSwap<FilterName>>,
    tracer: Arc<Tracer>,
}

impl RotoFilterRunner {
    thread_local!(
        static VM: ThreadLocalVM = RefCell::new(None);
    );

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
        let roto_scripts = component.roto_scripts().clone();
        let tracer = component.tracer().clone();

        Self {
            roto_scripts,
            gate,
            status_reporter,
            filter_name,
            tracer,
        }
    }

    #[cfg(test)]
    fn mock(
        roto_script: &str,
        filter_name: &str,
    ) -> (Self, crate::comms::GateAgent) {
        use crate::common::roto::RotoScriptOrigin;

        let roto_scripts = RotoScripts::default();
        roto_scripts
            .add_or_update_script(
                RotoScriptOrigin::Named("mock".to_string()),
                roto_script,
            )
            .unwrap();
        let (gate, gate_agent) = Gate::new(0);
        let gate = gate.into();
        let status_reporter = RotoFilterStatusReporter::default().into();
        let filter_name =
            Arc::new(ArcSwap::from_pointee(FilterName::from(filter_name)));
        let tracer = Arc::new(Tracer::new());

        let runner = Self {
            roto_scripts,
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

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        arc_self.gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone waiting to send important gate status
        // updates won't send them while we are in process_until() which will just eat them without handling them.
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

                            // Register as a direct update receiver with the new
                            // set of linked gates.
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
    ) -> Result<(), FilterError> {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                ..
            }) => {
                // Nothing to do, pass it on
                self.gate.update_data(Ok(update)).await;
            }

            Update::Single(payload) => self.filter_payload(payload).await?,

            Update::Bulk(payloads) => self.filter_payload(payloads).await?,

            _ => {
                self.status_reporter
                    .input_mismatch("Update::Single(_)", update);
            }
        }

        Ok(())
    }

    async fn filter_payload<T: Filterable>(
        &self,
        payload: T,
    ) -> Result<(), FilterError> {
        let tracer = self.tracer.bind(self.gate.id());

        if let Some(filtered_update) = Self::VM
            .with(|vm| {
                payload
                    .filter(
                        |value, received, trace_id| {
                            self.roto_scripts.exec_with_tracer(
                                vm,
                                &self.filter_name.load(),
                                value,
                                received,
                                tracer.clone(),
                                trace_id,
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
            self.gate.update_data(Ok(filtered_update)).await;
        }

        Ok(())
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
    use std::sync::atomic::Ordering::SeqCst;

    use bytes::Bytes;
    use roto::types::{
        builtin::BuiltinTypeValue, collections::BytesRecord,
        lazyrecord_types::BmpMessage, typevalue::TypeValue,
    };
    use routecore::{asn::Asn, bmp::message::PeerType};

    use crate::{
        bgp::encode::{Announcements, Prefixes},
        payload::{Payload, SourceId},
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
    };

    use super::*;

    const TEST_ROUTER_SYS_NAME: &str = "test-router";
    const TEST_ROUTER_SYS_DESC: &str = "test-desc";
    const TEST_PEER_ASN: u32 = 12345;

    const FILTER_OUT_ASN_ROTO: &str = r###"
            filter my-module {
                define {
                    rx msg: BmpMessage;
                }

                term has_asn {
                    // Compare the ASN for BMP message types that have a Per Peer Header
                    // Omiting the other message types has the same effect as the explicit
                    // 1 != 1 (false) result that we define here explicity.
                    match msg with {
                        InitiationMessage(i_msg) -> 1 != 1,
                        PeerDownNotification(pd_msg) -> pd_msg.per_peer_header.asn == <ASN>,
                        PeerUpNotification(pu_msg) -> pu_msg.per_peer_header.asn == <ASN>,
                        RouteMonitoring(rm_msg) -> rm_msg.per_peer_header.asn == <ASN>,
                        StatisticsReport(sr_msg) -> sr_msg.per_peer_header.asn == <ASN>,
                        TerminationMessage(t_msg) -> 1 != 1,
                    }
                }

                apply {
                    filter match has_asn matching {
                        return reject;
                    };
                    accept;
                }
            }
        "###;

    const FILTER_IN_ASN_ROTO: &str = r###"
            filter my-module {
                define {
                    rx msg: BmpMessage;
                }

                term has_asn {
                    // Compare the ASN for BMP message types that have a Per Peer Header
                    // We can't omit the other message types as without the explicit
                    // 1 == 1 (true) check the resulting logic isn't what we want.
                    match msg with {
                        InitiationMessage(i_msg) -> 1 == 1,
                        PeerDownNotification(pd_msg) -> pd_msg.per_peer_header.asn == <ASN>,
                        PeerUpNotification(pu_msg) -> pu_msg.per_peer_header.asn == <ASN>,
                        RouteMonitoring(rm_msg) -> rm_msg.per_peer_header.asn == <ASN>,
                        StatisticsReport(sr_msg) -> sr_msg.per_peer_header.asn == <ASN>,
                        TerminationMessage(t_msg) -> 1 == 1,
                    }
                }

                apply {
                    filter match has_asn matching {
                        return accept;
                    };
                    reject;
                }
            }
        "###;

    const MSG_TYPE_MATCHING_ROTO: &str = r###"
            filter my-module {
                define {
                    rx msg: BmpMessage;
                }

                term is_wanted_bmp_msg_type {
                    match msg with {
                        InitiationMessage(i_msg) -> 1 == 0,
                        PeerDownNotification(pd_msg) -> 1 == 1,
                        PeerUpNotification(pu_msg) -> 1 == 1,
                        RouteMonitoring(rm_msg) -> 1 == 0,
                        StatisticsReport(sr_msg) -> 1 == 0,
                        TerminationMessage(t_msg) -> 1 == 0,
                    }
                }

                apply {
                    filter match is_wanted_bmp_msg_type matching { return accept; };
                    return reject;
                }
            }
        "###;

    #[tokio::test]
    async fn bmp_messages_without_a_per_peer_header_should_not_be_filtered() {
        enable_logging("trace");
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source =
            interpolate_source(FILTER_OUT_ASN_ROTO, asn_to_ignore);
        let filter = mk_filter(&roto_source);

        assert!(
            !is_filtered(&filter, mk_filter_payload(mk_initiation_msg()))
                .await
        );
        assert!(
            !is_filtered(&filter, mk_filter_payload(mk_termination_msg()))
                .await
        );

        let metrics = get_testable_metrics_snapshot(
            &filter.status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("roto_filter_num_filtered_messages"),
            0,
        );
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn populated_asn_set_should_filter_out_only_matching() {
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source = interpolate_source(FILTER_OUT_ASN_ROTO, asn_to_ignore);
        let filter = mk_filter(&roto_source);

        assert!(!is_filtered(&filter, mk_filter_payload(mk_initiation_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_route_monitoring_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_peer_down_notification_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_peer_up_notification_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_statistics_report_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_termination_msg())).await);

        let metrics = get_testable_metrics_snapshot(&filter.status_reporter.metrics().unwrap());
        assert_eq!(metrics.with_name::<usize>("roto_filter_num_filtered_messages"), 4);
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn populated_asn_set_should_filter_in_only_matching() {
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source = interpolate_source(FILTER_IN_ASN_ROTO, asn_to_ignore);
        let filter = mk_filter(&roto_source);

        assert!(!is_filtered(&filter, mk_filter_payload(mk_initiation_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_route_monitoring_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_peer_down_notification_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_peer_up_notification_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_statistics_report_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_termination_msg())).await);

        let metrics = get_testable_metrics_snapshot(&filter.status_reporter.metrics().unwrap());
        assert_eq!(metrics.with_name::<usize>("roto_filter_num_filtered_messages"), 0);
    }

    #[rustfmt::skip]
    #[tokio::test]
    #[ignore = "this test doesn't work with roto yet"]
    async fn bmp_msg_type_should_match_as_expected() {
        enable_logging("trace");
        let roto_source = MSG_TYPE_MATCHING_ROTO;
        let filter = mk_filter(roto_source);

        assert!(!is_filtered(&filter, mk_filter_payload(mk_route_monitoring_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_peer_down_notification_msg())).await);
        assert!(is_filtered(&filter, mk_filter_payload(mk_peer_up_notification_msg())).await);
        assert!(!is_filtered(&filter, mk_filter_payload(mk_statistics_report_msg())).await);

        let metrics = get_testable_metrics_snapshot(&filter.status_reporter.metrics().unwrap());
        assert_eq!(metrics.with_name::<usize>("roto_filter_num_filtered_messages"), 2);
    }

    //-------- Test helpers --------------------------------------------------

    fn interpolate_source(
        source: &'static str,
        asn_to_ignore: Asn,
    ) -> String {
        source.replace("<ASN>", &asn_to_ignore.to_string())
    }

    fn mk_initiation_msg() -> Bytes {
        crate::bgp::encode::mk_initiation_msg(
            TEST_ROUTER_SYS_NAME,
            TEST_ROUTER_SYS_DESC,
        )
    }

    fn mk_termination_msg() -> Bytes {
        crate::bgp::encode::mk_termination_msg()
    }

    fn mk_per_peer_header() -> crate::bgp::encode::PerPeerHeader {
        crate::bgp::encode::PerPeerHeader {
            peer_type: PeerType::GlobalInstance.into(),
            peer_flags: 0,
            peer_distinguisher: [0u8; 8],
            peer_address: "127.0.0.1".parse().unwrap(),
            peer_as: Asn::from_u32(TEST_PEER_ASN),
            peer_bgp_id: [1u8, 2u8, 3u8, 4u8],
        }
    }

    fn mk_peer_up_notification_msg() -> Bytes {
        crate::bgp::encode::mk_peer_up_notification_msg(
            &mk_per_peer_header(),
            "10.0.0.1".parse().unwrap(),
            11019,
            4567,
            111,
            222,
            0,
            0,
            vec![],
            false,
        )
    }

    fn mk_peer_down_notification_msg() -> Bytes {
        crate::bgp::encode::mk_peer_down_notification_msg(
            &mk_per_peer_header(),
        )
    }

    fn mk_route_monitoring_msg() -> Bytes {
        crate::bgp::encode::mk_route_monitoring_msg(
            &mk_per_peer_header(),
            &Prefixes::default(),
            &Announcements::default(),
            &[],
        )
    }

    fn mk_statistics_report_msg() -> Bytes {
        crate::bgp::encode::mk_statistics_report_msg(&mk_per_peer_header())
    }

    fn mk_filter_payload(msg_buf: Bytes) -> Update {
        let source_id =
            SourceId::SocketAddr("127.0.0.1:8080".parse().unwrap());
        let bmp_msg =
            Arc::new(BytesRecord(BmpMessage::from_octets(msg_buf).unwrap()));
        let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg));
        Update::Single(Payload::new(source_id, value, None))
    }

    async fn is_filtered(filter: &RotoFilterRunner, update: Update) -> bool {
        let gate_metrics = filter.gate.metrics();
        let num_dropped_updates_before =
            gate_metrics.num_dropped_updates.load(SeqCst);
        let num_updates_before = gate_metrics.num_updates.load(SeqCst);

        filter.process_update(update).await.unwrap();

        let num_dropped_updates_after =
            gate_metrics.num_dropped_updates.load(SeqCst);
        let num_updates_after = gate_metrics.num_updates.load(SeqCst);

        // If not filtered then the number of updates processed by the gate should have increased,
        // either by failing to deliver the update i.e. dropping it, or by delivering it. So if the
        // dropped and not dropped metrics remain the same it means no attempt was made to send an
        // update through the Gate, i.e. it was filtered out before sending via the Gate.
        num_dropped_updates_before == num_dropped_updates_after
            && num_updates_before == num_updates_after
    }

    fn mk_filter(roto_source_code: &str) -> RotoFilterRunner {
        let (runner, _) =
            RotoFilterRunner::mock(roto_source_code, "my-module");
        runner
    }
}
