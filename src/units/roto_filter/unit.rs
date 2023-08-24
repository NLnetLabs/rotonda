use crate::{
    common::{
        file_io::{FileIo, TheFileIo},
        roto::{is_filtered_in_vm, ThreadLocalVM},
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Terminated},
    manager::{Component, WaitPoint},
    payload::{Payload, RawBmpPayload, Update},
    units::Unit,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::info;
use non_empty_vec::NonEmpty;
use roto::types::{
    builtin::BuiltinTypeValue, collections::BytesRecord, lazyrecord_types::BmpMessage,
    typedef::TypeDef, typevalue::TypeValue,
};
use serde::Deserialize;
use std::{cell::RefCell, ops::ControlFlow, path::PathBuf, sync::Arc, time::Instant};

use super::{metrics::RotoFilterMetrics, status_reporter::RotoFilterStatusReporter};

#[derive(Clone, Debug, Deserialize)]
pub struct RotoFilter {
    /// The set of units to receive updates from.
    sources: NonEmpty<DirectLink>,

    /// Path to roto script to use
    roto_path: PathBuf,
}

impl RotoFilter {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        RotoFilterRunner::new(gate, component, self.roto_path, TheFileIo::default())
            .run(self.sources, waitpoint)
            .await
    }
}

struct RotoFilterRunner {
    gate: Arc<Gate>,
    status_reporter: Arc<RotoFilterStatusReporter>,
    roto_source: Arc<ArcSwap<(std::time::Instant, String)>>,
    file_io: TheFileIo,
}

impl RotoFilterRunner {
    thread_local!(
        #[allow(clippy::type_complexity)]
        static VM: ThreadLocalVM = RefCell::new(None);
        static VM_RECORD_TYPE: RefCell<Option<TypeDef>> = RefCell::new(None);
    );

    fn new(gate: Gate, mut component: Component, roto_path: PathBuf, file_io: TheFileIo) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);

        // Setup metrics
        let metrics = Arc::new(RotoFilterMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(RotoFilterStatusReporter::new(&unit_name, metrics));

        let roto_source_code = file_io.read_to_string(roto_path).unwrap();
        let roto_source = (Instant::now(), roto_source_code);
        let roto_source = Arc::new(ArcSwap::from_pointee(roto_source));

        Self {
            gate,
            status_reporter,
            roto_source,
            file_io,
        }
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
                                Unit::RotoFilter(RotoFilter {
                                    sources: new_sources,
                                    roto_path: new_roto_path,
                                }),
                        } => {
                            // Replace the roto script with the new one
                            info!(
                                "Using roto script at path '{}'",
                                new_roto_path.to_string_lossy()
                            );
                            let roto_source_code =
                                arc_self.file_io.read_to_string(new_roto_path).unwrap();
                            let roto_source = (Instant::now(), roto_source_code);
                            let roto_source = Arc::new(roto_source);
                            arc_self.roto_source.store(roto_source);

                            // Notify that we have reconfigured ourselves
                            arc_self.status_reporter.reconfigured();

                            // Register as a direct update receiver with the new
                            // set of linked gates.
                            arc_self
                                .status_reporter
                                .upstream_sources_changed(sources.len(), new_sources.len());
                            sources = new_sources;
                            for link in sources.iter_mut() {
                                link.connect(arc_self.clone(), false).await.unwrap();
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
        gate: Arc<Gate>,
        status_reporter: Arc<RotoFilterStatusReporter>,
        update: Update,
        roto_source: Arc<ArcSwap<(Instant, String)>>,
    ) {
        // ---
        match &update {
            Update::Single(payload) => match Self::is_filtered(payload, roto_source).await {
                Ok(false) => gate.update_data(update).await,
                Ok(true) => {
                    if let Payload::RawBmp { router_addr, .. } = &payload {
                        status_reporter.message_filtered(*router_addr);
                    }
                }
                Err(err) => {
                    status_reporter.message_filtering_failure(err);
                }
            },

            Update::Bulk(_) => {
                status_reporter.input_mismatch("Update::Single(_)", "Update::Bulk(_)");
            }

            Update::QueryResult(..) => {
                // These should only be received by virtual RIBs, we shouldn't see these.
                status_reporter.input_mismatch("Update::Single(_)", "Update::QueryResult(_)");
            }

            Update::OutputStreamMessage(_) => {
                // pass it on, we don't (yet?) support filtering of these
                gate.update_data(update).await;
            }
        }
    }

    async fn is_filtered(
        payload: &Payload,
        roto_source: Arc<ArcSwap<(Instant, String)>>,
    ) -> Result<bool, String> {
        match payload {
            Payload::RawBmp {
                msg: RawBmpPayload::Msg(bytes),
                ..
            } => match BytesRecord::<BmpMessage>::new(bytes.clone()) {
                Ok(bmp_msg) => {
                    let payload =
                        TypeValue::Builtin(BuiltinTypeValue::BmpMessage(Arc::new(bmp_msg)));
                    Self::VM.with(
                        move |vm| match is_filtered_in_vm(vm, roto_source, payload) {
                            Ok(ControlFlow::Continue(_)) => Ok(false),
                            Ok(ControlFlow::Break(_)) => Ok(true),
                            Err(err) => Err(format!("Failed to execute Roto script: {err}")),
                        },
                    )
                }

                Err(err) => Err(format!("Failed to parse BMP message: {err}")),
            },

            Payload::RawBmp {
                msg: RawBmpPayload::Eof,
                ..
            } => {
                // Don't filter these out
                Ok(false)
            }

            _ => {
                // Everything else gets filtered out
                Ok(true)
            }
        }
    }
}

#[async_trait]
impl DirectUpdate for RotoFilterRunner {
    async fn direct_update(&self, update: Update) {
        let gate = self.gate.clone();
        let status_reporter = self.status_reporter.clone();
        let roto_source = self.roto_source.clone();
        Self::process_update(gate, status_reporter, update, roto_source).await;
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
    use bytes::Bytes;
    use chrono::Utc;
    use routecore::{asn::Asn, bmp::message::PeerType};

    use crate::{
        bgp::encode::{Announcements, Prefixes},
        tests::util::internal::enable_logging,
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
                    PeerDownNotification(pd_msg) -> 1 == 0,
                    PeerUpNotification(pu_msg) -> 1 == 0,
                    RouteMonitoring(rm_msg) -> 1 == 1,
                    StatisticsReport(sr_msg) -> 1 == 1,
                    TerminationMessage(t_msg) -> 1 == 0,
                }
            }

            apply {
                filter match is_wanted_bmp_msg_type matching { return accept; };
                return reject;
            }
        }
    "###;

    fn interpolate_source(source: &'static str, asn_to_ignore: Asn) -> String {
        source.replace("<ASN>", &asn_to_ignore.to_string())
    }

    fn mk_initiation_msg() -> Bytes {
        crate::bgp::encode::mk_initiation_msg(TEST_ROUTER_SYS_NAME, TEST_ROUTER_SYS_DESC)
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
        crate::bgp::encode::mk_peer_down_notification_msg(&mk_per_peer_header())
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

    fn mk_filter_payload(bmp_msg: Bytes) -> Payload {
        Payload::RawBmp {
            msg: RawBmpPayload::Msg(bmp_msg),
            received: Utc::now(),
            router_addr: "127.0.0.1:8080".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn bmp_messages_without_a_per_peer_header_should_not_be_filtered() {
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source = Arc::new(ArcSwap::from_pointee((
            Instant::now(),
            interpolate_source(FILTER_OUT_ASN_ROTO, asn_to_ignore),
        )));

        assert!(!RotoFilterRunner::is_filtered(
            &mk_filter_payload(mk_initiation_msg()),
            roto_source.clone()
        )
        .await
        .unwrap());
        assert!(!RotoFilterRunner::is_filtered(
            &mk_filter_payload(mk_termination_msg()),
            roto_source
        )
        .await
        .unwrap());
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn populated_asn_set_should_filter_out_only_matching() {
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), interpolate_source(FILTER_OUT_ASN_ROTO, asn_to_ignore))));

        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_initiation_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_route_monitoring_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_down_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_up_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_statistics_report_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_termination_msg()), roto_source).await.unwrap());
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn populated_asn_set_should_filter_in_only_matching() {
        let asn_to_ignore = TEST_PEER_ASN.into();
        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), interpolate_source(FILTER_IN_ASN_ROTO, asn_to_ignore))));

        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_initiation_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_route_monitoring_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_down_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_up_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_statistics_report_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_termination_msg()), roto_source).await.unwrap());
    }

    #[rustfmt::skip]
    #[tokio::test]
    #[ignore = "roto enum matching cannot do this yet"]
    async fn bmp_msg_type_should_match_as_expected() {
        enable_logging("trace");
        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), MSG_TYPE_MATCHING_ROTO.to_string())));

        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_route_monitoring_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_down_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(RotoFilterRunner::is_filtered(&mk_filter_payload(mk_peer_up_notification_msg()), roto_source.clone()).await.unwrap());
        assert!(!RotoFilterRunner::is_filtered(&mk_filter_payload(mk_statistics_report_msg()), roto_source).await.unwrap());
    }
}
