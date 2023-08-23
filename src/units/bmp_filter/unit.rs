use crate::{
    common::status_reporter::{AnyStatusReporter, UnitStatusReporter},
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Terminated},
    manager::{Component, WaitPoint},
    payload::{Payload, RawBmpPayload, Update},
    units::Unit,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use non_empty_vec::NonEmpty;
use routecore::asn::Asn;
use routecore::bmp::message::Message as BmpMsg;
use serde::Deserialize;
use std::sync::Arc;

use super::{metrics::BmpFilterMetrics, status_reporter::BmpFilterStatusReporter};

#[derive(Clone, Debug, Deserialize)]
pub struct BmpFilter {
    /// The set of units to receive updates from.
    sources: NonEmpty<DirectLink>,

    /// Skip BMP messages whose "Peer AS" matches one of the following ASNs.
    /// Useful for skipping iBGP messages.
    #[serde(default)]
    pub asns_to_ignore: Arc<Vec<Asn>>,
}

impl BmpFilter {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        BmpFilterRunner::new(self.asns_to_ignore, gate, component)
            .run(self.sources, waitpoint)
            .await
    }
}

struct BmpFilterRunner {
    asns_to_ignore: Arc<ArcSwap<Vec<Asn>>>,
    gate: Arc<Gate>,
    status_reporter: Arc<BmpFilterStatusReporter>,
}

impl BmpFilterRunner {
    fn new(asns_to_ignore: Arc<Vec<Asn>>, gate: Gate, mut component: Component) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);

        // Setup metrics
        let metrics = Arc::new(BmpFilterMetrics::new(&gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(BmpFilterStatusReporter::new(&unit_name, metrics));

        // Package ASNs to ignore inside an ArcSwap so we can modify them while in
        // use when we are reconfigured on the fly.
        let asns_to_ignore = Arc::new(ArcSwap::from(asns_to_ignore));

        Self {
            asns_to_ignore,
            gate,
            status_reporter,
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
                                Unit::BmpFilter(BmpFilter {
                                    sources: new_sources,
                                    asns_to_ignore: new_asns_to_ignore,
                                }),
                        } => {
                            // Replace the ASNs to ignore with the new ones
                            arc_self.asns_to_ignore.store(new_asns_to_ignore);

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
        status_reporter: Arc<BmpFilterStatusReporter>,
        update: Update,
        asns_to_ignore: Arc<ArcSwap<Vec<Asn>>>,
    ) {
        match &update {
            Update::Single(payload) => {
                if !is_filtered(payload, &asns_to_ignore) {
                    gate.update_data(update).await;
                } else if let Payload::RawBmp { router_addr, .. } = &payload {
                    status_reporter.message_filtered(*router_addr)
                }
            }

            Update::Bulk(_) => {
                status_reporter.input_mismatch("Update::Single(_)", "Update::Bulk(_)");
            }

            Update::QueryResult(..) => {
                status_reporter.input_mismatch("Update::Single(_)", "Update::QueryResult(_)");
            }

            Update::OutputStreamMessage(_) => {
                // pass it on, we don't (yet?) support doing anything with these
                gate.update_data(update).await;
            }
        }
    }
}

fn is_filtered(payload: &Payload, asns_to_ignore: &Arc<ArcSwap<Vec<Asn>>>) -> bool {
    match payload {
        Payload::RawBmp {
            msg: RawBmpPayload::Msg(bytes),
            ..
        } => {
            let msg = BmpMsg::from_octets(bytes).unwrap(); // should have been verified upstream

            let asn = match msg {
                BmpMsg::InitiationMessage(_msg) => None,
                BmpMsg::PeerUpNotification(msg) => Some(msg.per_peer_header().asn()),
                BmpMsg::RouteMonitoring(msg) => Some(msg.per_peer_header().asn()),
                BmpMsg::RouteMirroring(msg) => Some(msg.per_peer_header().asn()),
                BmpMsg::PeerDownNotification(msg) => Some(msg.per_peer_header().asn()),
                BmpMsg::StatisticsReport(msg) => Some(msg.per_peer_header().asn()),
                BmpMsg::TerminationMessage(_msg) => None,
            };

            // Filter it out if it's on the ASN ignore list
            asn.map(|asn| asns_to_ignore.load().contains(&asn))
                .unwrap_or_default()
        }

        Payload::RawBmp {
            msg: RawBmpPayload::Eof,
            ..
        } => {
            // Don't filter these out
            false
        }

        _ => {
            // Everything else gets filtered out
            true
        }
    }
}

#[async_trait]
impl DirectUpdate for BmpFilterRunner {
    async fn direct_update(&self, update: Update) {
        let gate = self.gate.clone();
        let status_reporter = self.status_reporter.clone();
        let asns_to_ignore = self.asns_to_ignore.clone();
        Self::process_update(gate, status_reporter, update, asns_to_ignore).await;
    }
}

impl AnyDirectUpdate for BmpFilterRunner {}

impl std::fmt::Debug for BmpFilterRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BmpFilterRunner").finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use chrono::Utc;
    use routecore::bmp::message::PeerType;

    use crate::bgp::encode::{Announcements, Prefixes};

    use super::*;

    const TEST_ROUTER_SYS_NAME: &str = "test-router";
    const TEST_ROUTER_SYS_DESC: &str = "test-desc";
    const TEST_PEER_ASN: u32 = 12345;

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

    #[tokio::test]
    async fn bmp_messages_without_a_per_peer_header_should_not_be_filtered() {
        let empty_asns_to_ignore = Vec::<Asn>::new();
        let empty_asns_to_ignore = Arc::new(ArcSwap::from(Arc::new(empty_asns_to_ignore)));

        let payload = mk_filter_payload(mk_initiation_msg());
        assert!(!is_filtered(&payload, &empty_asns_to_ignore));

        let payload = mk_filter_payload(mk_termination_msg());
        assert!(!is_filtered(&payload, &empty_asns_to_ignore));
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn empty_asn_set_should_filter_nothing() {
        let empty_asns_to_ignore = Vec::<Asn>::new();
        let empty_asns_to_ignore = Arc::new(ArcSwap::from(Arc::new(empty_asns_to_ignore)));

        assert!(!is_filtered(&mk_filter_payload(mk_initiation_msg()), &empty_asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_peer_up_notification_msg()), &empty_asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_route_monitoring_msg()), &empty_asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_peer_down_notification_msg()), &empty_asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_statistics_report_msg()), &empty_asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_termination_msg()), &empty_asns_to_ignore));
    }

    #[rustfmt::skip]
    #[tokio::test]
    async fn populated_asn_set_should_filter_only_matching() {
        let asns_to_ignore = vec![TEST_PEER_ASN.into()];
        let asns_to_ignore = Arc::new(ArcSwap::from(Arc::new(asns_to_ignore)));

        // BMP messages that lack a Per Peer Header have no Peer ASN to filter
        // on and so should not be filtered.
        assert!(!is_filtered(&mk_filter_payload(mk_initiation_msg()), &asns_to_ignore));
        assert!(!is_filtered(&mk_filter_payload(mk_termination_msg()), &asns_to_ignore));

        // The following BMP message types carry a Per Peer Header and Peer
        // ASN, which our helper functions set to TEST_PEER_ASN, and should
        // thus be filtered out.
        assert!(is_filtered(&mk_filter_payload(mk_route_monitoring_msg()), &asns_to_ignore));
        assert!(is_filtered(&mk_filter_payload(mk_peer_down_notification_msg()), &asns_to_ignore));
        assert!(is_filtered(&mk_filter_payload(mk_peer_up_notification_msg()), &asns_to_ignore));
        assert!(is_filtered(&mk_filter_payload(mk_statistics_report_msg()), &asns_to_ignore));
    }

    fn mk_filter_payload(bmp_msg: Bytes) -> Payload {
        Payload::RawBmp {
            msg: RawBmpPayload::Msg(bmp_msg),
            received: Utc::now(),
            router_addr: "127.0.0.1:8080".parse().unwrap(),
        }
    }
}
