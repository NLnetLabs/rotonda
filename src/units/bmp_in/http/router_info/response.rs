use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc, RwLock},
};

use chrono::{DateTime, Utc};
use hyper::{Body, Response, StatusCode};
use indoc::formatdoc;

use crate::{
    payload::RouterId,
    units::bmp_in::{metrics::BmpInMetrics, state_machine::metrics::BmpMetrics},
};

use super::RouterInfoApi;

impl RouterInfoApi {
    #[allow(clippy::too_many_arguments)]
    pub fn build_response(
        router_addr: SocketAddr,
        router_id: Arc<RouterId>,
        sys_name: &str,
        sys_desc: &str,
        sys_extra: &[String],
        conn_metrics: Arc<BmpInMetrics>,
        bmp_metrics: Arc<BmpMetrics>,
        connected_at: DateTime<Utc>,
        last_message_at: Arc<RwLock<DateTime<Utc>>>,
    ) -> Response<Body> {
        let sys_extra = sys_extra.join("|");
        let connected_at = connected_at.to_rfc3339();
        let last_message_at = last_message_at.read().unwrap().to_rfc3339();
        let router_bmp_metrics = bmp_metrics.router_metrics(router_id.clone());

        if let Some(router_conn_metrics) = conn_metrics.router_metrics(router_id) {
            let state = router_bmp_metrics
                .bmp_state_machine_state
                .load(Ordering::Relaxed);

            let num_connects: usize = router_conn_metrics.connection_count.load(Ordering::Relaxed);
            let num_msg_issues: usize = router_conn_metrics
                .num_invalid_bmp_messages
                .load(Ordering::Relaxed);
            let num_retried_bgp_updates_known_peer: usize = router_bmp_metrics
                .num_bgp_updates_with_recoverable_parsing_failures_for_known_peers
                .load(Ordering::Relaxed);
            let num_unusable_bgp_updates_known_peer: usize = router_bmp_metrics
                .num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers
                .load(Ordering::Relaxed);
            let num_retried_bgp_updates_unknown_peer: usize = router_bmp_metrics
                .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
                .load(Ordering::Relaxed);
            let num_unusable_bgp_updates_unknown_peer: usize = router_bmp_metrics
                .num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers
                .load(Ordering::Relaxed);
            let num_prefixes: usize = router_bmp_metrics
                .num_stored_prefixes
                .load(Ordering::Relaxed);
            let num_announce: usize = router_bmp_metrics.num_announcements.load(Ordering::Relaxed);
            let num_withdraw: usize = router_bmp_metrics.num_withdrawals.load(Ordering::Relaxed);
            let num_peers_up: usize = router_bmp_metrics.num_peers_up.load(Ordering::Relaxed);
            let num_peers_up_eor_capable: usize = router_bmp_metrics
                .num_peers_up_eor_capable
                .load(Ordering::Relaxed);
            let num_peers_up_dumping: usize = router_bmp_metrics
                .num_peers_up_dumping
                .load(Ordering::Relaxed);

            use std::fmt::Write;

            let mut error_report = String::new();
            let (start, end) = router_bmp_metrics.parse_errors.get();
            for err in start.iter().chain(end.iter()) {
                writeln!(error_report, "  When: {}", err.when.to_rfc3339()).unwrap();
                writeln!(error_report, "  What: {}", err.msg).unwrap();
                writeln!(error_report, "  Soft: {}", err.recoverable).unwrap();
                if let Some(pcaptext) = &err.pcaptext {
                    writeln!(error_report, "  PCAP: {}", pcaptext).unwrap();
                } else {
                    writeln!(error_report, "  PCAP: None").unwrap();
                }
                writeln!(error_report).unwrap();
            }

            let response_body = formatdoc!(
                "
            Router:
                Address      : {router_addr}
                State:       : {state} [Initiating -> Dumping -> Updating -> Terminated, or Aborted]
                SysName      : {sys_name}
                SysDesc      : {sys_desc}
                Extra        : {sys_extra}
            Timers:
                Connected at : {connected_at}
                Last message : {last_message_at}
            Counters:
                Connects     : {num_connects}
                Problem Msgs : {num_msg_issues} issues (e.g. RFC violation, parsing retried/failed, etc)
                BGP UPDATEs:
                    Soft Fail: {num_retried_bgp_updates_known_peer}/{num_retried_bgp_updates_unknown_peer} (known/unknown peer)
                    Hard Fail: {num_unusable_bgp_updates_known_peer}/{num_unusable_bgp_updates_unknown_peer} (known/unknown peer)
                Prefixes     : {num_prefixes}
                Announce     : {num_announce}
                Withdraw     : {num_withdraw}
                Peers Up     : {num_peers_up}
                EoR Capable  : {num_peers_up_eor_capable}
                Dumping      : {num_peers_up_dumping}

            Parse Errors: (most recent only)
            {error_report}
            "
            );

            Response::builder()
                .header("Content-Type", "text/plain")
                .body(Body::from(response_body))
                .unwrap()
        } else {
            Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "text/plain")
            .body("Not Found".into())
            .unwrap()
        }
    }
}
