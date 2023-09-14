use std::sync::{atomic::Ordering, Arc, RwLock};

use chrono::{DateTime, Utc};
use hyper::{Body, Response, StatusCode};
use indoc::formatdoc;

use crate::{
    payload::{RouterId, SourceId},
    units::bmp_in::{
        metrics::{BmpInMetrics, RouterMetrics},
        state_machine::{
            machine::{PeerAware, PeerStates},
            metrics::BmpMetrics,
        },
    }, http,
};

use super::RouterInfoApi;

pub enum Focus {
    None,

    Flags(String), // peer key

    Prefixes(String), // peer key
}

impl RouterInfoApi {
    #[allow(clippy::too_many_arguments)]
    pub fn build_response(
        http_resources: http::Resources,
        base_http_path: String,
        source_id: SourceId,
        router_id: Arc<RouterId>,
        sys_name: &str,
        sys_desc: &str,
        sys_extra: &[String],
        peer_states: Option<&PeerStates>,
        focus: Focus,
        conn_metrics: &Arc<BmpInMetrics>,
        bmp_metrics: &Arc<BmpMetrics>,
        connected_at: &DateTime<Utc>,
        last_message_at: &Arc<RwLock<DateTime<Utc>>>,
    ) -> Response<Body> {
        if let Some(router_conn_metrics) = conn_metrics.router_metrics(router_id.clone()) {
            let mut response_body = Self::build_response_header();

            response_body.push_str(&Self::build_response_body(
                http_resources,
                base_http_path,
                source_id,
                router_id,
                router_conn_metrics,
                sys_name,
                sys_desc,
                sys_extra,
                peer_states,
                focus,
                bmp_metrics,
                connected_at,
                last_message_at,
            ));

            Self::build_response_footer(&mut response_body);

            Response::builder()
                .header("Content-Type", "text/html")
                .body(Body::from(response_body))
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "text/html")
                .body("Not Found".into())
                .unwrap()
        }
    }

    fn build_response_header() -> String {
        formatdoc! {
            r#"
            <!DOCTYPE html>
            <html lang="en">
                <head>
                <meta charset="UTF-8">
                <style>
                    table {{
                    border-collapse: collapse;
                    }}
                    th, td {{
                    border: 1px solid black;
                    padding: 2px 20px 2px 20px;
                    }}
                </style>
                </head>
                <body>
                <pre>
            "#
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_response_body(
        http_resources: http::Resources,
        base_http_path: String,
        source_id: SourceId,
        router_id: Arc<RouterId>,
        router_conn_metrics: Arc<RouterMetrics>,
        sys_name: &str,
        sys_desc: &str,
        sys_extra: &[String],
        peer_states: Option<&PeerStates>,
        focus: Focus,
        bmp_metrics: &Arc<BmpMetrics>,
        connected_at: &DateTime<Utc>,
        last_message_at: &Arc<RwLock<DateTime<Utc>>>,
    ) -> String {
        let sys_extra = sys_extra.join("|");
        let connected_at = connected_at.to_rfc3339();
        let last_message_at = last_message_at.read().unwrap().to_rfc3339();
        let router_bmp_metrics = bmp_metrics.router_metrics(router_id.clone());

        let state = router_bmp_metrics
            .bmp_state_machine_state
            .load(Ordering::SeqCst);

        let num_connects: usize = router_conn_metrics.connection_count.load(Ordering::SeqCst);
        let num_msg_issues: usize = router_conn_metrics
            .num_invalid_bmp_messages
            .load(Ordering::SeqCst);
        let num_retried_bgp_updates_known_peer: usize = router_bmp_metrics
            .num_bgp_updates_with_recoverable_parsing_failures_for_known_peers
            .load(Ordering::SeqCst);
        let num_unusable_bgp_updates_known_peer: usize = router_bmp_metrics
            .num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers
            .load(Ordering::SeqCst);
        let num_retried_bgp_updates_unknown_peer: usize = router_bmp_metrics
            .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
            .load(Ordering::SeqCst);
        let num_unusable_bgp_updates_unknown_peer: usize = router_bmp_metrics
            .num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers
            .load(Ordering::SeqCst);
        let num_prefixes: usize = router_bmp_metrics
            .num_stored_prefixes
            .load(Ordering::SeqCst);
        let num_announce: usize = router_bmp_metrics.num_announcements.load(Ordering::SeqCst);
        let num_withdraw: usize = router_bmp_metrics.num_withdrawals.load(Ordering::SeqCst);
        let num_peers_up: usize = router_bmp_metrics.num_peers_up.load(Ordering::SeqCst);
        let num_peers_up_eor_capable: usize = router_bmp_metrics
            .num_peers_up_eor_capable
            .load(Ordering::SeqCst);
        let num_peers_up_dumping: usize = router_bmp_metrics
            .num_peers_up_dumping
            .load(Ordering::SeqCst);

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

        let mut peer_report = String::new();
        if let Some(peer_states) = peer_states {
            writeln!(peer_report, "<table>").unwrap();

            let table_header = formatdoc! {
                r#"
                <tr>
                    <th>Timestamp</th>
                    <th>IP Address</th>
                    <th>ASN</th>
                    <th># Prefixes</th>
                    <th>Flags</th>
                </tr>
                "#
            };

            peer_report.push_str(&table_header);

            for pph in peer_states.get_peers() {
                let peer_key = format!("{}", pph);
                let num_prefixes = peer_states
                    .get_announced_prefixes(&pph)
                    .map_or(0, |iter| iter.count());
                let prefixes_link = if num_prefixes > 0 {
                    format!(
                        "<a href=\"{}/prefixes/{}\">{}</a>",
                        &base_http_path, pph, num_prefixes
                    )
                } else {
                    "0".to_string()
                };
                writeln!(peer_report, "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:08b} [<a href=\"{}/flags/{}\">more</a>]</td></tr>",
                    pph.timestamp(),
                    pph.address(),
                    pph.asn(),
                    prefixes_link,
                    pph.flags(),
                    &base_http_path,
                    pph,
                ).unwrap();

                match &focus {
                    Focus::None => { /* Nothing to do */ }

                    #[rustfmt::skip]
                    Focus::Flags(focus_key) => {
                        if peer_key.as_str() == focus_key {
                            writeln!(peer_report, "<tr><td colspan=6><pre>").unwrap();
                            writeln!(peer_report, "    Peer Details: [<a href=\"{}\">close</a>]", base_http_path).unwrap();
                            writeln!(peer_report, "        Peer Type         : {:?}", pph.peer_type()).unwrap();
                            writeln!(peer_report, "        Peer Flags        :").unwrap();
                            writeln!(peer_report, "            V: {} ({})", pph.is_ipv6(), if pph.is_ipv4() { "IPv4" } else { "IPv6" }).unwrap();
                            writeln!(peer_report, "            L: {} ({})", pph.is_post_policy(), if pph.is_pre_policy() { "Pre" } else { "Post" }).unwrap();
                            writeln!(peer_report, "            A: {} ({})", pph.is_legacy_format(), if pph.is_legacy_format() { "Legacy 2-byte AS_PATH format" } else { "4-byte AS_PATH format (RFC6793)" }).unwrap();
                            writeln!(peer_report, "        Peer Distinguisher: {:?}", pph.distinguisher()).unwrap();
                            writeln!(peer_report, "        Peer Address      : {}", pph.address()).unwrap();
                            writeln!(peer_report, "        PeerAS            : {:?}", pph.asn()).unwrap();
                            writeln!(peer_report, "        Peer BGP ID       : {:?}", pph.bgp_id()).unwrap();
                            writeln!(peer_report, "        Timestamp         : {}", pph.timestamp()).unwrap();
                            writeln!(peer_report, "</pre></td></tr>").unwrap();
                        }
                    },

                    #[rustfmt::skip]
                    Focus::Prefixes(focus_key) => {
                        if peer_key.as_str() == focus_key {
                            if let Some(prefixes) = peer_states.get_announced_prefixes(&pph) {
                                writeln!(peer_report, "<tr><td colspan=6><pre>").unwrap();
                                writeln!(peer_report, "    Announced prefixes: [<a href=\"{}\">close</a>]", base_http_path).unwrap();
                                for prefix in prefixes {
                                    write!(peer_report, "        {}: ", prefix).unwrap();
                                    for rel_base_url in http_resources.rel_base_urls_for_component_type("rib") {
                                        write!(peer_report, "<a href=\"{}{}\">view</a> ", rel_base_url, prefix).unwrap();
                                    }
                                    writeln!(peer_report, "").unwrap();
                                }
                                writeln!(peer_report, "</pre></td></tr>").unwrap();
                            }
                        }
                    }
                }
            }

            writeln!(peer_report, "</table>").unwrap();
        }

        let response_body = formatdoc!(
            r#"
            Router:
                Source       : {source_id}
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
            
            Peers:
            {peer_report}
            "#
        );

        response_body
    }

    fn build_response_footer(response_body: &mut String) {
        response_body.push_str("    </pre>\n");
        response_body.push_str("  </body>\n");
        response_body.push_str("</html>\n");
    }
}
