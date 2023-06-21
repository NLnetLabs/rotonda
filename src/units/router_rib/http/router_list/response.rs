use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};

use hyper::{Body, Response};
use indoc::formatdoc;

use crate::{
    common::frim::FrimMap,
    units::router_rib::{
        metrics::RouterRibMetrics,
        state_machine::metrics::BmpMetrics,
        types::RouterInfo,
        util::{calc_u8_pc, format_router_id},
    },
};

use super::request::RouterListApi;

const MAX_INFO_TLV_LEN: usize = 60;

impl RouterListApi {
    pub fn build_response(
        keys: Vec<SocketAddr>,
        router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>,
        router_id_template: Arc<String>,
        bmp_metrics: Arc<BmpMetrics>,
        router_metrics: Arc<RouterRibMetrics>,
        http_api_path: std::borrow::Cow<str>,
    ) -> Response<Body> {
        let mut response_body = Self::build_response_header(router_info.clone());

        Self::build_response_body(
            keys,
            router_info,
            router_id_template,
            bmp_metrics,
            router_metrics,
            http_api_path,
            &mut response_body,
        );

        Self::build_response_footer(&mut response_body);

        Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(response_body))
            .unwrap()
    }

    fn build_response_header(router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>) -> String {
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
                <pre>Showing {num_routers} monitored routers:
                <table>
                    <tr>
                        <th>Router Address</th>
                        <th>sysName</th>
                        <th>sysDesc</th>
                        <th>State</th>
                        <th># Peers Up/EoR Capable/Dumping</th>
                        <th># Invalid Messages (Soft/Hard Parse Errors)</th>
                    </tr>
            "#,
            num_routers = router_info.len()
        }
    }

    fn build_response_footer(response_body: &mut String) {
        response_body.push_str("      </table>\n");
        response_body.push_str("    </pre>\n");
        response_body.push_str("  </body>\n");
        response_body.push_str("</html>\n");
    }

    fn build_response_body(
        keys: Vec<SocketAddr>,
        router_info: Arc<FrimMap<SocketAddr, Arc<RouterInfo>>>,
        router_id_template: Arc<String>,
        bmp_metrics: Arc<BmpMetrics>,
        router_metrics: Arc<RouterRibMetrics>,
        http_api_path: std::borrow::Cow<str>,
        response_body: &mut String,
    ) {
        for addr in keys.iter() {
            if let Some(this_router_info) = router_info.get(addr) {
                let fragment = match (&this_router_info.sys_name, &this_router_info.sys_desc) {
                    (Some(sys_name), Some(sys_desc)) => {
                        // Don't trust external input, it could contain HTML
                        // or JavaScript which when we output it would be
                        // rendered in the client browser.
                        let sys_name = html_escape::encode_safe(&sys_name);
                        let sys_desc = html_escape::encode_safe(&sys_desc);

                        let sys_name = if sys_name.len() > MAX_INFO_TLV_LEN {
                            &sys_name[0..=MAX_INFO_TLV_LEN]
                        } else {
                            &sys_name[..]
                        };
                        let sys_desc = if sys_desc.len() > MAX_INFO_TLV_LEN {
                            &sys_desc[0..=MAX_INFO_TLV_LEN]
                        } else {
                            &sys_desc[..]
                        };

                        let router_id =
                            Arc::new(format_router_id(router_id_template.clone(), sys_name, addr));
                        let metrics = bmp_metrics.router_metrics(router_id.clone());

                        let state = metrics.bmp_state_machine_state.load(Ordering::Relaxed);
                        let num_peers_up = metrics.num_peers_up.load(Ordering::Relaxed);
                        let num_peers_up_eor_capable =
                            metrics.num_peers_up_eor_capable.load(Ordering::Relaxed);
                        let num_peers_up_dumping =
                            metrics.num_peers_up_dumping.load(Ordering::Relaxed);
                        let num_peers_up_eor_capable_pc =
                            calc_u8_pc(num_peers_up, num_peers_up_eor_capable);
                        let num_peers_up_dumping_pc =
                            calc_u8_pc(num_peers_up_eor_capable, num_peers_up_dumping);
                        let num_invalid_bmp_messages = if let Some(router_metrics) = router_metrics.router_metrics(router_id) {
                            router_metrics
                                .num_invalid_bmp_messages
                                .load(Ordering::Relaxed)
                        } else {
                            0
                        };
                        let num_soft_parsing_failures =  metrics
                            .num_bgp_updates_with_recoverable_parsing_failures_for_known_peers
                            .load(Ordering::Relaxed)
                            + metrics
                                .num_bgp_updates_with_recoverable_parsing_failures_for_unknown_peers
                                .load(Ordering::Relaxed);
                        let num_hard_parsing_failures = metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_known_peers.load(Ordering::Relaxed) +
                            metrics.num_bgp_updates_with_unrecoverable_parsing_failures_for_unknown_peers.load(Ordering::Relaxed);

                        formatdoc! {
                            r#"
                                            <tr>
                                                <td><a href="{}{}">{}</a></td>
                                                <td><a href="{}{}">{}</a></td>
                                                <td>{}</td>
                                                <td>{}</td>
                                                <td>{}/{} ({}%)/{} ({}%)</td>
                                                <td>{} ({}/{})</td>
                                            </tr>
                                        "#,
                            http_api_path,
                            addr,
                            addr,
                            http_api_path,
                            sys_name,
                            sys_name,
                            sys_desc,
                            state,
                            num_peers_up,
                            num_peers_up_eor_capable,
                            num_peers_up_eor_capable_pc,
                            num_peers_up_dumping,
                            num_peers_up_dumping_pc,
                            num_invalid_bmp_messages,
                            num_soft_parsing_failures,
                            num_hard_parsing_failures,
                        }
                    }

                    _ => {
                        formatdoc! {
                            r#"
                                            <tr>
                                                <td><a href="{}{}">{}</a></td>
                                                <td>-</td>
                                                <td>-</td>
                                                <td>-</td>
                                                <td>-</td>
                                                <td>-</td>
                                            </tr>
                                        "#,
                            http_api_path, addr, addr
                        }
                    }
                };

                response_body.push_str(&fragment);
            }
        }
    }
}
