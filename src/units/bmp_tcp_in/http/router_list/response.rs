use std::sync::{atomic::Ordering::SeqCst, Arc};

use hyper::{Body, Response};
use indoc::formatdoc;
//use roto::types::builtin::ingress::IngressId;

use crate::{
    ingress, units::bmp_tcp_in::{
        state_machine::{BmpState, BmpStateDetails},
        util::{calc_u8_pc, format_source_id},
    }
};

use super::request::RouterListApi;

const MAX_INFO_TLV_LEN: usize = 60;

impl RouterListApi {
    pub async fn build_response(
        &self,
        keys: Vec<ingress::IngressId>,
        http_api_path: std::borrow::Cow<'_, str>,
    ) -> Response<Body> {
        let mut response_body = self.build_response_header(&keys);

        self.build_response_body(
            &keys,
            http_api_path,
            &mut response_body,
            self.ingresses.clone(),
        ).await;

        self.build_response_footer(&mut response_body);

        Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(response_body))
            .unwrap()
    }

    fn build_response_header(&self, keys: &[ingress::IngressId]) -> String {
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
                        <th>Ingress ID</th>
                        <th>Router Address</th>
                        <th>sysName</th>
                        <th>sysDesc</th>
                        <th>State</th>
                        <th># Peers Up/EoR Capable/Dumping</th>
                        <th># Invalid Messages (Soft/Hard Parse Errors)</th>
                    </tr>
            "#,
            num_routers = keys.len()
        }
    }

    fn build_response_footer(&self, response_body: &mut String) {
        response_body.push_str("      </table>\n");
        response_body.push_str("    </pre>\n");
        response_body.push_str("  </body>\n");
        response_body.push_str("</html>\n");
    }

    async fn build_response_body(
        &self,
        keys: &[ingress::IngressId],
        http_api_path: std::borrow::Cow<'_, str>,
        response_body: &mut String,
        ingresses: Arc<ingress::Register>,
    ) {

        for ingress_id in keys.iter() {
            let ingress_info = ingresses.get(*ingress_id);
            let addr = ingress_info.and_then(|i| i.remote_addr).unwrap_or([0,0,0,0].into());
            if let Some(state_machine) = self.router_states.get(ingress_id) {
                let locked = state_machine.lock().await;
                let (sys_name, sys_desc) = if let Some(sm) = locked.as_ref() {
                    match sm {
                        BmpState::Dumping(BmpStateDetails {
                            details,
                            ..
                        }) => {
                            (Some(&details.sys_name), Some(&details.sys_desc))
                        }
                        BmpState::Updating(BmpStateDetails {
                            details,
                            ..
                        }) => {
                            (Some(&details.sys_name), Some(&details.sys_desc))
                        }
                        _ => {
                            // no TLVs available
                            (None, None)
                        }
                    }
                } else {
                    (None, None)
                };

                let fragment = match (sys_name, sys_desc) {
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

                        let router_id = Arc::new(format_source_id(
                            &self.router_id_template.load(),
                            sys_name,
                            *ingress_id,
                        ));
                        let metrics = self
                            .bmp_metrics
                            .router_metrics(router_id.clone());

                        let state =
                            metrics.bmp_state_machine_state.load(SeqCst);
                        let num_peers_up = metrics.num_peers_up.load(SeqCst);
                        let num_peers_up_eor_capable =
                            metrics.num_peers_up_eor_capable.load(SeqCst);
                        let num_peers_up_dumping =
                            metrics.num_peers_up_dumping.load(SeqCst);
                        let num_peers_up_eor_capable_pc = calc_u8_pc(
                            num_peers_up,
                            num_peers_up_eor_capable,
                        );
                        let num_peers_up_dumping_pc = calc_u8_pc(
                            num_peers_up_eor_capable,
                            num_peers_up_dumping,
                        );
                        let num_invalid_bmp_messages =
                            if self.router_metrics.contains(&router_id) {
                                self.router_metrics
                                    .router_metrics(router_id)
                                    .num_invalid_bmp_messages
                                    .load(SeqCst)
                            } else {
                                0
                            };
                        let num_soft_parsing_failures = metrics
                            .num_bgp_updates_reparsed_due_to_incorrect_header_flags
                            .load(SeqCst);
                        let num_hard_parsing_failures = metrics
                            .num_unprocessable_bmp_messages
                            .load(SeqCst);

                        formatdoc! {
                            r#"
                                            <tr>
                                                <td><a href="{}{}">{}</a></td>
                                                <td><a href="{}{}">{}</a></td>
                                                <td><a href="{}{}">{}</a></td>
                                                <td>{}</td>
                                                <td>{}</td>
                                                <td>{}/{} ({}%)/{} ({}%)</td>
                                                <td>{} ({}/{})</td>
                                            </tr>
                                        "#,
                            http_api_path, ingress_id, ingress_id,
                            http_api_path, addr, addr,
                            http_api_path, sys_name, sys_name,
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
                                                <td>-</td>
                                            </tr>
                                        "#,
                            http_api_path, ingress_id, ingress_id
                        }
                    }
                };

                response_body.push_str(&fragment);
            }
        }
    }
}
