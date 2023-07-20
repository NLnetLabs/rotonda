use std::{fmt::Display, sync::Arc, time::Duration};

use super::{metrics::MqttMetrics, status_reporter::MqttStatusReporter};

use crate::{
    common::{
        json::mk_communities_json,
        status_reporter::{AnyStatusReporter, TargetStatusReporter},
    },
    comms::{AnyDirectUpdate, DirectLink, DirectUpdate, Terminated},
    manager::{Component, TargetCommand, WaitPoint},
    payload::{Action, Payload, RawBmpPayload, RouterId, Update},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mqtt::{
    AsyncClient, ClientError, ConnAck, ConnectReturnCode, Event, EventLoop, Incoming, MqttOptions,
    QoS,
};
use non_empty_vec::NonEmpty;
use roto::types::typevalue::TypeValue;
use routecore::bmp::message::Message as BmpMsg;
use routecore::{addr::Prefix, bgp::communities::Community};
use serde::Deserialize;
use serde_json::{json, Value};
use serde_with::{serde_as, DisplayFromStr};
use tokio::{
    sync::mpsc,
    time::{interval, timeout},
};

#[derive(Debug)]
enum MqttError {
    Error(ClientError),
    Timeout,
}

impl Display for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MqttError::Error(err) => err.fmt(f),
            MqttError::Timeout => f.write_str("MQTT Timeout"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Mqtt {
    /// The set of units to receive messages from.
    sources: NonEmpty<DirectLink>,

    #[serde(flatten)]
    config: Config,
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Mode {
    /// In filtering mode only announcements which match the defined triggers will be published
    /// to the MQTT broker.
    Filtering,

    /// In mirroring mode every update (both announcements and withdrawals) is published to the
    /// MQTT broker.
    Mirroring,
}

impl Default for Mode {
    fn default() -> Self {
        Self::Filtering
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize)]
struct Config {
    server_host: String,

    server_port: u16,

    #[serde(default = "Config::default_qos")]
    qos: i32,

    #[serde(default)]
    client_id: String,

    #[serde(default = "Config::default_topic_template")]
    topic_template: String,

    /// How long to wait in seconds before connecting again if the connection is closed.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_connect_retry_secs")]
    connect_retry_secs: Duration,

    /// How long to wait before timing out an attempt to publish a message.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "Config::default_publish_max_secs")]
    publish_max_secs: Duration,

    /// How many messages to buffer if publishing encounters delays
    #[serde(default = "Config::default_queue_size")]
    queue_size: u16,

    #[serde(default)]
    mode: Mode,

    #[serde_as(as = "Vec<DisplayFromStr>")]
    communities: Vec<Community>,
}

impl Config {
    /// The default MQTT quality of service setting to use
    ///   0 - At most once delivery
    ///   1 - At least once delivery
    ///   2 - Exactly once delivery
    pub fn default_qos() -> i32 {
        2
    }

    /// The default re-connect timeout in seconds.
    pub fn default_connect_retry_secs() -> Duration {
        Duration::from_secs(60)
    }

    /// The default publish timeout in seconds.
    pub fn default_publish_max_secs() -> Duration {
        Duration::from_secs(5)
    }

    /// The default MQTT topic prefix.
    pub fn default_topic_template() -> String {
        "rotonda/{id}".to_string()
    }

    /// The default re-connect timeout in seconds.
    pub fn default_queue_size() -> u16 {
        1000
    }
}

type SenderMsg = (DateTime<Utc>, (Value, String), Arc<RouterId>);

impl Mqtt {
    pub async fn run(
        self,
        component: Component,
        cmd: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        MqttRunner::new(self.config, component)
            .run(self.sources, cmd, waitpoint)
            .await
    }
}

// Being generic over T enables use of a mock file I/O implementation when testing.
struct MqttRunner {
    component: Component,
    config: Arc<Config>,
    sender: Option<mpsc::UnboundedSender<SenderMsg>>,
    status_reporter: Arc<MqttStatusReporter>,
}

impl MqttRunner {
    fn new(config: Config, mut component: Component) -> Self {
        let config = Arc::new(config);

        let metrics = Arc::new(MqttMetrics::new());
        component.register_metrics(metrics.clone());

        let status_reporter = Arc::new(MqttStatusReporter::new(component.name(), metrics));

        Self {
            component,
            config,
            sender: None,
            status_reporter,
        }
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        mut cmd_rx: mpsc::Receiver<TargetCommand>,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let _unit_name = component.name().clone();

        let (tx, rx) = mpsc::unbounded_channel();
        self.sender = Some(tx);

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        let mut create_opts = MqttOptions::new(
            arc_self.config.client_id.clone(),
            arc_self.config.server_host.clone(),
            arc_self.config.server_port,
        );
        create_opts.set_request_channel_capacity(arc_self.config.queue_size.into());
        create_opts.set_clean_session(false);
        create_opts.set_inflight(1000);
        create_opts.set_keep_alive(Duration::from_secs(20));
        create_opts.set_connection_timeout(arc_self.config.connect_retry_secs.as_secs());

        // Create a client & connect
        arc_self.status_reporter.connecting(&format!(
            "{}:{}",
            &arc_self.config.server_host, arc_self.config.server_port
        ));
        let cap = arc_self.config.queue_size.into();
        let (client, connection) = AsyncClient::new(create_opts, cap);
        let client = Arc::new(client);

        crate::tokio::spawn(
            "mqtt-publisher",
            Self::message_publisher(
                rx,
                arc_self.config.clone(),
                arc_self.status_reporter.clone(),
                client.clone(),
                Some(connection),
                waitpoint,
            ),
        );

        while let Some(cmd) = cmd_rx.recv().await {
            arc_self.status_reporter.command_received(&cmd);
            match cmd {
                TargetCommand::Reconfigure { new_config } => {
                    if let crate::targets::Target::Mqtt(Mqtt {
                        sources: new_sources,
                        ..
                        // config
                    }) = new_config
                    {
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
                }

                TargetCommand::ReportLinks { report } => {
                    report.set_sources(&sources);
                    report.set_graph_status(arc_self.status_reporter.metrics());
                }

                TargetCommand::Terminate => break,
            }
        }

        let _ = client.try_disconnect();

        arc_self.status_reporter.terminated();
        Err(Terminated)
    }

    async fn message_publisher(
        mut rx: mpsc::UnboundedReceiver<SenderMsg>,
        config: Arc<Config>,
        status_reporter: Arc<MqttStatusReporter>,
        client: Arc<AsyncClient>,
        connection: Option<EventLoop>,
        waitpoint: WaitPoint,
    ) {
        // TODO: support dynamic reconfiguration while we are running, e.g.
        // change of MQTT client settings.
        if let Some(mut connection) = connection {
            let status_reporter = status_reporter.clone();
            let connect_retry_secs = config.connect_retry_secs;
            let server_uri = format!("{}:{}", config.server_host, config.server_port);

            crate::tokio::spawn("MQTT connection", async move {
                let mut interval = interval(connect_retry_secs);
                interval.tick().await; // the first tick completes immediately

                // reconnects should be automatic
                loop {
                    status_reporter.connecting(&server_uri);
                    loop {
                        match connection.poll().await {
                            Ok(Event::Incoming(Incoming::ConnAck(ConnAck {
                                code: ConnectReturnCode::Success,
                                ..
                            }))) => {
                                status_reporter.connected(&server_uri);
                            }

                            Ok(_) => { /* No other events are handled specially at this time */ }

                            Err(err) => {
                                status_reporter
                                    .connection_error(err.to_string(), connect_retry_secs);
                                interval.tick().await;
                                break;
                            }
                        }
                    }
                }
            });
        }

        // Wait for other components to be, and signal to other components that we are, ready to start. All units and
        // targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        waitpoint.running().await;

        while let Some((received, (event, prefix_str), router_id)) = rx.recv().await {
            let content = event.to_string();
            let topic = config.topic_template.replace("{id}", &router_id);
            Self::publish_msg(
                status_reporter.clone(),
                Some(client.clone()),
                topic,
                received,
                prefix_str,
                content,
                config.qos,
                config.publish_max_secs,
                None::<fn() -> Result<(), MqttError>>,
            )
            .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_msg<F>(
        status_reporter: Arc<MqttStatusReporter>,
        client: Option<Arc<AsyncClient>>,
        topic: String,
        received: DateTime<Utc>,
        prefix_str: String,
        content: String,
        qos: i32,
        duration: Duration,
        test_publish: Option<F>,
    ) where
        F: Fn() -> Result<(), MqttError> + Send + 'static,
    {
        status_reporter.publishing();

        match Self::do_publish(client, &topic, content, qos, duration, test_publish).await {
            Ok(_) => {
                status_reporter.publish_ok(prefix_str, topic, received);
            }
            Err(err) => {
                status_reporter.publish_error(err);
            }
        }
    }

    async fn do_publish<F>(
        client: Option<Arc<AsyncClient>>,
        topic: &str,
        content: String,
        qos: i32,
        duration: Duration,
        test_publish: Option<F>,
    ) -> Result<(), MqttError>
    where
        F: Fn() -> Result<(), MqttError> + Send + 'static,
    {
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };

        if let Some(client) = client {
            match timeout(duration, client.publish(topic, qos, false, content)).await {
                Err(_) => Err(MqttError::Timeout),
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(MqttError::Error(err)),
            }
        } else if let Some(test_publish) = test_publish {
            // instead of using duration as a timeout, use it as a delay
            tokio::time::sleep(duration).await;
            test_publish()
        } else {
            Ok(())
        }
    }

    // fn prepare_for_publication(
    //     &self,
    //     action: Action,
    //     router_id: Arc<RouterId>,
    //     pfx: &Prefix,
    //     rib_el: &RibElement,
    // ) -> Option<(Value, String)> {
    //     match (self.config.mode, action) {
    //         (Mode::Filtering, Action::Announce) | (Mode::Mirroring, Action::Announce) => {
    //             if let Some(advert) = &rib_el.advert {
    //                 let triggered = advert.has_communities(&self.config.communities);

    //                 let publish = match self.config.mode {
    //                     Mode::Mirroring => true,
    //                     Mode::Filtering => triggered,
    //                 };

    //                 if publish {
    //                     let (addr, len) = pfx.addr_and_len();
    //                     let prefix_str = format!("{}/{}", addr, len);
    //                     let routing_information_base_name =
    //                         rib_el.routing_information_base.to_string();

    //                     let description = match self.config.mode {
    //                         Mode::Filtering => {
    //                             format!(
    //                                 "{} RTBH received from router {} on {}",
    //                                 prefix_str, router_id, routing_information_base_name
    //                             )
    //                         }
    //                         Mode::Mirroring => {
    //                             format!(
    //                                 "{} announcement received from router {} on {}",
    //                                 prefix_str, router_id, routing_information_base_name
    //                             )
    //                         }
    //                     };

    //                     let communities = mk_communities_json(
    //                         advert.standard_communities(),
    //                         advert.ext_communities(),
    //                         advert.large_communities(),
    //                     );

    //                     let as_path: Vec<String> =
    //                         advert.as_path.iter().map(|asn| asn.to_string()).collect();

    //                     let event = json!({
    //                         "prefix": &prefix_str,
    //                         "router": router_id,
    //                         "sourceAs": rib_el.neighbor.0.to_string(),
    //                         "routingInformationBaseName": routing_information_base_name,
    //                         "asPath": as_path,
    //                         "neighbor": rib_el.neighbor.1,
    //                         "communities": communities,
    //                         "description": description,
    //                     });

    //                     return Some((event, prefix_str));
    //                 }
    //             }
    //         }

    //         (Mode::Filtering, Action::Withdraw) => {
    //             // Nothing to do
    //         }

    //         (Mode::Mirroring, Action::Withdraw) => {
    //             let (addr, len) = pfx.addr_and_len();
    //             let prefix_str = format!("{}/{}", addr, len);
    //             let routing_information_base_name = rib_el.routing_information_base.to_string();
    //             let description = format!(
    //                 "{} withdrawal received from router {} on {}",
    //                 prefix_str, router_id, routing_information_base_name
    //             );

    //             let event = json!({
    //                 "prefix": &prefix_str,
    //                 "router": router_id,
    //                 "sourceAs": rib_el.neighbor.0.to_string(),
    //                 "routingInformationBaseName": routing_information_base_name,
    //                 "asPath": [],
    //                 "neighbor": rib_el.neighbor.1,
    //                 "communities": [],
    //                 "description": description,
    //             });

    //             return Some((event, prefix_str));
    //         }
    //     }

    //     None
    // }
}

#[async_trait]
impl DirectUpdate for MqttRunner {
    async fn direct_update(&self, update: Update) {
        match update {
            Update::Bulk(updates) => {
                todo!()
            }

            // Update::Bulk(updates) => {
            //     log::trace!("MQTT: Received direct update");
            //     for payload in updates {
            //         log::trace!("MQTT: Processing direct update payload");
            //         if let Payload::RouterSpecificRibElement(pfx, meta) = payload {
            //             let rib_el = meta.rib_el.load();
            //             let publish_details = self.prepare_for_publication(
            //                 todo!(), //action,
            //                 meta.router_id.clone(),
            //                 &pfx,
            //                 &rib_el,
            //             );

            //             if let Some(publish_details) = publish_details {
            //                 let msg = (rib_el.received, publish_details, meta.router_id.clone());
            //                 self.sender.as_ref().unwrap().send(msg).unwrap();
            //             }
            //         }
            //     }
            // }

            Update::Single(Payload::RawBmp { received, router_addr, msg: RawBmpPayload::Msg(bytes) }) => {
                let msg = BmpMsg::from_octets(bytes).unwrap();

                let (msg_type, pph, msg_type_specific) = match &msg {
                    BmpMsg::RouteMonitoring(msg) => (0, Some(msg.per_peer_header()), None),
                    BmpMsg::StatisticsReport(msg) => (1, Some(msg.per_peer_header()), None),
                    BmpMsg::PeerDownNotification(msg) => (2, Some(msg.per_peer_header()), Some(format!("{:?}", msg.reason()))),
                    BmpMsg::PeerUpNotification(msg) => (3, Some(msg.per_peer_header()), None),
                    BmpMsg::InitiationMessage(_msg) => (4, None, None),
                    BmpMsg::TerminationMessage(_msg) => (5, None, None),
                    BmpMsg::RouteMirroring(msg) => (6, Some(msg.per_peer_header()), None),
                };

                let pph_str: String = match pph {
                    Some(pph) => format!("{}", pph),
                    None => "No PPH".to_string(),
                };

                let event = json!({
                    "router": router_addr,
                    "msg_type": msg_type,
                    "pph": pph_str,
                    "msg_type_specific": msg_type_specific,
                });

                let publish_details = (event, "no-prefix".to_string());
                let msg = (received, publish_details, Arc::new(router_addr.to_string()));
                self.sender.as_ref().unwrap().send(msg).unwrap();
            }

            Update::Single(_) => {
                self.status_reporter.input_mismatch(
                    "Update::Single(Payload::RawBmp)",
                    "Update::Single(RawBmp)|Update::Bulk(_)",
                );
            }

            Update::QueryResult(..) => {
                self.status_reporter
                    .input_mismatch("Update::Single(Payload::RawBmp)", "Update::QueryResult(_)");
            }

            Update::OutputStreamMessage(messages) => {
                // Ahha, is this for us?
                // TODO: match on the topic, does it match something we are interested in?
                let received = Utc::now();
                for msg in messages {
                    let event = serde_json::to_value(&TypeValue::from(msg)).unwrap();
                    let publish_details = (event, "no-prefix".to_string());
                    let msg = (
                        received,
                        publish_details,
                        Arc::new("dummy-router-id".into()),
                    );
                    self.sender.as_ref().unwrap().send(msg).unwrap();
                }
            }
        }
    }
}

impl AnyDirectUpdate for MqttRunner {}

impl std::fmt::Debug for MqttRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttRunner").finish()
    }
}

//------------ Tests ---------------------------------------------------------

// #[cfg(test)]
// mod tests {
//     use std::{net::IpAddr, str::FromStr};

//     use assert_json_diff::{assert_json_matches_no_panic, CompareMode};
//     use routecore::{
//         asn::Asn,
//         bgp::{
//             communities::{ExtendedCommunity, LargeCommunity, StandardCommunity, Wellknown},
//             types::NextHop,
//         },
//     };

//     use crate::{
//         bgp::raw::communities::{
//             extended::sample_as2_specific_route_target_extended_community,
//             large::sample_large_community,
//         },
//         payload::{AdvertisedRouteDetails, RoutingInformationBase},
//     };

//     use super::*;

//     use routecore::bgp::communities::ExtendedCommunitySubType::*;
//     use routecore::bgp::communities::ExtendedCommunityType::*;

//     #[test]
//     fn server_host_and_port_and_communities_config_settings_must_be_provided() {
//         let empty = r#""#;
//         let empty_server_host_only = r#"server_host = """#;
//         let empty_server_port_only = r#"server_port = 0"#;
//         let empty_server_host_and_port_only = r#"
//         server_host = ""
//         server_port = 0
//         "#;
//         let empty_server_host_and_port_and_communities = r#"
//         server_host = ""
//         server_port = 0
//         communities = []
//         "#;

//         assert!(mk_config_from_toml(empty).is_err());
//         assert!(mk_config_from_toml(empty_server_host_only).is_err());
//         assert!(mk_config_from_toml(empty_server_port_only).is_err());
//         assert!(mk_config_from_toml(empty_server_host_and_port_only).is_err());
//         assert!(mk_config_from_toml(empty_server_host_and_port_and_communities).is_ok());
//     }

//     #[test]
//     fn communities_config_setting_supports_standard_well_known_community_names() {
//         let cfg = r#"
//         server_host = ""
//         server_port = 0
//         communities = ["NO_EXPORT", "BLACKHOLE", "NO_ADVERTISE", "NO_EXPORT_SUBCONFED"]
//         "#;

//         let communities = mk_config_from_toml(cfg).unwrap().communities;
//         assert_eq!(communities.len(), 4);
//         assert!(communities.contains(&Wellknown::Blackhole.into()));
//         assert!(communities.contains(&Wellknown::NoAdvertise.into()));
//         assert!(communities.contains(&Wellknown::NoExport.into()));
//         assert!(communities.contains(&Wellknown::NoExportSubconfed.into()));
//     }

//     #[test]
//     fn communities_config_setting_supports_rfc1997_private_communities() {
//         let cfg = r#"
//         server_host = ""
//         server_port = 0
//         communities = ["123:456"]
//         "#;

//         let communities = mk_config_from_toml(cfg).unwrap().communities;
//         assert_eq!(communities.len(), 1);
//         let actual_community = &communities[0];

//         let [asn_high, asn_low] = 123u16.to_be_bytes();
//         let [tag_high, tag_low] = 456u16.to_be_bytes();
//         let expected_community = Community::from([asn_high, asn_low, tag_high, tag_low]);

//         assert!(matches!(actual_community, Community::Standard(_)));
//         assert!(matches!(expected_community, Community::Standard(_)));
//         assert_eq!(*actual_community, expected_community);
//         if let Community::Standard(actual_community) = actual_community {
//             assert_eq!(actual_community.asn(), Some(Asn::from_u32(123)));
//             assert_eq!(actual_community.tag().unwrap().value(), 456);
//         }
//     }

//     #[test]
//     fn communities_config_setting_supports_rfc4360_extended_communities() {
//         let cfg = r#"
//         server_host = ""
//         server_port = 0
//         communities = ["0x000200220000D508"]
//         "#;

//         let communities = mk_config_from_toml(cfg).unwrap().communities;
//         assert_eq!(communities.len(), 1);
//         let actual_community = &communities[0];

//         let expected_community = Community::from([0x00, 0x02, 0x00, 0x22, 0x00, 0x00, 0xD5, 0x08]);

//         assert!(matches!(actual_community, Community::Extended(_)));
//         assert!(matches!(expected_community, Community::Extended(_)));
//         assert_eq!(*actual_community, expected_community);
//         if let Community::Extended(actual_community) = actual_community {
//             assert_eq!(
//                 actual_community.types(),
//                 (TransitiveTwoOctetSpecific, RouteTarget)
//             )
//         }
//     }

//     #[test]
//     fn communities_config_setting_supports_rfc8092_large_communities() {
//         let cfg = r#"
//         server_host = ""
//         server_port = 0
//         communities = ["64496:4294967295:2"]
//         "#;

//         let communities = mk_config_from_toml(cfg).unwrap().communities;
//         assert_eq!(communities.len(), 1);
//         let actual_community = &communities[0];

//         let expected_community = Community::from([
//             0x00, 0x00, 0xFB, 0xF0, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x02,
//         ]);

//         assert!(matches!(actual_community, Community::Large(_)));
//         assert!(matches!(expected_community, Community::Large(_)));
//         assert_eq!(*actual_community, expected_community);
//         if let Community::Large(actual_community) = actual_community {
//             assert_eq!(actual_community.global(), 64496_u32);
//             assert_eq!(actual_community.local1(), 4294967295_u32);
//             assert_eq!(actual_community.local2(), 2_u32);
//         }
//     }

//     #[test]
//     fn filter_withdrawals() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_standard_community()]);

//         // And a candidate that should NOT be selected by the runner for publication
//         let bad = mk_qualifying_publication_candidate().with_action(Action::Withdraw);

//         // Then the candidate should NOT be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, bad).is_none());
//     }

//     #[test]
//     fn filter_incomplete_announcements() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_standard_community()]);

//         // And a candidate that should NOT be selected by the runner for publication
//         let bad = mk_qualifying_publication_candidate().with_advert(None);

//         // Then the candidate should NOT be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, bad).is_none());
//     }

//     #[test]
//     fn filter_unmatched_community() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_standard_community()]);

//         // And a candidate that should NOT be selected by the runner for publication
//         let bad = mk_qualifying_publication_candidate()
//             .with_communities(vec![Wellknown::NoExport.into()]);

//         // Then the candidate should NOT be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, bad).is_none());
//     }

//     #[test]
//     fn publish_matching_standard_community() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_standard_community()]);

//         // And a candidate that should be selected by the runner for publication
//         let good = mk_qualifying_publication_candidate();

//         // Then the candidate should be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, good).is_some());
//     }

//     #[test]
//     fn publish_matching_extended_community() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_extended_community()]);

//         // And a candidate that should be selected by the runner for publication
//         let good = mk_qualifying_publication_candidate();

//         // Then the candidate should be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, good).is_some());
//     }

//     #[test]
//     fn publish_matching_large_community() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_large_community()]);

//         // And a candidate that should be selected by the runner for publication
//         let good = mk_qualifying_publication_candidate();

//         // Then the candidate should be selected for publication
//         assert!(prepare_candidate_for_publication(&runner, good).is_some());
//     }

//     #[test]
//     fn generate_correct_json_for_publishing() {
//         // Given an MQTT target runner that will match on a particular community
//         let runner = mk_mqtt_runner(Mode::Filtering, vec![mk_expected_standard_community()]);

//         // And a candidate that should be selected by the runner for publication
//         let good = mk_qualifying_publication_candidate();

//         // Then the candidate should be selected for publication
//         let (actual_json, _) = prepare_candidate_for_publication(&runner, good).unwrap();

//         // And the produced message to be published should match the expected JSON format
//         let expected_json = json!({
//             "prefix": "1.2.3.0/24",
//             "router": "test-router",
//             "sourceAs": "AS1818",
//             "routingInformationBaseName": "Post-Policy-RIB",
//             "asPath": [
//                 "AS123",
//                 "AS456"
//             ],
//             "neighbor": "4.5.6.7",
//             "communities": [
//                 {
//                     "rawFields": ["0xFFFF029A"],
//                     "type": "standard",
//                     "parsed": {
//                         "value": { "type": "well-known", "attribute": "BLACKHOLE" }
//                     }
//                 },
//                 {
//                     "rawFields": ["0x00", "0x02", "0x0022", "0x0000D508"],
//                     "type": "extended",
//                     "parsed": {
//                         "type": "as2-specific",
//                         "rfc7153SubType": "route-target",
//                         "transitive": true,
//                         "globalAdmin": { "type": "asn", "value": "AS34" },
//                         "localAdmin": 54536
//                     }
//                 },
//                 {
//                     "type": "large",
//                     "rawFields": ["0x0022", "0x0100", "0x0200"],
//                     "parsed": {
//                       "globalAdmin": { "type": "asn", "value": "AS34" },
//                       "localDataPart1": 256,
//                       "localDataPart2": 512
//                     }
//                 }
//             ],
//             "description": "1.2.3.0/24 RTBH received from router test-router on Post-Policy-RIB"
//         });

//         assert_json_eq(actual_json, expected_json);
//     }

//     // --- Test helpers -----------------------------------------------------------------------------------------------

//     struct CandidatePublicationDetails {
//         action: Action,
//         router_id: Arc<String>,
//         prefix: Prefix,
//         rib_el: RibElement,
//     }

//     impl CandidatePublicationDetails {
//         fn new(action: Action, router_id: &str, prefix: Prefix, rib_el: RibElement) -> Self {
//             Self {
//                 action,
//                 router_id: Arc::new(router_id.to_string()),
//                 prefix,
//                 rib_el,
//             }
//         }

//         fn with_action(mut self, action: Action) -> Self {
//             self.action = action;
//             self
//         }

//         fn with_advert(mut self, advert: Option<AdvertisedRouteDetails>) -> Self {
//             self.rib_el.advert = advert;
//             self
//         }

//         fn with_communities(mut self, communities: Vec<Community>) -> Self {
//             if let Some(advert) = &mut self.rib_el.advert {
//                 advert.communities = mk_raw_communities_tuple(communities);
//             }
//             self
//         }
//     }

//     #[rustfmt::skip]
//     type PublishableMessage = (Value /* msg json */, String /* mqtt topic prefix */);

//     fn mk_config_from_toml(toml: &str) -> Result<Config, toml::de::Error> {
//         toml::de::from_slice::<Config>(toml.as_bytes())
//     }

//     fn assert_json_eq(actual_json: Value, expected_json: Value) {
//         let config = assert_json_diff::Config::new(CompareMode::Strict);
//         if let Err(err) = assert_json_matches_no_panic(&actual_json, &expected_json, config) {
//             eprintln!(
//                 "Actual JSON: {}",
//                 serde_json::to_string_pretty(&actual_json).unwrap()
//             );
//             eprintln!(
//                 "Expected JSON: {}",
//                 serde_json::to_string_pretty(&expected_json).unwrap()
//             );
//             panic!("JSON doesn't match expectations: {}", err);
//         }
//     }

//     fn prepare_candidate_for_publication(
//         runner: &MqttRunner,
//         CandidatePublicationDetails {
//             action,
//             router_id,
//             prefix,
//             rib_el,
//         }: CandidatePublicationDetails,
//     ) -> Option<PublishableMessage> {
//         runner.prepare_for_publication(action, router_id, &prefix, &rib_el)
//     }

//     /// Make an internal event that should qualify for MQTT publication
//     fn mk_qualifying_publication_candidate() -> CandidatePublicationDetails {
//         mk_event(
//             Action::Announce,
//             "1.2.3.0/24",
//             1818,
//             "4.5.6.7",
//             &[123, 456],
//             vec![
//                 mk_expected_standard_community(),
//                 mk_expected_extended_community(),
//                 mk_expected_large_community(),
//             ],
//         )
//     }

//     fn mk_expected_standard_community() -> Community {
//         Wellknown::Blackhole.into()
//     }

//     fn mk_expected_extended_community() -> Community {
//         sample_as2_specific_route_target_extended_community().into()
//     }

//     fn mk_expected_large_community() -> Community {
//         sample_large_community().into()
//     }

//     fn mk_event(
//         action: Action,
//         pfx: &str,
//         asn: u32,
//         peer: &str,
//         as_path: &[u32],
//         communities: Vec<Community>,
//     ) -> CandidatePublicationDetails {
//         let router_id = "test-router";
//         let pfx = Prefix::from_str(pfx).unwrap();
//         let rib_name = RoutingInformationBase::PostPolicy;
//         let neighbor = (Asn::from_u32(asn), IpAddr::from_str(peer).unwrap());
//         let as_path = as_path
//             .iter()
//             .map(|&v| Asn::from_u32(v))
//             .collect::<Vec<_>>();

//         let advert = if as_path.is_empty() || communities.is_empty() {
//             None
//         } else {
//             Some(AdvertisedRouteDetails {
//                 as_path,
//                 next_hop: NextHop::Empty,
//                 communities: mk_raw_communities_tuple(communities),
//             })
//         };

//         let rib_el = RibElement::new(rib_name, neighbor, advert);

//         CandidatePublicationDetails::new(action, router_id, pfx, rib_el)
//     }

//     fn mk_raw_communities_tuple(
//         communities: Vec<Community>,
//     ) -> (
//         Vec<StandardCommunity>,
//         Vec<ExtendedCommunity>,
//         Vec<LargeCommunity>,
//     ) {
//         let mut standard = vec![];
//         let mut extended = vec![];
//         let mut large = vec![];

//         for c in communities {
//             match c {
//                 Community::Standard(c) => standard.push(c),
//                 Community::Extended(c) => extended.push(c),
//                 Community::Ipv6Extended(_) => { /* TODO */ }
//                 Community::Large(c) => large.push(c),
//             }
//         }

//         (standard, extended, large)
//     }

//     fn mk_mqtt_runner(mode: Mode, communities: Vec<Community>) -> MqttRunner {
//         let config = Config {
//             mode,
//             communities,
//             ..Default::default()
//         };

//         MqttRunner::new(config, Component::default())
//     }

//     // TODO: test Mode::Mirroring
// }
