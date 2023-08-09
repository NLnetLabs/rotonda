use clap::{crate_authors, crate_version, Command};
use futures::{
    future::{select, Either},
    pin_mut,
};
use log::{error, info, warn};
use rotonda::config::{Config, ConfigFile};
use rotonda::log::ExitError;
use rotonda::manager::Manager;
use std::process::exit;
use std::{env::current_dir, path::PathBuf};
use tokio::{
    runtime::{self, Runtime},
    signal::{self, unix::signal, unix::SignalKind},
};

fn run_with_cmdline_args() -> Result<(), ExitError> {
    Config::init()?;

    let app = Command::new("rotonda")
        .version(crate_version!())
        .author(crate_authors!());

    let matches = Config::config_args(app).get_matches();

    let cur_dir = current_dir().map_err(|err| {
        error!("Fatal: cannot get current directory ({}). Aborting.", err);
        ExitError
    })?;

    // TODO: Drop privileges, get listen fd from systemd, create PID file,
    // fork, detach from the parent process, change user and group, etc.
    // In a word: daemonize.
    // Prior art:
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/operation.rs#L509
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/process.rs#L241
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/process.rs#L363

    let mut manager = Manager::new();
    let (config_path, config) = Config::from_arg_matches(&matches, &cur_dir, &mut manager)?;
    let runtime = run_with_config(&mut manager, config)?;
    runtime.block_on(handle_signals(config_path, manager))
}

async fn handle_signals(conf_path: PathBuf, mut manager: Manager) -> Result<(), ExitError> {
    let mut hup_signals = signal(SignalKind::hangup()).map_err(|err| {
        error!("Fatal: cannot listen for HUP signals ({}). Aborting.", err);
        ExitError
    })?;

    loop {
        let ctrl_c = signal::ctrl_c();
        pin_mut!(ctrl_c);

        let hup = hup_signals.recv();
        pin_mut!(hup);

        match select(hup, ctrl_c).await {
            Either::Left((None, _)) => {
                error!("Fatal: listening for SIGHUP signals failed. Aborting.");
                manager.terminate();
                return Err(ExitError);
            }
            Either::Left((Some(_), _)) => {
                // HUP signal received
                info!(
                    "SIGHUP signal received, re-reading configuration file '{}'",
                    conf_path.display()
                );
                match ConfigFile::load(&conf_path) {
                    Ok(config_file) => match manager.load(config_file) {
                        Err(_) => {
                            error!("Failed to re-read config file '{}'", conf_path.display());
                        }
                        Ok(mut config) => {
                            config.log.switch_logging(true)?;
                            manager.spawn(&mut config);
                            info!("Configuration changes applied");
                        }
                    },
                    Err(err) => {
                        error!(
                            "Failed to re-read config file '{}': {}",
                            conf_path.display(),
                            err
                        );
                    }
                }
            }
            Either::Right((Err(err), _)) => {
                error!(
                    "Fatal: listening for CTRL-C (SIGINT) signals failed ({}). Aborting.",
                    err
                );
                manager.terminate();
                return Err(ExitError);
            }
            Either::Right((Ok(_), _)) => {
                // CTRL-C received
                warn!("CTRL-C (SIGINT) received, shutting down.");
                manager.terminate();
                return Ok(());
            }
        }
    }
}

fn run_with_config(manager: &mut Manager, mut config: Config) -> Result<Runtime, ExitError> {
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Make the runtime the default for Tokio related functions that assume a default runtime.
    let _guard = runtime.enter();

    config
        .http
        .run(manager.metrics(), manager.http_resources())?;

    manager.spawn(&mut config);
    Ok(runtime)
}

fn main() {
    match run_with_cmdline_args() {
        Ok(_) => exit(0),
        Err(ExitError) => exit(1),
    }
}

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        net::IpAddr,
        str::FromStr,
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    use atomic_enum::atomic_enum;
    use prometheus_parse::Value;
    use rotonda::{
        bgp::encode::{
            mk_initiation_msg, mk_peer_up_notification_msg, mk_route_monitoring_msg, Announcements,
            MyPeerType, PerPeerHeader, Prefixes,
        },
        config::Source,
        metrics::{self, OutputFormat},
    };
    use rotonda_store::prelude::Prefix;
    use routecore::{asn::Asn, bmp::message::PeerType};
    use rumqttd::{Broker, Notification};
    use tokio::{io::AsyncWriteExt, net::TcpStream, time::sleep};

    use super::*;

    const MAX_TIME_TO_WAIT_SECS: u64 = 3;
    const METRIC_PREFIX: &str = "rotonda_";

    #[cfg(feature = "mqtt")]
    #[test]
    fn integration_test() {
        Config::init().unwrap();

        let base_config_toml = r#"
        http_listen = "127.0.0.1:8080"
        log_target = "stderr"
        log_level = "trace"

        [units.bmp-tcp-in]
        type = "bmp-tcp-in"
        listen = "127.0.0.1:11019"

        [targets.logger]
        type = "bmp-fs-out"
        sources = ["filter"]
        path = "/tmp/bmp.log"
        mode = "merge"
        format = "log"

        [units.filter]
        type = "bmp-pre-filter"
        sources = ["bmp-tcp-in"]
        asn_to_ignore = 1299

        [units.routers]
        type = "bmp-in"
        sources = ["filter"]

        [units.global-rib]
        type = "rib-unit"
        sources = ["routers"]
        roto_path = ["etc/filter.roto", "etc/filter.roto", "etc/filter.roto"]

        [targets.dummy-null]
        type = "null-out"
        source = "bmp-tcp-in"
        "#;

        let null_target_toml = r#"
        [targets.null]
        type = "null-out"
        sources = ["global-rib", "filter"]
        "#;

        let mqtt_target_toml = r#"
        [targets.local-broker]
        type = "mqtt-out"
        qos = 1 # 1=at-least-once, as rumqttd doesn't support the default value 2=exactly-once
        server_host = "127.0.0.1"
        server_port = 1883
        client_id = "rotonda"
        communities = ["BLACKHOLE"]
        sources = ["global-rib"]
        "#;

        let test_prefix = Prefix::from_str("127.0.0.1/32").unwrap();
        let test_prefix2 = Prefix::from_str("127.0.0.2/32").unwrap();
        let mut config_bytes = base_config_toml.as_bytes().to_vec();
        config_bytes.extend_from_slice(null_target_toml.as_bytes());
        let config_file = ConfigFile::new(config_bytes, Source::default());

        let mut manager = Manager::new();
        let config = Config::from_config_file(config_file, &mut manager)
            .expect("The supplied config is invalid");
        let runtime =
            run_with_config(&mut manager, config).expect("The application failed to start");

        // ---

        let mqttd_config = r#"
        id = 0

        [router]
        instant_ack = true
        max_segment_size = 10240
        max_segment_count = 10
        max_read_len = 10240
        max_connections = 10001

        [v4.1]
        name = "v4-1"
        listen = "127.0.0.1:1883"
        next_connection_delay_ms = 1
            [v4.1.connections]
            connection_timeout_ms = 5000
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 5120
            max_inflight_count = 200
            max_inflight_size = 1024

        [v5.1]
        name = "v5-1"
        listen = "127.0.0.1:1884"
        next_connection_delay_ms = 1
            [v5.1.connections]
            connection_timeout_ms = 60000
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 20480
            max_inflight_count = 500
            max_inflight_size = 1024

        [ws]

        [console]
        listen = "127.0.0.1:3030"
        "#;

        let config: rumqttd::Config = toml::de::from_str(mqttd_config).unwrap();
        let mut broker = Broker::new(config);
        let (mut link_tx, mut link_rx) = broker.link("localclient").unwrap();

        std::thread::spawn(move || {
            broker.start().unwrap();
        });

        runtime.block_on(async {
            let link_report_update_time = manager.link_report_updated_at();

            // We have to subscribe to the broker _before_ we publish to it
            link_tx.subscribe("rotonda/#").unwrap();
            assert!(matches!(link_rx.recv(), Ok(Some(Notification::DeviceAck(_)))));

            eprintln!("Subscribed to MQTT broker, sending BMP messages...");
            let mut bmp_conn = wait_for_bmp_connect().await;
            let local_addr = format!("{}", bmp_conn.local_addr().unwrap());
            let sys_name = bmp_initiate(&mut bmp_conn).await;
            bmp_peer_up(&mut bmp_conn).await;
            bmp_route_announce(&mut bmp_conn, test_prefix).await;

            // check to see if the internal "gate" counters are correct
            eprintln!("Checking counter metrics...");
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "bmp-tcp-in")), 3).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "filter")), 3).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "routers")), 1).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "global-rib")), 1).await;

            // check metrics to see if the number of routes etc is as expected
            eprintln!("Checking state metrics...");
            assert_metric_eq(manager.metrics(), "bmp_state_num_up_peers_total", Some(("router", &sys_name)), 1).await;
            assert_metric_eq(manager.metrics(), "bmp_tcp_in_num_bmp_messages_received_total", Some(("router", &local_addr)), 3).await;
            assert_metric_eq(manager.metrics(), "rib_unit_num_routes_announced_total", Some(("component", "global-rib")), 1).await;

            // query the route to make sure it was stored
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 1);

            // verify that there is no MQTT connection yet
            assert_metric_ne(manager.metrics(), "mqtt_target_connection_established_count_total", Some(("component", "local-broker")), 0).await;

            // save the last link report update time
            // wait for the manager to update the link report so that it will be included in the trace log output
            while manager.link_report_updated_at().duration_since(link_report_update_time).as_secs() < 1 {
                eprintln!("Waiting for link report to be updated");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let link_report_update_time = manager.link_report_updated_at();

            // reconfigure to use an MQTT target
            eprintln!("Reconfiguring...");
            let mut config_bytes = base_config_toml.as_bytes().to_vec();
            config_bytes.extend_from_slice(mqtt_target_toml.as_bytes());
            let config_file = ConfigFile::new(config_bytes, Source::default());
            let mut config = manager.load(config_file).unwrap();
            manager.spawn(&mut config);

            // verify that there is now an MQTT connection
            assert_metric_eq(manager.metrics(), "mqtt_target_connection_established_count_total", Some(("component", "local-broker")), 1).await;

            // push another route in and check the metrics
            eprintln!("Sending another BMP route announcement");
            bmp_route_announce(&mut bmp_conn, test_prefix2).await;

            eprintln!("Checking counter metrics...");
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "bmp-tcp-in")), 4).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "filter")), 4).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "routers")), 2).await;
            assert_metric_eq(manager.metrics(), "num_updates_total", Some(("component", "global-rib")), 2).await;

            eprintln!("Checking state metrics...");
            assert_metric_eq(manager.metrics(), "bmp_tcp_in_num_bmp_messages_received_total", Some(("router", &local_addr)), 4).await;
            assert_metric_eq(manager.metrics(), "rib_unit_num_routes_announced_total", Some(("component", "global-rib")), 2).await;
            assert_metric_eq(manager.metrics(), "mqtt_target_publish_count_total", Some(("component", "local-broker")), 1).await;

            // query the route to make sure it was stored
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix2).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 1);

            // wait for the manager to update the link report so that it will be included in the trace log output
            while manager.link_report_updated_at().duration_since(link_report_update_time).as_secs() < 1 {
                eprintln!("Waiting for link report to be updated");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // shut down rotonda, we're finished with it
            manager.terminate();

            // receive the message that was published to the MQTT broker topic
            eprintln!("Receiving MQTT message...");
            let msg = link_rx.recv().unwrap();
            assert!(matches!(msg, Some(Notification::Forward(_))));

            eprintln!("Checking MQTT message...");
            if let Some(Notification::Forward(forward)) = msg {
                let expected_json = serde_json::json!({
                    "prefix": "127.0.0.2/32",
                    "router": "my-sys-name",
                    "sourceAs": "AS12345",
                    "routingInformationBaseName": "Pre-Policy-RIB",
                    "asPath": [
                        "AS123",
                        "AS456",
                        "AS789"
                    ],
                    "neighbor": "10.0.0.1",
                    "communities": [
                        {
                            "rawFields": [
                                "0xFFFF029A"
                            ],
                            "type": "standard",
                            "parsed": {
                                "value": {
                                    "type": "well-known",
                                    "attribute": "BLACKHOLE"
                                }
                            }
                        },
                        {
                            "rawFields": [
                                "0x007B",
                                "0x002C"
                            ],
                            "type": "standard",
                            "parsed": {
                                "value": {
                                    "type": "private",
                                    "asn": "AS123",
                                    "tag": 44
                                }
                            }
                        }
                    ],
                    "description": "127.0.0.2/32 RTBH received from router my-sys-name on Pre-Policy-RIB"
                });
                let actual_json: serde_json::Value = serde_json::from_slice(&forward.publish.payload).unwrap();
                assert_json_eq(actual_json, expected_json);
            } else {
                unreachable!();
            }
        })
    }

    async fn query_prefix(test_prefix: Prefix) -> serde_json::Value {
        reqwest::get(&format!(
            "http://localhost:8080/prefixes/{}?details=communities",
            test_prefix
        ))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
    }

    async fn assert_metric_eq(
        metrics: metrics::Collection,
        metric_name: &str,
        label: Option<(&str, &str)>,
        wanted_v: i64,
    ) {
        let duration = Duration::from_secs(MAX_TIME_TO_WAIT_SECS);
        let result = Arc::new(AtomicMetricLookupResult::default());

        #[allow(clippy::expect_fun_call)]
        if tokio::time::timeout(
            duration,
            wait_for_metric(&metrics, metric_name, label, wanted_v, result.clone()),
        )
        .await
        .is_err()
        {
            if result.load(Ordering::SeqCst) != MetricLookupResult::Ok {
                eprintln!("Metric dump: {:#?}", get_metrics(&metrics));
                panic!(
                    "Metric '{}' with label '{:?}' != {} after {} seconds (reason: {})",
                    metric_name,
                    label,
                    wanted_v,
                    duration.as_secs(),
                    result.load(Ordering::SeqCst),
                );
            }
        }
    }

    async fn assert_metric_ne(
        metrics: metrics::Collection,
        metric_name: &str,
        label: Option<(&str, &str)>,
        wanted_v: i64,
    ) {
        let duration = Duration::from_secs(MAX_TIME_TO_WAIT_SECS);
        let result = Arc::new(AtomicMetricLookupResult::default());

        #[allow(clippy::expect_fun_call)]
        if tokio::time::timeout(
            duration,
            wait_for_metric(&metrics, metric_name, label, wanted_v, result.clone()),
        )
        .await
        .is_ok()
        {
            if result.load(Ordering::SeqCst) != MetricLookupResult::Ok {
                eprintln!("Metric dump: {:#?}", get_metrics(&metrics));
                panic!(
                    "Metric '{}' with label '{:?}' != {} after {} seconds (reason: {})",
                    metric_name,
                    label,
                    wanted_v,
                    duration.as_secs(),
                    result.load(Ordering::SeqCst),
                );
            }
        }
    }

    async fn wait_for_metric(
        metrics: &metrics::Collection,
        metric_name: &str,
        label: Option<(&str, &str)>,
        wanted_v: i64,
        result: Arc<AtomicMetricLookupResult>,
    ) {
        let full_metric_name = format!("{}{}", METRIC_PREFIX, metric_name);
        loop {
            if get_metrics(metrics).get(&full_metric_name, label, result.clone()) == Some(wanted_v)
            {
                result.store(MetricLookupResult::Ok, Ordering::SeqCst);
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    fn get_metrics(metrics: &metrics::Collection) -> prometheus_parse::Scrape {
        let prom_txt = metrics.assemble(OutputFormat::Prometheus);
        let lines: Vec<_> = prom_txt.lines().map(|s| Ok(s.to_owned())).collect();
        let metrics = prometheus_parse::Scrape::parse(lines.into_iter())
            .expect("Error while querying metrics");
        metrics
    }

    async fn wait_for_bmp_connect() -> TcpStream {
        loop {
            match bmp_connect().await {
                Ok(stream) => return stream,
                Err(err) => eprintln!("Error connecting to BMP server: {}, retrying..", err),
            }

            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn bmp_connect() -> Result<TcpStream, String> {
        let duration = Duration::from_secs(MAX_TIME_TO_WAIT_SECS);
        tokio::time::timeout(duration, TcpStream::connect("localhost:11019"))
            .await
            .map_err(|elapsed_err| elapsed_err.to_string())?
            .map_err(|connect_err| connect_err.to_string())
    }

    async fn bmp_initiate(stream: &mut TcpStream) -> String {
        let sys_name = "my-sys-name".to_string();
        stream
            .write_all(&mk_initiation_msg(&sys_name, "my-sys-desc"))
            .await
            .expect("Error while sending BMP 'initiate' message");
        sys_name
    }

    async fn bmp_peer_up(stream: &mut TcpStream) {
        let local_address: IpAddr = IpAddr::from_str("127.0.0.1").unwrap();
        let local_port: u16 = 80;
        let remote_port: u16 = 81;
        let sent_open_asn: u16 = 888;
        let received_open_asn: u16 = 999;
        let sent_bgp_identifier: u32 = 0;
        let received_bgp_id: u32 = 0;

        let per_peer_header = mk_per_peer_header(received_bgp_id);

        stream
            .write_all(&mk_peer_up_notification_msg(
                &per_peer_header,
                local_address,
                local_port,
                remote_port,
                sent_open_asn,
                received_open_asn,
                sent_bgp_identifier,
                received_bgp_id,
                vec![],
                true,
            ))
            .await
            .expect("Error while sending BMP 'peer up' message");
    }

    async fn bmp_route_announce(stream: &mut TcpStream, prefix: Prefix) {
        let per_peer_header = mk_per_peer_header(0);
        let withdrawals = Prefixes::default();
        let announcements = Announcements::from_str(&format!(
            "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 {}",
            prefix
        ))
        .unwrap();

        let msg_buf = mk_route_monitoring_msg(&per_peer_header, &withdrawals, &announcements, &[]);
        stream
            .write_all(&msg_buf)
            .await
            .expect("Error while sending 'route monitoring' message");
    }

    fn mk_per_peer_header(received_bgp_id: u32) -> PerPeerHeader {
        let peer_type: MyPeerType = PeerType::GlobalInstance.into();
        let peer_flags: u8 = 0;
        let peer_address: IpAddr = IpAddr::from_str("10.0.0.1").unwrap();
        let peer_as: Asn = Asn::from_u32(12345);
        let peer_bgp_id = received_bgp_id.to_be_bytes();
        let peer_distinguisher: [u8; 8] = [0; 8];

        PerPeerHeader {
            peer_type,
            peer_flags,
            peer_distinguisher,
            peer_address,
            peer_as,
            peer_bgp_id,
        }
    }

    #[atomic_enum]
    #[derive(Default, PartialEq, Eq)]
    enum MetricLookupResult {
        #[default]
        NotQueried,
        NameNotFound,
        LabelNotFound,
        ValueNotMatched,
        Ok,
    }

    impl Default for AtomicMetricLookupResult {
        fn default() -> Self {
            Self::new(Default::default())
        }
    }

    impl std::fmt::Display for MetricLookupResult {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                MetricLookupResult::NotQueried => write!(f, "NotQueried"),
                MetricLookupResult::NameNotFound => write!(f, "NameNotFound"),
                MetricLookupResult::LabelNotFound => write!(f, "LabelNotFound"),
                MetricLookupResult::ValueNotMatched => write!(f, "ValueNotMatched"),
                MetricLookupResult::Ok => write!(f, "Ok"),
            }
        }
    }

    trait MetricGetter {
        fn get(
            &self,
            metric_name: &str,
            label: Option<(&str, &str)>,
            result: Arc<AtomicMetricLookupResult>,
        ) -> Option<i64>;
    }

    impl MetricGetter for prometheus_parse::Scrape {
        fn get(
            &self,
            metric_name: &str,
            label: Option<(&str, &str)>,
            result: Arc<AtomicMetricLookupResult>,
        ) -> Option<i64> {
            fn is_wanted(
                sample: &prometheus_parse::Sample,
                metric_name: &str,
                label: &Option<(&str, &str)>,
                result: Arc<AtomicMetricLookupResult>,
            ) -> bool {
                if sample.metric == metric_name {
                    if let Some((label_name, label_value)) = label {
                        if let Some(v) = sample.labels.get(label_name) {
                            if v == *label_value {
                                result.store(MetricLookupResult::Ok, Ordering::SeqCst);
                                return true;
                            } else {
                                result.store(MetricLookupResult::ValueNotMatched, Ordering::SeqCst);
                            }
                        } else {
                            result.store(MetricLookupResult::LabelNotFound, Ordering::SeqCst);
                        }
                    } else {
                        result.store(MetricLookupResult::Ok, Ordering::SeqCst);
                        return true;
                    }
                } else {
                    result.store(MetricLookupResult::NameNotFound, Ordering::SeqCst);
                }

                false
            }

            fn sample_as_i64(sample: &prometheus_parse::Sample) -> i64 {
                match sample.value {
                    Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => v as i64,
                    _ => 0,
                }
            }

            self.samples
                .iter()
                .find(|sample| is_wanted(sample, metric_name, &label, result.clone()))
                .map(sample_as_i64)
        }
    }

    pub fn assert_json_eq(actual_json: serde_json::Value, expected_json: serde_json::Value) {
        use assert_json_diff::{assert_json_matches_no_panic, CompareMode};

        let config = assert_json_diff::Config::new(CompareMode::Strict);
        if let Err(err) = assert_json_matches_no_panic(&actual_json, &expected_json, config) {
            eprintln!(
                "Actual JSON: {}",
                serde_json::to_string_pretty(&actual_json).unwrap()
            );
            eprintln!(
                "Expected JSON: {}",
                serde_json::to_string_pretty(&expected_json).unwrap()
            );
            panic!("JSON doesn't match expectations: {}", err);
        }
    }
}
