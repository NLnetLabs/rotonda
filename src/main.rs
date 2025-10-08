use clap::{crate_authors, crate_version, error::ErrorKind, Command};
use futures::{
    future::{select, Either},
    pin_mut,
};
use log::{debug, error, info, warn};
use rotonda::log::ExitError;
use rotonda::manager::Manager;
use rotonda::{
    config::{Config, ConfigFile, Source},
    log::Terminate,
};
use std::env::current_dir;
use std::process::exit;
use tokio::{
    runtime::{self, Runtime},
    signal::{self, unix::signal, unix::SignalKind},
};

fn run_with_cmdline_args() -> Result<(), Terminate> {
    Config::init()?;

    let app = Command::new("rotonda")
        .version(crate_version!())
        .author(crate_authors!())
        .next_line_help(true);

    let config_args = Config::config_args(app);
    let matches = config_args.try_get_matches().map_err(|err| {
        let _ = err.print();
        match err.kind() {
            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                Terminate::normal()
            }
            _ => Terminate::other(2),
        }
    })?;

    let cur_dir = current_dir().map_err(|err| {
        error!("Fatal: cannot get current directory ({}). Aborting.", err);
        ExitError
    })?;

    // TODO: Drop privileges, get listen fd from systemd, create PID file,
    // fork, detach from the parent process, change user and group, etc. In a
    // word: daemonize. Prior art:
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/operation.rs#L509
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/process.rs#L241
    //   - https://github.com/NLnetLabs/routinator/blob/main/src/process.rs#L363

    let mut manager = Manager::new();
    let (config_source, config) =
        Config::from_arg_matches(&matches, &cur_dir, &mut manager)?;
    debug!("application working directory {:?}", cur_dir);
    debug!("configuration source file {:?}", config_source);
    debug!("roto script {:?}", &config.roto_script);
    let roto_script = config.roto_script.clone();
    let runtime = run_with_config(&mut manager, config)?;
    runtime.block_on(handle_signals(config_source, roto_script, manager))?;
    Ok(())
}

async fn handle_signals(
    config_source: Source,
    roto_script: Option<std::path::PathBuf>,
    mut manager: Manager,
) -> Result<(), ExitError> {
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
                error!(
                    "Fatal: listening for SIGHUP signals failed. Aborting."
                );
                manager.terminate();
                return Err(ExitError);
            }
            Either::Left((Some(_), _)) => {
                // HUP signal received
                match config_source.path() {
                    Some(config_path) => {
                        info!(
                        "SIGHUP signal received, re-reading configuration file '{}'",
                        config_path.display()
                        );
                        match ConfigFile::load(&config_path) {
                            Ok(config_file) => {
                                match Config::from_config_file(
                                    config_file,
                                    &mut manager,
                                ) {
                                    Err(_) => {
                                        error!(
                                            "Failed to re-read config file '{}'",
                                            config_path.display()
                                        );
                                    }
                                    Ok((_source, mut config)) => {
                                        // XXX spawn() already restarts the http servers, but it
                                        // does not apply the config (so new/altered
                                        // http_ng_interfaces are not respected)
                                        // calling restart_http_ng_with_config then _again_
                                        // restarts the http tasks.
                                        // maybe we should go for a 'reload_config' and then let
                                        // spawn do the rest?
                                        manager.reload_http_ng_config(&config);
                                        manager.spawn(&mut config);
                                        info!(
                                            "Configuration changes applied"
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                error!(
                                    "Failed to re-read config file '{}': {}",
                                    config_path.display(),
                                    err
                                );
                            }
                        }
                    }
                    None => {
                        if let Some(ref rsp) = roto_script {
                            info!("SIGHUP signal received, re-loading roto scripts \
                            from location {:?}", rsp);
                        } else {
                            error!(
                                "No location for roto scripts. Not reloading"
                            );
                            continue;
                        }
                        match manager.compile_roto_script(&roto_script) {
                            Ok(_) => {
                                info!("Done reloading roto scripts");
                            }
                            Err(e) => {
                                error!("Cannot reload roto scripts: {e}. Not reloading");
                            }
                        };
                    }
                }
            }
            Either::Right((Err(err), _)) => {
                error!(
                    "Fatal: listening for CTRL-C (SIGINT) signals failed \
                    ({}). Aborting.",
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

fn run_with_config(
    manager: &mut Manager,
    mut config: Config,
) -> Result<Runtime, ExitError> {
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Make the runtime the default for Tokio related functions that assume a
    // default runtime.
    let _guard = runtime.enter();

    manager.http_ng_api_arc().lock().unwrap().set_interfaces(config.http_ng_listen.clone().into_iter().flatten());
    manager.spawn(&mut config);
    manager.http_ng_api_arc().lock().unwrap().start();

    Ok(runtime)
}

fn main() {
    let exit_code = match run_with_cmdline_args() {
        Ok(_) => Terminate::normal(),
        Err(terminate) => terminate,
    }
    .exit_code();

    info!("Exiting with exit code {exit_code}");

    exit(exit_code);
}

// --- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        net::IpAddr,
        str::FromStr,
        sync::{atomic::Ordering::SeqCst, Arc},
        time::{Duration, Instant},
    };

    use atomic_enum::atomic_enum;
    use inetnum::{addr::Prefix, asn::Asn};
    use prometheus_parse::Value;
    use rotonda::{
        bgp::encode::{
            mk_initiation_msg, mk_peer_down_notification_msg,
            mk_peer_up_notification_msg, mk_route_monitoring_msg,
            Announcements, MyPeerType, PerPeerHeader, Prefixes,
        },
        config::Source,
        metrics::{self, OutputFormat},
        tests::util::assert_json_eq,
    };
    use routecore::{bmp::message::PeerType};
    use rumqttd::{local::LinkRx, Broker, Notification};
    use serde_json::Number;
    use tokio::{io::AsyncWriteExt, net::TcpStream, time::sleep};

    use super::*;

    const MAX_TIME_TO_WAIT_SECS: u64 = 3;
    const METRIC_PREFIX: &str = "rotonda_";

    // This test runs Rotonda as if it were run from the command line using a
    // config file. It:
    //   - Configures Rotonda to expect BMP input and to produce MQTT output.
    //   - Simulates a monitored router by sending BMP messages over a TCP
    //     connection to Rotonda.
    //   - Runs a real MQTT broker (using the rumqttd Rust library) to which
    //     Rotonda will publish messages, and from which the test can retrieve
    //     published messages.
    //   - Inspects the state of Rotonda using its Prometheus metrics
    //     endpoint, its RIB HTTP API and by looking at the MQTT messages
    //     received at the MQTT broker.
    #[test]
    #[ignore = "needs more adaptation after refactoring"]
    fn integration_test() {
        //    ___ ___ _____ _   _ ___
        //   / __| __|_   _| | | | _ \
        //   \__ \ _|  | | | |_| |  _/
        //   |___/___| |_|  \___/|_|
        //

        // Uncomment this to investigate roto script parsing and execution
        // issues.
        // std::env::set_var("ROTONDA_ROTO_LOG", "1");

        let test_prefix = Prefix::from_str("127.0.0.1/32").unwrap();
        let test_prefix2 = Prefix::from_str("127.0.0.2/32").unwrap();

        // ===================================================================
        // Initialize the logging system.
        // ===================================================================
        Config::init().unwrap();

        // ===================================================================
        // Define a base Rotonda config. It defines a BMP input and a RIB with
        // two vRIBs.
        // ===================================================================
        let base_config_toml = r#"
        roto_scripts_path = "test-data/"

        http_listen = "127.0.0.1:8080"
        log_target = "stderr"
        log_level = "trace"

        [units.bmp-tcp-in]
        type = "bmp-tcp-in"
        listen = "127.0.0.1:11019"
        filter_name = "bmp-in-filter"

        [units.global-rib]
        type = "rib"
        sources = ["bmp-tcp-in"]
        filter_names = ["my-module", "my-module", "my-module"]

        [targets.dummy-null]
        type = "null-out"
        source = "bmp-tcp-in"
        "#;

        // ===================================================================
        // Define a config fragment for a null target.
        // We'll use this first.
        // ===================================================================
        let null_target_toml = r#"
        [targets.null]
        type = "null-out"
        sources = ["global-rib"]
        "#;

        // ===================================================================
        // Define a config fragment for an MQTT target.
        // We'll reconfigure Rotonda to use this instead of the null target.
        // ===================================================================
        let mqtt_target_toml = r#"
        [targets.local-broker]
        type = "mqtt-out"
        qos = 2
        destination = "127.0.0.1"
        client_id = "rotonda"
        communities = ["BLACKHOLE"]
        sources = ["global-rib"]
        connect_retry_secs = 1
        "#;

        //    ___ _____ _   ___ _____   ___  ___ _____ ___  _  _ ___   _
        //   / __|_   _/_\ | _ \_   _| | _ \/ _ \_   _/ _ \| \| |   \ /_\
        //   \__ \ | |/ _ \|   / | |   |   / (_) || || (_) | .` | |) / _ \
        //   |___/ |_/_/ \_\_|_\ |_|   |_|_\\___/ |_| \___/|_|\_|___/_/ \_\
        //

        // ===================================================================
        // Run Rotonda using the base + null target config. This will also
        // start a Tokio runtime which we then also use below to run the
        // actual integration test steps.
        // ===================================================================
        let mut config_bytes = base_config_toml.as_bytes().to_vec();
        config_bytes.extend_from_slice(null_target_toml.as_bytes());
        eprintln!(
            "The following Rotonda config file will be used:\n{}",
            String::from_utf8_lossy(&config_bytes)
        );
        let config_file = ConfigFile::new(
            config_bytes,
            Source::default(),
            //Default::default(),
        );

        let mut manager = Manager::new();
        let (_conf_source, config) =
            Config::from_config_file(config_file.unwrap(), &mut manager)
                .expect("The supplied config is invalid");
        let runtime = run_with_config(&mut manager, config)
            .expect("The application failed to start");

        // ===================================================================
        // Define a config file for the RUMQTTD MQTT broker. It looks like a
        // lot but basically just says listen on 127.0.0.1 on port 1883.
        // ===================================================================
        let mqttd_config = r#"
        id = 0

        [router]
        instant_ack = true
        max_segment_size = 10240
        max_segment_count = 10
        max_read_len = 10240
        max_connections = 10001
        max_outgoing_packet_count = 10

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

        //    ___ _____ _   ___ _____   ___ ___  ___  _  _____ ___
        //   / __|_   _/_\ | _ \_   _| | _ ) _ \/ _ \| |/ / __| _ \
        //   \__ \ | |/ _ \|   / | |   | _ \   / (_) | ' <| _||   /
        //   |___/ |_/_/ \_\_|_\ |_|   |___/_|_\\___/|_|\_\___|_|_\
        //

        // ===================================================================
        // Run the MQTT broker using the above config in its own thread.
        // ===================================================================
        let config: rumqttd::Config =
            toml::de::from_str(mqttd_config).unwrap();
        let mut broker = Broker::new(config);
        let (mut link_tx, mut link_rx) = broker.link("localclient").unwrap();

        std::thread::spawn(move || {
            broker.start().unwrap();
        });

        //    ___ _   _ _  _   _____ ___ ___ _____ ___ _
        //   | _ \ | | | \| | |_   _| __/ __|_   _/ __| |
        //   |   / |_| | .` |   | | | _|\__ \ | | \__ \_|
        //   |_|_\\___/|_|\_|   |_| |___|___/ |_| |___(_)
        //

        runtime.block_on(async {
            // Save the time the component graph report was last updated so
            // that we can easily tell if it has changed.
            let link_report_update_time = manager.link_report_updated_at();

            // Subscribe our test program to the MQTT broker _before_ Rotonda
            // publishes any messages to it.
            link_tx.subscribe("rotonda/#").unwrap();
            assert!(matches!(
                link_rx.recv(),
                Ok(Some(Notification::DeviceAck(_)))
            ));

            //      _   _  _ _  _  ___  _   _ _  _  ___ ___   ___  ___  _   _ _____ ___   _ 
            //     /_\ | \| | \| |/ _ \| | | | \| |/ __| __| | _ \/ _ \| | | |_   _| __| / |
            //    / _ \| .` | .` | (_) | |_| | .` | (__| _|  |   / (_) | |_| | | | | _|  | |
            //   /_/ \_\_|\_|_|\_|\___/ \___/|_|\_|\___|___| |_|_\\___/ \___/  |_| |___| |_|
            //                                                                              

            // Emulate a BMP capable router connected to Rotonda
            eprintln!("Subscribed to MQTT broker, sending BMP messages...");
            let mut bmp_conn = wait_for_bmp_connect().await;
            let sys_name = bmp_initiate(&mut bmp_conn).await;
            bmp_peer_up(&mut bmp_conn, 1).await;
            bmp_route_announce(&mut bmp_conn, 1, test_prefix).await;

            //     ___ _  _ ___ ___ _  __  ___ _____ _ _____ ___ 
            //    / __| || | __/ __| |/ / / __|_   _/_\_   _| __|
            //   | (__| __ | _| (__| ' <  \__ \ | |/ _ \| | | _| 
            //    \___|_||_|___\___|_|\_\ |___/ |_/_/ \_\_| |___|
            //                                                   

            // Check to see if the internal "gate" Prometheus metrics are
            // correct. We expect a single message to be pushed out of the
            // bmp-tcp-in units gate, a route announcement message.
            eprintln!("Checking counter metrics...");
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "bmp-tcp-in")],
                1, // 1 route
            )
            .await;
            // And likewise that same message should pass into and out of
            // the RIB unit.
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                1,
            )
            .await;

            // Check metrics to see if the number of routes etc is as
            // expected.
            eprintln!("Checking state metrics...");
            assert_metric_eq(
                manager.metrics(),
                "bmp_state_num_up_peers_total",
                &[("router", &sys_name)],
                1,
            )
            .await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "unknown")], [0, 0, 0, 0, 1, 0, 0]).await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "my-sys-name")], [1, 0, 0, 1, 0, 0, 0]).await;

            assert_metric_eq(
                manager.metrics(),
                "rib_unit_num_routes_announced_total",
                &[("component","global-rib")],
                1,
            )
            .await;

            // Query the route via the RIB HTTP API to make sure that it was stored.
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 1);
            assert_eq!(json_routes[0]["route"]["prefix"].as_str(), Some("127.0.0.1/32"));
            assert_eq!(json_routes[0]["route"]["peer_ip"].as_str(), Some("10.0.0.1"));
            assert_eq!(json_routes[0]["route"]["peer_asn"].as_number(), Some(&Number::from_str("12346").unwrap()));
            assert_eq!(json_routes[0]["route"]["router_id"].as_str(), Some("my-sys-name"));
            assert_eq!(json_routes[0]["status"].as_str(), Some("InConvergence"));

            // Expect no change in the number of updates output by the RIB gate.
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                1,
            )
            .await;

            // And that we can also query it via a VRIB too.
            let res = query_vrib_prefix(test_prefix).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 1);

            // Expect the number of updates output by the RIB gate to have
            // INCREASED BY ONE. This is because the vRIB HTTP API query is
            // handled by it querying the upstream pRIB and the result of that
            // query flows out of the pRIB gate down to the vRIB.
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                2, // 1 route + 1 vRIB query
            )
            .await;

            // Verify that there is no MQTT connection yet.
            assert_metric_ne(
                manager.metrics(),
                "mqtt_target_connection_established_state",
                &[("component", "local-broker")],
                0,
            )
            .await;

            // Save the last link report update time
            // Wait for the manager to update the link report so that it will be included in the trace log output
            while manager
                .link_report_updated_at()
                .duration_since(link_report_update_time)
                .as_secs()
                < 1
            {
                eprintln!("Waiting for link report to be updated");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let link_report_update_time = manager.link_report_updated_at();

            //    ___ ___ ___ ___  _  _ ___ ___ ___ _   _ ___ ___ 
            //   | _ \ __/ __/ _ \| \| | __|_ _/ __| | | | _ \ __|
            //   |   / _| (_| (_) | .` | _| | | (_ | |_| |   / _| 
            //   |_|_\___\___\___/|_|\_|_| |___\___|\___/|_|_\___|
            //                                                    

            // Reconfigure to use an MQTT target
            let mut config_bytes = base_config_toml.as_bytes().to_vec();
            config_bytes.extend_from_slice(mqtt_target_toml.as_bytes());
            eprintln!(
                "Sending Rotonda a SIGHUP signal to reconfigure itself using the following config:\n{}",
                String::from_utf8_lossy(&config_bytes)
            );
            let config_file = ConfigFile::new(
                config_bytes,
                Source::default(),
                //Default::default(),
            );
            let (_source, mut config) =
                Config::from_config_file(config_file.unwrap(), &mut manager).unwrap();
            manager.spawn(&mut config);

            // Verify that there is now an MQTT connection
            assert_metric_eq(
                manager.metrics(),
                "mqtt_target_connection_established_state",
                &[("component", "local-broker")],
                1,
            )
            .await;

            //      _   _  _ _  _  ___  _   _ _  _  ___ ___   ___  ___  _   _ _____ ___   ___ 
            //     /_\ | \| | \| |/ _ \| | | | \| |/ __| __| | _ \/ _ \| | | |_   _| __| |_  )
            //    / _ \| .` | .` | (_) | |_| | .` | (__| _|  |   / (_) | |_| | | | | _|   / / 
            //   /_/ \_\_|\_|_|\_|\___/ \___/|_|\_|\___|___| |_|_\\___/ \___/  |_| |___| /___|
            //                                                                                

            // Push another route in and check the metrics
            eprintln!("Sending another BMP route announcement");
            bmp_route_announce(&mut bmp_conn, 1, test_prefix2).await;

            //     ___ _  _ ___ ___ _  __  ___ _____ _ _____ ___ 
            //    / __| || | __/ __| |/ / / __|_   _/_\_   _| __|
            //   | (__| __ | _| (__| ' <  \__ \ | |/ _ \| | | _| 
            //    \___|_||_|___\___|_|\_\ |___/ |_/_/ \_\_| |___|
            //                                                   

            eprintln!("Checking counter metrics...");
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "bmp-tcp-in")],
                2, // 1 route + 1 route
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                3, // 1 route + 1 vRIB query + 1 route
            )
            .await;

            eprintln!("Checking state metrics...");
            // Expect one more route monitoring message
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "unknown")], [0, 0, 0, 0, 1, 0, 0]).await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "my-sys-name")], [2, 0, 0, 1, 0, 0, 0]).await;
            assert_metric_eq(
                manager.metrics(),
                "rib_unit_num_routes_announced_total",
                &[("component", "global-rib")],
                2,
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "mqtt_target_publish_count_total",
                &[("component", "local-broker")],
                3, // +3: 1 for each invocation of the "my-module" filter in the pRIB and 2 vRIBs
            )
            .await;

            // Query the route to make sure it was stored
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix2).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 1);
            assert_eq!(json_routes[0]["route"]["prefix"].as_str(), Some("127.0.0.2/32"));
            assert_eq!(json_routes[0]["route"]["peer_ip"].as_str(), Some("10.0.0.1"));
            assert_eq!(json_routes[0]["route"]["peer_asn"].as_number(), Some(&Number::from_str("12346").unwrap()));
            assert_eq!(json_routes[0]["route"]["router_id"].as_str(), Some("my-sys-name"));
            assert_eq!(json_routes[0]["status"].as_str(), Some("InConvergence"));

            let res = query_vrib_prefix(test_prefix2).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 1);

            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                4, // 1 route + 1 vRIB query + 1 route + vRIB query
            )
            .await;

            //     ___  _   _ ___ _____   __  ___ ___  ___  _  _____ ___ 
            //    / _ \| | | | __| _ \ \ / / | _ ) _ \/ _ \| |/ / __| _ \
            //   | (_) | |_| | _||   /\ V /  | _ \   / (_) | ' <| _||   /
            //    \__\_\\___/|___|_|_\ |_|   |___/_|_\\___/|_|\_\___|_|_\
            //                                                           

            // Three MQTT messages are published, one for every time the
            // "my-module" filter was executed due to this line in the Rotonda
            // config file defined above:
            //
            //     filter_names = ["my-module", "my-module", "my-module"]
            //
            // This line created a physical RIB and two eastward virtual RIBs,
            // each configured to use the same Roto script. The physical RIB
            // receives the route from our test BMP client and on success
            // passes the route down the pipeline to the first vRIB, which
            // does the same and passes it to the next vRIB.
            //
            // At each stage the roto script also produces an output message
            // which is injected into the pipeline, resulting in the original
            // BGP UPDATE message and 3 additional output messages flowing out
            // of the final vRIB to the MQTT target, the former being ignored
            // but the latter three being published to the MQTT broker.
            //
            // Below we handle the three MQTT messages published in response
            // to the output messages generated by the RIB units.
            query_broker(&mut link_rx, 3);

            //    ___ ___ ___ ___   _   _ ___     _       _   _  _ _  _  ___  _   _ _  _  ___ ___ 
            //   | _ \ __| __| _ \ | | | | _ \  _| |_    /_\ | \| | \| |/ _ \| | | | \| |/ __| __|
            //   |  _/ _|| _||   / | |_| |  _/ |_   _|  / _ \| .` | .` | (_) | |_| | .` | (__| _| 
            //   |_| |___|___|_|_\  \___/|_|     |_|   /_/ \_\_|\_|_|\_|\___/ \___/|_|\_|\___|___|
            //                                                                                    

            bmp_peer_up(&mut bmp_conn, 2).await;
            bmp_route_announce(&mut bmp_conn, 2, test_prefix).await;
            bmp_route_announce(&mut bmp_conn, 2, test_prefix2).await;

            //     ___ _  _ ___ ___ _  __  ___ _____ _ _____ ___ 
            //    / __| || | __/ __| |/ / / __|_   _/_\_   _| __|
            //   | (__| __ | _| (__| ' <  \__ \ | |/ _ \| | | _| 
            //    \___|_||_|___\___|_|\_\ |___/ |_/_/ \_\_| |___|
            //                                                   

            eprintln!("Checking counter metrics...");
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "bmp-tcp-in")],
                4, // 1 route + 1 route + 2 routes
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                6, // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes
            )
            .await;

            eprintln!("Checking state metrics...");
            // Expect two more route monitoring messages and a peer up message
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "unknown")], [0, 0, 0, 0, 1, 0, 0]).await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "my-sys-name")], [4, 0, 0, 2, 0, 0, 0]).await;
            assert_metric_eq(
                manager.metrics(),
                "rib_unit_num_routes_announced_total",
                &[("component", "global-rib")],
                4, // Two more than above
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "mqtt_target_publish_count_total",
                &[("component", "local-broker")],
                9, // +3: 1 for each invocation of the "my-module" filter in the pRIB and 2 vRIBs
            )
            .await;

            // Query the routes to make sure they are stored
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 2);

            let mut wanted_peers = vec![
                ("10.0.0.1", Number::from_str("12346").unwrap()),
                ("10.0.0.2", Number::from_str("12347").unwrap())
            ];
            for json_route in json_routes {
                assert_eq!(json_route["route"]["prefix"].as_str(), Some("127.0.0.1/32"));
                assert_eq!(json_route["route"]["router_id"].as_str(), Some("my-sys-name"));
                assert_eq!(json_route["status"].as_str(), Some("InConvergence"));
                let found_peer = (
                    json_route["route"]["peer_ip"].as_str().unwrap(),
                    json_route["route"]["peer_asn"].as_number().unwrap()
                );
                if let Some(idx) = wanted_peers.iter().position(|(ip, asn)| {
                    ip == &found_peer.0 && asn == found_peer.1
                }) {
                    wanted_peers.remove(idx);
                }
            }
            assert!(wanted_peers.is_empty());

            let res = query_vrib_prefix(test_prefix).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 2);

            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix2).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 2);

            let mut wanted_peers = vec![
                ("10.0.0.1", Number::from_str("12346").unwrap()),
                ("10.0.0.2", Number::from_str("12347").unwrap())
            ];
            for json_route in json_routes {
                assert_eq!(json_route["route"]["prefix"].as_str(), Some("127.0.0.2/32"));
                assert_eq!(json_route["route"]["router_id"].as_str(), Some("my-sys-name"));
                assert_eq!(json_route["status"].as_str(), Some("InConvergence"));
                let found_peer = (
                    json_route["route"]["peer_ip"].as_str().unwrap(),
                    json_route["route"]["peer_asn"].as_number().unwrap()
                );
                if let Some(idx) = wanted_peers.iter().position(|(ip, asn)| {
                    ip == &found_peer.0 && asn == found_peer.1
                }) {
                    wanted_peers.remove(idx);
                }
            }
            assert!(wanted_peers.is_empty());

            let res = query_vrib_prefix(test_prefix2).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 2);

            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                8, // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes + 2 vRIB queries
            )
            .await;

            //     ___  _   _ ___ _____   __  ___ ___  ___  _  _____ ___ 
            //    / _ \| | | | __| _ \ \ / / | _ ) _ \/ _ \| |/ / __| _ \
            //   | (_) | |_| | _||   /\ V /  | _ \   / (_) | ' <| _||   /
            //    \__\_\\___/|___|_|_\ |_|   |___/_|_\\___/|_|\_\___|_|_\
            //                                                           

            query_broker(&mut link_rx, 6);

            // __      _____ _____ _  _ ___  ___    ___      __  ___  ___  _   _ _____ ___   _ 
            // \ \    / /_ _|_   _| || |   \| _ \  /_\ \    / / | _ \/ _ \| | | |_   _| __| / |
            //  \ \/\/ / | |  | | | __ | |) |   / / _ \ \/\/ /  |   / (_) | |_| | | | | _|  | |
            //   \_/\_/ |___| |_| |_||_|___/|_|_\/_/ \_\_/\_/   |_|_\\___/ \___/  |_| |___| |_|
            //                                                                                 

            eprintln!("Sending a BMP withdrawal of the first route announcement");
            bmp_route_withdraw(&mut bmp_conn, 1, test_prefix).await;

            //     ___ _  _ ___ ___ _  __  ___ _____ _ _____ ___ 
            //    / __| || | __/ __| |/ / / __|_   _/_\_   _| __|
            //   | (__| __ | _| (__| ' <  \__ \ | |/ _ \| | | _| 
            //    \___|_||_|___\___|_|\_\ |___/ |_/_/ \_\_| |___|
            //                                                   

            eprintln!("Checking counter metrics...");
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "bmp-tcp-in")],
                5, // 1 route + 1 route + 2 routes + 1 withdrawal
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                9, // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes + 2 vRIB queries + 1 withdrawal
            )
            .await;

            eprintln!("Checking state metrics...");
            // Expect one more route monitoring message
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "unknown")], [0, 0, 0, 0, 1, 0, 0]).await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "my-sys-name")], [5, 0, 0, 2, 0, 0, 0]).await;
            assert_metric_eq(
                manager.metrics(),
                "rib_unit_num_routes_announced_total",
                &[("component", "global-rib")],
                3, // One less than above
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "mqtt_target_publish_count_total",
                &[("component", "local-broker")],
                12, // +3: 1 for each invocation of the "my-module" filter in the pRIB and 2 vRIBs
            )
            .await;

            // Query the route to make sure it is still stored but is now withdrawn
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 2);

            let mut wanted_peers = vec![
                ("10.0.0.1", Number::from_str("12346").unwrap(), "Withdrawn"),
                ("10.0.0.2", Number::from_str("12347").unwrap(), "InConvergence")
            ];
            for json_route in json_routes {
                assert_eq!(json_route["route"]["prefix"].as_str(), Some("127.0.0.1/32"));
                assert_eq!(json_route["route"]["router_id"].as_str(), Some("my-sys-name"));
                let found_peer = (
                    json_route["route"]["peer_ip"].as_str().unwrap(),
                    json_route["route"]["peer_asn"].as_number().unwrap(),
                    json_route["status"].as_str().unwrap(),
                );
                if let Some(idx) = wanted_peers.iter().position(|(ip, asn, status)| {
                    ip == &found_peer.0 && asn == found_peer.1 && status == &found_peer.2
                }) {
                    wanted_peers.remove(idx);
                }
            }
            assert!(wanted_peers.is_empty());

            let res = query_vrib_prefix(test_prefix).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 2);

            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                10, // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes + 2 vRIB queries + 1 withdrawal + 1 vRIB query
            )
            .await;

            //     ___  _   _ ___ _____   __  ___ ___  ___  _  _____ ___ 
            //    / _ \| | | | __| _ \ \ / / | _ ) _ \/ _ \| |/ / __| _ \
            //   | (_) | |_| | _||   /\ V /  | _ \   / (_) | ' <| _||   /
            //    \__\_\\___/|___|_|_\ |_|   |___/_|_\\___/|_|\_\___|_|_\
            //                                                           

            query_broker(&mut link_rx, 3);

            //    ___ ___ ___ ___   ___   _____      ___  _ 
            //   | _ \ __| __| _ \ |   \ / _ \ \    / / \| |
            //   |  _/ _|| _||   / | |) | (_) \ \/\/ /| .` |
            //   |_| |___|___|_|_\ |___/ \___/ \_/\_/ |_|\_|
            //                                              

            eprintln!("Sending a BMP peer down which should cause the second route to also be withdrawn");
            bmp_peer_down(&mut bmp_conn, 1).await;

            //     ___ _  _ ___ ___ _  __  ___ _____ _ _____ ___ 
            //    / __| || | __/ __| |/ / / __|_   _/_\_   _| __|
            //   | (__| __ | _| (__| ' <  \__ \ | |/ _ \| | | _| 
            //    \___|_||_|___\___|_|\_\ |___/ |_/_/ \_\_| |___|
            //                                                   

            eprintln!("Checking counter metrics...");
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "bmp-tcp-in")],
                5, // 1 route + 1 route + 2 routes + 1 withdrawal
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                11, // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes + 2 vRIB queries + 1 withdrawal + 1 vRIB query + 1 withdrawal
            )
            .await;

            eprintln!("Checking state metrics...");
            // Expect a peer down message
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "unknown")], [0, 0, 0, 0, 1, 0, 0]).await;
            assert_bmp_messages_received(manager.metrics(), &[("component","bmp-tcp-in"), ("router", "my-sys-name")], [5, 0, 1, 2, 0, 0, 0]).await;
            assert_metric_eq(
                manager.metrics(),
                "rib_unit_num_routes_announced_total",
                &[("component", "global-rib")],
                2, // One less than above
            )
            .await;
            assert_metric_eq(
                manager.metrics(),
                "mqtt_target_publish_count_total",
                &[("component", "local-broker")],
                15, // +3: 1 for each invocation of the "my-module" filter in the pRIB and 2 vRIBs
            )
            .await;

            // Query the route to make sure it is still stored but is now withdrawn
            eprintln!("Querying prefix store...");
            let res = query_prefix(test_prefix2).await;
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
            let json_routes = res.get("data").unwrap().as_array().unwrap();
            assert_eq!(json_routes.len(), 2);

            let mut wanted_peers = vec![
                ("10.0.0.1", Number::from_str("12346").unwrap(), "Withdrawn"),
                ("10.0.0.2", Number::from_str("12347").unwrap(), "InConvergence")
            ];
            for json_route in json_routes {
                assert_eq!(json_route["route"]["prefix"].as_str(), Some("127.0.0.2/32"));
                assert_eq!(json_route["route"]["router_id"].as_str(), Some("my-sys-name"));
                let found_peer = (
                    json_route["route"]["peer_ip"].as_str().unwrap(),
                    json_route["route"]["peer_asn"].as_number().unwrap(),
                    json_route["status"].as_str().unwrap(),
                );
                if let Some(idx) = wanted_peers.iter().position(|(ip, asn, status)| {
                    ip == &found_peer.0 && asn == found_peer.1 && status == &found_peer.2
                }) {
                    wanted_peers.remove(idx);
                }
            }
            assert!(wanted_peers.is_empty());

            let res = query_vrib_prefix(test_prefix2).await;
            assert_eq!(res.get("data").unwrap().as_array().unwrap().len(), 2);

            assert_metric_eq(
                manager.metrics(),
                "num_updates_total",
                &[("component", "global-rib")],
                // 1 route + 1 vRIB query + 1 route + vRIB query + 2 routes +
                // 2 vRIB queries + 1 withdrawal + 1 vRIB query + 1 withdrawal
                // + 1 vRIB query
                12,
            )
            .await;

            //     ___  _   _ ___ _____   __  ___ ___  ___  _  _____ ___ 
            //    / _ \| | | | __| _ \ \ / / | _ ) _ \/ _ \| |/ / __| _ \
            //   | (_) | |_| | _||   /\ V /  | _ \   / (_) | ' <| _||   /
            //    \__\_\\___/|___|_|_\ |_|   |___/_|_\\___/|_|\_\___|_|_\
            //                                                           

            query_broker(&mut link_rx, 3);

            //    ___ _____ ___  ___   ___  ___ _____ ___  _  _ ___   _   
            //   / __|_   _/ _ \| _ \ | _ \/ _ \_   _/ _ \| \| |   \ /_\  
            //   \__ \ | || (_) |  _/ |   / (_) || || (_) | .` | |) / _ \ 
            //   |___/ |_| \___/|_|   |_|_\\___/ |_| \___/|_|\_|___/_/ \_\
            //                                                            

            // Wait for the manager to update the link report so that it will
            // be included in the trace log output
            while manager
                .link_report_updated_at()
                .duration_since(link_report_update_time)
                .as_secs()
                < 1
            {
                eprintln!("Waiting for link report to be updated");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            manager.terminate();

            //    ___ _  _ ___    _ 
            //   | __| \| |   \  | |
            //   | _|| .` | |) | |_|
            //   |___|_|\_|___/  (_)
            //                      
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

    async fn query_vrib_prefix(test_prefix: Prefix) -> serde_json::Value {
        reqwest::get(&format!(
            "http://localhost:8080/prefixes/0/{}?details=communities",
            test_prefix
        ))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
    }

    // The wanted metric values array is in the same order as RFC 7854
    // defines the BMP message types:
    //
    //       *  Type = 0: Route Monitoring
    //       *  Type = 1: Statistics Report
    //       *  Type = 2: Peer Down Notification
    //       *  Type = 3: Peer Up Notification
    //       *  Type = 4: Initiation Message
    //       *  Type = 5: Termination Message
    //       *  Type = 6: Route Mirroring Message
    async fn assert_bmp_messages_received(
        metrics: metrics::Collection,
        base_labels: &[(&str, &str)],
        wanted_values: [i64; 7],
    ) {
        const METRIC_NAME: &str =
            "bmp_tcp_in_num_bmp_messages_received_total";
        const MSG_TYPES: [&str; 7] = [
            "Route Monitoring",
            "Statistics Report",
            "Peer Down Notification",
            "Peer Up Notification",
            "Initiation Message",
            "Termination Message",
            "Route Mirroring Message",
        ];
        let expected_total: i64 = wanted_values.iter().sum();

        // Check the counters for each BMP message type defined by RFC 7854
        assert_metric_eq(
            metrics.clone(),
            METRIC_NAME,
            base_labels,
            expected_total,
        )
        .await;

        for (msg_type, &wanted_value) in
            MSG_TYPES.iter().zip(wanted_values.iter())
        {
            assert_metric_eq(
                metrics.clone(),
                METRIC_NAME,
                &[base_labels, &[("msg_type", msg_type)]].concat(),
                wanted_value,
            )
            .await;
        }
    }

    async fn assert_metric_eq(
        metrics: metrics::Collection,
        metric_name: &str,
        labels: &[(&str, &str)],
        wanted_v: i64,
    ) {
        let duration = Duration::from_secs(MAX_TIME_TO_WAIT_SECS);
        let result = Arc::new(AtomicMetricLookupResult::default());

        #[allow(clippy::collapsible_if)]
        if tokio::time::timeout(
            duration,
            wait_for_metric(
                &metrics,
                metric_name,
                labels,
                wanted_v,
                result.clone(),
            ),
        )
        .await
        .is_err()
        {
            if result.load(SeqCst) != MetricLookupResult::MetricValueMatched {
                eprintln!("Metric dump: {:#?}", get_metrics(&metrics));
                panic!(
                    "Metric '{}' with label '{:?}' != {} after {} seconds (reason: {})",
                    metric_name,
                    labels,
                    wanted_v,
                    duration.as_secs(),
                    result.load(SeqCst),
                );
            }
        }
    }

    async fn assert_metric_ne(
        metrics: metrics::Collection,
        metric_name: &str,
        labels: &[(&str, &str)],
        unwanted_v: i64,
    ) {
        let duration = Duration::from_secs(MAX_TIME_TO_WAIT_SECS);
        let result = Arc::new(AtomicMetricLookupResult::default());

        #[allow(clippy::collapsible_if)]
        if tokio::time::timeout(
            duration,
            wait_for_metric(
                &metrics,
                metric_name,
                labels,
                unwanted_v,
                result.clone(),
            ),
        )
        .await
        .is_ok()
        {
            if result.load(SeqCst) != MetricLookupResult::MetricValueMatched {
                eprintln!("Metric dump: {:#?}", get_metrics(&metrics));
                panic!(
                    "Metric '{}' with label '{:?}' == {} after {} seconds (reason: {})",
                    metric_name,
                    labels,
                    unwanted_v,
                    duration.as_secs(),
                    result.load(SeqCst),
                );
            }
        }
    }

    async fn wait_for_metric(
        metrics: &metrics::Collection,
        metric_name: &str,
        labels: &[(&str, &str)],
        wanted_v: i64,
        result: Arc<AtomicMetricLookupResult>,
    ) {
        let full_metric_name = format!("{}{}", METRIC_PREFIX, metric_name);
        loop {
            let found_v = get_metrics(metrics).get(
                &full_metric_name,
                labels,
                result.clone(),
            );

            if found_v == Some(wanted_v) {
                result.store(MetricLookupResult::MetricValueMatched, SeqCst);
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    fn get_metrics(
        metrics: &metrics::Collection,
    ) -> prometheus_parse::Scrape {
        let prom_txt = metrics.assemble(OutputFormat::Prometheus);
        let lines: Vec<_> =
            prom_txt.lines().map(|s| Ok(s.to_owned())).collect();
        prometheus_parse::Scrape::parse(lines.into_iter())
            .expect("Error while querying metrics")
    }

    async fn wait_for_bmp_connect() -> TcpStream {
        loop {
            match bmp_connect().await {
                Ok(stream) => return stream,
                Err(err) => eprintln!(
                    "Error connecting to BMP server: {}, retrying..",
                    err
                ),
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

    async fn bmp_peer_up(stream: &mut TcpStream, peer_n: u8) {
        let local_address: IpAddr = IpAddr::from_str("127.0.0.1").unwrap();
        let local_port: u16 = 80;
        let remote_port: u16 = 81;
        let sent_open_asn: u16 = 888;
        let received_open_asn: u16 = 999;
        let sent_bgp_identifier: u32 = 0;
        let received_bgp_id: u32 = 0;

        let per_peer_header = mk_per_peer_header(received_bgp_id, peer_n);

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

    async fn bmp_peer_down(stream: &mut TcpStream, peer_n: u8) {
        let received_bgp_id: u32 = 0;
        let per_peer_header = mk_per_peer_header(received_bgp_id, peer_n);

        stream
            .write_all(&mk_peer_down_notification_msg(&per_peer_header))
            .await
            .expect("Error while sending BMP 'peer down' message");
    }

    async fn bmp_route_announce(
        stream: &mut TcpStream,
        peer_n: u8,
        prefix: Prefix,
    ) {
        let received_bgp_id: u32 = 0;
        let per_peer_header = mk_per_peer_header(received_bgp_id, peer_n);

        let withdrawals = Prefixes::default();
        let announcements = Announcements::from_str(&format!(
            "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 {}",
            prefix
        ))
        .unwrap();
        bmp_route_monitoring(
            stream,
            per_peer_header,
            withdrawals,
            announcements,
        )
        .await;
    }

    async fn bmp_route_withdraw(
        stream: &mut TcpStream,
        peer_n: u8,
        prefix: Prefix,
    ) {
        let received_bgp_id: u32 = 0;
        let per_peer_header = mk_per_peer_header(received_bgp_id, peer_n);

        let withdrawals = Prefixes::new(vec![prefix]);
        let announcements = Announcements::None;
        bmp_route_monitoring(
            stream,
            per_peer_header,
            withdrawals,
            announcements,
        )
        .await;
    }

    async fn bmp_route_monitoring(
        stream: &mut TcpStream,
        per_peer_header: PerPeerHeader,
        withdrawals: Prefixes,
        announcements: Announcements,
    ) {
        let msg_buf = mk_route_monitoring_msg(
            &per_peer_header,
            &withdrawals,
            &announcements,
            &[],
        );
        stream
            .write_all(&msg_buf)
            .await
            .expect("Error while sending 'route monitoring' message");
    }

    fn mk_per_peer_header(received_bgp_id: u32, peer_n: u8) -> PerPeerHeader {
        let peer_type: MyPeerType = PeerType::GlobalInstance.into();
        let peer_flags: u8 = 0;
        let peer_address: IpAddr =
            IpAddr::from_str(&format!("10.0.0.{peer_n}")).unwrap();
        let peer_as: Asn = Asn::from_u32(12345 + (peer_n as u32));
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

    fn query_broker(link_rx: &mut LinkRx, num_expected_messages: usize) {
        for _ in 1..=num_expected_messages {
            eprintln!("Receiving MQTT message...");
            let msg = link_rx.recv().unwrap();
            assert!(matches!(msg, Some(Notification::Forward(_))));

            eprintln!("Checking MQTT message...");
            if let Some(Notification::Forward(forward)) = msg {
                assert_eq!(forward.publish.topic, "rotonda/testing");
                let expected_json = serde_json::json!({
                    "message": "🤭 I encountered 1818"
                });
                let actual_json: serde_json::Value =
                    serde_json::from_slice(&forward.publish.payload).unwrap();
                assert_json_eq(actual_json, expected_json);
            } else {
                unreachable!();
            }
        }

        eprintln!("Check that no more MQTT messages are waiting...");
        // On MacOS ARM systems `link_rx.recv()` blocks forever here while
        // on Linux/x86_64 systems it returns quickly if there are now
        // pending messages. So use `recv_deadline()` here as it meets our
        // needs on both architectures.
        let deadline =
            Instant::now().checked_add(Duration::from_secs(3)).unwrap();
        if let Ok(Some(notification)) = link_rx.recv_deadline(deadline) {
            dbg!(notification);
            panic!("Unexpected MQTT message received");
        }
    }

    // From least specific match to most specific match, top to bottom
    #[atomic_enum]
    #[derive(Default, PartialEq, Eq)]
    enum MetricLookupResult {
        #[default]
        NotQueried,
        MetricNameNotFound,
        LabelNameNotFound,
        LabelValueNotMatched,
        MetricValueNotMatched,
        MetricValueMatched,
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
                MetricLookupResult::MetricNameNotFound => {
                    write!(f, "NameNotFound")
                }
                MetricLookupResult::LabelNameNotFound => {
                    write!(f, "LabelNotFound")
                }
                MetricLookupResult::LabelValueNotMatched => {
                    write!(f, "ValueNotMatched")
                }
                MetricLookupResult::MetricValueNotMatched => {
                    write!(f, "Found")
                }
                MetricLookupResult::MetricValueMatched => write!(f, "Ok"),
            }
        }
    }

    trait MetricGetter {
        fn get(
            &self,
            metric_name: &str,
            labels: &[(&str, &str)],
            result: Arc<AtomicMetricLookupResult>,
        ) -> Option<i64>;
    }

    impl MetricGetter for prometheus_parse::Scrape {
        fn get(
            &self,
            metric_name: &str,
            labels: &[(&str, &str)],
            result: Arc<AtomicMetricLookupResult>,
        ) -> Option<i64> {
            fn is_wanted(
                sample: &prometheus_parse::Sample,
                metric_name: &str,
                labels: &[(&str, &str)],
                result: Arc<AtomicMetricLookupResult>,
            ) -> bool {
                if sample.metric == metric_name {
                    if labels.is_empty() {
                        result.store(
                            MetricLookupResult::MetricValueNotMatched,
                            SeqCst,
                        );
                        return true;
                    }

                    let mut found_count = 0;
                    for (label_name, label_value) in labels {
                        if let Some(v) = sample.labels.get(label_name) {
                            if v == *label_value {
                                found_count += 1;
                            } else {
                                let res = result.compare_exchange(
                                    MetricLookupResult::NotQueried,
                                    MetricLookupResult::LabelValueNotMatched,
                                    SeqCst,
                                    SeqCst,
                                );
                                if res.is_err()
                                    && res != Err(MetricLookupResult::MetricValueNotMatched)
                                {
                                    result.store(
                                        MetricLookupResult::LabelValueNotMatched,
                                        SeqCst,
                                    );
                                }
                                break;
                            }
                        } else {
                            let res = result.compare_exchange(
                                MetricLookupResult::NotQueried,
                                MetricLookupResult::LabelNameNotFound,
                                SeqCst,
                                SeqCst,
                            );
                            if res.is_err()
                                && res != Err(
                                    MetricLookupResult::MetricValueNotMatched,
                                )
                                && res != Err(
                                    MetricLookupResult::LabelValueNotMatched,
                                )
                            {
                                result.store(
                                    MetricLookupResult::LabelNameNotFound,
                                    SeqCst,
                                );
                            }
                            break;
                        }
                    }

                    if found_count == labels.len() {
                        result.store(
                            MetricLookupResult::MetricValueNotMatched,
                            SeqCst,
                        );
                        return true;
                    } else {
                        let res = result.compare_exchange(
                            MetricLookupResult::NotQueried,
                            MetricLookupResult::LabelNameNotFound,
                            SeqCst,
                            SeqCst,
                        );
                        if res.is_err()
                            && res
                                != Err(
                                    MetricLookupResult::MetricValueNotMatched,
                                )
                            && res
                                != Err(
                                    MetricLookupResult::LabelValueNotMatched,
                                )
                        {
                            result.store(
                                MetricLookupResult::LabelNameNotFound,
                                SeqCst,
                            );
                        }
                        return false;
                    }
                } else {
                    let res = result.compare_exchange(
                        MetricLookupResult::NotQueried,
                        MetricLookupResult::MetricNameNotFound,
                        SeqCst,
                        SeqCst,
                    );
                    if res.is_err()
                        && res
                            != Err(MetricLookupResult::MetricValueNotMatched)
                        && res
                            != Err(MetricLookupResult::LabelValueNotMatched)
                        && res != Err(MetricLookupResult::LabelNameNotFound)
                    {
                        result.store(
                            MetricLookupResult::MetricNameNotFound,
                            SeqCst,
                        );
                    }
                }

                false
            }

            fn sample_as_i64(sample: &prometheus_parse::Sample) -> i64 {
                match sample.value {
                    Value::Counter(v)
                    | Value::Gauge(v)
                    | Value::Untyped(v) => v as i64,
                    _ => 0,
                }
            }

            let sum: i64 = self
                .samples
                .iter()
                .filter(|sample| {
                    is_wanted(sample, metric_name, labels, result.clone())
                })
                .map(sample_as_i64)
                .sum();

            if result.load(SeqCst)
                == MetricLookupResult::MetricValueNotMatched
            {
                Some(sum)
            } else {
                None
            }
        }
    }
}
