//! MvpConfig.
//!
//! This module provides facilities to override the embedded MVP configuration
//! via [`MvpConfig`].
use clap::{Arg, ArgAction, ArgMatches, Command};
use const_format::formatcp;
use log::error;
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::log::Terminate;

use crate::targets::DEF_MQTT_PORT;
use crate::units::TracingMode;

//------------ Constants -----------------------------------------------------

// NOTE: Unit and target names MUST match the names used in `rotonda.conf`!

pub const CFG_UNIT_BGP_IN: &str = "bgp-in";
pub const CFG_UNIT_BMP_IN: &str = "bmp-in";
pub const CFG_UNIT_RIB_IN_PRE: &str = "rib-in-pre";
pub const CFG_UNIT_RIB_IN_POST: &str = "rib-in-post";

pub const CFG_TARGET_NULL: &str = "null";
pub const CFG_TARGET_BMP_PROXY: &str = "bmp-proxy";
pub const CFG_TARGET_MQTT: &str = "mqtt";

pub const ARG_CONFIG: &str = "config";
pub const ARG_PRINT_CONFIG_AND_EXIT: &str = "print-config-and-exit";
pub const ARG_HTTP_LISTEN: &str = "http-listen";
pub const ARG_BGP_LISTEN: &str = "bgp-listen";
pub const ARG_BMP_LISTEN: &str = "bmp-listen";
pub const ARG_BMP_TRACING_MODE: &str = "bmp-tracing-mode";
pub const ARG_BMP_PROXY_DESTINATION: &str = "bmp-proxy-destination";
pub const ARG_MQTT_DESTINATION: &str = "mqtt-destination";

//------------ MvpConfig -----------------------------------------------------

/// Logging configuration.
#[derive(Default, Deserialize)]
pub struct MvpConfig {
    /// Alternate BGP listen address
    #[serde(default)]
    pub bgp_listen_addr: Option<SocketAddr>,

    /// Alternate BMP listen address
    #[serde(default)]
    pub bmp_listen_addr: Option<SocketAddr>,

    /// Whether to proxy BMP connections and if so to where
    #[serde(default)]
    pub bmp_proxy_destination_addr: Option<SocketAddr>,

    /// Alternate HTTP listen address
    #[serde(default)]
    pub http_listen_addr: Option<SocketAddr>,

    /// Ignore missing Roto filters
    #[serde(default)]
    pub ignore_missing_filters: bool,

    /// Whether to publish to MQTT and if so to where
    #[serde(default)]
    pub mqtt_destination_addr: Option<String>,

    /// Print finalised embedded config and exit
    #[serde(default)]
    pub print_config_and_exit: bool,

    #[serde(default)]
    pub tracing_mode: Option<TracingMode>,
}

impl MvpConfig {
    /// Configures a clap app with the options for logging.
    pub fn config_args(app: Command) -> Command {
        app.next_help_heading("Options for use with the embedded config file")
            .arg(
                Arg::new(ARG_PRINT_CONFIG_AND_EXIT)
                    .long(ARG_PRINT_CONFIG_AND_EXIT)
                    .required(false)
                    .action(ArgAction::SetTrue)
                    .conflicts_with(ARG_CONFIG)
                    .help("Prints the configuration that will be used and then exits"),
            )
            .arg(
                Arg::new(ARG_HTTP_LISTEN)
                    .long(ARG_HTTP_LISTEN)
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with(ARG_CONFIG)
                    .help("Listen for HTTP connections on this address"),
            )
            .arg(
                Arg::new(ARG_BGP_LISTEN)
                    .long(ARG_BGP_LISTEN)
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with(ARG_CONFIG)
                    .help("Listen for BGP connections on this address"),
            )
            .arg(
                Arg::new(ARG_BMP_TRACING_MODE)
                    .long(ARG_BMP_TRACING_MODE)
                    .required(false)
                    .value_name("<Off|IfRequested|On>")
                    .conflicts_with(ARG_CONFIG)
                    .help("Whether and how to enable tracing of BMP messages"),
            )
            .arg(
                Arg::new(ARG_BMP_LISTEN)
                    .long(ARG_BMP_LISTEN)
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with(ARG_CONFIG)
                    .help("Listen for BMP connections on this address"),
            )
            .arg(
                Arg::new(ARG_BMP_PROXY_DESTINATION)
                    .long(ARG_BMP_PROXY_DESTINATION)
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with(ARG_CONFIG)
                    .help("Proxy BMP connections to this address"),
            )
            .arg(
                Arg::new(ARG_MQTT_DESTINATION)
                    .long(ARG_MQTT_DESTINATION)
                    .required(false)
                    .value_name("IP or IP:PORT")
                    .conflicts_with(ARG_CONFIG)
                    .help(formatcp!(
                        "Publish MQTT messages to this address [default port: {DEF_MQTT_PORT}]"
                    )),
            )
    }

    /// Update the MVP override configuration from command line arguments.
    ///
    /// This should be called after the configuration file has been loaded.
    pub fn update_with_arg_matches(
        &mut self,
        matches: &ArgMatches,
    ) -> Result<(), Terminate> {
        self.apply_mvp_matches(matches)
    }

    /// Applies the MVP override command line arguments to the config.
    fn apply_mvp_matches(
        &mut self,
        matches: &ArgMatches,
    ) -> Result<(), Terminate> {
        self.bgp_listen_addr =
            Self::from_str_value_of(matches, ARG_BGP_LISTEN)?;
        self.bmp_listen_addr =
            Self::from_str_value_of(matches, ARG_BMP_LISTEN)?;
        self.bmp_proxy_destination_addr =
            Self::from_str_value_of(matches, ARG_BMP_PROXY_DESTINATION)?;
        self.http_listen_addr =
            Self::from_str_value_of(matches, ARG_HTTP_LISTEN)?;
        self.mqtt_destination_addr =
            Self::from_str_value_of(matches, ARG_MQTT_DESTINATION)?;
        self.print_config_and_exit =
            matches.get_flag(ARG_PRINT_CONFIG_AND_EXIT);
        self.tracing_mode =
            Self::from_str_value_of(matches, ARG_BMP_TRACING_MODE)?;

        // If a config file is specified it should be valid, but if not specified then we are using an embedded MVP
        // config file that references Roto filters that the user may not have the required .roto files for, e.g.
        // if they installed Rotonda using `cargo install`. In the MVP case we therefore want to warn about missing
        // filters rather than it be a hard failure.
        self.ignore_missing_filters = !matches.contains_id(ARG_CONFIG);

        Ok(())
    }

    /// Try to convert a string encoded value.
    ///
    /// This helper function just changes error handling. Instead of returning
    /// the actual conversion error, it logs it as an invalid value for entry
    /// `key` and returns the standard error.
    fn from_str_value_of<T>(
        matches: &ArgMatches,
        key: &str,
    ) -> Result<Option<T>, Terminate>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match matches.get_one::<String>(key) {
            Some(value) => match T::from_str(value) {
                Ok(value) => Ok(Some(value)),
                Err(err) => {
                    error!("Invalid value for {}: {}.", key, err);
                    Err(Terminate::error())
                }
            },
            None => Ok(None),
        }
    }
}

//------------ Tests ---------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::{Path, PathBuf};

    use clap::Command;
    use const_format::concatcp;

    use crate::{
        common::file_io::TheFileIo,
        config::Config,
        log::{LogFilter, LogTarget},
        manager::Manager,
        mvp::{
            CFG_TARGET_BMP_PROXY, CFG_TARGET_MQTT, CFG_TARGET_NULL,
            CFG_UNIT_BGP_IN, CFG_UNIT_BMP_IN, CFG_UNIT_RIB_IN_POST,
            CFG_UNIT_RIB_IN_PRE,
        },
        tests::util::internal::enable_logging,
    };

    #[test]
    fn mvp_config_without_args_should_be_correct() {
        let app = Command::new("test");
        let arg_vec = vec!["rotonda"];
        let matches = Config::config_args(app)
            .try_get_matches_from(arg_vec)
            .unwrap();
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when the expected roto scripts exist in the mock filesystem
        let readable_paths: [(PathBuf, String); 5] = [
            (
                "etc/filter.roto".into(),
                include_str!("../etc/filter.roto").into(),
            ),
            (
                "etc/bgp-in-filter.roto".into(),
                include_str!("../etc/bgp-in-filter.roto").into(),
            ),
            (
                "etc/bmp-in-filter.roto".into(),
                include_str!("../etc/bmp-in-filter.roto").into(),
            ),
            (
                "etc/rib-in-pre.roto".into(),
                include_str!("../etc/rib-in-pre-filter.roto").into(),
            ),
            (
                "etc/rib-in-post.roto".into(),
                include_str!("../etc/rib-in-post-filter.roto").into(),
            ),
        ];
        let mock_io = TheFileIo::new(readable_paths);
        manager.set_file_io(mock_io);

        // when loaded into the manager
        let (config_source, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager)
                .unwrap();

        // then there should be no config file path
        assert!(!config_source.is_path());

        // and the configuration should be correct
        assert_eq!(conf.roto_scripts_path, Some("etc/".into()));
        assert_eq!(conf.log.log_target, LogTarget::Stderr);
        assert_eq!(
            conf.log.log_level,
            LogFilter::try_from("info".to_string()).unwrap()
        );

        assert_eq!(conf.http.listen(), &["127.0.0.1:8080".parse().unwrap()]);

        let units = conf.units.units();
        assert_eq!(units.len(), 4);
        assert!(units.contains_key(CFG_UNIT_BGP_IN));
        assert!(units.contains_key(CFG_UNIT_BMP_IN));
        assert!(units.contains_key(CFG_UNIT_RIB_IN_PRE));
        assert!(units.contains_key(CFG_UNIT_RIB_IN_POST));

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 1);
        assert!(targets.contains_key(CFG_TARGET_NULL));
    }

    #[test]
    fn mvp_config_with_missing_roto_scripts_should_still_work() {
        let app = Command::new("test");
        let arg_vec = vec!["rotonda"];
        let matches = Config::config_args(app)
            .try_get_matches_from(arg_vec)
            .unwrap();
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when loaded into the manager
        let (config_source, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager)
                .unwrap();

        // then there should be no config file path
        assert!(!config_source.is_path());

        // and the configuration should be correct
        assert_eq!(conf.roto_scripts_path, Some("etc/".into()));
        assert_eq!(conf.log.log_target, LogTarget::Stderr);
        assert_eq!(
            conf.log.log_level,
            LogFilter::try_from("info".to_string()).unwrap()
        );

        assert_eq!(conf.http.listen(), &["127.0.0.1:8080".parse().unwrap()]);

        let units = conf.units.units();
        assert_eq!(units.len(), 4);
        assert!(units.contains_key(CFG_UNIT_BGP_IN));
        assert!(units.contains_key(CFG_UNIT_BMP_IN));
        assert!(units.contains_key(CFG_UNIT_RIB_IN_PRE));
        assert!(units.contains_key(CFG_UNIT_RIB_IN_POST));

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 1);
        assert!(targets.contains_key(CFG_TARGET_NULL));
    }

    #[test]
    fn mvp_config_with_both_mvp_specific_cmd_line_arg_and_config_file_arg_should_fail(
    ) {
        let app = Command::new("test");
        let arg_vec = vec![
            "rotonda",
            "--config",
            "some/config/file/path",
            "--bmp-proxy-destination",
            "127.0.0.1:12345",
        ];
        let res = Config::config_args(app).try_get_matches_from(arg_vec);
        assert!(
            matches!(res, Err(err) if err.kind() == clap::error::ErrorKind::ArgumentConflict)
        );
    }

    #[test]
    fn mvp_config_with_proxy_destination_cmd_line_arg_should_have_proxy_target(
    ) {
        let app = Command::new("test");
        let arg_vec = vec![
            "rotonda",
            concatcp!("--", ARG_BMP_PROXY_DESTINATION),
            "127.0.0.1:12345",
        ];
        let matches = Config::config_args(app)
            .try_get_matches_from(arg_vec)
            .unwrap();
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when loaded into the manager
        let (_, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager)
                .unwrap();

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains_key(CFG_TARGET_NULL));
        assert!(targets.contains_key(CFG_TARGET_BMP_PROXY));
    }

    #[test]
    fn mvp_config_with_mqtt_destination_cmd_line_arg_should_have_mqtt_target()
    {
        let app = Command::new("test");
        let arg_vec = vec![
            "rotonda",
            concatcp!("--", ARG_MQTT_DESTINATION),
            "127.0.0.1:12345",
        ];
        let matches = Config::config_args(app)
            .try_get_matches_from(arg_vec)
            .unwrap();
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when loaded into the manager
        let (_, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager)
                .unwrap();

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains_key(CFG_TARGET_NULL));
        assert!(targets.contains_key(CFG_TARGET_MQTT));
    }

    #[test]
    fn mvp_config_with_proxy_and_mqtt_destination_cmd_line_args_should_have_both_targets(
    ) {
        enable_logging("trace");
        let app = Command::new("test");
        let arg_vec = vec![
            "rotonda",
            concatcp!("--", ARG_BMP_PROXY_DESTINATION),
            "127.0.0.1:12345",
            concatcp!("--", ARG_MQTT_DESTINATION),
            "127.0.0.1",
        ];
        let matches = Config::config_args(app)
            .try_get_matches_from(arg_vec)
            .unwrap();
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when loaded into the manager
        let (_, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager)
                .unwrap();

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 3);
        assert!(targets.contains_key(CFG_TARGET_NULL));
        assert!(targets.contains_key(CFG_TARGET_BMP_PROXY));
        assert!(targets.contains_key(CFG_TARGET_MQTT));
    }
}
