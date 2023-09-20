//! MvpConfig.
//!
//! This module provides facilities to override the embedded MVP configuration
//! via [`MvpConfig`].
use clap::{Arg, ArgAction, ArgMatches, Command};
use log::error;
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::log::Terminate;

//------------ MvpConfig -----------------------------------------------------

/// Logging configuration.
#[derive(Default, Deserialize)]
pub struct MvpConfig {
    /// Whether to proxy and if so to where
    #[serde(default)]
    pub proxy_destination_addr: Option<SocketAddr>,

    /// Alternate HTTP listen address
    #[serde(default)]
    pub http_listen_addr: Option<SocketAddr>,

    /// Alternate BGP listen address
    #[serde(default)]
    pub bgp_listen_addr: Option<SocketAddr>,

    /// Alternate BMP listen address
    #[serde(default)]
    pub bmp_listen_addr: Option<SocketAddr>,

    /// Print finalised embedded config and exit
    #[serde(default)]
    pub print_config_and_exit: bool,

    /// Ignore missing Roto filters
    #[serde(default)]
    pub ignore_missing_filters: bool,
}

impl MvpConfig {
    /// Configures a clap app with the options for logging.
    pub fn config_args(app: Command) -> Command {
        app.next_help_heading("Options for use with the embedded config file")
            .arg(
                Arg::new("print-config-and-exit")
                    .long("print-config-and-exit")
                    .required(false)
                    .action(ArgAction::SetTrue)
                    .conflicts_with("config")
                    .help("Prints the configuration that will be used and then exits"),
            )
            .arg(
                Arg::new("http-listen")
                    .long("http-listen")
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with("config")
                    .help("Listen for HTTP connections on this address"),
            )
            .arg(
                Arg::new("bgp-listen")
                    .long("bgp-listen")
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with("config")
                    .help("Listen for BGP connections on this address"),
            )
            .arg(
                Arg::new("bmp-listen")
                    .long("bmp-listen")
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with("config")
                    .help("Listen for BMP connections on this address"),
            )
            .arg(
                Arg::new("proxy-destination")
                    .long("proxy-destination")
                    .required(false)
                    .value_name("IP:PORT")
                    .conflicts_with("config")
                    .help("Proxy BMP connections to this address"),
            )
    }

    /// Update the MVP override configuration from command line arguments.
    ///
    /// This should be called after the configuration file has been loaded.
    pub fn update_with_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), Terminate> {
        self.apply_mvp_matches(matches)
    }

    /// Applies the MVP override command line arguments to the config.
    fn apply_mvp_matches(&mut self, matches: &ArgMatches) -> Result<(), Terminate> {
        self.proxy_destination_addr = Self::from_str_value_of(matches, "proxy-destination")?;
        self.http_listen_addr = Self::from_str_value_of(matches, "http-listen")?;
        self.bgp_listen_addr = Self::from_str_value_of(matches, "bgp-listen")?;
        self.bmp_listen_addr = Self::from_str_value_of(matches, "bmp-listen")?;
        self.print_config_and_exit = matches.get_flag("print-config-and-exit");

        // If a config file is specified it should be valid, but if not specified then we are using an embedded MVP
        // config file that references Roto filters that the user may not have the required .roto files for, e.g.
        // if they installed Rotonda using `cargo install`. In the MVP case we therefore want to warn about missing
        // filters rather than it be a hard failure.
        self.ignore_missing_filters = !matches.contains_id("config");

        Ok(())
    }

    /// Try to convert a string encoded value.
    ///
    /// This helper function just changes error handling. Instead of returning
    /// the actual conversion error, it logs it as an invalid value for entry
    /// `key` and returns the standard error.
    fn from_str_value_of<T>(matches: &ArgMatches, key: &str) -> Result<Option<T>, Terminate>
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
    use std::path::{Path, PathBuf};

    use clap::Command;

    use crate::{
        common::file_io::TheFileIo,
        config::Config,
        log::{LogFilter, LogTarget},
        manager::Manager,
    };

    #[test]
    fn mvp_config_without_args_should_be_correct() {
        let app = Command::new("test");
        let arg_vec = vec!["my_prog"];
        let matches = Config::config_args(app).get_matches_from(arg_vec);
        let cur_dir = Path::new("");
        let mut manager = Manager::default();

        // when the expected roto scripts exist in the mock filesystem
        let readable_paths: [(PathBuf, String); 4] = [
            (
                "etc/filter.roto".into(),
                include_str!("../etc/filter.roto").into(),
            ),
            (
                "etc/bmp-asn-filter.roto".into(),
                include_str!("../etc/bmp-asn-filter.roto").into(),
            ),
            (
                "etc/rib-in-pre.roto".into(),
                include_str!("../etc/rib-in-pre.roto").into(),
            ),
            (
                "etc/rib-in-post.roto".into(),
                include_str!("../etc/rib-in-post.roto").into(),
            ),
        ];
        let mock_io = TheFileIo::new(readable_paths);
        manager.set_file_io(mock_io);

        // when loaded into the manager
        let (config_source, conf) =
            Config::from_arg_matches(&matches, cur_dir, &mut manager).unwrap();

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
        assert_eq!(units.len(), 6); // bgp-in, bmp-tcp-in, bmp-asn-filter, bmp-in, rib-in-pre, rib-in-post
                                    // TODO: check the exact units

        let targets = conf.targets.targets();
        assert_eq!(targets.len(), 1); // null, no MQTT or proxy without specific cmd line args
                                      // TODO: check the exact targets
    }

    // TODO: check that the MVP command line args have the desired effect
}
