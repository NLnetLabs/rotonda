//! MvpConfig.
//!
//! This module provides facilities to override the embedded MVP configuration
//! via [`MvpConfig`].
use clap::{Arg, ArgMatches, Command};
use log::error;
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::log::Failed;

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
}

impl MvpConfig {
    /// Configures a clap app with the options for logging.
    pub fn config_args(app: Command) -> Command {
        app.arg(
            Arg::new("proxy-destination")
                .long("proxy-destination")
                .required(false)
                .takes_value(true)
                .value_name("IP:PORT")
                .conflicts_with("config")
                .help("Proxy BMP connections to this address"),
        )
        .arg(
            Arg::new("http-listen")
                .long("http-listen")
                .required(false)
                .takes_value(true)
                .value_name("IP:PORT")
                .conflicts_with("config")
                .help("Listen for HTTP connections on this address"),
        )
        .arg(
            Arg::new("bgp-listen")
                .long("bgp-listen")
                .required(false)
                .takes_value(true)
                .value_name("IP:PORT")
                .conflicts_with("config")
                .help("Listen for BGP connections on this address"),
        )
        .arg(
            Arg::new("bmp-listen")
                .long("bmp-listen")
                .required(false)
                .takes_value(true)
                .value_name("IP:PORT")
                .conflicts_with("config")
                .help("Listen for BMP connections on this address"),
        )
    }

    /// Update the MVP override configuration from command line arguments.
    ///
    /// This should be called after the configuration file has been loaded.
    pub fn update_with_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), Failed> {
        self.apply_mvp_matches(matches)
    }

    /// Applies the MVP override command line arguments to the config.
    fn apply_mvp_matches(&mut self, matches: &ArgMatches) -> Result<(), Failed> {
        self.proxy_destination_addr = Self::from_str_value_of(matches, "proxy-destination")?;
        self.http_listen_addr = Self::from_str_value_of(matches, "http-listen")?;
        self.bgp_listen_addr = Self::from_str_value_of(matches, "bgp-listen")?;
        self.bmp_listen_addr = Self::from_str_value_of(matches, "bmp-listen")?;
        Ok(())
    }

    /// Try to convert a string encoded value.
    ///
    /// This helper function just changes error handling. Instead of returning
    /// the actual conversion error, it logs it as an invalid value for entry
    /// `key` and returns the standard error.
    fn from_str_value_of<T>(matches: &ArgMatches, key: &str) -> Result<Option<T>, Failed>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match matches.value_of(key) {
            Some(value) => match T::from_str(value) {
                Ok(value) => Ok(Some(value)),
                Err(err) => {
                    error!("Invalid value for {}: {}.", key, err);
                    Err(Failed)
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
        let (config_path, conf) =
            Config::from_arg_matches(&matches, &cur_dir, &mut manager).unwrap();

        // then there should be no config file path
        assert!(config_path.is_none());

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
