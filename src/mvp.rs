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
        self.apply_log_matches(matches)
    }

    /// Applies the MVP override command line arguments to the config.
    fn apply_log_matches(&mut self, matches: &ArgMatches) -> Result<(), Failed> {
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
