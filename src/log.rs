//! Logging.
//!
//! This module provides facilities to set up logging based on a configuration
//! via [`LogConfig`].
//!
//! The module also provides two error types [`Failed`] and [`ExitError`] that
//! indicate that error information has been logged and a consumer can just
//! return quietly.
use crate::config::ConfigPath;
use clap::{Arg, ArgMatches, Command};
use log::{error, LevelFilter, Log};
use serde::Deserialize;
use std::convert::TryFrom;
use std::path::Path;
use std::str::FromStr;
use std::{fmt, io, process};

//------------ LogConfig -----------------------------------------------------

/// Logging configuration.
#[derive(Deserialize)]
pub struct LogConfig {
    /// Where to log to?
    #[serde(default)]
    pub log_target: LogTarget,

    /// If logging to a file, use this file.
    ///
    /// This isn’t part of `log_target` for deserialization reasons.
    #[serde(default)]
    pub log_file: ConfigPath,

    /// The syslog facility when logging to syslog.
    ///
    /// This isn’t part of `log_target` for deserialization reasons.
    #[cfg(unix)]
    #[serde(default)]
    pub log_facility: LogFacility,

    /// The minimum log level to actually log.
    #[serde(default)]
    pub log_level: LogFilter,
}

impl LogConfig {
    /// Configures a clap app with the options for logging.
    pub fn config_args(app: Command) -> Command {
        app.arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .multiple_occurrences(true)
                .help("Log more information, twice or thrice for even more"),
        )
        .arg(
            Arg::new("quiet")
                .short('q')
                .long("quiet")
                .multiple_occurrences(true)
                .conflicts_with("verbose")
                .help("Log less information, twice for no information"),
        )
        .arg(Arg::new("syslog").long("syslog").help("Log to syslog"))
        .arg(
            Arg::new("syslog-facility")
                .long("syslog-facility")
                .takes_value(true)
                .default_value("daemon")
                .help("Facility to use for syslog logging"),
        )
        .arg(
            Arg::new("logfile")
                .long("logfile")
                .takes_value(true)
                .value_name("PATH")
                .help("Log to this file"),
        )
    }

    /// Update the logging configuration from command line arguments.
    ///
    /// This should be called after the configuration file has been loaded.
    pub fn update_with_arg_matches(
        &mut self,
        matches: &ArgMatches,
        cur_dir: &Path,
    ) -> Result<(), Failed> {
        // log_level
        for _ in 0..matches.occurrences_of("verbose") {
            self.log_level.increase()
        }
        for _ in 0..matches.occurrences_of("quiet") {
            self.log_level.decrease()
        }

        self.apply_log_matches(matches, cur_dir)?;

        Ok(())
    }

    /// Applies the logging-specific command line arguments to the config.
    ///
    /// This is the Unix version that also considers syslog as a valid
    /// target.
    #[cfg(unix)]
    fn apply_log_matches(&mut self, matches: &ArgMatches, cur_dir: &Path) -> Result<(), Failed> {
        if matches.is_present("syslog") {
            self.log_target = LogTarget::Syslog;
            if let Some(value) = Self::from_str_value_of(matches, "syslog-facility")? {
                self.log_facility = value
            }
        } else if let Some(file) = matches.value_of("logfile") {
            if file == "-" {
                self.log_target = LogTarget::Stderr
            } else {
                self.log_target = LogTarget::File;
                self.log_file = cur_dir.join(file).into();
            }
        }
        Ok(())
    }

    /// Applies the logging-specific command line arguments to the config.
    ///
    /// This is the non-Unix version that does not use syslog.
    #[cfg(not(unix))]
    #[allow(clippy::unnecessary_wraps)]
    fn apply_log_matches(&mut self, matches: &ArgMatches, cur_dir: &Path) -> Result<(), Failed> {
        if let Some(file) = matches.value_of("logfile") {
            if file == "-" {
                self.log_target = LogTarget::Stderr
            } else {
                self.log_target = LogTarget::File;
                self.log_file = cur_dir.join(file).into();
            }
        }
        Ok(())
    }

    /// Try to convert a string encoded value.
    ///
    /// This helper function just changes error handling. Instead of returning
    /// the actual conversion error, it logs it as an invalid value for entry
    /// `key` and returns the standard error.
    #[allow(dead_code)] // unused on Windows
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

    /// Initialize logging.
    ///
    /// All diagnostic output of RTRTR is done via logging, never to
    /// stderr directly. Thus, it is important to initalize logging before
    /// doing anything else that may result in such output. This function
    /// does exactly that. It sets a maximum log level of `warn`, leading
    /// only printing important information, and directs all logging to
    /// stderr.
    pub fn init_logging() -> Result<(), ExitError> {
        log::set_max_level(log::LevelFilter::Warn);
        if let Err(err) = log_reroute::init() {
            eprintln!("Failed to initialize logger: {}.", err);
            return Err(ExitError);
        };
        let dispatch = fern::Dispatch::new()
            .level(log::LevelFilter::Error)
            .chain(io::stderr())
            .into_log()
            .1;
        log_reroute::reroute_boxed(dispatch);
        Ok(())
    }

    /// Switches logging to the configured target.
    ///
    /// Once the configuration has been successfully loaded, logging should
    /// be switched to whatever the user asked for via this method.
    #[allow(unused_variables)] // for cfg(not(unix))
    pub fn switch_logging(&self, daemon: bool) -> Result<(), Failed> {
        let logger = match self.log_target {
            #[cfg(unix)]
            LogTarget::Default => {
                if daemon {
                    self.syslog_logger()?
                } else {
                    self.stderr_logger(false)
                }
            }
            #[cfg(not(unix))]
            LogTarget::Default => self.stderr_logger(daemon),
            #[cfg(unix)]
            LogTarget::Syslog => self.syslog_logger()?,
            LogTarget::Stderr => self.stderr_logger(daemon),
            LogTarget::File => self.file_logger()?,
        };
        log_reroute::reroute_boxed(logger);
        log::set_max_level(self.log_level.0);
        Ok(())
    }

    /// Creates a syslog logger and configures correctly.
    #[cfg(unix)]
    fn syslog_logger(&self) -> Result<Box<dyn Log>, Failed> {
        let mut formatter = syslog::Formatter3164 {
            facility: self.log_facility.0,
            ..Default::default()
        };
        if formatter.hostname.is_none() {
            formatter.hostname = Some("routinator".into());
        }
        let formatter = formatter;
        let logger = syslog::unix(formatter.clone())
            .or_else(|_| syslog::tcp(formatter.clone(), ("127.0.0.1", 601)))
            .or_else(|_| syslog::udp(formatter, ("127.0.0.1", 0), ("127.0.0.1", 514)));
        match logger {
            Ok(logger) => Ok(Box::new(syslog::BasicLogger::new(logger))),
            Err(err) => {
                error!("Cannot connect to syslog: {}", err);
                Err(Failed)
            }
        }
    }

    /// Creates a stderr logger.
    ///
    /// If we are in daemon mode, we add a timestamp to the output.
    fn stderr_logger(&self, daemon: bool) -> Box<dyn Log> {
        self.fern_logger(daemon).chain(io::stderr()).into_log().1
    }

    /// Creates a file logger using the file provided by `path`.
    fn file_logger(&self) -> Result<Box<dyn Log>, Failed> {
        let file = match fern::log_file(&self.log_file) {
            Ok(file) => file,
            Err(err) => {
                error!(
                    "Failed to open log file '{}': {}",
                    self.log_file.display(),
                    err
                );
                return Err(Failed);
            }
        };
        Ok(self.fern_logger(true).chain(file).into_log().1)
    }

    /// Creates and returns a fern logger.
    fn fern_logger(&self, timestamp_and_level: bool) -> fern::Dispatch {
        // TODO: These env var controls are not changeable by the operator at runtime which may make them less useful.
        // They also require you to already be logging at trace level which means that you also see lots of other trace
        // logging but if you enable one of these env vars you clearly actually want to see the logging that you are
        // enabling, not a bunch of other logging as well. So I think this needs some more thought.
        let mqtt_log_level = match std::env::var("ROTONDA_MQTT_LOG") {
            Ok(_) => self.log_level.0.min(LevelFilter::Trace),
            Err(_) => self.log_level.0.min(LevelFilter::Warn),
        };
        let rotonda_store_log_level = match std::env::var("ROTONDA_STORE_LOG") {
            Ok(_) => self.log_level.0.min(LevelFilter::Trace),
            Err(_) => self.log_level.0.min(LevelFilter::Warn),
        };
        let roto_log_level = match std::env::var("ROTONDA_ROTO_LOG") {
            Ok(_) => self.log_level.0.min(LevelFilter::Trace),
            Err(_) => self.log_level.0.min(LevelFilter::Warn),
        };

        let debug_enabled = self.log_level.0 >= LevelFilter::Debug;

        let mut res = fern::Dispatch::new();

        // Don't log module paths (e.g. rotonda::xxx::yyy) for our own code modules as we the StatusLoggers
        // take care of making it clear which unit or target instance is logging which is more useful for readers of
        // the logs. Do log module paths for messages logged (unexpectedly if warnings or errors) from other modules,
        // i.e. crate dependencies, as we won't know anything about where those messages come from and if they were
        // logged without a corresponding error that we could catch with Err then we won't have any additional context
        // from our own code about where they came from. When the main log level is set to at debug or trace, then
        // always log module paths in order to have the greatest level of information possible available.
        if timestamp_and_level {
            res = res.format(move |out, message, record| {
                let module_path = record.module_path().unwrap_or("");
                let show_module = debug_enabled || !module_path.starts_with("rotonda");
                out.finish(format_args!(
                    "[{}] {:5} {}{}{}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    if show_module { module_path } else { "" },
                    if show_module { ": " } else { "" },
                    message
                ))
            });
        } else {
            res = res.format(move |out, message, record| {
                let module_path = record.module_path().unwrap_or("");
                let show_module = debug_enabled || !module_path.starts_with("rotonda");
                out.finish(format_args!(
                    "{}{}{}",
                    if show_module { module_path } else { "" },
                    if show_module { ": " } else { "" },
                    message
                ))
            });
        }

        // Note: The tracing::span directives below are needed to prevent dependent crates that use Tokio Tracing from
        // accidentally violating the log levels we define here. When "there are no attributes associated with a span,
        // then it creates a log record with the target 'tracing::span' rather than the target specified by the span's
        // metadata" [1]. This is a problem because the Fern `level_for()` filtering mechanism filters by target, not
        // by module [2]. To prevent this we explicitly add catch all directives for the 'tracing::span' target.
        //
        // References:
        //   [1]: https://github.com/daboross/fern/issues/85#issuecomment-944305183
        //   [2]: https://github.com/daboross/fern/issues/109
        //
        // The trigger for adding this was using rumqttd for functional testing as then the rumqttd log line shown
        // below is immune to the `level_for("rumqttd")` directive we use and only `level_for("tracing::span")` has an
        // effect on it. E.g. when log level is set to warn the following log line should not be seen, but it is unless
        // we filter out 'tracing::span':
        //
        // [2022-12-08 13:00:06] INFO  rumqttd::router::routing: disconnect;

        // Disable or limit logging from some modules which add too much noise for too little benefit for our use case.
        res = res
            .level(self.log_level.0)
            .level_for("rustls", LevelFilter::Error)
            .level_for("rumqttd", LevelFilter::Warn)
            .level_for("tracing::span", LevelFilter::Off);

        // Boost the log level of modules for which the operator has requested more diagnostics for.
        res = res
            .level_for("rotonda_store", rotonda_store_log_level)
            .level_for("rumqttc", mqtt_log_level)
            .level_for("roto", roto_log_level);

        if debug_enabled {
            // Don't enable too much logging for some modules even if the main log level is set to debug or trace.
            res = res
                .level_for("tokio_reactor", LevelFilter::Info)
                .level_for("hyper", LevelFilter::Info)
                .level_for("reqwest", LevelFilter::Info)
                .level_for("h2", LevelFilter::Info)
                .level_for("mio", LevelFilter::Info);

            // Conversely, when the main log level is at least debug, disable limitations on logging normally in place
            // for some modules.
            res = res
                .level_for("rumqttd", self.log_level.0)
                .level_for("tracing::span", self.log_level.0);
        }

        res
    }
}

//------------ LogTarget -----------------------------------------------------

/// The target to log to.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum LogTarget {
    /// Use the system default.
    #[default]
    #[serde(rename = "default")]
    Default,

    /// Syslog.
    #[cfg(unix)]
    #[serde(rename = "syslog")]
    Syslog,

    /// Stderr.
    #[serde(rename = "stderr")]
    Stderr,

    /// A file.
    #[serde(rename = "file")]
    File,
}

//------------ LogFacility ---------------------------------------------------

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(try_from = "String")]
pub struct LogFacility(syslog::Facility);

#[cfg(unix)]
impl Default for LogFacility {
    fn default() -> Self {
        LogFacility(syslog::Facility::LOG_DAEMON)
    }
}

#[cfg(unix)]
impl TryFrom<String> for LogFacility {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        syslog::Facility::from_str(&value)
            .map(LogFacility)
            .map_err(|_| format!("unknown syslog facility {}", &value))
    }
}

#[cfg(unix)]
impl FromStr for LogFacility {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        syslog::Facility::from_str(s)
            .map(LogFacility)
            .map_err(|_| "unknown facility")
    }
}

//------------ LogFilter -----------------------------------------------------

#[derive(Deserialize)]
#[serde(try_from = "String")]
pub struct LogFilter(log::LevelFilter);

impl LogFilter {
    pub fn increase(&mut self) {
        use log::LevelFilter::*;

        self.0 = match self.0 {
            Off => Error,
            Error => Warn,
            Warn => Info,
            Info => Debug,
            Debug => Trace,
            Trace => Trace,
        }
    }

    pub fn decrease(&mut self) {
        use log::LevelFilter::*;

        self.0 = match self.0 {
            Off => Off,
            Error => Off,
            Warn => Error,
            Info => Warn,
            Debug => Info,
            Trace => Debug,
        }
    }
}

impl Default for LogFilter {
    fn default() -> Self {
        LogFilter(log::LevelFilter::Warn)
    }
}

impl TryFrom<String> for LogFilter {
    type Error = log::ParseLevelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        log::LevelFilter::from_str(&value).map(LogFilter)
    }
}

//------------ Failed --------------------------------------------------------

/// An error happened that has been logged.
///
/// This is a marker type that can be used in results to indicate that if an
/// error happend, it has been logged and doesn’t need further treatment.
#[derive(Clone, Copy, Debug)]
pub struct Failed;

//------------ ExitError -----------------------------------------------------

/// An error happened that should cause the process to exit.
#[derive(Debug)]
pub struct ExitError;

impl ExitError {
    /// Exits the process.
    pub fn exit(self) -> ! {
        process::exit(1)
    }
}

impl From<Failed> for ExitError {
    fn from(_: Failed) -> ExitError {
        ExitError
    }
}
