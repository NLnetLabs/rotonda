//! Logging.
//!
//! This module provides facilities to set up logging based on a configuration
//! via [`LogConfig`].
//!
//! The module also provides two error types [`Failed`] and [`ExitError`] that
//! indicate that error information has been logged and a consumer can just
//! return quietly.
use crate::config::ConfigPath;
use chrono::{DateTime, Utc};
use clap::{Arg, ArgAction, ArgMatches, Command};
use crossbeam_utils::atomic::AtomicCell;
use log::{error, LevelFilter, Log};
use serde::Deserialize;
use std::convert::TryFrom;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, io};
use uuid::Uuid;

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
        app.next_help_heading("Options related to logging")
            .arg(
                Arg::new("quiet")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::Count)
                    .conflicts_with("verbose")
                    .help(" Log less information, twice for no information"),
            )
            .arg(
                Arg::new("verbose")
                    .short('v')
                    .long("verbose")
                    .action(ArgAction::Count)
                    .help(" Log more information, twice or thrice for even more"),
            )
            .arg(
                Arg::new("logfile")
                    .long("logfile")
                    .value_name("PATH")
                    .help(" Log to this file"),
            )
            .arg(
                Arg::new("syslog")
                    .long("syslog")
                    .action(ArgAction::SetTrue)
                    .help(" Log to syslog"),
            )
            .arg(
                Arg::new("syslog-facility")
                    .long("syslog-facility")
                    .default_value("daemon")
                    .value_name("FACILITY")
                    .help(" Facility to use for syslog logging"),
            )
    }

    /// Update the logging configuration from command line arguments.
    ///
    /// This should be called after the configuration file has been loaded.
    pub fn update_with_arg_matches(
        &mut self,
        matches: &ArgMatches,
        cur_dir: &Path,
    ) -> Result<(), Terminate> {
        // log_level
        for _ in 0..matches.get_count("verbose") {
            self.log_level.increase()
        }
        for _ in 0..matches.get_count("quiet") {
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
    fn apply_log_matches(&mut self, matches: &ArgMatches, cur_dir: &Path) -> Result<(), Terminate> {
        if matches.get_flag("syslog") {
            self.log_target = LogTarget::Syslog;
            if let Some(value) = Self::from_str_value_of(matches, "syslog-facility")? {
                self.log_facility = value
            }
        } else if let Some(file) = matches.get_one::<String>("logfile") {
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
    fn apply_log_matches(&mut self, matches: &ArgMatches, cur_dir: &Path) -> Result<(), Terminate> {
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

    /// Initialize logging.
    ///
    /// All diagnostic output of Rotonda is done via logging, never to
    /// stderr directly. Thus, it is important to initalize logging before
    /// doing anything else that may result in such output. This function
    /// does exactly that. It sets a maximum log level of `warn`, leading
    /// only printing important information, and directs all logging to
    /// stderr.
    pub fn init_logging() -> Result<(), Terminate> {
        log::set_max_level(log::LevelFilter::Warn);
        if let Err(err) = log_reroute::init() {
            eprintln!("Failed to initialize logger: {}.", err);
            Err(ExitError)?;
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
    pub fn switch_logging(&self, daemon: bool) -> Result<(), Terminate> {
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
    fn syslog_logger(&self) -> Result<Box<dyn Log>, Terminate> {
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
                Err(Terminate::error())
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
    fn file_logger(&self) -> Result<Box<dyn Log>, Terminate> {
        let file = match fern::log_file(&self.log_file) {
            Ok(file) => file,
            Err(err) => {
                error!(
                    "Failed to open log file '{}': {}",
                    self.log_file.display(),
                    err
                );
                return Err(Terminate::error());
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

#[derive(Debug, Deserialize, PartialEq)]
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

//------------ Terminate -----------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum TerminateReason {
    Failed(i32),
    Normal,
}

/// Something happened that means the application should terminate.
///
/// This is a marker type that can be used in results to indicate that if an
/// error happend, it has been logged and doesn’t need further treatment.
///
/// It can also be used to trigger abort for other reasons, e.g. orderly exit
/// on demand.
#[derive(Clone, Copy, Debug)]
pub struct Terminate(TerminateReason);

impl Terminate {
    pub fn normal() -> Self {
        Self(TerminateReason::Normal)
    }

    pub fn error() -> Self {
        Self(TerminateReason::Failed(1))
    }

    pub fn other(exit_code: i32) -> Self {
        assert_ne!(exit_code, 0);
        Self(TerminateReason::Failed(exit_code))
    }

    pub fn reason(&self) -> TerminateReason {
        self.0
    }

    pub fn exit_code(&self) -> i32 {
        match self.reason() {
            TerminateReason::Failed(n) => n,
            TerminateReason::Normal => 0,
        }
    }
}

//------------ ExitError -----------------------------------------------------

/// An error happened that should cause the process to exit.
#[derive(Debug)]
pub struct ExitError;

impl From<Terminate> for ExitError {
    fn from(terminate: Terminate) -> ExitError {
        match terminate.reason() {
            TerminateReason::Failed(_) => ExitError,
            TerminateReason::Normal => unreachable!(),
        }
    }
}

impl From<ExitError> for Terminate {
    fn from(_: ExitError) -> Self {
        Terminate::error()
    }
}

//----------- Tracer ---------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MsgRelation {
    /// The message relates to the Gate.
    GATE,

    /// The message relates to the component that owns the Gate.
    COMPONENT,

    /// All.
    ALL,
}

#[derive(Clone, Debug)]
pub struct TraceMsg {
    pub timestamp: DateTime<Utc>,
    pub gate_id: Uuid,
    pub msg: String,
    pub msg_relation: MsgRelation,
}

impl TraceMsg {
    pub fn new(gate_id: Uuid, msg: String, msg_relation: MsgRelation) -> Self {
        let timestamp = Utc::now();
        Self {
            timestamp,
            gate_id,
            msg,
            msg_relation,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Trace {
    msgs: Vec<TraceMsg>,
}

impl Trace {
    pub const fn new() -> Self {
        Self { msgs: Vec::new() }
    }

    pub fn append_msg(&mut self, gate_id: Uuid, msg: String, msg_relation: MsgRelation) {
        self.msgs.push(TraceMsg::new(gate_id, msg, msg_relation));
    }

    pub fn clear(&mut self) {
        self.msgs.clear();
    }

    pub fn msg_indices(&self, gate_id: Uuid, msg_relation: MsgRelation) -> Vec<usize> {
        self.msgs
            .iter()
            .enumerate()
            .filter_map(|(idx, msg)| {
                if msg.gate_id == gate_id
                    && (msg_relation == MsgRelation::ALL || msg.msg_relation == msg_relation)
                {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn msgs(&self) -> &[TraceMsg] {
        &self.msgs
    }
}

pub struct Tracer {
    traces: Arc<Mutex<[Trace; 256]>>,
    next_tracing_id: Arc<AtomicU8>,
}

impl std::fmt::Debug for Tracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lock = self.traces.lock().unwrap();
        let traces: Vec<&Trace> = lock.iter().filter(|trace| !trace.msgs.is_empty()).collect();
        f.debug_struct("Tracer").field("traces", &traces).finish()
    }
}

impl Tracer {
    pub fn new() -> Self {
        const EMPTY_TRACE: Trace = Trace::new();
        Self {
            traces: Arc::new(Mutex::new([EMPTY_TRACE; 256])),
            next_tracing_id: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn next_tracing_id(&self) -> u8 {
        self.next_tracing_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn reset_trace_id(&self, trace_id: u8) {
        self.traces.lock().unwrap()[trace_id as usize].clear();
    }

    pub fn note_gate_event(&self, trace_id: u8, gate_id: Uuid, msg: String) {
        self.traces.lock().unwrap()[trace_id as usize].append_msg(gate_id, msg, MsgRelation::GATE);
    }

    pub fn note_component_event(&self, trace_id: u8, gate_id: Uuid, msg: String) {
        self.traces.lock().unwrap()[trace_id as usize].append_msg(
            gate_id,
            msg,
            MsgRelation::COMPONENT,
        );
    }

    pub fn get_trace(&self, trace_id: u8) -> Trace {
        self.traces.lock().unwrap()[trace_id as usize].clone()
    }
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct BoundTracer {
    tracer: Arc<Tracer>,
    gate_id: Uuid,
}

impl BoundTracer {
    pub fn bind(tracer: Arc<Tracer>, gate_id: Uuid) -> Self {
        Self {
            tracer,
            gate_id,
        }
    }

    pub fn note_event(&self, trace_id: u8, msg: String) {
        self.tracer.note_component_event(trace_id, self.gate_id, msg)
    }
}