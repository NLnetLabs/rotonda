//! Rotonda
#![allow(renamed_and_removed_lints)]
#![allow(clippy::unknown_clippy_lints)]

pub mod common;
pub mod comms;
pub mod config;
pub mod http_ng;
pub mod webui;
pub mod cli;
pub mod ingress;
pub mod log;
pub mod manager;
pub mod metrics;
pub mod payload;
pub mod roto_runtime;
pub mod targets;
pub mod tokio;
pub mod tracing;
pub mod units;

pub mod representation;

pub mod tests;
pub use tests::util::bgp;
