//! Rotonda
#![allow(renamed_and_removed_lints)]
#![allow(clippy::unknown_clippy_lints)]

pub mod common;
pub mod comms;
pub mod config;
pub mod http;
pub mod log;
pub mod manager;
pub mod metrics;
pub mod payload;
pub mod targets;
pub mod tokio;
pub mod units;

pub(crate) mod tests;

pub use tests::util::bgp;
