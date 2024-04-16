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
pub mod mvp;
pub mod payload;
pub mod targets;
#[cfg(test)]
pub mod tests;
#[cfg(test)]
pub use tests::util::bgp;
pub mod tokio;
pub mod tracing;
pub mod units;

pub mod ingress;
