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
use std::sync::atomic::{AtomicU64, Ordering};

pub use tests::util::bgp;


static LTIME: AtomicU64 = AtomicU64::new(1);
pub fn ltime() -> std::num::NonZeroU64 {
    std::num::NonZeroU64::new(LTIME.fetch_add(1, Ordering::Relaxed)).unwrap()
}
pub fn read_ltime() -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(LTIME.load(Ordering::Relaxed)).unwrap()
}
