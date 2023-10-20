//! Data processing units.
//!
//! Rotonda provides the means for flexible data processing through
//! interconnected entities called _units._ Each unit produces a constantly
//! updated data set. Other units can subscribe to updates from these sets.
//! Alternatively, they can produce their own data set from external input.
//! Different types of units exist that perform different tasks. They can
//! all be plugged together all kinds of ways.
//!
//! This module contains all the units currently available. It provides
//! access to them via a grand enum `Unit` that contains all unit types as
//! variants.
//!
//! Units can be created from configuration via serde deserialization. They
//! are started by spawning them into an async runtime and then just keep
//! running there.

//------------ Sub-modules ---------------------------------------------------
//
// These contain all the actual unit types grouped by shared functionality.
mod bgp_tcp_in;
mod bmp_tcp_in;
mod filter;
mod rib_unit;
pub use rib_unit::{
    unit::{RibType, RibUnit},
    RibValue,
};

//------------ Unit ----------------------------------------------------------

use crate::comms::Gate;
use crate::manager::{Component, WaitPoint};
use serde::Deserialize;

/// The fundamental entity for data processing.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Unit {
    #[serde(rename = "bgp-tcp-in")]
    BgpTcpIn(bgp_tcp_in::unit::BgpTcpIn),

    #[serde(rename = "bmp-tcp-in")]
    BmpTcpIn(bmp_tcp_in::unit::BmpTcpIn),

    #[serde(rename = "filter")]
    Filter(filter::unit::Filter),

    #[serde(rename = "rib")]
    RibUnit(rib_unit::unit::RibUnit),
}

impl Unit {
    pub async fn run(self, component: Component, gate: Gate, waitpoint: WaitPoint) {
        let _ = match self {
            Unit::BgpTcpIn(unit) => unit.run(component, gate, waitpoint).await,
            Unit::BmpTcpIn(unit) => unit.run(component, gate, waitpoint).await,
            Unit::Filter(unit) => unit.run(component, gate, waitpoint).await,
            Unit::RibUnit(unit) => unit.run(component, gate, waitpoint).await,
        };
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Unit::BgpTcpIn(_) => "bgp-tcp-in",
            Unit::BmpTcpIn(_) => "bmp-tcp-in",
            Unit::Filter(_) => "filter",
            Unit::RibUnit(_) => "rib",
        }
    }
}
