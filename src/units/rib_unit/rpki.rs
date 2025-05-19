//! RPKI related types and handlers for the Rib.
//!

use inetnum::{addr::Prefix, asn::Asn};
use serde::Serialize;


/// RPKI related information for individual routes
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
#[derive(Serialize)]
pub struct RpkiInfo {
    rov: RovStatus,
}

impl RpkiInfo {
    /// Create an `RpkiInfo` from a [`RovStatus`]
    pub fn rov_status(&self) -> RovStatus {
        self.rov
    }
}

impl From<u8> for RpkiInfo {
    fn from(value: u8) -> Self {
        let rov = match value {
            1 => RovStatus::NotFound,
            2 => RovStatus::Valid,
            4 => RovStatus::Invalid,
            _ => RovStatus::NotChecked
        };

        Self { rov }
    }
}

impl From<RpkiInfo> for u8 {
    fn from(value: RpkiInfo) -> Self {
        match value.rov {
            RovStatus::NotChecked => 0,
            RovStatus::NotFound => 1,
            RovStatus::Valid => 2,
            RovStatus::Invalid => 4,
        }
    }
}


impl From<RovStatus> for RpkiInfo {
    fn from(value: RovStatus) -> Self {
        Self { rov: value }
    }
}

/// RPKI Route Origin Validation status for a route
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
#[derive(Serialize)]
pub enum RovStatus {
    #[default]
    NotChecked,
    NotFound,
    Valid,
    Invalid,
}


/// Route Origin Validation status update for a route
#[derive(Copy, Clone, Debug)]
pub struct RovStatusUpdate {
    /// The prefix of the route
    pub prefix: Prefix,

    /// The old `RovStatus
    pub old_status: RovStatus,

    /// The new `RovStatus
    pub new_status: RovStatus,

    /// The origin `Asn`, i.e. right-most in 'AS_PATH' of this route
    pub origin: Asn,

    /// The peer `Asn` from the route was received
    pub peer_asn: Asn,
}

impl RovStatusUpdate {
    pub fn new(
        prefix: Prefix,
        old_status: RovStatus,
        new_status: RovStatus,
        origin: Asn,
        peer_asn: Asn,
    ) -> Self {
        Self {
            prefix, old_status, new_status, origin, peer_asn,
        }
    }
}





//// TODO
//// move in from rib_unit::unit.rs
//// - RtrCache, VrpStore et al
//
//// create helpers to
//// - update the prefix store based on RtrUpdates
//// - D-R-Y the Full+Delta handling in unit.rs
//
//
//// use the return value to update e.g. RotondaRoute, Store, ..
//fn check_rov_for_route(
//    rtr_cache: &RtrCache,
//    route: &SomeRoute,
//    // or
//    prefix: &Prefix,
//    origin_asn: Asn,
//
//) -> RovStatus {
//
//}
//
//
//fn update_rov_in_store(
//    store: &Store,
//    rtr_cache: &RtrCache,
//    vrp_update: VrpUpdate,
//) {
//
//}
