//! RPKI related types and handlers for the Rib.
//!

use std::{collections::HashSet, sync::RwLock};

use inetnum::{addr::Prefix, asn::Asn};
use log::warn;
use rotonda_store::{match_options::{IncludeHistory, MatchOptions, MatchType}, rib::{config::MemoryOnlyConfig, StarCastRib}};
use serde::{Deserialize, Serialize};


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
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

    /// The previous `RovStatus`
    pub previous_status: RovStatus,

    /// The current `RovStatus`
    pub current_status: RovStatus,

    /// The origin `Asn`, i.e. right-most in 'AS_PATH' of this route
    pub origin: Asn,

    /// The peer `Asn` from the route was received
    pub peer_asn: Asn,
}

impl RovStatusUpdate {
    pub fn new(
        prefix: Prefix,
        previous_status: RovStatus,
        current_status: RovStatus,
        origin: Asn,
        peer_asn: Asn,
    ) -> Self {
        Self {
            prefix, previous_status, current_status, origin, peer_asn,
        }
    }
}



type VrpStore = StarCastRib<MaxLenList, MemoryOnlyConfig>;

#[derive(Clone, Debug, Default)]
pub struct MaxLenList {
    list: Vec<u8>,
}
impl MaxLenList {
    pub fn push(&mut self, value: u8) {
        self.list.push(value);
    }
    pub fn remove(&mut self, index: usize) {
        self.list.remove(index);
    }
    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.list.iter()
    }
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
}
impl AsRef<[u8]> for MaxLenList {
    fn as_ref(&self) -> &[u8] {
        self.list.as_ref()
    }
}
impl From<Vec<u8>> for MaxLenList {
    fn from(value: Vec<u8>) -> Self {
        Self { list: value }
    }
}
impl std::fmt::Display for MaxLenList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.list.iter()).finish()
    }
}
impl rotonda_store::prefix_record::Meta for MaxLenList {
    type Orderable<'a> = ();

    type TBI = ();

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}


pub struct RtrCache {
    pub route_origins: RwLock<HashSet<rpki::rtr::payload::RouteOrigin>>,
    pub router_keys: RwLock<HashSet<rpki::rtr::payload::RouterKey>>,
    pub aspas: RwLock<HashSet<rpki::rtr::payload::Aspa>>,
    pub vrps: VrpStore,
}

impl RtrCache {
    pub fn check_rov(&self, prefix: &Prefix, origin: Asn) -> RovStatus {
        #[allow(unused_assignments)]
        let mut covered = false;
        let mut valid = false;

        let match_options = MatchOptions {
            match_type: MatchType::LongestMatch,
            include_withdrawn: false,
            include_less_specifics: true,
            include_more_specifics: false,
            mui: None,
            include_history: IncludeHistory::None,
        };

        let guard = &rotonda_store::epoch::pin();
        let res = match self.vrps.match_prefix(
            prefix,
            &match_options,
            guard
        ) {
            Ok(res) => res,
            Err(e) => {
                warn!("could not lookup {}: {}", &prefix, e);
                return RovStatus::NotChecked;
            }
        };
        // check exact/longest matches

        // NB: Because the VRP Store (rotonda_store::StarCastRib) will not
        // actually remove any prefixes, checking whether res.records is empty
        // or not is not enough to determine ROV coverage for `prefix`.
        // Instead, we have to check the stored MaxLenList: if that is not
        // empty, `prefix` is covered.
        'outer: for r in &res.records {
            covered = covered || !r.meta.is_empty();
            for maxlen in r.meta.iter() {
                #[allow(clippy::collapsible_if)]
                if prefix.len() <= *maxlen {
                    if r.multi_uniq_id == u32::from(origin) {
                        valid = true;
                        break 'outer;
                    }
                }
            }
        }

        // check less specifics
        if !valid {
            if let Some(less_specifics) = res.less_specifics {
                'outer: for r in less_specifics.iter() {
                    for record in r.meta.iter() {
                        covered = covered || !record.meta.is_empty();
                        for maxlen in record.meta.iter() {
                            #[allow(clippy::collapsible_if)]
                            if prefix.len() <= *maxlen {
                                if record.multi_uniq_id == u32::from(origin) {
                                    valid = true;
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
        }

        match (covered, valid) {
            (true, true) => RovStatus::Valid,
            (true, false) => RovStatus::Invalid,
            (false, true) => unreachable!(),
            (false, false) => RovStatus::NotFound,
        }
    }
}


impl Default for RtrCache {
    fn default() -> Self {
        Self {
            route_origins: Default::default(),
            router_keys: Default::default(),
            aspas: Default::default(),
            vrps: VrpStore::try_default().unwrap(),
        }
    }
}




//// TODO
//
//// create helpers to
//// - update the prefix store based on RtrUpdates
//// - D-R-Y the Full+Delta handling in unit.rs
//
//fn update_rov_in_store(
//    store: &Store,
//    rtr_cache: &RtrCache,
//    vrp_update: VrpUpdate,
//) {
//
//}
