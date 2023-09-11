//! Things not implemented in the routecore crate that we use.
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use routecore::bgp::message::{
    update::{AddPath, FourOctetAsn},
    SessionConfig,
};

// Based on code in bgmp::main.rs:
pub fn generate_alternate_config(peer_config: &SessionConfig) -> Option<SessionConfig> {
    let mut alt_peer_config = *peer_config;
    if peer_config.four_octet_asn == FourOctetAsn::Disabled {
        alt_peer_config.enable_four_octet_asn();
    } else if peer_config.add_path == AddPath::Disabled {
        alt_peer_config.enable_addpath();
    } else if peer_config.add_path == AddPath::Enabled {
        alt_peer_config.disable_addpath();
    } else if peer_config.four_octet_asn == FourOctetAsn::Enabled {
        alt_peer_config.disable_four_octet_asn();
    } else {
        return None;
    }
    Some(alt_peer_config)
}

pub fn mk_hash<T: Hash>(val: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}
