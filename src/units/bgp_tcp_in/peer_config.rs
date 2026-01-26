//! Local configuration of BGP peers.
//!
//! This module contains types to conveniently parse general configuration
//! (`BgpTcpIn`) and a collection (`PeerConfigs`) of per peer configuration
//! (`PeerConfig`) from a TOML file. Once we know which of the PeerConfigs
//! should be used for an incoming connection, we can construct a
//! `CombinedConfig` comprised of the general part and the peer specific
//! parameters. This `CombinedConfig` implements the `BgpConfig` trait from
//! `rotonda_fsm`, and can thus be used to create a
//! `crate::rotonda_fsm::bgp::Session`.
//!
//! Note that we rely on the TOML configuration file using Serde for now, but
//! that we want to move this configuration to Roto in the future.

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;

use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use routecore::bgp::fsm::session::BgpConfig;
use routecore::bgp::types::AfiSafiType;
use serde::Deserialize;

/// Enum carrying either a exact IP address, or a `Prefix`.
#[derive(
    Clone, Copy, Debug, Deserialize, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
#[serde(untagged)]
pub enum PrefixOrExact {
    Exact(IpAddr),
    Prefix(Prefix),
}

impl PrefixOrExact {
    pub fn is_exact(&self) -> bool {
        match self {
            Self::Exact(_) => true,
            Self::Prefix(_) => false,
        }
    }

    pub fn contains(&self, addr: IpAddr) -> bool {
        match self {
            Self::Exact(a) => *a == addr,
            Self::Prefix(p) => p.contains(addr),
        }
    }
}

impl From<Prefix> for PrefixOrExact {
    fn from(p: Prefix) -> Self {
        PrefixOrExact::Prefix(p)
    }
}

impl From<IpAddr> for PrefixOrExact {
    fn from(a: IpAddr) -> Self {
        PrefixOrExact::Exact(a)
    }
}

/// Enum carrying one specific ASN, or a list of zero or multiple ASNs.
#[derive(Clone, Debug, Deserialize, Hash, Eq, PartialEq)]
#[serde(untagged)]
pub enum OneOrManyAsns {
    Many(Vec<Asn>), // if empty, allow all
    One(Asn),
}

impl OneOrManyAsns {
    fn is_single(&self) -> bool {
        match self {
            OneOrManyAsns::One(_) => true,
            OneOrManyAsns::Many(_) => false,
        }
    }

    fn contains(&self, other: Asn) -> bool {
        match self {
            OneOrManyAsns::One(asn) => *asn == other,
            OneOrManyAsns::Many(asns) => asns.contains(&other),
        }
    }
}

/// Ordered collection of `PeerConfig`s, keyed on `PrefixOrExact`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct PeerConfigs(BTreeMap<PrefixOrExact, PeerConfig>);

impl PeerConfigs {
    /// Returns the PrefixOrExact and PeerConfig for `key`, if any.
    pub fn get(&self, key: IpAddr) -> Option<(PrefixOrExact, &PeerConfig)> {
        self.0
            .iter()
            .find(|&(k, _cfg)| match k {
                PrefixOrExact::Exact(e) => *e == key,
                PrefixOrExact::Prefix(p) => p.contains(key),
            })
            .map(|hit| (*hit.0, hit.1))
    }

    /// Returns the PeerConfig for `key`, if any.
    pub fn get_exact(&self, key: &PrefixOrExact) -> Option<&PeerConfig> {
        self.0.get(key)
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&PrefixOrExact, &PeerConfig)> {
        self.0.iter()
    }
}

/// Configuration for a remote BGP peer.
#[derive(Clone, Deserialize)]
pub struct PeerConfig {
    name: String,
    remote_asn: OneOrManyAsns,
    hold_time: Option<u16>,
    #[serde(default)]
    md5_key: Option<String>,
    #[serde(default)]
    protocols: Vec<AfiSafiType>,
    #[serde(default)]
    addpath: Vec<AfiSafiType>,
}

impl fmt::Debug for PeerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerConfig")
            .field("name", &self.name)
            .field("remote_asn", &self.remote_asn)
            .field("hold_time", &self.hold_time)
            .field("protocols", &self.protocols)
            .field("addpath", &self.addpath)
            .field("md5_key", &self.md5_key.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl PeerConfig {
    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            name: "MOCK".to_string(),
            remote_asn: OneOrManyAsns::Many(vec![]),
            hold_time: None,
            md5_key: None,
            protocols: vec![],
            addpath: vec![],
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn single_asn(&self) -> bool {
        self.remote_asn.is_single()
    }

    fn accept_remote_asn(&self, remote: Asn) -> bool {
        if let OneOrManyAsns::Many(ref asns) = self.remote_asn {
            if asns.is_empty() {
                return true;
            }
        }
        self.remote_asn.contains(remote)
    }

    pub fn md5_key(&self) -> Option<&str> {
        self.md5_key.as_deref().filter(|value| !value.is_empty())
    }
}

impl PartialEq for PeerConfig {
    fn eq(&self, other: &PeerConfig) -> bool {
        self.remote_asn == other.remote_asn
            && self.hold_time == other.hold_time
            && self.md5_key == other.md5_key
    }
}

/// Combination of general and peer specific parameters.
pub struct CombinedConfig {
    // The general parameters from BgpTcpIn.
    my_asn: Asn,
    my_bgp_id: [u8; 4],
    remote_prefix_or_exact: PrefixOrExact,
    // The peer specific config.
    peer_config: PeerConfig,
}

impl CombinedConfig {
    pub fn new(
        b: BgpTcpIn,
        peer_config: PeerConfig,
        remote_prefix_or_exact: PrefixOrExact,
    ) -> CombinedConfig {
        CombinedConfig {
            my_asn: b.my_asn,
            my_bgp_id: b.my_bgp_id,
            remote_prefix_or_exact,
            peer_config,
        }
    }
    pub fn peer_config(&self) -> &PeerConfig {
        &self.peer_config
    }
}

use super::unit::BgpTcpIn;

//------------ BgpConfig trait -----------------------------------------------

impl BgpConfig for CombinedConfig {
    fn local_asn(&self) -> Asn {
        self.my_asn
    }
    fn bgp_id(&self) -> [u8; 4] {
        self.my_bgp_id
    }

    fn remote_addr_allowed(&self, remote_addr: IpAddr) -> bool {
        self.remote_prefix_or_exact.contains(remote_addr)
    }

    fn remote_asn_allowed(&self, remote_asn: Asn) -> bool {
        self.peer_config.accept_remote_asn(remote_asn)
    }

    fn hold_time(&self) -> Option<u16> {
        self.peer_config.hold_time
    }

    fn is_exact(&self) -> bool {
        self.remote_prefix_or_exact.is_exact()
            && self.peer_config.single_asn()
    }

    fn protocols(&self) -> Vec<AfiSafiType> {
        self.peer_config.protocols.clone()
    }

    fn addpath(&self) -> Vec<AfiSafiType> {
        self.peer_config.addpath.clone()
    }
}

pub trait ConfigExt {
    fn remote_prefix_or_exact(&self) -> PrefixOrExact;
}

impl ConfigExt for CombinedConfig {
    fn remote_prefix_or_exact(&self) -> PrefixOrExact {
        self.remote_prefix_or_exact
    }
}

//------------ Tests ---------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::units::Unit;
    use std::str::FromStr;

    use super::*;

    #[test]
    fn it_works() {
        let toml = r#"

type = "bgp-tcp-in"
listen = "10.1.0.254:11179"
my_asn = 65001
my_bgp_id = [1, 2, 3, 4]

[peers."0.0.0.0/0"]
name = "Bgpsink"
remote_asn = []

[peers."1.2.3.0/24"]
name = "Peer-in-subnet"
remote_asn = [100, 200]
hold_time = 10

[peers."2.3.4.5/32"]
name = "Peer-in-32-subnet"
remote_asn = 100
hold_time = 10

[peers."2.3.4.6"]
name = "Peer-exact"
remote_asn = 100
hold_time = 10
md5_key = "s3cr3t"

[peers."2.3.4.7"]
name = "Explicit-protocols"
remote_asn = 100
protocols = ["Ipv4Unicast", "L2VpnEvpn"]
addpath = ["Ipv4Unicast", "Ipv6Unicast"]
"#;

        let Unit::BgpTcpIn(cfg) = toml::from_str::<Unit>(toml).unwrap()
        else {
            unreachable!()
        };

        let ip1 = IpAddr::from_str("1.2.3.10").unwrap();
        let asn1 = Asn::from_u32(100);
        let asn2 = Asn::from_u32(101);
        let cfg1 = cfg.peer_configs.get(ip1).unwrap();
        println!("{:?}", cfg1);
        for k in cfg.peer_configs.0.keys() {
            println!("key: {:?}", k);
        }
        assert!(cfg1.1.name == "Peer-in-subnet");
        assert!(cfg1.1.remote_asn.contains(asn1));
        assert!(cfg1.1.accept_remote_asn(asn1));
        assert!(!cfg1.1.accept_remote_asn(asn2));

        let ip2 = IpAddr::from_str("2.3.4.6").unwrap();
        let cfg2 = cfg.peer_configs.get(ip2).unwrap();
        assert!(cfg.peer_configs.get(ip2).unwrap().1.name == "Peer-exact");
        assert!(!cfg2.1.accept_remote_asn(Asn::from_u32(1234)));
        assert_eq!(cfg2.1.md5_key(), Some("s3cr3t"));

        let cfg3 = cfg
            .peer_configs
            .get(IpAddr::from_str("10.10.10.10").unwrap())
            .unwrap();
        assert!(cfg3.1.name == "Bgpsink");
        assert!(cfg3.1.accept_remote_asn(Asn::from_u32(1234)));

        let ip6 = IpAddr::from_str("2001:0db8::1").unwrap();
        assert!(cfg.peer_configs.get(ip6).is_none());

        let cfg4 = cfg
            .peer_configs
            .get(IpAddr::from_str("2.3.4.7").unwrap())
            .unwrap();
        assert!(cfg4.1.name == "Explicit-protocols");
        assert_eq!(
            cfg4.1.protocols,
            vec![AfiSafiType::Ipv4Unicast, AfiSafiType::L2VpnEvpn]
        );
        assert_eq!(
            cfg4.1.addpath,
            vec![AfiSafiType::Ipv4Unicast, AfiSafiType::Ipv6Unicast]
        );
    }
}
